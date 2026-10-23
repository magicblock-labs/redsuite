use std::{cell::Cell, rc::Rc, time::Duration};
use tokio::sync::Notify;

use anyhow::{ensure, Context};
use futures_util::future::{try_join, try_join_all};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    netfault::{BaseProxies, Selector},
    programs::{CellHeader, CellId, HashState, SettlementMode, REDLINE},
    rpc_types::response::TransactionError,
    system,
    topology::{self, ErOptions, PrivateEr},
    wait, BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use signer::Signer;
use tokio::time::{self, Instant};

use super::{await_local_value, BASE_REFRESH, BASE_REWRITE, ER_WRITE, FETCHES};
use crate::{
    program::{self, layout, DELEGATION_PROGRAM_ID},
    ACCOUNT_OWNERS, ACCOUNT_SPACE, PAYER_LAMPORTS,
};

const LRU_CAPACITY: usize = 256;
const READONLY_COUNT: usize = 3;
const DELEGATED_COUNT: usize = 2;
const PENDING_COUNT: usize = 2;
const FUNDING_BATCH: usize = 16;
const FETCH_BATCH: usize = 32;
const TIMEOUT: Duration = Duration::from_secs(20);
const EVICTIONS: &str = "engine_keeper_account_cache_evictions";
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(180);

pub enum Mode {
    Churn,
    UndelegationReconnectGap,
}

pub struct CacheLifecycle {
    base: Rc<BaseCtx>,
    churn_cycles: usize,
    reconnect_before_settlement: bool,
    http: BaseProxies,
    ws: BaseProxies,
    private: PrivateEr,
}

impl PrivateErScenario<(), Mode> for CacheLifecycle {
    async fn setup(base: Rc<BaseCtx>, mode: Mode) -> Result<Self> {
        let (label, churn_cycles) = match mode {
            Mode::Churn => ("cache-lifecycle", READONLY_COUNT),
            Mode::UndelegationReconnectGap => ("undelegation-reconnect-gap", 0),
        };
        let reconnect_before_settlement = matches!(mode, Mode::UndelegationReconnectGap);
        let http = BaseProxies::spawn(&base).await?;
        let ws = BaseProxies::spawn(&base).await?;
        let mut endpoints = http.endpoints();
        endpoints.ws_url = ws.endpoints().ws_url;
        if reconnect_before_settlement {
            let (grpc_url, _) = base.grpc().context("base L1 Yellowstone gRPC endpoint missing")?;
            endpoints.grpc_url = Some(grpc_url.clone());
        }
        let private = topology::private_er(
            &base,
            ErOptions {
                label: label.into(),
                settings: Box::new(|params| {
                    params.engine.accountsdb.lru_capacity = LRU_CAPACITY;
                }),

                base_endpoints: Some(endpoints),
            },
        )
        .await?;
        Ok(Self {
            base,
            churn_cycles,
            reconnect_before_settlement,
            http,
            ws,
            private,
        })
    }

    async fn run(self) -> Result<()> {
        let Self {
            base,
            churn_cycles,
            reconnect_before_settlement,
            http,
            ws,
            private,
        } = self;
        let observe = |method, key: &Pubkey| {
            ws.intercept(Selector::method(method).response().notification().account(key))
        };
        let er = private.ctx();
        let validator = er.identity();
        let funder = base.funded_payer(PAYER_LAMPORTS).await?;
        let owner = funder.pubkey();
        let payer = base.delegated_payer(&funder, validator, PAYER_LAMPORTS).await?;
        let keys = base
            .cells(REDLINE, &funder)
            .create_many(
                ACCOUNT_SPACE,
                validator,
                READONLY_COUNT + DELEGATED_COUNT + PENDING_COUNT,
            )
            .await?;
        let (readonly, protected_keys) = keys.split_at(READONLY_COUNT);
        let (delegated, pending) = protected_keys.split_at(DELEGATED_COUNT);
        let (traffic_keys, er_written_keys) = protected_keys.split_at(1);
        let stale: Vec<_> = if reconnect_before_settlement {
            Vec::new()
        } else {
            protected_keys.iter().map(|key| observe("programNotification", key)).collect()
        };
        let delegates: Vec<_> = (READONLY_COUNT..keys.len())
            .map(|index| {
                REDLINE.delegate(
                    owner,
                    CellId::new(owner, index as u64),
                    Some(validator),
                    u32::MAX,
                )
            })
            .collect();
        base.submit_and_confirm(&funder, &delegates).await?;
        let stale = try_join_all(stale.iter().map(|trap| trap.wait(TIMEOUT))).await?;
        er.accounts(protected_keys).await?;
        er.cells(REDLINE, &payer).write(ER_WRITE, er_written_keys).await?;
        let settlement =
            http.stall(Selector::method("sendTransaction").request().account(&pending[0]));
        er.submit_and_confirm(
            &funder,
            &[REDLINE
                .batch()
                .nonce(1)
                .settle(owner, pending, SettlementMode::Undelegate)
                .build()],
        )
        .await?;
        http.await_held(Selector::default(), TIMEOUT).await?;
        let protected = er.local_accounts(&ACCOUNT_OWNERS, protected_keys).await?;
        ensure!(protected.iter().all(Option::is_some));
        let base_delegated = base.accounts(delegated).await?;
        let pool: Vec<_> = (0..LRU_CAPACITY * 2).map(|_| Keypair::new().pubkey()).collect();
        try_join_all(pool.chunks(FUNDING_BATCH).map(|chunk| async {
            let transfers: Vec<_> =
                chunk.iter().map(|key| system::transfer(&owner, key, 1_000_000)).collect();
            base.submit_and_confirm(&funder, &transfers).await
        }))
        .await?;
        let eviction_count = async || -> Result<f64> {
            er.scrape_metrics().await?.get(EVICTIONS).context("eviction metric missing")
        };
        let before = eviction_count().await?;
        let progress = Cell::new(0u64);
        let changed = Notify::new();
        let progressed = async |start| {
            wait::on_change(&changed, TIMEOUT, "warmed traffic recovers", || {
                Ok((progress.get() > start).then_some(()))
            })
            .await
        };
        let completed = Cell::new(false);
        let done = Cell::new(false);
        let sender = er.sender(Rc::new(payer));
        let traffic = async {
            let mut expected = protected.clone();
            let mut hash = [0; layout::HASH_SIZE];
            let mut id = 0;
            while !done.get() {
                id += 1;
                let ixs = [REDLINE.fold_hashes(id, 0, traffic_keys)];
                let signature = wait::retry("warmed submission recovers", TIMEOUT, async || {
                    sender.submit(&ixs).await
                })
                .await?;
                let outcome = er.api().await_transaction(&signature).await?;
                let meta = transaction_meta(&outcome)?;
                if let Some(error) = &meta.err {
                    ensure!(
                        TransactionError::from(error.clone())
                            == TransactionError::ProgramAccountNotFound
                    );
                } else {
                    hash = HashState::fold(id, &[hash], 0);
                    let data = &mut expected[0].as_mut().unwrap().data;
                    let mut header = CellHeader::try_decode(data)?;
                    header.value = id;
                    header.revision += 2;
                    header.write(data)?;
                    data[layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE]
                        .copy_from_slice(&hash);
                }
                let count = if completed.get() { delegated.len() } else { protected_keys.len() };
                let actual = er.local_accounts(&ACCOUNT_OWNERS, &protected_keys[..count]).await?;
                ensure!(actual == expected[..count]);
                if meta.err.is_none() {
                    progress.set(progress.get() + 1);
                    changed.notify_waiters();
                }
            }
            Ok(())
        };
        let faults = async {
            let mut blackout = Vec::new();
            let mut evictions = before;
            progressed(0).await?;
            for held in stale {
                held.release();
            }
            if reconnect_before_settlement {
                for chunk in pool.chunks(FETCH_BATCH) {
                    er.accounts(chunk).await?;
                }
                let next = eviction_count().await?;
                ensure!(next > evictions);
                evictions = next;
                blackout.push(ws.stall(Selector::default().response().notification()));
                for key in protected_keys {
                    blackout.push(http.stall(Selector::methods(&FETCHES).response().account(key)));
                }
                ws.close_connections();
            }
            for (cycle, &target) in readonly[..churn_cycles].iter().enumerate() {
                for chunk in pool.chunks(FETCH_BATCH) {
                    er.accounts(chunk).await?;
                }
                let next = eviction_count().await?;
                ensure!(next > evictions);
                evictions = next;
                let cold = er.local_accounts(&ACCOUNT_OWNERS, &[target]).await?;
                ensure!(cold[0].is_none());
                let reconnect = observe("accountSubscribe", &pending[0]);
                ws.close_connections();
                let reconnect = reconnect.wait(TIMEOUT).await?;
                let registered = observe("accountSubscribe", &target);
                let old = http.intercept(Selector::methods(&FETCHES).response().account(&target));
                let latest = BASE_REFRESH + 1 + cycle as u64;
                let (read, ()) = try_join(er.account(&target), async {
                    registered.wait(TIMEOUT).await?.release();
                    let old = old.wait(TIMEOUT).await?;
                    let notification = observe("accountNotification", &target);
                    base.cells(REDLINE, &funder).write(latest, &[target]).await?;
                    notification.wait(TIMEOUT).await?.release();
                    await_local_value(er, "latest base value observed locally", &[target], latest)
                        .await?;
                    old.release();
                    progressed(progress.get()).await?;
                    reconnect.release();
                    Ok(())
                })
                .await?;
                ensure!(crate::account_id(read) == Some(latest));
                await_local_value(er, "latest base value observed locally", &[target], latest)
                    .await?;
                ensure!(base
                    .accounts(pending)
                    .await?
                    .iter()
                    .all(|a| matches!(a, Some(a) if a.owner == DELEGATION_PROGRAM_ID)));
            }
            completed.set(true);
            settlement.remove();
            base.api()
                .await_accounts(pending, "base undelegation", async || {
                    let actual = base.accounts(pending).await?;
                    for (actual, expected) in actual.iter().zip(&protected[delegated.len()..]) {
                        let actual = actual.as_ref().map(|a| (a.owner, &a.data));
                        let expected = expected.as_ref().map(|a| (program::id(), &a.data));
                        ensure!(actual == expected);
                    }
                    Result::Ok(())
                })
                .await?;
            let released_at = Instant::now();
            base.cells(REDLINE, &funder).write(BASE_REWRITE, pending).await?;
            let recovery = if reconnect_before_settlement {
                // Observe late recovery for diagnostics, but retain the
                // TIMEOUT deadline checked after teardown below.
                Some(
                    await_local_value(
                        er,
                        "ER discovers the completed undelegation",
                        pending,
                        BASE_REWRITE,
                    )
                    .await
                    .map(|()| released_at.elapsed()),
                )
            } else {
                // Completed accounts are evictable mirrors. A later reconnect
                // gap can invalidate them, so verify normal access refetches them.
                er.api()
                    .await_accounts(
                        pending,
                        "completed accounts refetch current base state",
                        async || {
                            let actual: Vec<_> = er
                                .accounts(pending)
                                .await?
                                .into_iter()
                                .map(crate::account_id)
                                .collect();
                            ensure!(actual == vec![Some(BASE_REWRITE); pending.len()]);
                            Result::Ok(())
                        },
                    )
                    .await?;
                None
            };
            base.cells(REDLINE, &funder).write(BASE_REFRESH, readonly).await?;
            // Base confirmation does not wait for ER subscription delivery.
            er.api()
                .await_accounts(
                    readonly,
                    "final readonly mirrors synchronized",
                    async || {
                        let fetched = er.accounts(readonly).await?;
                        for account in fetched {
                            ensure!(crate::account_id(account) == Some(BASE_REFRESH));
                        }
                        Result::Ok(())
                    },
                )
                .await?;
            let local = er.local_accounts(&ACCOUNT_OWNERS, readonly).await?;
            // Readonly entries may be evicted after the fetch.
            for local in local.into_iter().flatten() {
                ensure!(CellHeader::written_id(&local.data) == Some(BASE_REFRESH));
            }
            done.set(true);
            Result::Ok(recovery)
        };
        let (_, recovery) = time::timeout(SCENARIO_TIMEOUT, try_join(traffic, faults)).await??;
        let actual = base.accounts(delegated).await?;
        ensure!(actual == base_delegated);
        private.finish().await?;
        http.finish()?;
        ws.finish()?;
        if let Some(recovery) = recovery {
            let elapsed = recovery?;
            ensure!(elapsed <= TIMEOUT);
        }
        Ok(())
    }
}
