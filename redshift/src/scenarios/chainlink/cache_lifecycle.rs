use std::{cell::Cell, rc::Rc, time::Duration};
use tokio::sync::Notify;

use account::Account;
use anyhow::{ensure, Context};
use futures_util::future::{try_join, try_join_all};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    netfault::{BaseProxies, Held, RuleHandle, Selector, Trap},
    programs::{CellHeader, CellId, HashState, SettlementMode, REDLINE},
    rpc_types::response::TransactionError,
    system,
    topology::{self, ErOptions, PrivateEr},
    wait, BaseCtx, ChainCtx, PrivateErScenario, Result, TxSender,
};
use signer::Signer;
use tokio::time::{self, Instant};
use tracing::instrument;

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
        let (accounts, stale, settlement) = self.prepare_accounts().await?;
        let progress = TrafficProgress::default();
        let faults = async {
            progress.await_sent(0).await?;
            for held in stale {
                held.release();
            }
            let _blackout = self.churn(&accounts, &progress).await?;
            self.complete_settlement(&accounts, &progress, settlement).await
        };
        let (_, recovery) = time::timeout(
            SCENARIO_TIMEOUT,
            try_join(self.traffic(&accounts, &progress), faults),
        )
        .await??;

        let delegated = &accounts.keys[READONLY_COUNT..READONLY_COUNT + DELEGATED_COUNT];
        ensure!(self.base.accounts(delegated).await? == accounts.base_delegated);
        self.finish().await?;
        if let Some(recovery) = recovery {
            ensure!(recovery? <= TIMEOUT);
        }
        Ok(())
    }
}

struct CacheAccounts {
    funder: Keypair,
    keys: Vec<Pubkey>,
    protected: Vec<Option<Account>>,
    base_delegated: Vec<Option<Account>>,
    pool: Vec<Pubkey>,
    sender: TxSender,
    evictions: f64,
}

#[derive(Default)]
struct TrafficProgress {
    sent: Cell<u64>,
    changed: Notify,
    completed: Cell<bool>,
    done: Cell<bool>,
}

impl TrafficProgress {
    async fn await_sent(&self, start: u64) -> Result<()> {
        wait::on_change(&self.changed, TIMEOUT, "warmed traffic recovers", || {
            Ok((self.sent.get() > start).then_some(()))
        })
        .await
    }
}

impl CacheLifecycle {
    fn observe(&self, method: &str, key: &Pubkey) -> Trap {
        self.ws
            .intercept(Selector::method(method).response().notification().account(key))
    }

    async fn evictions(&self) -> Result<f64> {
        self.private
            .ctx()
            .scrape_metrics()
            .await?
            .get(EVICTIONS)
            .context("eviction metric missing")
    }

    #[instrument(skip_all, err)]
    async fn prepare_accounts(&self) -> Result<(CacheAccounts, Vec<Held>, RuleHandle)> {
        let er = self.private.ctx();
        let validator = er.identity();
        let funder = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let owner = funder.pubkey();
        let payer = self.base.delegated_payer(&funder, validator, PAYER_LAMPORTS).await?;
        let keys = self
            .base
            .cells(REDLINE, &funder)
            .create_many(
                ACCOUNT_SPACE,
                validator,
                READONLY_COUNT + DELEGATED_COUNT + PENDING_COUNT,
            )
            .await?;
        let protected_keys = &keys[READONLY_COUNT..];
        let (delegated, pending) = protected_keys.split_at(DELEGATED_COUNT);
        let er_written_keys = &protected_keys[1..];
        let stale: Vec<_> = if self.reconnect_before_settlement {
            Vec::new()
        } else {
            protected_keys
                .iter()
                .map(|key| self.observe("programNotification", key))
                .collect()
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
        self.base.submit_and_confirm(&funder, &delegates).await?;
        let stale = try_join_all(stale.iter().map(|trap| trap.wait(TIMEOUT))).await?;
        er.accounts(protected_keys).await?;
        er.cells(REDLINE, &payer).write(ER_WRITE, er_written_keys).await?;
        let settlement = self
            .http
            .stall(Selector::method("sendTransaction").request().account(&pending[0]));
        er.submit_and_confirm(
            &funder,
            &[REDLINE
                .batch()
                .nonce(1)
                .settle(owner, pending, SettlementMode::Undelegate)
                .build()],
        )
        .await?;
        self.http.await_held(Selector::default(), TIMEOUT).await?;
        let protected = er.local_accounts(&ACCOUNT_OWNERS, protected_keys).await?;
        ensure!(protected.iter().all(Option::is_some));
        let base_delegated = self.base.accounts(delegated).await?;
        let pool: Vec<_> = (0..LRU_CAPACITY * 2).map(|_| Keypair::new().pubkey()).collect();
        try_join_all(pool.chunks(FUNDING_BATCH).map(|chunk| async {
            let transfers: Vec<_> =
                chunk.iter().map(|key| system::transfer(&owner, key, 1_000_000)).collect();
            self.base.submit_and_confirm(&funder, &transfers).await
        }))
        .await?;
        let before = self.evictions().await?;
        let sender = er.sender(Rc::new(payer));
        Ok((
            CacheAccounts {
                funder,
                keys,
                protected,
                base_delegated,
                pool,
                sender,
                evictions: before,
            },
            stale,
            settlement,
        ))
    }

    #[instrument(skip_all, err)]
    async fn traffic(&self, accounts: &CacheAccounts, progress: &TrafficProgress) -> Result<()> {
        let er = self.private.ctx();
        let protected_keys = &accounts.keys[READONLY_COUNT..];
        let delegated = &protected_keys[..DELEGATED_COUNT];
        let traffic_keys = &protected_keys[..1];

        let mut expected = accounts.protected.clone();
        let mut hash = [0; layout::HASH_SIZE];
        let mut id = 0;
        while !progress.done.get() {
            id += 1;
            let ixs = [REDLINE.fold_hashes(id, 0, traffic_keys)];
            let signature = wait::retry("warmed submission recovers", TIMEOUT, async || {
                accounts.sender.submit(&ixs).await
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
            let count =
                if progress.completed.get() { delegated.len() } else { protected_keys.len() };
            let actual = er.local_accounts(&ACCOUNT_OWNERS, &protected_keys[..count]).await?;
            ensure!(actual == expected[..count]);
            if meta.err.is_none() {
                progress.sent.set(progress.sent.get() + 1);
                progress.changed.notify_waiters();
            }
        }
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn churn(
        &self,
        accounts: &CacheAccounts,
        progress: &TrafficProgress,
    ) -> Result<Vec<RuleHandle>> {
        let er = self.private.ctx();
        let (readonly, protected_keys) = accounts.keys.split_at(READONLY_COUNT);
        let pending = &protected_keys[DELEGATED_COUNT..];
        let mut blackout = Vec::new();
        let mut evictions = accounts.evictions;
        if self.reconnect_before_settlement {
            for chunk in accounts.pool.chunks(FETCH_BATCH) {
                er.accounts(chunk).await?;
            }
            let next = self.evictions().await?;
            ensure!(next > evictions);
            evictions = next;
            blackout.push(self.ws.stall(Selector::default().response().notification()));
            for key in protected_keys {
                blackout.push(self.http.stall(Selector::methods(&FETCHES).response().account(key)));
            }
            self.ws.close_connections();
        }
        for (cycle, &target) in readonly[..self.churn_cycles].iter().enumerate() {
            for chunk in accounts.pool.chunks(FETCH_BATCH) {
                er.accounts(chunk).await?;
            }
            let next = self.evictions().await?;
            ensure!(next > evictions);
            evictions = next;
            let cold = er.local_accounts(&ACCOUNT_OWNERS, &[target]).await?;
            ensure!(cold[0].is_none());
            let reconnect = self.observe("accountSubscribe", &pending[0]);
            self.ws.close_connections();
            let reconnect = reconnect.wait(TIMEOUT).await?;
            let registered = self.observe("accountSubscribe", &target);
            let old = self.http.intercept(Selector::methods(&FETCHES).response().account(&target));
            let latest = BASE_REFRESH + 1 + cycle as u64;
            let (read, ()) = try_join(er.account(&target), async {
                registered.wait(TIMEOUT).await?.release();
                let old = old.wait(TIMEOUT).await?;
                let notification = self.observe("accountNotification", &target);
                self.base.cells(REDLINE, &accounts.funder).write(latest, &[target]).await?;
                notification.wait(TIMEOUT).await?.release();
                await_local_value(er, "latest base value observed locally", &[target], latest)
                    .await?;
                old.release();
                progress.await_sent(progress.sent.get()).await?;
                reconnect.release();
                Ok(())
            })
            .await?;
            ensure!(crate::account_id(read) == Some(latest));
            await_local_value(er, "latest base value observed locally", &[target], latest).await?;
            ensure!(self
                .base
                .accounts(pending)
                .await?
                .iter()
                .all(|a| matches!(a, Some(a) if a.owner == DELEGATION_PROGRAM_ID)));
        }

        Ok(blackout)
    }

    #[instrument(skip_all, err)]
    async fn complete_settlement(
        &self,
        accounts: &CacheAccounts,
        progress: &TrafficProgress,
        settlement: RuleHandle,
    ) -> Result<Option<Result<Duration>>> {
        let er = self.private.ctx();
        let (readonly, protected_keys) = accounts.keys.split_at(READONLY_COUNT);
        let (delegated, pending) = protected_keys.split_at(DELEGATED_COUNT);
        progress.completed.set(true);
        settlement.remove();
        self.base
            .api()
            .await_accounts(pending, "base undelegation", async || {
                let actual = self.base.accounts(pending).await?;
                for (actual, expected) in actual.iter().zip(&accounts.protected[delegated.len()..])
                {
                    let actual = actual.as_ref().map(|a| (a.owner, &a.data));
                    let expected = expected.as_ref().map(|a| (program::id(), &a.data));
                    ensure!(actual == expected);
                }
                Result::Ok(())
            })
            .await?;
        let released_at = Instant::now();
        self.base.cells(REDLINE, &accounts.funder).write(BASE_REWRITE, pending).await?;
        let recovery = if self.reconnect_before_settlement {
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
        self.base.cells(REDLINE, &accounts.funder).write(BASE_REFRESH, readonly).await?;
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
        progress.done.set(true);
        Result::Ok(recovery)
    }

    #[instrument(skip_all, err)]
    async fn finish(self) -> Result<()> {
        self.private.finish().await?;
        self.http.finish()?;
        self.ws.finish()?;
        Ok(())
    }
}
