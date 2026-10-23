use std::{cell::Cell, rc::Rc, time::Duration};

use async_trait::async_trait;
use futures_util::future::{try_join, try_join_all};
use json::JsonValueTrait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq,
    netfault::{Action, BaseProxies, Selector},
    prep, system, topology,
    topology::ErOptions,
    BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use signer::Signer;

use crate::program::{self, instruction::build, layout::*, utils::fold_hash};

use super::{
    await_local_value, init_accounts, write, BASE_REFRESH, BASE_REWRITE, ER_WRITE, FETCHES,
};

const LRU_CAPACITY: usize = 256;
const READONLY_COUNT: usize = 3;
const DELEGATED_COUNT: usize = 2;
const PENDING_COUNT: usize = 2;
const FUNDING_BATCH: usize = 16;
const FETCH_BATCH: usize = 32;
const TIMEOUT: Duration = Duration::from_secs(20);
const EVICTIONS: &str = "engine_keeper_account_cache_evictions";
const RECOVERY_OBSERVATION: Duration = Duration::from_secs(100);
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(180);

pub enum CacheLifecycle {
    Churn,
    UndelegationReconnectGap,
}

#[async_trait(?Send)]
impl PrivateErScenario for CacheLifecycle {
    fn name(&self) -> &str {
        match self {
            Self::Churn => "redshift/cache_lifecycle",
            Self::UndelegationReconnectGap => "redshift/undelegation_reconnect_gap",
        }
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let (label, churn_cycles) = match self {
            Self::Churn => ("cache-lifecycle", READONLY_COUNT),
            Self::UndelegationReconnectGap => ("undelegation-reconnect-gap", 0),
        };
        let reconnect_before_settlement = matches!(self, Self::UndelegationReconnectGap);
        let http = BaseProxies::spawn(base).await?;
        let ws = BaseProxies::spawn(base).await?;
        let observe = |method, key: &Pubkey| {
            ws.intercept(Selector::method(method).response().notification().account(key))
        };
        let mut endpoints = http.endpoints();
        endpoints.ws_url = ws.endpoints().ws_url;
        if reconnect_before_settlement {
            let (grpc_url, _) = base.grpc().ok_or("base L1 Yellowstone gRPC endpoint missing")?;
            endpoints.grpc_url = Some(grpc_url.to_owned());
        }
        let private = topology::private_er(
            base,
            ErOptions {
                label: label.into(),
                settings: Box::new(|params| {
                    params.engine.accountsdb.lru_capacity = LRU_CAPACITY;
                }),
                request_timeout: Some(TIMEOUT),
                base_endpoints: Some(endpoints),
            },
        )
        .await?;
        let er = private.ctx();
        let validator = er.identity();
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let owner = funder.pubkey();
        let payer = prep::delegated_payer(base, &funder, validator, crate::PAYER_LAMPORTS).await?;
        let keys = init_accounts(
            base,
            &funder,
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
        let delegates: Vec<_> = protected_keys
            .iter()
            .zip(READONLY_COUNT as u8..)
            .map(|(&key, seed)| build::delegate(owner, key, owner, seed, validator))
            .collect();
        base.submit_and_confirm(&funder, &delegates).await?;
        let stale = try_join_all(stale.iter().map(|trap| trap.wait(TIMEOUT))).await?;
        er.accounts(protected_keys).await?;
        write(er, &payer, ER_WRITE, er_written_keys).await?;
        let settlement =
            http.stall(Selector::method("sendTransaction").request().account(&pending[0]));
        er.submit_and_confirm(
            &funder,
            &[build::commit_and_undelegate_accounts(1, owner, pending)],
        )
        .await?;
        check::poll("undelegation submission held", TIMEOUT, || async {
            http.events().iter().any(|e| e.action == Action::Held)
        })
        .await?;
        let protected = crate::local_accounts(er, protected_keys).await?;
        check!(protected.iter().all(Option::is_some), "protected residency")?;
        let base_delegated = base.accounts(delegated).await?;
        let pool: Vec<_> = (0..LRU_CAPACITY * 2).map(|_| Keypair::new().pubkey()).collect();
        try_join_all(pool.chunks(FUNDING_BATCH).map(|chunk| async {
            let transfers: Vec<_> =
                chunk.iter().map(|key| system::transfer(&owner, key, 1_000_000)).collect();
            base.submit_and_confirm(&funder, &transfers).await
        }))
        .await?;
        let eviction_count = async || -> Result<f64> {
            er.scrape_metrics()
                .await?
                .get(EVICTIONS)
                .ok_or_else(|| "eviction metric missing".into())
        };
        let before = eviction_count().await?;
        let progress = Cell::new(0u64);
        let progressed = async |start| {
            check::poll("warmed traffic recovers", TIMEOUT, || async {
                progress.get() > start
            })
            .await
        };
        let completed = Cell::new(false);
        let done = Cell::new(false);
        let sender = er.sender(Rc::new(payer));
        let traffic = async {
            let mut expected = protected.clone();
            let mut hash = [0; HASH_SIZE];
            let mut id = 0;
            while !done.get() {
                id += 1;
                let ixs = [build::hash_fold(id, 0, traffic_keys)];
                let signature = check::poll_for("warmed submission recovers", TIMEOUT, || {
                    sender.submit(&ixs)
                })
                .await?;
                let outcome = er.api().await_transaction(&signature, TIMEOUT).await?;
                if let Some(error) = &outcome.err {
                    check_eq!(
                        error.as_str(),
                        Some("ProgramAccountNotFound"),
                        "only transient program eviction may fail traffic"
                    )?;
                } else {
                    hash = fold_hash(id, &[hash], 0);
                    let data = &mut expected[0].as_mut().unwrap().data;
                    data[ID_OFFSET..HASH_OFFSET].copy_from_slice(&id.to_le_bytes());
                    data[HASH_OFFSET..HASH_OFFSET + HASH_SIZE].copy_from_slice(&hash);
                }
                let count = if completed.get() { delegated.len() } else { protected_keys.len() };
                let actual = crate::local_accounts(er, &protected_keys[..count]).await?;
                check_eq!(actual, expected[..count], "protected local state")?;
                if outcome.err.is_none() {
                    progress.set(progress.get() + 1);
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
                check!(next > evictions, "evictions before blackout")?;
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
                check!(next > evictions, "evictions before reconnect {cycle}")?;
                evictions = next;
                let cold = crate::local_accounts(er, &[target]).await?;
                check!(cold[0].is_none(), "new subscription target is cold")?;
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
                    write(base, &funder, latest, &[target]).await?;
                    notification.wait(TIMEOUT).await?.release();
                    await_local_value(
                        er,
                        "latest base value observed locally",
                        &[target],
                        latest,
                        TIMEOUT,
                    )
                    .await?;
                    old.release();
                    progressed(progress.get()).await?;
                    reconnect.release();
                    Ok(())
                })
                .await?;
                check_eq!(
                    crate::account_id(read),
                    Some(latest),
                    "HTTP read stays current"
                )?;
                await_local_value(
                    er,
                    "latest base value observed locally",
                    &[target],
                    latest,
                    TIMEOUT,
                )
                .await?;
                check!(
                    base.accounts(pending)
                        .await?
                        .iter()
                        .all(|a| matches!(a, Some(a) if a.owner == program::DELEGATION_PROGRAM_ID)),
                    "undelegation stayed pending through churn"
                )?;
            }
            completed.set(true);
            settlement.remove();
            check::poll_for("base undelegation", TIMEOUT, || async {
                let actual = base.accounts(pending).await?;
                for (actual, expected) in actual.iter().zip(&protected[delegated.len()..]) {
                    let actual = actual.as_ref().map(|a| (a.owner, &a.data));
                    let expected = expected.as_ref().map(|a| (program::id(), &a.data));
                    check_eq!(actual, expected, "settled owner and state")?;
                }
                Result::Ok(())
            })
            .await?;
            let released_at = tokio::time::Instant::now();
            write(base, &funder, BASE_REWRITE, pending).await?;
            let recovery = if reconnect_before_settlement {
                // Observe late recovery for diagnostics, but retain the
                // TIMEOUT deadline checked after teardown below.
                Some(
                    await_local_value(
                        er,
                        "ER discovers the completed undelegation",
                        pending,
                        BASE_REWRITE,
                        RECOVERY_OBSERVATION,
                    )
                    .await
                    .map(|()| released_at.elapsed()),
                )
            } else {
                // Completed accounts are evictable mirrors. A later reconnect
                // gap can invalidate them, so verify normal access refetches them.
                check::poll_for(
                    "completed accounts refetch current base state",
                    TIMEOUT,
                    || async {
                        let actual: Vec<_> = er
                            .accounts(pending)
                            .await?
                            .into_iter()
                            .map(crate::account_id)
                            .collect();
                        check_eq!(
                            actual,
                            vec![Some(BASE_REWRITE); pending.len()],
                            "completed undelegation values"
                        )?;
                        Result::Ok(())
                    },
                )
                .await?;
                None
            };
            write(base, &funder, BASE_REFRESH, readonly).await?;
            // Base confirmation does not wait for ER subscription delivery.
            check::poll_for("final readonly mirrors synchronized", TIMEOUT, || async {
                let fetched = er.accounts(readonly).await?;
                for (key, account) in readonly.iter().zip(fetched) {
                    check_eq!(
                        crate::account_id(account),
                        Some(BASE_REFRESH),
                        "final fetch of {key}"
                    )?;
                }
                Result::Ok(())
            })
            .await?;
            let local = crate::local_accounts(er, readonly).await?;
            for (key, local) in readonly.iter().zip(local) {
                // Readonly entries may be evicted after the fetch.
                if let Some(local) = local {
                    check_eq!(
                        crate::written_id(&local.data),
                        Some(BASE_REFRESH),
                        "retained local value of {key}"
                    )?;
                }
            }
            done.set(true);
            Result::Ok(recovery)
        };
        let (_, recovery) =
            tokio::time::timeout(SCENARIO_TIMEOUT, try_join(traffic, faults)).await??;
        let actual = base.accounts(delegated).await?;
        check_eq!(actual, base_delegated, "base unchanged by ER-only writes")?;
        private.finish().await?;
        http.finish()?;
        ws.finish()?;
        if let Some(recovery) = recovery {
            let elapsed = recovery?;
            check!(
                elapsed <= TIMEOUT,
                "ER discovered the completed undelegation only after {elapsed:.1?}"
            )?;
        }
        Ok(())
    }
}
