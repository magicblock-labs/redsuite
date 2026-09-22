use std::{cell::Cell, collections::HashMap, rc::Rc, time::Duration};

use account::Account;
use async_trait::async_trait;
use futures_util::future::{try_join, try_join_all};
use json::JsonValueTrait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq,
    netfault::{self, Action, BaseProxies, Selector},
    prep, system, topology,
    topology::ErOptions,
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport,
};
use signer::Signer;

use crate::program::{self, instruction::build, layout::*, utils::fold_hash};

const TIMEOUT: Duration = Duration::from_secs(20);
const EVICTIONS: &str = "engine_keeper_account_cache_evictions";
const RECOVERY_OBSERVATION: Duration = Duration::from_secs(100);

pub enum CacheLifecycle {
    Churn,
    UndelegationReconnectGap,
}

fn id(account: Option<Account>) -> Option<u64> {
    account.and_then(|a| crate::written_id(&a.data))
}

async fn local(er: &ErCtx, keys: &[Pubkey]) -> Result<Vec<Option<Account>>> {
    let mut accounts = HashMap::new();
    for owner in [program::id(), program::DELEGATION_PROGRAM_ID] {
        accounts.extend(er.api().get_program_accounts(&owner).await?);
    }
    Ok(keys.iter().map(|key| accounts.get(key).cloned()).collect())
}

async fn value(er: &ErCtx, keys: &[Pubkey], expected: u64) -> Result<()> {
    check::poll_for("latest base value observed locally", TIMEOUT, || async {
        for (key, account) in keys.iter().zip(local(er, keys).await?) {
            check_eq!(id(account), Some(expected), "base value of {key}")?;
        }
        Result::Ok(())
    })
    .await?;
    Ok(())
}

#[async_trait(?Send)]
impl PrivateErScenario for CacheLifecycle {
    fn name(&self) -> &str {
        match self {
            Self::Churn => "redshift/cache_lifecycle",
            Self::UndelegationReconnectGap => {
                "redshift/undelegation_reconnect_gap"
            }
        }
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let (label, churn_cycles) = match self {
            Self::Churn => ("cache-lifecycle", 3),
            Self::UndelegationReconnectGap => ("undelegation-reconnect-gap", 1),
        };
        let reconnect_before_settlement =
            matches!(self, Self::UndelegationReconnectGap);
        let http = BaseProxies::spawn(base).await?;
        let ws = BaseProxies::spawn(base).await?;
        let observe = |method, key: &Pubkey| {
            ws.intercept(
                Selector::method(method)
                    .response()
                    .notification()
                    .account(key),
            )
        };
        let mut endpoints = http.endpoints();
        endpoints.ws_url = ws.endpoints().ws_url;
        let private = topology::private_er(
            base,
            ErOptions {
                label: label.into(),
                env: vec![(
                    "MBV_ENGINE__ACCOUNTSDB__LRU_CAPACITY".into(),
                    "256".into(),
                )],
                request_timeout: Some(TIMEOUT),
                base_endpoints: Some(endpoints),
            },
        )
        .await?;
        let er = private.ctx();
        let validator = er.identity();
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let owner = funder.pubkey();
        let set_base = async |id, keys: &[Pubkey]| {
            let ix = build::simple_byte_set(id, keys);
            base.submit_and_confirm(&funder, &[ix]).await
        };
        let payer = prep::delegated_payer(
            base,
            &funder,
            validator,
            crate::PAYER_LAMPORTS,
        )
        .await?;
        let keys =
            try_join_all((0..7).map(|seed| {
                crate::init_account(base, &funder, seed, validator)
            }))
            .await?;
        let (readonly, protected_keys) = keys.split_at(3);
        let (delegated, pending) = protected_keys.split_at(2);
        let stale: Vec<_> = protected_keys
            .iter()
            .map(|key| observe("programNotification", key))
            .collect();
        let delegates: Vec<_> = protected_keys
            .iter()
            .zip(3..)
            .map(|(&key, seed)| {
                build::delegate(owner, key, owner, seed, validator)
            })
            .collect();
        base.submit_and_confirm(&funder, &delegates).await?;
        let stale =
            try_join_all(stale.iter().map(|trap| trap.wait(TIMEOUT))).await?;
        er.accounts(protected_keys).await?;
        er.submit_and_confirm(
            &payer,
            &[build::simple_byte_set(700, &protected_keys[1..])],
        )
        .await?;
        let settlement = http.stall(
            Selector::method("sendTransaction")
                .request()
                .account(&pending[0]),
        );
        er.submit_and_confirm(
            &funder,
            &[build::commit_and_undelegate_accounts(1, owner, pending)],
        )
        .await?;
        check::poll("undelegation submission held", TIMEOUT, || async {
            http.events().iter().any(|e| e.action == Action::Held)
        })
        .await?;
        let protected = local(er, protected_keys).await?;
        check!(protected.iter().all(Option::is_some), "protected residency")?;
        let base_delegated = base.accounts(delegated).await?;
        let pool: Vec<_> = (0..512).map(|_| Keypair::new().pubkey()).collect();
        try_join_all(pool.chunks(16).map(|chunk| async {
            let transfers: Vec<_> = chunk
                .iter()
                .map(|key| system::transfer(&owner, key, 1_000_000))
                .collect();
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
                let ixs = [build::hash_fold(id, 0, &delegated[..1])];
                let signature = check::poll_for(
                    "warmed submission recovers",
                    TIMEOUT,
                    || sender.submit(&ixs),
                )
                .await?;
                let outcome =
                    er.api().await_transaction(&signature, TIMEOUT).await?;
                if let Some(error) = &outcome.err {
                    check_eq!(
                        error.as_str(),
                        Some("ProgramAccountNotFound"),
                        "only transient program eviction may fail traffic"
                    )?;
                } else {
                    hash = fold_hash(id, &[hash], 0);
                    let data = &mut expected[0].as_mut().unwrap().data;
                    data[ID_OFFSET..HASH_OFFSET]
                        .copy_from_slice(&id.to_le_bytes());
                    data[HASH_OFFSET..HASH_OFFSET + HASH_SIZE]
                        .copy_from_slice(&hash);
                }
                let count = if completed.get() { 2 } else { 4 };
                let actual = local(er, &protected_keys[..count]).await?;
                check_eq!(actual, expected[..count], "protected local state")?;
                if outcome.err.is_none() {
                    progress.set(progress.get() + 1);
                }
            }
            Ok(())
        };
        let faults = async {
            let mut evictions = before;
            progressed(0).await?;
            for held in stale {
                held.release();
            }
            for (cycle, &target) in readonly[..churn_cycles].iter().enumerate()
            {
                for chunk in pool.chunks(32) {
                    er.accounts(chunk).await?;
                }
                let next = eviction_count().await?;
                check!(next > evictions, "evictions before reconnect {cycle}")?;
                evictions = next;
                let cold = local(er, &[target]).await?;
                check!(cold[0].is_none(), "new subscription target is cold")?;
                let reconnect = observe("accountSubscribe", &pending[0]);
                ws.close_connections();
                let reconnect = reconnect.wait(TIMEOUT).await?;
                let registered = observe("accountSubscribe", &target);
                let old = http.intercept(
                    Selector::methods(&[
                        "getAccountInfo",
                        "getMultipleAccounts",
                    ])
                    .response()
                    .account(&target),
                );
                let latest = cycle as u64 + 10;
                let (read, ()) = try_join(er.account(&target), async {
                    registered.wait(TIMEOUT).await?.release();
                    let old = old.wait(TIMEOUT).await?;
                    let notification = observe("accountNotification", &target);
                    set_base(latest, &[target]).await?;
                    notification.wait(TIMEOUT).await?.release();
                    value(er, &[target], latest).await?;
                    old.release();
                    progressed(progress.get()).await?;
                    reconnect.release();
                    Ok(())
                })
                .await?;
                check_eq!(id(read), Some(latest), "HTTP read stays current")?;
                value(er, &[target], latest).await?;
                check!(
                    base.accounts(pending).await?.iter().all(|a|
                        matches!(a, Some(a) if a.owner == program::DELEGATION_PROGRAM_ID)),
                    "undelegation stayed pending through churn"
                )?;
            }
            if reconnect_before_settlement {
                // Allow settlement as subscriptions are being rebuilt.
                let resubscribing = ws
                    .intercept(Selector::method("accountSubscribe").response());
                ws.close_connections();
                resubscribing.wait(TIMEOUT).await?.release();
            }
            completed.set(true);
            settlement.remove();
            check::poll_for("base undelegation", TIMEOUT, || async {
                let actual = base.accounts(pending).await?;
                for (actual, expected) in actual.iter().zip(&protected[2..]) {
                    let actual = actual.as_ref().map(|a| (a.owner, &a.data));
                    let expected =
                        expected.as_ref().map(|a| (program::id(), &a.data));
                    check_eq!(actual, expected, "settled owner and state")?;
                }
                Result::Ok(())
            })
            .await?;
            let released_at = tokio::time::Instant::now();
            set_base(900, pending).await?;
            let recovery = if reconnect_before_settlement {
                // Observe late recovery for diagnostics, but retain the
                // TIMEOUT deadline checked after teardown below.
                Some(
                    check::poll_for(
                        "ER discovers the completed undelegation",
                        RECOVERY_OBSERVATION,
                        || async {
                            match local(er, pending).await {
                                Ok(observed)
                                    if observed.iter().all(|a| {
                                        id(a.clone()) == Some(900)
                                    }) =>
                                {
                                    Ok(released_at.elapsed())
                                }
                                Ok(observed) => Err(format!(
                                    "{:?}",
                                    observed
                                        .iter()
                                        .map(|a| id(a.clone()))
                                        .collect::<Vec<_>>()
                                )),
                                Err(error) => Err(error.to_string()),
                            }
                        },
                    )
                    .await,
                )
            } else {
                value(er, pending, 900).await?;
                None
            };
            set_base(901, readonly).await?;
            er.accounts(readonly).await?;
            value(er, readonly, 901).await?;
            done.set(true);
            Result::Ok((evictions - before, recovery))
        };
        let (_, (evictions, recovery)) = tokio::time::timeout(
            Duration::from_secs(180),
            try_join(traffic, faults),
        )
        .await??;
        let actual = base.accounts(delegated).await?;
        check_eq!(actual, base_delegated, "base unchanged by ER-only writes")?;
        let mut report = ScenarioReport::ok(self.name())
            .setting("cache evictions", evictions)
            .setting("warmed transactions", progress.get())
            .setting(
                "reconnects",
                churn_cycles + usize::from(reconnect_before_settlement),
            );
        if let Some(recovery) = &recovery {
            let recovered_after = match recovery {
                Ok(elapsed) => format!("{elapsed:.1?}"),
                Err(_) => format!("not within {RECOVERY_OBSERVATION:?}"),
            };
            report = report.setting("er recovered after", recovered_after);
        }
        private.finish().await?;
        let mut events = http.finish()?;
        events.extend(ws.finish()?);
        let report = netfault::report_events(report, &events);
        if let Some(recovery) = recovery {
            let elapsed = recovery?;
            check!(
                elapsed <= TIMEOUT,
                "ER discovered the completed undelegation only after {elapsed:.1?}"
            )?;
        }
        Ok(report)
    }
}
