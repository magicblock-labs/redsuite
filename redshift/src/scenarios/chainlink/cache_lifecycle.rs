use std::{cell::Cell, rc::Rc, time::Duration};

use async_trait::async_trait;
use futures_util::future::{try_join, try_join_all};
use json::JsonValueTrait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq,
    netfault::{Action, BaseProxies, Selector, Stage},
    prep, system, topology,
    topology::ErOptions,
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signer::Signer;

use crate::program::{self, instruction::build, layout::*, utils::fold_hash};

const TIMEOUT: Duration = Duration::from_secs(20);
const EVICTIONS: &str = "engine_keeper_account_cache_evictions";
const FETCHES: [&str; 2] = ["getAccountInfo", "getMultipleAccounts"];

pub enum CacheLifecycle {
    Churn,
    UndelegationReconnectGap,
}

async fn value(er: &ErCtx, keys: &[Pubkey], expected: u64) -> Result<()> {
    check::poll_for("latest base value observed locally", TIMEOUT, || async {
        for (key, account) in
            keys.iter().zip(crate::local_accounts(er, keys).await?)
        {
            check_eq!(
                crate::account_id(account),
                Some(expected),
                "base value of {key}"
            )?;
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

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let (label, churn_cycles) = match self {
            Self::Churn => ("cache-lifecycle", 3),
            Self::UndelegationReconnectGap => ("undelegation-reconnect-gap", 0),
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
        if reconnect_before_settlement {
            let (grpc_url, _) = base.grpc().ok_or(
                "redshift/undelegation_reconnect_gap needs the base L1 \
                 Yellowstone gRPC feed; run `redsuite stack down` and retry \
                 on a host where the plugin resolves",
            )?;
            endpoints.grpc_url = Some(grpc_url.to_owned());
        }
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
                ..Default::default()
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
        let acquire = async |keys: &[Pubkey]| -> Result<()> {
            check::poll_for("ordinary mirrors acquired", TIMEOUT, || async {
                check!(
                    er.accounts(keys).await?.iter().all(Option::is_some),
                    "mirror acquisition incomplete"
                )?;
                Result::Ok(())
            })
            .await?;
            Ok(())
        };
        // Buffer ordinary mirror updates before delegation. ChainSync receives
        // lifecycle events through gRPC, not WebSocket program subscriptions.
        acquire(protected_keys).await?;
        let stale: Vec<_> = if reconnect_before_settlement {
            Vec::new()
        } else {
            protected_keys
                .iter()
                .map(|key| observe("accountNotification", key))
                .collect()
        };
        if !reconnect_before_settlement {
            set_base(42, protected_keys).await?;
        }
        let stale =
            try_join_all(stale.iter().map(|trap| trap.wait(TIMEOUT))).await?;
        let delegates: Vec<_> = protected_keys
            .iter()
            .zip(3..)
            .map(|(&key, seed)| {
                build::delegate(owner, key, owner, seed, validator)
            })
            .collect();
        base.submit_and_confirm(&funder, &delegates).await?;
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
        let protected = crate::local_accounts(er, protected_keys).await?;
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
            let mut hash = expected[0].as_ref().unwrap().data
                [HASH_OFFSET..HASH_OFFSET + HASH_SIZE]
                .try_into()?;
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
                let actual =
                    crate::local_accounts(er, &protected_keys[..count]).await?;
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
                for chunk in pool.chunks(32) {
                    acquire(chunk).await?;
                }
                let next = eviction_count().await?;
                check!(next > evictions, "evictions before blackout")?;
                evictions = next;
                blackout.push(ws.stall(Selector::default().notification()));
                for key in protected_keys {
                    blackout.push(http.stall(
                        Selector::methods(&FETCHES).response().account(key),
                    ));
                }
                ws.close_connections();
                // A fresh mirror proves the replacement socket can acknowledge
                // subscriptions before lifecycle handling needs to acquire one.
                let canary = Keypair::new().pubkey();
                base.submit_and_confirm(
                    &funder,
                    &[system::transfer(&owner, &canary, 1_000_000)],
                )
                .await?;
                check::poll_for(
                    "replacement socket acquires a fresh mirror",
                    TIMEOUT,
                    || async {
                        let account = er
                            .account(&canary)
                            .await?
                            .ok_or("replacement subscription not ready")?;
                        Result::Ok(account)
                    },
                )
                .await?;
            }
            for (cycle, &target) in readonly[..churn_cycles].iter().enumerate()
            {
                for chunk in pool.chunks(32) {
                    acquire(chunk).await?;
                }
                let next = eviction_count().await?;
                check!(next > evictions, "evictions before reconnect {cycle}")?;
                evictions = next;
                let cold = crate::local_accounts(er, &[target]).await?;
                check!(cold[0].is_none(), "new subscription target is cold")?;
                ws.close_connections();
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
                let acquire = async {
                    let account = check::poll_for(
                        "cold mirror acquisition after reconnect",
                        TIMEOUT,
                        || async {
                            let account = er
                                .account(&target)
                                .await?
                                .ok_or("mirror not acquired yet")?;
                            Result::Ok(account)
                        },
                    )
                    .await?;
                    Result::Ok(account)
                };
                let (read, ()) = try_join(acquire, async {
                    registered.wait(TIMEOUT).await?.release();
                    let old = old.wait(TIMEOUT).await?;
                    let notification = observe("accountNotification", &target);
                    set_base(latest, &[target]).await?;
                    notification.wait(TIMEOUT).await?.release();
                    // The HTTP acquisition owns the lease. Its queued notification
                    // can materialize only after the held response releases that lease.
                    old.release();
                    value(er, &[target], latest).await?;
                    progressed(progress.get()).await?;
                    Ok(())
                })
                .await?;
                check_eq!(
                    read.owner,
                    program::id(),
                    "HTTP read returns the ordinary account image"
                )?;
                value(er, &[target], latest).await?;
                check!(
                    base.accounts(pending).await?.iter().all(|a|
                        matches!(a, Some(a) if a.owner == program::DELEGATION_PROGRAM_ID)),
                    "undelegation stayed pending through churn"
                )?;
            }
            completed.set(true);
            // ChainSync release handling must subscribe and refetch; restoring
            // the transport is not a promise of recovery during a total outage.
            for rule in blackout {
                rule.remove();
            }
            ws.restore();
            http.restore();
            let refresh_events = http.events().len();
            let refreshes: Vec<_> = pending
                .iter()
                .map(|key| {
                    http.stall(
                        Selector::methods(&FETCHES).response().account(key),
                    )
                })
                .collect();
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
            // Hold every matching response: Committor can read the same keys
            // before ChainSync, so a one-shot interception is not a refresh barrier.
            // Observe holds concurrently: ChainSync refreshes releases serially
            // in provider order, not necessarily in this test's key order.
            // Waiting for another key first would leave the active fetch stalled.
            let refreshes = pending.iter().zip(refreshes).zip(&protected[2..]);
            try_join_all(refreshes.map(|((key, rule), expected)| async {
                let address = key.to_string();
                check::poll("readonly refetch held", TIMEOUT, || async {
                    http.events().iter().skip(refresh_events).any(|event| {
                        event.action == Action::Held
                            && event.operation.as_ref().is_some_and(
                                |operation| {
                                    operation.accounts.contains(&address)
                                        && operation.stage == Stage::Response
                                },
                            )
                    })
                })
                .await?;
                check_eq!(
                    crate::local_accounts(er, &[*key]).await?[0],
                    *expected,
                    "resident state while readonly refetch is held"
                )?;
                rule.remove();
                Result::Ok(())
            }))
            .await?;
            set_base(900, pending).await?;
            value(er, pending, 900).await?;
            set_base(901, readonly).await?;
            let fetched = er.accounts(readonly).await?;
            let local = crate::local_accounts(er, readonly).await?;
            for ((key, fetched), local) in
                readonly.iter().zip(fetched).zip(local)
            {
                check_eq!(
                    crate::account_id(fetched),
                    Some(901),
                    "final fetch of {key}"
                )?;
                // Readonly entries may be evicted after the fetch.
                if let Some(local) = local {
                    check_eq!(
                        crate::written_id(&local.data),
                        Some(901),
                        "retained local value of {key}"
                    )?;
                }
            }
            done.set(true);
            Result::Ok(())
        };
        tokio::time::timeout(
            Duration::from_secs(180),
            try_join(traffic, faults),
        )
        .await??;
        let actual = base.accounts(delegated).await?;
        check_eq!(actual, base_delegated, "base unchanged by ER-only writes")?;
        private.finish().await?;
        http.finish()?;
        ws.finish()?;
        Ok(())
    }
}
