use std::{rc::Rc, time::Duration};

use async_trait::async_trait;
use redsuite_core::redline::{copy_three, Accounts};
use redsuite_core::report::Unit;
use redsuite_core::{
    check_eq, prep,
    profile::{LoopMode, ProfileValues},
    runner::{execute, execute_and_sync, Pacing, RunConfig},
    transport::ws::{AccountUpdates, SignatureConfirmations},
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
    TxSender,
};

const PAYER_LAMPORTS: u64 = 2_000_000_000;

const CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: u8,
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    accounts: 16,
    warmup: 100,
    iterations: 400,
    rate: 200,
    concurrency: 64,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    accounts: 64,
    warmup: 3_000,
    iterations: 30_000,
    rate: 1_000,
    concurrency: 256,
};

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
};

pub struct WarmIngress;

#[async_trait(?Send)]
impl Scenario<ScenarioReport> for WarmIngress {
    fn name(&self) -> &str {
        "redline/rpc_warm_ingress"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = PROFILES.select(base.config().profile);
        let payers =
            prep::funded_payers(base, profile.payers, PAYER_LAMPORTS).await?;
        let pdas = Accounts::new(crate::ACCOUNT_SPACE, er.identity())
            .init_delegated(base, &payers[0], profile.accounts)
            .await?;
        prep::await_clones(
            er,
            &pdas,
            crate::ACCOUNT_SPACE as usize,
            Duration::from_secs(15),
        )
        .await?;

        let senders: Vec<TxSender> = payers
            .into_iter()
            .map(|payer| er.sender(Rc::new(payer)))
            .collect();

        let warmup = execute(
            RunConfig {
                iterations: profile.warmup,
                rate: Pacing::PerSecond(profile.rate),
                concurrency: profile.concurrency,
            },
            |id| {
                let sender = senders[(id as usize) % senders.len()].clone();
                let (ix, _) = copy_three(&pdas, id);
                async move { sender.submit(&[ix]).await.map(|_| ()) }
            },
        )
        .await?;
        check_eq!(
            warmup.failed,
            0,
            "warmup deliveries failed: {:?}",
            warmup.first_error
        )?;

        let updates = Rc::new(
            AccountUpdates::connect(er.ws_url(), crate::account_update_id)
                .await?,
        );
        for pda in &pdas {
            updates.account_subscribe(pda).await?;
        }
        updates
            .await_subscribed(pdas.len(), Duration::from_secs(5))
            .await?;
        let sigs = Rc::new(SignatureConfirmations::connect(er.ws_url()).await?);

        // warmup discarded and setup complete — open the measured window
        let before = er.scrape_metrics().await?;

        let offset = profile.warmup;
        let request = |iteration: u64| {
            let id = offset + iteration;
            let sender = senders[(id as usize) % senders.len()].clone();
            let (ix, [tracked_dest, _]) = copy_three(&pdas, id);
            updates.track(id, tracked_dest);
            let sigs = sigs.clone();
            async move {
                // sign → subscribe → deliver: the signature subscription
                // must exist before the tx can confirm
                let tx = sender.prepare(&[ix]).await?;
                sigs.subscribe(id, &tx.signatures[0]).await?;
                sender.submit_prepared(&tx).await?;
                Ok(())
            }
        };
        let sync = |iteration: u64| {
            let id = offset + iteration;
            let sigs = sigs.clone();
            let updates = updates.clone();
            async move {
                tokio::time::timeout(CONFIRM_TIMEOUT, async move {
                    sigs.await_id(id).await?;
                    updates.await_id(id).await
                })
                .await
                .map_err(|_| {
                    format!(
                        "confirmations for id {id} not within {CONFIRM_TIMEOUT:?}"
                    )
                })?
            }
        };
        let cfg = RunConfig {
            iterations: profile.iterations,
            rate: Pacing::PerSecond(profile.rate),
            concurrency: profile.concurrency,
        };
        // Open loop (default): the rate permit is released on delivery —
        // sustained pressure. Closed loop: held until every confirmation
        // for the id arrives — true round-trip under backpressure.
        let mode = base.config().loop_mode;
        let outcome = if mode == LoopMode::Closed {
            execute_and_sync(cfg, request, sync).await?
        } else {
            execute(cfg, request).await?
        };
        check_eq!(
            outcome.failed,
            0,
            "measured iterations failed: {:?}",
            outcome.first_error
        )?;

        // open loop drains here; a closed loop has already settled per id
        sigs.await_all(CONFIRM_TIMEOUT).await?;
        updates.await_settled(CONFIRM_TIMEOUT).await?;
        // window closes only once the fan-out settled — the after scrape
        // must cover everything the measured load caused
        let after = er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);

        // validator-side cross-checks, gated on what this build exposes:
        // the no-op gate (our load provably hit the validator) and the
        // nothing-failed-on-chain invariant
        if let Some(processed) =
            delta.counter(crate::metrics::ENGINE_TRANSACTIONS)
        {
            if processed < profile.iterations as f64 {
                eprintln!(
                    "[redsuite] {}: warning: validator processed {processed} \
                     txs in the measured window, expected at least {}",
                    self.name(),
                    profile.iterations
                );
            }
        }
        if let Some(failed) =
            delta.counter_all(crate::metrics::FAILED_TRANSACTIONS)
        {
            check_eq!(failed, 0.0, "transactions failed on the validator")?;
        }

        let update_outcome = updates.finalize();
        check_eq!(
            update_outcome.observed + update_outcome.superseded,
            profile.iterations as usize,
            "every tracked write must be observed or superseded"
        )?;
        let sig_outcome = sigs.finalize();
        check_eq!(
            sig_outcome.failed,
            0,
            "transactions failed on-chain: {:?}",
            sig_outcome.first_failure
        )?;
        check_eq!(
            sig_outcome.unconfirmed,
            0,
            "signatures unconfirmed after {CONFIRM_TIMEOUT:?}"
        )?;
        check_eq!(
            sig_outcome.confirmed,
            profile.iterations as usize,
            "every measured tx must confirm"
        )?;

        for (idx, pda) in pdas.iter().enumerate() {
            let last_id = (offset + 1..=offset + profile.iterations)
                .rev()
                .find(|&id| copy_three(&pdas, id).1.contains(pda));
            let Some(last_id) = last_id else {
                continue;
            };
            let on_er = er.account(pda).await?.ok_or("pda not on er")?;
            check_eq!(
                crate::account_update_id(&on_er.data),
                Some(last_id),
                "er copy must hold the last id written to pda {idx}"
            )?;
        }

        let mut report = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("loop", mode.name())
            .setting("confirm timeout s", CONFIRM_TIMEOUT.as_secs())
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("warmup iters", profile.warmup)
            .setting("measured iters", profile.iterations)
            .setting("offered tps", profile.rate)
            .setting("concurrency", profile.concurrency)
            .observe("delivery us", Unit::Micros, outcome.delivery)
            .observe("signature latency us", Unit::Micros, sig_outcome.latency)
            .observe("achieved rps", Unit::Rps, outcome.rps)
            .observe("account-update lag us", Unit::Micros, update_outcome.lag)
            .metric("achieved tps", Unit::Tps, outcome.achieved_rps())
            .metric(
                "measured wall s",
                Unit::Seconds,
                outcome.wall.as_secs_f64(),
            )
            .metric("superseded", Unit::Count, update_outcome.superseded as f64)
            // validator-side numbers (histogram window averages, converted
            // to us) — never comparable 1:1 with the client-side stats
            // above, but a divergence points at the harness, not the
            // validator (R1)
            .metric_if(
                "validator tx processing avg us",
                Unit::Micros,
                delta
                    .histogram_avg("mbv_transaction_processing_time")
                    .map(|seconds| seconds * 1e6),
            )
            .metric_if(
                "validator ensure accounts avg us",
                Unit::Micros,
                delta
                    .histogram_avg(
                        r#"mbv_ensure_accounts_time{kind="transaction"}"#,
                    )
                    .map(|seconds| seconds * 1e6),
            )
            .metric_if(
                "validator txs in window",
                Unit::Count,
                delta.counter(crate::metrics::ENGINE_TRANSACTIONS),
            )
            .metric_if(
                "monitored accounts (gauge)",
                Unit::Count,
                delta.gauge("mbv_monitored_accounts_gauge"),
            );
        if let Some(sync) = outcome.sync {
            report = report.observe("sync round-trip us", Unit::Micros, sync);
        }
        Ok(report)
    }
}
