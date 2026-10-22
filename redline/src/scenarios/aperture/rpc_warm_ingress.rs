use std::{rc::Rc, sync::Arc};

use anyhow::{ensure, Context};
use pubkey::Pubkey;
use redsuite_core::{
    profile::{LoopMode, ProfileValues},
    programs::{CellHeader, REDLINE},
    redline::Accounts,
    report::Unit,
    runner::{execute, execute_and_sync, Pacing, RunConfig, RunOutcome},
    transport::NETWORK_TIMEOUT,
    transport::{
        clock::SendClock,
        ws::{AccountUpdates, SignatureConfirmations, SignatureOutcome, UpdateOutcome},
    },
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport, TxSender,
};
use tracing::{instrument, warn};

use crate::{metrics, ACCOUNT_SPACE};

const PAYER_LAMPORTS: u64 = 2_000_000_000;

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

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

pub struct WarmIngress {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    senders: Vec<TxSender>,
    pdas: Vec<Pubkey>,
}

impl Scenario<ScenarioReport> for WarmIngress {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let payers = base.funded_payers(profile.payers, PAYER_LAMPORTS).await?;
        let pdas = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_delegated(base.as_ref(), &payers[0], profile.accounts)
            .await?;
        er.await_clones(&pdas, ACCOUNT_SPACE as usize).await?;

        let senders: Vec<TxSender> =
            payers.into_iter().map(|payer| er.sender(Rc::new(payer))).collect();

        Ok(Self { base, er, profile, senders, pdas })
    }

    async fn run(self) -> Result<ScenarioReport> {
        self.warm_up().await?;
        let measurement = self.measure().await?;
        self.check_final_state().await?;
        Ok(measurement.report(self.profile, self.base.config().loop_mode))
    }
}

struct Measurement {
    outcome: RunOutcome,
    signatures: SignatureOutcome,
    updates: UpdateOutcome,
    delta: MetricsDelta,
}

impl WarmIngress {
    #[instrument(skip_all, err)]
    async fn warm_up(&self) -> Result<()> {
        let warmup = execute(
            RunConfig {
                iterations: self.profile.warmup,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            |id| {
                let sender = self.senders[(id as usize) % self.senders.len()].clone();
                let (ix, _) = REDLINE.copy_three(&self.pdas, id);
                sender.staged([ix])
            },
        )
        .await?;
        ensure!(warmup.failed == 0);

        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn measure(&self) -> Result<Measurement> {
        let offset = self.profile.warmup;
        let clock = Arc::new(SendClock::new(offset + 1, self.profile.iterations as usize));
        let updates = Rc::new(
            AccountUpdates::connect(self.er.ws_url(), CellHeader::written_id, clock.clone())
                .await?,
        );
        for pda in &self.pdas {
            updates.account_subscribe(pda).await?;
        }
        updates.await_subscribed(self.pdas.len()).await?;
        let sigs = Rc::new(SignatureConfirmations::connect(self.er.ws_url(), clock.clone()).await?);

        // warmup discarded and setup complete — open the measured window
        let before = self.er.scrape_metrics().await?;

        let request = |iteration: u64| {
            let id = offset + iteration;
            let sender = self.senders[(id as usize) % self.senders.len()].clone();
            let (ix, [tracked_dest, _]) = REDLINE.copy_three(&self.pdas, id);
            updates.track(id, tracked_dest);
            let sigs = sigs.clone();
            let clock = clock.clone();
            async move {
                // sign → subscribe → deliver: the signature subscription
                // must exist before the tx can confirm
                let tx = sender.prepare(&[ix]).await?;
                sigs.subscribe(id, &tx.signatures[0]).await?;
                Ok(clock.stamped(id, sender.deliver(tx)))
            }
        };
        let sync = |iteration: u64| {
            let id = offset + iteration;
            let sigs = sigs.clone();
            let updates = updates.clone();
            async move {
                sigs.await_id(id).await?;
                updates.await_id(id).await
            }
        };
        let cfg = RunConfig {
            iterations: self.profile.iterations,
            rate: Pacing::PerSecond(self.profile.rate),
            concurrency: self.profile.concurrency,
        };
        // Open loop (default): the rate permit is released on delivery —
        // sustained pressure. Closed loop: held until every confirmation
        // for the id arrives — true round-trip under backpressure.
        let mode = self.base.config().loop_mode;
        let outcome = if mode == LoopMode::Closed {
            execute_and_sync(cfg, request, sync).await?
        } else {
            execute(cfg, request).await?
        };
        ensure!(outcome.failed == 0);

        // open loop drains here; a closed loop has already settled per id
        sigs.await_all().await?;
        updates.await_settled().await?;
        // window closes only once the fan-out settled — the after scrape
        // must cover everything the measured load caused
        let after = self.er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);

        // validator-side cross-checks, gated on what this build exposes:
        // the no-op gate (our load provably hit the validator) and the
        // nothing-failed-on-chain invariant
        if let Some(processed) = delta.counter(metrics::ENGINE_TRANSACTIONS) {
            if processed < self.profile.iterations as f64 {
                warn!(
                    processed,
                    self.profile.iterations, "validator transaction count",
                );
            }
        }
        if let Some(failed) = delta.counter_all(metrics::FAILED_TRANSACTIONS) {
            ensure!(failed == 0.0);
        }

        let update_outcome = updates.finalize();
        ensure!(
            (update_outcome.observed + update_outcome.superseded)
                == (self.profile.iterations as usize)
        );
        let sig_outcome = sigs.finalize();
        ensure!(sig_outcome.failed == 0);
        ensure!(sig_outcome.unconfirmed == 0);
        ensure!(sig_outcome.confirmed == (self.profile.iterations as usize));

        Ok(Measurement {
            outcome,
            signatures: sig_outcome,
            updates: update_outcome,
            delta,
        })
    }

    #[instrument(skip_all, err)]
    async fn check_final_state(&self) -> Result<()> {
        let offset = self.profile.warmup;
        for pda in self.pdas.iter() {
            let last_id = (offset + 1..=offset + self.profile.iterations)
                .rev()
                .find(|&id| REDLINE.copy_three(&self.pdas, id).1.contains(pda));
            let Some(last_id) = last_id else {
                continue;
            };
            let on_er = self.er.account(pda).await?.context("pda not on er")?;
            ensure!(CellHeader::written_id(&on_er.data) == Some(last_id));
        }

        Ok(())
    }
}

impl Measurement {
    fn report(&self, profile: &Profile, mode: LoopMode) -> ScenarioReport {
        let mut report = ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("loop", mode.name())
            .setting("confirm timeout s", NETWORK_TIMEOUT.as_secs())
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("warmup iters", profile.warmup)
            .setting("measured iters", profile.iterations)
            .setting("offered tps", profile.rate)
            .setting("concurrency", profile.concurrency)
            .observe("delivery us", Unit::Micros, self.outcome.delivery)
            .client_side(&self.outcome)
            .observe("signature latency us", Unit::Micros, self.signatures.latency)
            .observe("achieved rps", Unit::Rps, self.outcome.rps)
            .observe("account-update lag us", Unit::Micros, self.updates.lag)
            .metric("achieved tps", Unit::Tps, self.outcome.achieved_rps())
            .metric(
                "measured wall s",
                Unit::Seconds,
                self.outcome.wall.as_secs_f64(),
            )
            .metric("superseded", Unit::Count, self.updates.superseded as f64)
            // validator-side numbers (histogram window averages, converted
            // to us) — never comparable 1:1 with the client-side stats
            // above, but a divergence points at the harness, not the
            // validator (R1)
            .metric_if(
                "validator tx processing avg us",
                Unit::Micros,
                self.delta
                    .histogram_avg("mbv_transaction_processing_time")
                    .map(|seconds| seconds * 1e6),
            )
            .metric_if(
                "validator ensure accounts avg us",
                Unit::Micros,
                self.delta
                    .histogram_avg(
                        r#"mbv_ensure_accounts_time{kind="transaction"}"#,
                    )
                    .map(|seconds| seconds * 1e6),
            )
            .metric_if(
                "validator txs in window",
                Unit::Count,
                self.delta.counter(metrics::ENGINE_TRANSACTIONS),
            )
            .metric_if(
                "monitored accounts (gauge)",
                Unit::Count,
                self.delta.gauge("mbv_monitored_accounts_gauge"),
            );
        if let Some(sync) = self.outcome.sync {
            report = report.observe("sync round-trip us", Unit::Micros, sync);
        }
        report
    }
}
