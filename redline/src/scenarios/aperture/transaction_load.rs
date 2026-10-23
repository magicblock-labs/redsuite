use std::{rc::Rc, sync::Arc, time::Instant};

use anyhow::{ensure, Context};
use instruction::Instruction;
use pubkey::Pubkey;
use redsuite_core::{
    config::{LoopMode, ProfileValues},
    programs::{CellHeader, HashState, REDLINE},
    redline::{causal, Accounts},
    report::Unit,
    runner::{execute, execute_and_sync, Pacing, RunConfig, RunOutcome},
    transport::{
        clock::SendClock,
        ws::{AccountUpdates, SignatureConfirmations, SignatureOutcome, UpdateOutcome},
        NETWORK_TIMEOUT,
    },
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport, TxSender,
};
use tokio::sync::watch::Receiver;
use tracing::{info, instrument};

use crate::{metrics, program::layout, RunControl, ACCOUNT_SPACE};

const PAYER_LAMPORTS: u64 = 2_000_000_000;

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: usize,
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
    iterations: 1_024,
    rate: 1_000,
    concurrency: 256,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

#[derive(Clone, Copy)]
pub enum Shape {
    Write,
    Copy,
    Compute,
}

/// Measures transaction delivery, confirmation, and updates on warmed accounts.
pub struct TransactionLoad {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    senders: Vec<TxSender>,
    pdas: Vec<Pubkey>,
    shape: Shape,
}

impl Scenario<(ScenarioReport, Result<()>), Shape> for TransactionLoad {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, shape: Shape) -> Result<Self> {
        let profile = match shape {
            Shape::Write => &WRITE,
            Shape::Copy => PROFILES.select(base.config().profile),
            Shape::Compute => &COMPUTE,
        };
        let payers = base.funded_payers(profile.payers, PAYER_LAMPORTS).await?;
        let pdas = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base.as_ref(), &payers, profile.accounts, true)
            .await?;
        er.await_clones(&pdas, ACCOUNT_SPACE as usize).await?;

        let senders: Vec<TxSender> =
            payers.into_iter().map(|payer| er.sender(Rc::new(payer))).collect();

        Ok(Self {
            base,
            er,
            profile,
            senders,
            pdas,
            shape,
        })
    }

    async fn run(self) -> Result<(ScenarioReport, Result<()>)> {
        let compute_ratio = if matches!(self.shape, Shape::Compute) {
            let mut costs = Vec::new();
            for iters in [1, 180] {
                let ix = REDLINE.hash(0, causal::HASH_INIT.to_bytes(), iters, &self.pdas[..1]);
                let signature = self.senders[0]
                    .submit(&[causal::compute_unit_limit(causal::CU_LIMIT), ix])
                    .await?;
                costs
                    .push(crate::probe_cus(&self.er, &signature, "compute contrast", iters).await?);
            }
            let ratio = costs[1] / costs[0];
            ensure!(ratio >= 10.0, "compute contrast was not exercised");
            Some(ratio)
        } else {
            None
        };
        self.warm_up().await?;
        let options = RunControl::from_config(&self.base.config())?;
        let mut remaining = options.duration;
        let mut cursor = self.profile.warmup;
        let mut delivered = 0;
        let mut peak = 0;
        let mut report = ScenarioReport::ok();
        let result: Result<()> = async {
            while !remaining.is_zero() && !*options.stop.borrow() {
                let measured = self.measure(cursor, remaining, options.stop.clone()).await?;
                if measured.outcome.delivered == 0 {
                    break;
                }
                self.check_final_state(cursor, measured.outcome.delivered).await?;
                remaining = remaining.saturating_sub(measured.outcome.admission_wall);
                cursor += measured.outcome.delivered;
                delivered += measured.outcome.delivered;
                peak = peak.max(measured.outcome.delivery.max);
                info!(
                    delivered,
                    p50_us = measured.outcome.delivery.median,
                    p95_us = measured.outcome.delivery.quantile95,
                    max_us = measured.outcome.delivery.max,
                    "transaction load window"
                );
                report = measured
                    .report(self.profile, self.base.config().loop_mode)
                    .setting("scope", "last observation window");
            }
            ensure!(
                delivered > 0 || *options.stop.borrow(),
                "no measured transactions completed"
            );
            Ok(())
        }
        .await;
        report = report
            .setting("completion", options.completion(&result))
            .setting(
                "payload",
                match self.shape {
                    Shape::Write => "write",
                    Shape::Copy => "copy",
                    Shape::Compute => "sha256",
                },
            )
            .metric("total delivered", Unit::Count, delivered as f64)
            .metric("peak delivery us", Unit::Micros, peak as f64);
        report = report.metric_if("heavy/light compute-unit ratio", Unit::Ratio, compute_ratio);
        Ok((report, result))
    }
}

struct Measurement {
    outcome: RunOutcome,
    signatures: SignatureOutcome,
    updates: UpdateOutcome,
    delta: MetricsDelta,
}

impl TransactionLoad {
    #[instrument(skip_all, err)]
    async fn warm_up(&self) -> Result<()> {
        let warmup = execute(
            RunConfig {
                until: None,
                iterations: self.profile.warmup,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            |id| {
                let sender = self.senders[(id as usize) % self.senders.len()].clone();
                sender.staged(self.instructions(id))
            },
        )
        .await?;
        ensure!(warmup.failed == 0);

        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn measure(
        &self,
        offset: u64,
        remaining: std::time::Duration,
        stop: Receiver<bool>,
    ) -> Result<Measurement> {
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
        let deadline = Instant::now()
            .checked_add(remaining)
            .context("duration exceeds monotonic clock range")?;

        let request = |iteration: u64| {
            let id = offset + iteration;
            let sender = self.senders[(id as usize) % self.senders.len()].clone();
            let instructions = self.instructions(id);
            let tracked_dest = self.targets(id)[0];
            updates.track(id, tracked_dest);
            let sigs = sigs.clone();
            let clock = clock.clone();
            async move {
                // sign → subscribe → deliver: the signature subscription
                // must exist before the tx can confirm
                let tx = sender.prepare(&instructions).await?;
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
            until: Some((deadline, stop)),
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

        let update_outcome = updates.finalize();
        ensure!(
            (update_outcome.observed + update_outcome.superseded) == (outcome.delivered as usize)
        );
        let sig_outcome = sigs.finalize();
        ensure!(sig_outcome.failed == 0);
        ensure!(sig_outcome.confirmed == (outcome.delivered as usize));

        Ok(Measurement {
            outcome,
            signatures: sig_outcome,
            updates: update_outcome,
            delta,
        })
    }

    #[instrument(skip_all, err)]
    async fn check_final_state(&self, offset: u64, count: u64) -> Result<()> {
        for pda in self.pdas.iter() {
            let last_id =
                (offset + 1..=offset + count).rev().find(|&id| self.targets(id).contains(pda));
            let Some(last_id) = last_id else {
                continue;
            };
            let on_er = self.er.account(pda).await?.context("pda not on er")?;
            ensure!(CellHeader::written_id(&on_er.data) == Some(last_id));
            if matches!(self.shape, Shape::Compute) {
                let expected = HashState::chain(causal::HASH_INIT.to_bytes(), 180);
                ensure!(
                    on_er.data[layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE]
                        == expected
                );
            }
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
            .setting("window capacity", profile.iterations)
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

const WRITE: Profile = Profile {
    name: "write",
    payers: 1,
    accounts: 4,
    warmup: 4,
    iterations: 40,
    rate: 20,
    concurrency: 1,
};
impl TransactionLoad {
    fn targets(&self, id: u64) -> [Pubkey; 2] {
        match self.shape {
            Shape::Write | Shape::Compute => [self.pdas[((id - 1) as usize) % self.pdas.len()]; 2],
            Shape::Copy => REDLINE.copy_three(&self.pdas, id).1,
        }
    }
    fn instructions(&self, id: u64) -> Vec<Instruction> {
        match self.shape {
            Shape::Write => vec![REDLINE.write(id, &self.targets(id)[..1])],
            Shape::Copy => vec![REDLINE.copy_three(&self.pdas, id).0],
            Shape::Compute => vec![
                causal::compute_unit_limit(causal::CU_LIMIT),
                REDLINE.hash(
                    id,
                    causal::HASH_INIT.to_bytes(),
                    180,
                    &self.targets(id)[..1],
                ),
            ],
        }
    }
}
const COMPUTE: Profile = Profile {
    name: "compute",
    payers: 256,
    accounts: 256,
    warmup: 256,
    iterations: 1024,
    rate: 2_000,
    concurrency: 256,
};
