use std::{rc::Rc, sync::Arc, time::Instant};

use anyhow::Context;
use pubkey::Pubkey;
use redsuite_core::{
    check,
    config::ProfileValues,
    prep,
    programs::REDLINE,
    redline::{execute_kernel, Accounts},
    report::Unit,
    runner::{Pacing, RunOutcome, ThreadRunConfig},
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
};
use tracing::{debug, instrument};

use crate::ACCOUNT_SPACE;

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;

const TX_PROCESSING_HISTOGRAM: &str = "mbv_transaction_processing_time";

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: usize,
    threads: usize,
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 16,
    accounts: 16,
    threads: 4,
    warmup: 200,
    iterations: 2_000,
    rate: 400,
    concurrency: 64,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 64,
    accounts: 64,
    threads: 8,
    warmup: 2_000,
    iterations: 25_000,
    rate: 2_500,
    concurrency: 512,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

struct CellOutcome {
    threads: usize,
    outcome: RunOutcome,
    tx_processing_avg_us: Option<f64>,
}

/// Measures one versus multiple driver threads without requiring a particular latency gap.
pub struct LoadGeneratorThreadScaling {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    pool: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
}

impl Scenario<(ScenarioReport, Result<()>)> for LoadGeneratorThreadScaling {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers = base.funded_payers(profile.payers, PREP_PAYER_LAMPORTS).await?;
        let pool = Arc::new(
            Accounts::new(ACCOUNT_SPACE, er.identity())
                .init_batched(base.as_ref(), &prep_payers, profile.accounts, true)
                .await?,
        );
        er.await_clones(&pool, ACCOUNT_SPACE as usize).await?;
        let payer_bytes = prep::payer_bytes(&prep_payers);

        Ok(Self {
            base,
            er,
            profile,
            pool,
            payer_bytes,
        })
    }

    async fn run(self) -> Result<(ScenarioReport, Result<()>)> {
        self.warm_up().await?;
        let options = crate::RunControl::from_config(&self.base.config())?;
        let mut cursor = self.profile.warmup;
        let mut cells = Vec::new();
        let result: Result<()> = async {
            for threads in [1, self.profile.threads] {
                let mut remaining = options.duration / 2;
                let mut last = None;
                while !remaining.is_zero() && !*options.stop.borrow() {
                    let deadline = Instant::now()
                        .checked_add(remaining)
                        .context("duration exceeds monotonic clock range")?;
                    let cell = self
                        .measure_cell(threads, &mut cursor, deadline, options.stop.clone())
                        .await?;
                    if cell.outcome.delivered == 0 {
                        break;
                    }
                    remaining = remaining.saturating_sub(cell.outcome.admission_wall);
                    tracing::info!(
                        threads,
                        delivered = cell.outcome.delivered,
                        p95_us = cell.outcome.delivery.quantile95,
                        "driver diagnostic window"
                    );
                    last = Some(cell);
                }
                check!(last.is_some(), "diagnostic phase not observed");
                cells.extend(last);
            }
            Ok(())
        }
        .await;
        let report = if cells.len() == 2 {
            self.summarize(&cells).setting("scope", "last window per phase")
        } else {
            ScenarioReport::ok()
        };
        Ok((
            report.setting("completion", options.completion(&result)),
            result,
        ))
    }
}

impl LoadGeneratorThreadScaling {
    async fn execute(
        &self,
        threads: usize,
        iterations: u64,
        offset: u64,
        until: Option<(Instant, tokio::sync::watch::Receiver<bool>)>,
    ) -> Result<RunOutcome> {
        let pool = self.pool.clone();
        execute_kernel(
            ThreadRunConfig {
                until,
                threads,
                iterations,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            self.er.api().url().clone(),
            self.er.ws_url().clone(),
            self.payer_bytes.clone(),
            0,
            move |id| REDLINE.copy_three(&pool, offset + id).0,
            None,
        )
        .await
    }

    #[instrument(skip_all, err)]
    async fn warm_up(&self) -> Result<()> {
        let warmup = self.execute(self.profile.threads, self.profile.warmup, 0, None).await?;
        warmup.check_failures();

        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn measure_cell(
        &self,
        threads: usize,
        id_offset: &mut u64,
        deadline: Instant,
        stop: tokio::sync::watch::Receiver<bool>,
    ) -> Result<CellOutcome> {
        let before = self.er.scrape_metrics().await?;
        let outcome = self
            .execute(
                threads,
                self.profile.iterations,
                *id_offset,
                Some((deadline, stop)),
            )
            .await?;
        let after = self.er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);
        *id_offset += self.profile.iterations;

        let cell = CellOutcome {
            threads,
            outcome,
            tx_processing_avg_us: delta
                .histogram_avg(TX_PROCESSING_HISTOGRAM)
                .map(|seconds| seconds * 1e6),
        };
        debug!(
            cell.threads,
            cell.outcome.delivery.median,
            cell.tx_processing_avg_us,
            "protocol boundary cell finished",
        );
        cell.outcome.check_failures();

        Ok(cell)
    }

    fn summarize(&self, cells: &[CellOutcome]) -> ScenarioReport {
        let single = &cells[0];
        let multi = &cells[1];

        let p50_ratio = single.outcome.delivery.median.max(1) as f64
            / multi.outcome.delivery.median.max(1) as f64;
        ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("threads boundary", self.profile.threads)
            .setting("payers", self.profile.payers)
            .setting("accounts", self.profile.accounts)
            .setting("window capacity per phase", self.profile.iterations)
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .with_flags(&single.outcome)
            .with_flags(&multi.outcome)
            .metric(
                "single p50 us",
                Unit::Micros,
                single.outcome.delivery.median as f64,
            )
            .metric(
                "single p95 us",
                Unit::Micros,
                single.outcome.delivery.quantile95 as f64,
            )
            .metric(
                "single achieved tps",
                Unit::Tps,
                single.outcome.achieved_rps(),
            )
            .metric(
                "multi p50 us",
                Unit::Micros,
                multi.outcome.delivery.median as f64,
            )
            .metric(
                "multi p95 us",
                Unit::Micros,
                multi.outcome.delivery.quantile95 as f64,
            )
            .metric(
                "multi achieved tps",
                Unit::Tps,
                multi.outcome.achieved_rps(),
            )
            .metric("p50 ratio", Unit::Ratio, p50_ratio)
            .metric_if(
                "single validator tx avg us",
                Unit::Micros,
                single.tx_processing_avg_us,
            )
            .metric_if(
                "multi validator tx avg us",
                Unit::Micros,
                multi.tx_processing_avg_us,
            )
    }
}
