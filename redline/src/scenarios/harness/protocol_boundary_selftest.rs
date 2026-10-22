use std::{rc::Rc, sync::Arc};

use anyhow::ensure;
use pubkey::Pubkey;
use redsuite_core::{
    prep,
    profile::ProfileValues,
    programs::REDLINE,
    redline::{execute_kernel, Accounts},
    report::Unit,
    runner::{Pacing, RunOutcome, ThreadRunConfig},
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
};
use tracing::{debug, instrument, warn};

use crate::ACCOUNT_SPACE;

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const ARTIFACT_P50_RATIO: f64 = 5.0;
const VALIDATOR_AGREEMENT_RATIO: f64 = 3.0;

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
    expect_artifact: bool,
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
    expect_artifact: false,
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
    expect_artifact: true,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

struct CellOutcome {
    threads: usize,
    outcome: RunOutcome,
    tx_processing_avg_us: Option<f64>,
}

pub struct ProtocolBoundarySelftest {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    pool: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
}

impl Scenario<ScenarioReport> for ProtocolBoundarySelftest {
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

    async fn run(self) -> Result<ScenarioReport> {
        self.warm_up()?;
        let mut id_offset = self.profile.warmup;
        let mut cells: Vec<CellOutcome> = Vec::new();
        for threads in [1, self.profile.threads] {
            cells.push(self.measure_cell(threads, &mut id_offset).await?);
        }

        Ok(self.summarize(&cells))
    }
}

impl ProtocolBoundarySelftest {
    fn execute(&self, threads: usize, iterations: u64, offset: u64) -> Result<RunOutcome> {
        let pool = self.pool.clone();
        execute_kernel(
            ThreadRunConfig {
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
    }

    #[instrument(skip_all, err)]
    fn warm_up(&self) -> Result<()> {
        let warmup = self.execute(self.profile.threads, self.profile.warmup, 0)?;
        ensure!(warmup.failed == 0);

        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn measure_cell(&self, threads: usize, id_offset: &mut u64) -> Result<CellOutcome> {
        let before = self.er.scrape_metrics().await?;
        let outcome = self.execute(threads, self.profile.iterations, *id_offset)?;
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
        ensure!(cell.outcome.failed == 0);

        self.report_cell(&cell)?;

        Ok(cell)
    }

    fn report_cell(&self, cell: &CellOutcome) -> Result<()> {
        let cell_report = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("threads", cell.threads)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("payers", self.profile.payers)
            .setting("accounts", self.profile.accounts)
            .setting("measured iters", self.profile.iterations)
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .observe("delivery us", Unit::Micros, cell.outcome.delivery)
            .client_side(&cell.outcome)
            .metric("achieved tps", Unit::Tps, cell.outcome.achieved_rps())
            .metric_if(
                "validator tx processing avg us",
                Unit::Micros,
                cell.tx_processing_avg_us,
            );
        self.base.report_cell(&format!("threads{}", cell.threads), &cell_report)
    }

    fn summarize(&self, cells: &[CellOutcome]) -> ScenarioReport {
        let single = &cells[0];
        let multi = &cells[1];

        if let (Some(single_validator_us), Some(multi_validator_us)) =
            (single.tx_processing_avg_us, multi.tx_processing_avg_us)
        {
            let validator_ratio = (single_validator_us / multi_validator_us)
                .max(multi_validator_us / single_validator_us);
            if validator_ratio > VALIDATOR_AGREEMENT_RATIO {
                warn!(
                    single_validator_us,
                    multi_validator_us,
                    "validator processing latency differs; client comparison is confounded",
                );
            }
        }

        let p50_ratio = single.outcome.delivery.median.max(1) as f64
            / multi.outcome.delivery.median.max(1) as f64;
        let artifact_detected = p50_ratio >= ARTIFACT_P50_RATIO;
        debug!(
            p50_ratio,
            artifact_detected, "client boundary comparison finished",
        );
        if self.profile.expect_artifact && !artifact_detected {
            warn!(
                self.profile.rate,
                p50_ratio, "expected client-thread latency gap absent; try a higher rate",
            );
        }
        if !self.profile.expect_artifact && artifact_detected {
            warn!(
                p50_ratio,
                "unexpected client-thread latency gap at baseline rate",
            );
        }

        ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("threads boundary", self.profile.threads)
            .setting("payers", self.profile.payers)
            .setting("accounts", self.profile.accounts)
            .setting("measured iters per cell", self.profile.iterations)
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .setting("expect artifact", self.profile.expect_artifact)
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
            .metric(
                "artifact detected",
                Unit::Count,
                if artifact_detected { 1.0 } else { 0.0 },
            )
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
