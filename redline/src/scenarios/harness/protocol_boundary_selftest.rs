use std::{rc::Rc, sync::Arc, time::Duration};

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
use tracing::{debug, warn};
use url::Url;

use crate::ACCOUNT_SPACE;

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
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

fn run_cell(
    er_rpc_url: Url,
    config: ThreadRunConfig,
    id_offset: u64,
    pool: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
) -> Result<RunOutcome> {
    execute_kernel(
        config,
        er_rpc_url,
        payer_bytes,
        0,
        move |id| REDLINE.copy_three(&pool, id_offset + id).0,
        None,
    )
}

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
        er.await_clones(&pool, ACCOUNT_SPACE as usize, CLONE_TIMEOUT).await?;
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
        let Self {
            base,
            er,
            profile,
            pool,
            payer_bytes,
        } = self;
        let er_rpc_url = er.api().url().clone();
        let warmup = run_cell(
            er_rpc_url.clone(),
            ThreadRunConfig {
                threads: profile.threads,
                iterations: profile.warmup,
                rate: Pacing::PerSecond(profile.rate),
                concurrency: profile.concurrency,
            },
            0,
            pool.clone(),
            payer_bytes.clone(),
        )?;
        ensure!(
            warmup.failed == 0,
            "failures={:?}; first error={:?}",
            warmup.failed,
            warmup.first_error
        );

        let mut id_offset = profile.warmup;
        let mut cells: Vec<CellOutcome> = Vec::new();
        for threads in [1, profile.threads] {
            let before = er.scrape_metrics().await?;
            let outcome = run_cell(
                er_rpc_url.clone(),
                ThreadRunConfig {
                    threads,
                    iterations: profile.iterations,
                    rate: Pacing::PerSecond(profile.rate),
                    concurrency: profile.concurrency,
                },
                id_offset,
                pool.clone(),
                payer_bytes.clone(),
            )?;
            let after = er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);
            id_offset += profile.iterations;

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
            ensure!(
                cell.outcome.failed == 0,
                "failures={:?}; first error={:?}",
                cell.outcome.failed,
                cell.outcome.first_error
            );

            let cell_report = ScenarioReport::ok()
                .setting("profile", profile.name)
                .setting("threads", threads)
                .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
                .setting("payers", profile.payers)
                .setting("accounts", profile.accounts)
                .setting("measured iters", profile.iterations)
                .setting("offered tps", profile.rate)
                .setting("concurrency", profile.concurrency)
                .observe("delivery us", Unit::Micros, cell.outcome.delivery)
                .client_side(&cell.outcome)
                .metric("achieved tps", Unit::Tps, cell.outcome.achieved_rps())
                .metric_if(
                    "validator tx processing avg us",
                    Unit::Micros,
                    cell.tx_processing_avg_us,
                );
            base.report_cell(&format!("threads{threads}"), &cell_report)?;
            cells.push(cell);
        }

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
        if profile.expect_artifact && !artifact_detected {
            warn!(
                profile.rate,
                p50_ratio, "expected client-thread latency gap absent; try a higher rate",
            );
        }
        if !profile.expect_artifact && artifact_detected {
            warn!(
                p50_ratio,
                "unexpected client-thread latency gap at baseline rate",
            );
        }

        Ok(ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("threads boundary", profile.threads)
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("measured iters per cell", profile.iterations)
            .setting("offered tps", profile.rate)
            .setting("concurrency", profile.concurrency)
            .setting("expect artifact", profile.expect_artifact)
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
            ))
    }
}
