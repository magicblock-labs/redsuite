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
use tracing::{debug, instrument, warn};

use crate::{metrics, ACCOUNT_SPACE};

const PAYER_LAMPORTS: u64 = 2_000_000_000;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(300);
const TX_COUNT: &str = metrics::ENGINE_TRANSACTIONS;
// healthy cells must stay within this factor of the widest cell's p50
const FLAT_FACTOR: f64 = 3.0;
// a cell beyond this factor is the cliff
const CLIFF_FACTOR: f64 = 5.0;
// campaign's healthy client p50 bound, host-calibrated slack
const HEALTHY_P50_US: f64 = 15_000.0;

struct Profile {
    name: &'static str,
    payers: usize,
    // Hot write-set sizes; widest first as the low-contention baseline.
    // narrowest last so a wedged cell cannot poison the others
    cells: &'static [u8],
    threads: usize,
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    cells: &[16, 4],
    threads: 4,
    warmup: 1_000,
    iterations: 10_000,
    rate: 2_000,
    concurrency: 512,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    cells: &[32, 16, 8, 4],
    threads: 8,
    warmup: 10_000,
    iterations: 100_000,
    rate: 14_000,
    concurrency: 2_048,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

struct CellResult {
    hot: u8,
    outcome: RunOutcome,
}

pub struct HotAccountCliff {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    payer_bytes: Arc<Vec<[u8; 64]>>,
    pdas: Vec<Pubkey>,
}

impl Scenario<ScenarioReport> for HotAccountCliff {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let pool = *profile.cells.iter().max().unwrap();
        let payers = base.funded_payers(profile.payers, PAYER_LAMPORTS).await?;
        let pdas = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_delegated(base.as_ref(), &payers[0], pool)
            .await?;
        er.await_clones(&pdas, ACCOUNT_SPACE as usize).await?;
        let payer_bytes = prep::payer_bytes(&payers);

        Ok(Self {
            base,
            er,
            profile,
            payer_bytes,
            pdas,
        })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let mut offset = 0u64;
        let mut cells: Vec<CellResult> = Vec::new();
        for &hot in self.profile.cells {
            cells.push(self.measure_cell(hot, &mut offset).await?);
        }

        Ok(self.summarize(&cells))
    }
}

impl HotAccountCliff {
    fn execute(
        &self,
        hot_set: Arc<Vec<Pubkey>>,
        iterations: u64,
        offset: u64,
    ) -> Result<RunOutcome> {
        execute_kernel(
            ThreadRunConfig {
                threads: self.profile.threads,
                iterations,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            self.er.api().url().clone(),
            self.er.ws_url().clone(),
            self.payer_bytes.clone(),
            offset,
            // Hot-set sizes are powers of two, coprime with the copy stride.
            move |id| REDLINE.copy_three(&hot_set, id).0,
            None,
        )
    }

    #[instrument(skip_all, err, fields(hot = hot))]
    async fn measure_cell(&self, hot: u8, offset: &mut u64) -> Result<CellResult> {
        let hot_set: Arc<Vec<Pubkey>> = Arc::new(self.pdas[..hot as usize].to_vec());

        let count_before_warmup = self.er.scrape_metrics().await?.get(TX_COUNT);
        let warm = self.execute(hot_set.clone(), self.profile.warmup, *offset)?;
        ensure!(warm.failed == 0);
        *offset += self.profile.warmup;
        if let Some(seen) = count_before_warmup {
            crate::await_executed(&self.er, seen + self.profile.warmup as f64, DRAIN_TIMEOUT)
                .await?;
        }

        let before = self.er.scrape_metrics().await?;
        let outcome = self.execute(hot_set, self.profile.iterations, *offset)?;
        *offset += self.profile.iterations;
        ensure!(outcome.failed == 0);
        if let Some(seen) = before.get(TX_COUNT) {
            crate::await_executed(
                &self.er,
                seen + self.profile.iterations as f64,
                DRAIN_TIMEOUT,
            )
            .await?;
        }
        let after = self.er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);
        if let Some(failed) = delta.counter_all(metrics::FAILED_TRANSACTIONS) {
            ensure!(failed == 0.0);
        }

        let cell = CellResult { hot, outcome };
        debug!(
            hot,
            delivered_tps = cell.outcome.achieved_rps(),
            "hot account cell finished",
        );

        let cell_report = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("hot accounts", hot)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("driver threads", self.profile.threads)
            .setting("measured iters", self.profile.iterations)
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .observe("delivery us", Unit::Micros, cell.outcome.delivery)
            .client_side(&cell.outcome)
            .metric("achieved tps", Unit::Tps, cell.outcome.achieved_rps())
            .metric_if(
                "validator tx processing avg us",
                Unit::Micros,
                delta
                    .histogram_avg("mbv_transaction_processing_time")
                    .map(|seconds| seconds * 1e6),
            )
            .metric_if(
                "max lock contention queue",
                Unit::Count,
                delta.gauge("mbv_max_lock_contention_queue_size"),
            )
            .metric_if(
                "validator txs in window",
                Unit::Count,
                delta.counter(metrics::ENGINE_TRANSACTIONS),
            );
        self.base.report_cell(&format!("hot{hot}"), &cell_report)?;
        Ok(cell)
    }

    fn summarize(&self, cells: &[CellResult]) -> ScenarioReport {
        // High baseline latency confounds comparisons of contention effects.
        // Validator processing latency helps separate execution from client delay.
        let p50 = |cell: &CellResult| cell.outcome.delivery.median as f64;
        let baseline_p50_us = p50(&cells[0]);
        if baseline_p50_us > HEALTHY_P50_US {
            warn!(
                hot = cells[0].hot,
                baseline_p50_us,
                self.profile.rate,
                "baseline median exceeds latency bound; sweep may be load-limited",
            );
        }
        for cell in cells.iter().filter(|cell| cell.hot >= 16) {
            let bound_us = (baseline_p50_us * FLAT_FACTOR).max(HEALTHY_P50_US);
            if p50(cell) > bound_us {
                warn!(
                    cell.hot,
                    p50_us = p50(cell),
                    bound_us,
                    baseline_hot = cells[0].hot,
                    baseline_p50_us,
                    "hot account latency exceeded healthy bound",
                );
            }
        }
        // cliff position = the widest hot-set whose p50 blows past the
        // baseline.
        let cliff = cells
            .iter()
            .find(|cell| p50(cell) > baseline_p50_us * CLIFF_FACTOR)
            .map(|cell| cell.hot);

        let cell_names: Vec<String> =
            self.profile.cells.iter().map(|hot| hot.to_string()).collect();
        let mut summary = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("cells", cell_names.join("/"))
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("driver threads", self.profile.threads)
            .setting("measured iters per cell", self.profile.iterations)
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .setting(
                "cliff",
                cliff
                    .map(|hot| format!("hot-set {hot}"))
                    .unwrap_or_else(|| format!("none at {} tps", self.profile.rate)),
            );
        for cell in cells {
            summary = summary
                .with_flags(&cell.outcome)
                .metric(
                    format!("hot{} delivery p50 us", cell.hot),
                    Unit::Micros,
                    p50(cell),
                )
                .metric(
                    format!("hot{} delivery p95 us", cell.hot),
                    Unit::Micros,
                    cell.outcome.delivery.quantile95 as f64,
                )
                .metric(
                    format!("hot{} achieved tps", cell.hot),
                    Unit::Tps,
                    cell.outcome.achieved_rps(),
                );
        }
        summary
    }
}
