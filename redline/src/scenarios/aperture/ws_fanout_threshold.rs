use std::{collections::HashMap, rc::Rc, sync::Arc, time::Duration};

use anyhow::{anyhow, ensure};
use pubkey::Pubkey;
use redsuite_core::{
    prep,
    profile::ProfileValues,
    programs::{CellHeader, REDLINE},
    redline::{execute_kernel, Accounts},
    report::Unit,
    runner::{Pacing, RunOutcome, ThreadRunConfig},
    stats::{ObservationsStats, StreamingStats},
    transport::{
        clock::SendClock,
        subpool::{ExpectedWrites, SubscriberPool},
        NETWORK_TIMEOUT,
    },
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
};
use tracing::{debug, instrument, warn};

use crate::{metrics, ACCOUNT_SPACE};

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;

const LAG_THRESHOLD: Duration = Duration::from_secs(1);
const CLIFF_P95_US: i32 = 1_000_000;

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: usize,
    ladder: &'static [usize],
    subscriber_threads: usize,
    threads: usize,
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    accounts: 16,
    ladder: &[2, 4, 8],
    subscriber_threads: 4,
    threads: 2,
    warmup: 100,
    iterations: 600,
    rate: 200,
    concurrency: 64,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    accounts: 64,
    ladder: &[16, 32, 64],
    subscriber_threads: 16,
    threads: 8,
    warmup: 10_000,
    iterations: 100_000,
    rate: 10_000,
    concurrency: 2_048,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

struct CellOutcome {
    connections: usize,
    outcome: RunOutcome,
    missing_final: usize,
    incomplete: usize,
    received_min: u64,
    received_max: u64,
    lag: ObservationsStats,
    over_threshold: u64,
    received_total: u64,
}

pub struct WsFanoutThreshold {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    payer_bytes: Arc<Vec<[u8; 64]>>,
    pool: Arc<Vec<Pubkey>>,
}

impl Scenario<ScenarioReport> for WsFanoutThreshold {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers = base.funded_payers(profile.payers, PREP_PAYER_LAMPORTS).await?;
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base.as_ref(), &prep_payers, profile.accounts, true)
            .await?;
        er.await_clones(&pool, ACCOUNT_SPACE as usize).await?;
        let payer_bytes = prep::payer_bytes(&prep_payers);
        let pool: Arc<Vec<Pubkey>> = Arc::new(pool);

        Ok(Self {
            base,
            er,
            profile,
            payer_bytes,
            pool,
        })
    }

    async fn run(self) -> Result<ScenarioReport> {
        self.warm_up().await?;
        let mut id_cursor = self.profile.warmup;
        let mut cells: Vec<CellOutcome> = Vec::new();
        for &connections in self.profile.ladder {
            cells.push(self.measure_cell(connections, &mut id_cursor).await?);
        }

        self.summarize(&cells)
    }
}

impl WsFanoutThreshold {
    async fn execute(
        &self,
        iterations: u64,
        offset: u64,
        clock: Option<Arc<SendClock>>,
    ) -> Result<RunOutcome> {
        let pool = self.pool.clone();
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
            move |id| REDLINE.copy_three(&pool, id).0,
            clock,
        )
        .await
    }

    #[instrument(skip_all, err)]
    async fn warm_up(&self) -> Result<()> {
        let warmup = self.execute(self.profile.warmup, 0, None).await?;
        ensure!(warmup.failed == 0);

        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn measure_cell(&self, connections: usize, id_cursor: &mut u64) -> Result<CellOutcome> {
        let clock = Arc::new(SendClock::new(
            *id_cursor + 1,
            self.profile.iterations as usize,
        ));
        let expected = Arc::new(self.cell_expected_writes(*id_cursor));
        let mut subscribers = SubscriberPool::start(
            self.er.ws_url(),
            &self.pool,
            connections,
            self.profile.subscriber_threads,
            clock.clone(),
            expected.clone(),
            Arc::new(CellHeader::written_id),
            LAG_THRESHOLD,
        );
        subscribers.await_subscribed().await?;

        let before = self.er.scrape_metrics().await?;
        let outcome = self.execute(self.profile.iterations, *id_cursor, Some(clock)).await?;
        ensure!(outcome.failed == 0);

        subscribers.await_final().await;
        let after = self.er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);
        let conn_reports = subscribers.finalize().await?;
        if let Some(error) = conn_reports.iter().find_map(|conn| conn.error.as_ref()) {
            return Err(anyhow!(
                "ws{connections}: subscriber pool failed: {error:#}"
            ));
        }
        let missing_final = conn_reports.iter().map(|conn| conn.missing_final).sum();
        let incomplete = conn_reports.iter().map(|conn| conn.incomplete).sum();

        let received_min = conn_reports.iter().map(|conn| conn.received).min().unwrap_or(0);
        let received_max = conn_reports.iter().map(|conn| conn.received).max().unwrap_or(0);
        let received_total: u64 = conn_reports.iter().map(|conn| conn.received).sum();
        let over_threshold: u64 = conn_reports.iter().map(|conn| conn.over_threshold).sum();
        let mut lag_stats = StreamingStats::new();
        for conn_report in conn_reports {
            lag_stats.merge(conn_report.lag);
        }
        let lag = lag_stats.finalize(false);

        let cell_outcome = CellOutcome {
            connections,
            outcome,
            missing_final,
            incomplete,
            received_min,
            received_max,
            lag,
            over_threshold,
            received_total,
        };
        debug!(
            connections,
            missing_final, incomplete, "fan-out cell finished",
        );

        self.report_cell(&cell_outcome, &delta)?;
        if let Some(failed_txs) = delta.counter_all(metrics::FAILED_TRANSACTIONS) {
            ensure!(failed_txs == 0.0);
        }

        *id_cursor += self.profile.iterations;

        Ok(cell_outcome)
    }

    fn report_cell(&self, cell: &CellOutcome, delta: &MetricsDelta) -> Result<()> {
        let cell_report = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("ws connections", cell.connections)
            .setting("subscriber threads", self.profile.subscriber_threads)
            .setting("driver threads", self.profile.threads)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("payers", self.profile.payers)
            .setting("accounts", self.profile.accounts)
            .setting("measured iters", self.profile.iterations)
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .setting("drain timeout s", NETWORK_TIMEOUT.as_secs())
            .observe("delivery us", Unit::Micros, cell.outcome.delivery)
            .client_side(&cell.outcome)
            .observe("fanout lag us", Unit::Micros, cell.lag)
            .metric("achieved tps", Unit::Tps, cell.outcome.achieved_rps())
            .metric(
                "writes produced",
                Unit::Count,
                (self.profile.iterations * 2) as f64,
            )
            .metric("received total", Unit::Count, cell.received_total as f64)
            .metric(
                "received per-conn min",
                Unit::Count,
                cell.received_min as f64,
            )
            .metric(
                "received per-conn max",
                Unit::Count,
                cell.received_max as f64,
            )
            .metric(
                "received per-conn spread",
                Unit::Count,
                (cell.received_max - cell.received_min) as f64,
            )
            .metric(
                "missing final states",
                Unit::Count,
                cell.missing_final as f64,
            )
            .metric(
                "incomplete conn-account pairs",
                Unit::Count,
                cell.incomplete as f64,
            )
            .metric(
                "notifications over 1s",
                Unit::Count,
                cell.over_threshold as f64,
            )
            .metric_if(
                "validator txs in window",
                Unit::Count,
                delta.counter(metrics::ENGINE_TRANSACTIONS),
            )
            .metric_if(
                "validator tx processing avg us",
                Unit::Micros,
                delta
                    .histogram_avg("mbv_transaction_processing_time")
                    .map(|seconds| seconds * 1e6),
            );
        self.base.report_cell(&format!("ws{}", cell.connections), &cell_report)
    }

    fn summarize(&self, cells: &[CellOutcome]) -> Result<ScenarioReport> {
        let baseline = &cells[0];
        if baseline.lag.quantile95 >= CLIFF_P95_US {
            warn!(
                baseline.connections,
                baseline.lag.quantile95, "baseline already exceeds fan-out cliff threshold",
            );
        }
        if baseline.missing_final > 0 {
            warn!(
                baseline.connections,
                baseline.missing_final, "baseline subscriptions missed final state",
            );
        }
        if baseline.received_min != baseline.received_max {
            warn!(
                baseline.connections,
                baseline.received_min,
                baseline.received_max,
                "baseline connections received unequal notification counts",
            );
        }

        for cell in cells {
            ensure!(cell.outcome.delivered > 0);
        }

        let cliff = cells
            .iter()
            .find(|cell| {
                cell.missing_final > 0
                    || cell.received_min != cell.received_max
                    || cell.lag.quantile95 >= CLIFF_P95_US
            })
            .map(|cell| cell.connections)
            .unwrap_or(0);

        let mut summary = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting(
                "ws ladder",
                self.profile
                    .ladder
                    .iter()
                    .map(|connections| connections.to_string())
                    .collect::<Vec<_>>()
                    .join("/"),
            )
            .setting("subscriber threads", self.profile.subscriber_threads)
            .setting("driver threads", self.profile.threads)
            .setting("payers", self.profile.payers)
            .setting("accounts", self.profile.accounts)
            .setting("measured iters per cell", self.profile.iterations)
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .metric("cliff at ws conns (0 = none)", Unit::Count, cliff as f64)
            .metric(
                "baseline missing finals",
                Unit::Count,
                baseline.missing_final as f64,
            )
            .metric(
                "baseline received spread",
                Unit::Count,
                (baseline.received_max - baseline.received_min) as f64,
            );
        for cell in cells {
            let cell_name = format!("ws{}", cell.connections);
            summary = summary
                .with_flags(&cell.outcome)
                .metric(
                    format!("{cell_name} lag p50 us"),
                    Unit::Micros,
                    cell.lag.median as f64,
                )
                .metric(
                    format!("{cell_name} lag p95 us"),
                    Unit::Micros,
                    cell.lag.quantile95 as f64,
                )
                .metric(
                    format!("{cell_name} lag max us"),
                    Unit::Micros,
                    cell.lag.max as f64,
                )
                .metric(
                    format!("{cell_name} achieved tps"),
                    Unit::Tps,
                    cell.outcome.achieved_rps(),
                )
                .metric(
                    format!("{cell_name} received spread"),
                    Unit::Count,
                    (cell.received_max - cell.received_min) as f64,
                )
                .metric(
                    format!("{cell_name} missing finals"),
                    Unit::Count,
                    cell.missing_final as f64,
                )
                .metric(
                    format!("{cell_name} incomplete pairs"),
                    Unit::Count,
                    cell.incomplete as f64,
                )
                .metric(
                    format!("{cell_name} over 1s"),
                    Unit::Count,
                    cell.over_threshold as f64,
                )
                .metric(
                    format!("{cell_name} received total"),
                    Unit::Count,
                    cell.received_total as f64,
                )
                .metric(
                    format!("{cell_name} failed"),
                    Unit::Count,
                    cell.outcome.failed as f64,
                );
        }
        Ok(summary)
    }

    fn cell_expected_writes(&self, first_id: u64) -> ExpectedWrites {
        let mut expected: ExpectedWrites = HashMap::new();
        for id in first_id + 1..=first_id + self.profile.iterations {
            let (_, dests) = REDLINE.copy_three(&self.pool, id);
            for dest in dests {
                expected.entry(dest).or_default().push(id);
            }
        }
        expected
    }
}
