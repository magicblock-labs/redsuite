use std::{rc::Rc, time::Duration};

use anyhow::ensure;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    host,
    profile::ProfileValues,
    programs::REDLINE,
    redline::Accounts,
    report::Unit,
    runner::{execute, Pacing, RunConfig, RunOutcome},
    topology::{self, ErOptions, PrivateEr},
    wait, BaseCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport, TxSender,
};
use tracing::{debug, instrument, warn};

use crate::ACCOUNT_SPACE;

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const FLATNESS_P50_FACTOR: f64 = 1.5;

const TX_PROCESSING_HISTOGRAM: &str = "mbv_transaction_processing_time";
const LEDGER_TRANSACTIONS_GAUGE: &str = "engine_ledger_transactions";
const LEDGER_FILL_TIMEOUT: Duration = Duration::from_secs(5);

const BLOCKTIME: Duration = Duration::from_millis(50);
const PROD_SUPERBLOCK_SLOTS: u64 = 72_000;

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: usize,
    fill: u64,
    window: u64,
    rate: u32,
    concurrency: usize,
    frequent_superblock_slots: u64,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    accounts: 16,
    fill: 2_000,
    window: 2_000,
    rate: 200,
    concurrency: 64,
    frequent_superblock_slots: 150,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    accounts: 64,
    fill: 60_000,
    window: 30_000,
    rate: 1_000,
    concurrency: 256,
    frequent_superblock_slots: 400,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

struct WindowOutcome {
    outcome: RunOutcome,
    tx_processing_avg_us: Option<f64>,
    storage_growth_bytes: u64,
}

struct CellOutcome {
    name: &'static str,
    superblock_slots: u64,
    fill_growth_bytes: u64,
    fill_ledger_txs: f64,
    windows: [WindowOutcome; 2],
}

pub struct StorageProdsizeSustain {
    base: Rc<BaseCtx>,
    profile: &'static Profile,
    pool: Vec<Pubkey>,
    payers: Vec<Rc<Keypair>>,
}

impl Scenario<ScenarioReport> for StorageProdsizeSustain {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers = base.funded_payers(profile.payers, PREP_PAYER_LAMPORTS).await?;
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base.as_ref(), &prep_payers, profile.accounts, true)
            .await?;
        let payers: Vec<Rc<Keypair>> = prep_payers.into_iter().map(Rc::new).collect();

        Ok(Self { base, profile, pool, payers })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let cells_spec = [
            ("prod_cadence", PROD_SUPERBLOCK_SLOTS),
            ("frequent_snapshot", self.profile.frequent_superblock_slots),
        ];

        let mut cells: Vec<CellOutcome> = Vec::new();
        for (cell_name, superblock_slots) in cells_spec {
            cells.push(self.measure_cell(cell_name, superblock_slots).await?);
        }

        self.check_cells(&cells)?;
        Ok(self.summarize(&cells))
    }
}

impl StorageProdsizeSustain {
    #[instrument(skip_all, err, fields(cell = cell_name))]
    async fn measure_cell(
        &self,
        cell_name: &'static str,
        superblock_slots: u64,
    ) -> Result<CellOutcome> {
        let private = topology::private_er(
            &self.base,
            ErOptions {
                label: format!("s11-{cell_name}"),
                settings: Box::new(move |params| {
                    params.engine.blockstore.blocktime = BLOCKTIME;
                    params.engine.blockstore.superblock = superblock_slots;
                }),
                ..Default::default()
            },
        )
        .await?;
        let cell_er = private.ctx();
        cell_er.await_clones(&self.pool, ACCOUNT_SPACE as usize).await?;
        let senders: Vec<TxSender> =
            self.payers.iter().map(|payer| cell_er.sender(payer.clone())).collect();

        let storage_at_boot = host::dir_size_bytes(private.storage_dir())?;
        let ledger_txs_at_boot =
            cell_er.scrape_metrics().await?.get(LEDGER_TRANSACTIONS_GAUGE).unwrap_or(0.0);
        let fill = execute(
            RunConfig {
                iterations: self.profile.fill,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            |id| {
                let sender = senders[(id as usize) % senders.len()].clone();
                let (ix, _) = REDLINE.copy_three(&self.pool, id);
                sender.staged([ix])
            },
        )
        .await?;
        ensure!(fill.failed == 0);
        wait::retry(
            &format!(
                "{cell_name}: the ledger records the {} fill transactions",
                self.profile.fill
            ),
            LEDGER_FILL_TIMEOUT,
            async || {
                let metrics = cell_er.scrape_metrics().await?;
                let filled =
                    metrics.get(LEDGER_TRANSACTIONS_GAUGE).unwrap_or(0.0) - ledger_txs_at_boot;
                ensure!(filled >= self.profile.fill as f64);
                Ok(())
            },
        )
        .await?;
        let fill_ledger_txs =
            cell_er.scrape_metrics().await?.get(LEDGER_TRANSACTIONS_GAUGE).unwrap_or(0.0)
                - ledger_txs_at_boot;
        let fill_growth_bytes =
            host::dir_size_bytes(private.storage_dir())?.saturating_sub(storage_at_boot);

        let window_a = self.execute_window(&private, &senders, self.profile.fill).await?;
        let window_b = self
            .execute_window(&private, &senders, self.profile.fill + self.profile.window)
            .await?;

        let cell = CellOutcome {
            name: cell_name,
            superblock_slots,
            fill_growth_bytes,
            fill_ledger_txs,
            windows: [window_a, window_b],
        };
        debug!(
            cell.name,
            cell.fill_ledger_txs, "storage sustain cell finished",
        );

        self.report_cell(&cell)?;
        drop(private);
        Ok(cell)
    }

    fn report_cell(&self, cell: &CellOutcome) -> Result<()> {
        let mut cell_report = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("superblock slots", cell.superblock_slots)
            .setting("block time", "50ms")
            .setting("payers", self.profile.payers)
            .setting("accounts", self.profile.accounts)
            .setting("fill iters", self.profile.fill)
            .setting("window iters", self.profile.window)
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .metric(
                "fill storage growth mb",
                Unit::Megabytes,
                cell.fill_growth_bytes as f64 / 1e6,
            )
            .metric("fill ledger txs", Unit::Count, cell.fill_ledger_txs);
        for (window_name, window) in [("A", &cell.windows[0]), ("B", &cell.windows[1])] {
            cell_report = cell_report
                .observe(
                    format!("window {window_name} delivery us"),
                    Unit::Micros,
                    window.outcome.delivery,
                )
                .client_side_as(
                    format!("window {window_name} scheduling delay us"),
                    &window.outcome,
                )
                .metric(
                    format!("window {window_name} achieved tps"),
                    Unit::Tps,
                    window.outcome.achieved_rps(),
                )
                .metric(
                    format!("window {window_name} storage growth mb"),
                    Unit::Megabytes,
                    window.storage_growth_bytes as f64 / 1e6,
                )
                .metric_if(
                    format!("window {window_name} validator tx avg us"),
                    Unit::Micros,
                    window.tx_processing_avg_us,
                );
        }
        self.base.report_cell(cell.name, &cell_report)
    }

    #[instrument(skip_all, err)]
    fn check_cells(&self, cells: &[CellOutcome]) -> Result<()> {
        for cell in cells {
            for window in &cell.windows {
                ensure!(window.outcome.failed == 0);
            }
            let window_a_p50_us = cell.windows[0].outcome.delivery.median.max(1) as f64;
            let window_b_p50_us = cell.windows[1].outcome.delivery.median as f64;
            if window_b_p50_us > window_a_p50_us * FLATNESS_P50_FACTOR {
                warn!(
                    cell.name,
                    window_a_p50_us, window_b_p50_us, "median latency drifted across equal windows",
                );
            }
        }

        Ok(())
    }

    fn summarize(&self, cells: &[CellOutcome]) -> ScenarioReport {
        let mut summary = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("block time", "50ms")
            .setting(
                "superblocks",
                format!(
                    "prod {} / frequent {}",
                    PROD_SUPERBLOCK_SLOTS, self.profile.frequent_superblock_slots
                ),
            )
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency);
        for cell in cells {
            summary = summary
                .with_flags(&cell.windows[0].outcome)
                .with_flags(&cell.windows[1].outcome)
                .metric(
                    format!("{} window A p50 us", cell.name),
                    Unit::Micros,
                    cell.windows[0].outcome.delivery.median as f64,
                )
                .metric(
                    format!("{} window B p50 us", cell.name),
                    Unit::Micros,
                    cell.windows[1].outcome.delivery.median as f64,
                )
                .metric(
                    format!("{} window p50 B/A ratio", cell.name),
                    Unit::Ratio,
                    cell.windows[1].outcome.delivery.median as f64
                        / cell.windows[0].outcome.delivery.median.max(1) as f64,
                )
                .metric(
                    format!("{} window B p95 us", cell.name),
                    Unit::Micros,
                    cell.windows[1].outcome.delivery.quantile95 as f64,
                )
                .metric(
                    format!("{} fill growth mb", cell.name),
                    Unit::Megabytes,
                    cell.fill_growth_bytes as f64 / 1e6,
                )
                .metric(
                    format!("{} fill ledger txs", cell.name),
                    Unit::Count,
                    cell.fill_ledger_txs,
                )
                .metric_if(
                    format!("{} window B validator tx avg us", cell.name),
                    Unit::Micros,
                    cell.windows[1].tx_processing_avg_us,
                );
        }
        summary
    }

    async fn execute_window(
        &self,
        private: &PrivateEr,
        senders: &[TxSender],
        first_id: u64,
    ) -> Result<WindowOutcome> {
        let er = private.ctx();
        let storage_dir = private.storage_dir();
        let storage_before = host::dir_size_bytes(storage_dir)?;
        let before = er.scrape_metrics().await?;
        let outcome = execute(
            RunConfig {
                iterations: self.profile.window,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            |iteration| {
                let id = first_id + iteration;
                let sender = senders[(id as usize) % senders.len()].clone();
                let (ix, _) = REDLINE.copy_three(&self.pool, id);
                sender.staged([ix])
            },
        )
        .await?;
        let after = er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);
        let storage_after = host::dir_size_bytes(storage_dir)?;
        Ok(WindowOutcome {
            outcome,
            tx_processing_avg_us: delta
                .histogram_avg(TX_PROCESSING_HISTOGRAM)
                .map(|seconds| seconds * 1e6),
            storage_growth_bytes: storage_after.saturating_sub(storage_before),
        })
    }
}
