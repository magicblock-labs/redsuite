use std::{rc::Rc, time::Duration};

use anyhow::ensure;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    profile::ProfileValues,
    programs::REDLINE,
    redline::Accounts,
    report::Unit,
    runner::{execute, Pacing, RunConfig, RunOutcome},
    topology::{self, ErOptions},
    transport::NETWORK_TIMEOUT,
    BaseCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
};
use tracing::{debug, instrument, warn};

const ACCOUNT_SPACE: u32 = 2048;
const READ_WIDTH: usize = 4;
const PAYER_LAMPORTS: u64 = 2_000_000_000;

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;

const RESUBSCRIBE: Duration = Duration::from_millis(50);

const MONITORED_GAUGE: &str = "engine_keeper_account_cache_entries";
const EVICTED_COUNTER: &str = "engine_keeper_account_cache_evictions";
const ENSURE_HISTOGRAM: &str = r#"mbv_ensure_accounts_time{kind="transaction"}"#;

struct Profile {
    name: &'static str,
    // non-delegated accounts — they stay monitored after cloning
    working_set: usize,
    prep_payers: usize,
    healthy_cap: usize,
    thrash_cap: usize,
    healthy_iterations: u64,
    thrash_iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    working_set: 600,
    prep_payers: 6,
    healthy_cap: 2_000,
    thrash_cap: 100,
    healthy_iterations: 1_500,
    thrash_iterations: 128,
    rate: 200,
    concurrency: 64,
};

const FULL: Profile = Profile {
    name: "full",
    working_set: 1_000,
    prep_payers: 10,
    healthy_cap: 3_000,
    thrash_cap: 125,
    healthy_iterations: 4_000,
    thrash_iterations: 256,
    rate: 200,
    concurrency: 64,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

fn sample_accounts(pool: &[Pubkey], seed: u64, width: usize) -> Vec<Pubkey> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut chosen: Vec<usize> = Vec::with_capacity(width);
    while chosen.len() < width {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let index = (state % pool.len() as u64) as usize;
        if !chosen.contains(&index) {
            chosen.push(index);
        }
    }
    chosen.into_iter().map(|index| pool[index]).collect()
}

struct Cell {
    name: &'static str,
    cap: usize,
    iterations: u64,
    prewarm: bool,
}

struct CellOutcome {
    name: &'static str,
    outcome: RunOutcome,
    ensure_avg_s: Option<f64>,
    monitored_end: f64,
    evictions: f64,
}

pub struct EnsureGateStall {
    base: Rc<BaseCtx>,
    profile: &'static Profile,
    pool: Vec<Pubkey>,
    payer: Rc<Keypair>,
}

impl Scenario<ScenarioReport> for EnsureGateStall {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers = base.funded_payers(profile.prep_payers, PREP_PAYER_LAMPORTS).await?;
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base.as_ref(), &prep_payers, profile.working_set, false)
            .await?;
        let payer = Rc::new(base.funded_payer(PAYER_LAMPORTS).await?);

        Ok(Self { base, profile, pool, payer })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let cells_spec = [
            Cell {
                name: "healthy",
                cap: self.profile.healthy_cap,
                iterations: self.profile.healthy_iterations,
                prewarm: true,
            },
            Cell {
                name: "thrash",
                cap: self.profile.thrash_cap,
                iterations: self.profile.thrash_iterations,
                prewarm: false,
            },
        ];

        let mut cells: Vec<CellOutcome> = Vec::new();
        for cell in cells_spec {
            cells.push(self.measure_cell(cell).await?);
        }

        self.check_cells(&cells)?;
        Ok(self.summarize(&cells))
    }
}

impl EnsureGateStall {
    #[instrument(skip_all, err, fields(cell = %cell.name))]
    async fn measure_cell(&self, cell: Cell) -> Result<CellOutcome> {
        let private = topology::private_er(
            &self.base,
            ErOptions {
                label: format!("s6-{}", cell.name),
                settings: Box::new(move |params| {
                    params.engine.accountsdb.lru_capacity = cell.cap;
                    // campaign parity: fast resubscription keeps the
                    // churn cadence high
                    params.chainlink.resubscription_delay = RESUBSCRIBE;
                }),

                ..Default::default()
            },
        )
        .await?;
        let cell_er = private.ctx();
        if cell.prewarm {
            cell_er.prewarm(&self.pool, ACCOUNT_SPACE as usize).await?;
        }
        let sender = cell_er.sender(self.payer.clone());

        let before = cell_er.scrape_metrics().await?;
        let request = |id: u64| {
            let accounts = sample_accounts(&self.pool, id, READ_WIDTH);
            let ix = REDLINE.inspect(&accounts);
            let sender = sender.clone();
            sender.staged([ix])
        };
        let outcome = execute(
            RunConfig {
                iterations: cell.iterations,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            request,
        )
        .await?;
        let after = cell_er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);

        let cell_outcome = CellOutcome {
            name: cell.name,
            outcome,
            ensure_avg_s: delta.histogram_avg(ENSURE_HISTOGRAM),
            // recorded for context only: the gauge refreshes on the
            // 60 s subscription reconciler, so short windows read stale
            monitored_end: delta.gauge(MONITORED_GAUGE).unwrap_or(0.0),
            evictions: delta.counter(EVICTED_COUNTER).unwrap_or(0.0),
        };
        debug!(
            cell_outcome.name,
            cell_outcome.outcome.failed, cell_outcome.evictions, "ensure-gate cell finished",
        );

        self.report_cell(&cell, &cell_outcome)?;
        drop(private);
        Ok(cell_outcome)
    }

    fn report_cell(&self, cell: &Cell, cell_outcome: &CellOutcome) -> Result<()> {
        let cell_report = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("cap", cell.cap)
            .setting("working set", self.profile.working_set)
            .setting("read width", READ_WIDTH)
            .setting("account space", ACCOUNT_SPACE)
            .setting("prewarmed", cell.prewarm)
            .setting("iterations", cell.iterations)
            .setting("offered rate /s", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .setting("request timeout s", NETWORK_TIMEOUT.as_secs())
            .observe("delivery us", Unit::Micros, cell_outcome.outcome.delivery)
            .client_side(&cell_outcome.outcome)
            .metric(
                "achieved tps",
                Unit::Tps,
                cell_outcome.outcome.achieved_rps(),
            )
            .metric(
                "delivered",
                Unit::Count,
                cell_outcome.outcome.delivered as f64,
            )
            .metric("failed", Unit::Count, cell_outcome.outcome.failed as f64)
            .metric_if(
                "validator ensure avg s",
                Unit::Seconds,
                cell_outcome.ensure_avg_s,
            )
            .metric(
                "monitored accounts (end)",
                Unit::Count,
                cell_outcome.monitored_end,
            )
            .metric("evictions in window", Unit::Count, cell_outcome.evictions);
        self.base.report_cell(cell.name, &cell_report)
    }

    #[instrument(skip_all, err)]
    fn check_cells(&self, cells: &[CellOutcome]) -> Result<()> {
        let healthy = &cells[0];
        let thrash = &cells[1];

        ensure!(healthy.outcome.failed == 0);
        if healthy.outcome.delivery.median >= 1_000_000 {
            warn!(
                healthy.outcome.delivery.median,
                "healthy cell median exceeds latency bound",
            );
        }
        if let Some(ensure_avg_s) = healthy.ensure_avg_s {
            if ensure_avg_s >= 0.005 {
                warn!(
                    ensure_avg_s,
                    "healthy cell mean ensure latency exceeds bound",
                );
            }
        }

        // The engine's account cache is a per-bucket sampled LRU, so a
        // fraction of the working set can evict below capacity; only
        // systematic eviction marks a broken knob.
        let healthy_tolerance = (self.profile.working_set as f64) * 0.15;
        ensure!(healthy.evictions <= healthy_tolerance);

        ensure!(thrash.outcome.delivered > 0);
        ensure!(thrash.evictions > 0.0);

        Ok(())
    }

    fn summarize(&self, cells: &[CellOutcome]) -> ScenarioReport {
        let healthy = &cells[0];
        let thrash = &cells[1];
        let slowdown = if healthy.outcome.delivery.median > 0 {
            thrash.outcome.delivery.median as f64 / healthy.outcome.delivery.median as f64
        } else {
            0.0
        };

        let mut summary = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("working set", self.profile.working_set)
            .setting("read width", READ_WIDTH)
            .setting("account space", ACCOUNT_SPACE)
            .setting(
                "caps",
                format!(
                    "healthy {} / thrash {}",
                    self.profile.healthy_cap, self.profile.thrash_cap
                ),
            )
            .setting("offered rate /s", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .setting("request timeout s", NETWORK_TIMEOUT.as_secs())
            .metric("thrash p50 slowdown x", Unit::Ratio, slowdown);
        for cell in cells {
            summary = summary
                .metric(
                    format!("{} achieved tps", cell.name),
                    Unit::Tps,
                    cell.outcome.achieved_rps(),
                )
                .with_flags(&cell.outcome)
                .metric(
                    format!("{} p50 us", cell.name),
                    Unit::Micros,
                    cell.outcome.delivery.median as f64,
                )
                .metric(
                    format!("{} p95 us", cell.name),
                    Unit::Micros,
                    cell.outcome.delivery.quantile95 as f64,
                )
                .metric(
                    format!("{} max us", cell.name),
                    Unit::Micros,
                    cell.outcome.delivery.max as f64,
                )
                .metric(
                    format!("{} failed", cell.name),
                    Unit::Count,
                    cell.outcome.failed as f64,
                )
                .metric(
                    format!("{} evictions", cell.name),
                    Unit::Count,
                    cell.evictions,
                )
                .metric_if(
                    format!("{} ensure avg s", cell.name),
                    Unit::Seconds,
                    cell.ensure_avg_s,
                );
        }
        summary
    }
}
