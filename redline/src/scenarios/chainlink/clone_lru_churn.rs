use std::{
    future,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::ensure;
use pubkey::Pubkey;
use redsuite_core::{
    profile::ProfileValues,
    redline::Accounts,
    report::Unit,
    runner::{execute, Pacing, RunConfig, RunOutcome},
    topology,
    topology::ErOptions,
    transport::NETWORK_TIMEOUT,
    BaseCtx, ChainCtx, ErCtx, Metrics, MetricsDelta, Result, Scenario, ScenarioReport,
};
use tokio::time;
use tracing::{debug, instrument, warn};

const ACCOUNT_SPACE: u32 = 2048;
const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;

const MONITORED_GAUGE: &str = "engine_keeper_account_cache_entries";
const EVICTED_COUNTER: &str = "engine_keeper_account_cache_evictions";
const FETCHES_FOUND_COUNTER: &str = "mbv_account_fetches_found_count";
const ENSURE_HISTOGRAM: &str = r#"mbv_ensure_accounts_time{kind="account"}"#;

const SETTLE_POLL: Duration = Duration::from_millis(500);
const RESUBSCRIBE: Duration = Duration::from_millis(50);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);

async fn settled_scrape(er: &ErCtx) -> Result<Metrics> {
    let mut last = er.scrape_metrics().await?;
    let deadline = time::Instant::now() + SETTLE_TIMEOUT;
    while time::Instant::now() < deadline {
        time::sleep(SETTLE_POLL).await;
        let next = er.scrape_metrics().await?;
        let stable = next.get(EVICTED_COUNTER) == last.get(EVICTED_COUNTER)
            && next.value_sum(FETCHES_FOUND_COUNTER) == last.value_sum(FETCHES_FOUND_COUNTER);
        last = next;
        if stable {
            break;
        }
    }
    Ok(last)
}

struct Profile {
    name: &'static str,
    working_set: usize,
    prep_payers: usize,
    closure_cap: usize,
    ladder: &'static [usize],
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

// The engine's account cache rounds the configured capacity up to a power
// of two with a floor of 256 (keeper HashCache::with_capacity(256, cap)), so
// a ladder rung only churns when that EFFECTIVE capacity stays under the
// working set: lite [360, 200] -> [512, 256] under 600, full
// [900, 500, 125] -> [1024, 512, 256] under 1200.
const LITE: Profile = Profile {
    name: "lite",
    working_set: 600,
    prep_payers: 6,
    closure_cap: 800,
    ladder: &[360, 200],
    warmup: 300,
    iterations: 800,
    rate: 200,
    concurrency: 64,
};

const FULL: Profile = Profile {
    name: "full",
    working_set: 1_200,
    prep_payers: 12,
    closure_cap: 1_500,
    ladder: &[900, 500, 125],
    warmup: 1_000,
    iterations: 3_000,
    rate: 200,
    concurrency: 64,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

fn pick_account(pool: &[Pubkey], seed: u64) -> Pubkey {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    pool[(state % pool.len() as u64) as usize]
}

struct Cell {
    name: String,
    cap: usize,
    closure: bool,
}

struct CellOutcome {
    name: String,
    outcome: RunOutcome,
    evictions: f64,
    eviction_rate: f64,
    fetches_found: Option<f64>,
    ensure_avg_s: Option<f64>,
    monitored_end: f64,
}

pub struct CloneLruChurn {
    base: Rc<BaseCtx>,
    profile: &'static Profile,
    pool: Vec<Pubkey>,
}

impl Scenario<ScenarioReport> for CloneLruChurn {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers = base.funded_payers(profile.prep_payers, PREP_PAYER_LAMPORTS).await?;
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base.as_ref(), &prep_payers, profile.working_set, false)
            .await?;

        Ok(Self { base, profile, pool })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let mut cells_spec = vec![Cell {
            name: "closure".to_owned(),
            cap: self.profile.closure_cap,
            closure: true,
        }];
        for &cap in self.profile.ladder {
            cells_spec.push(Cell {
                name: format!("cap{cap}"),
                cap,
                closure: false,
            });
        }

        let mut cells: Vec<CellOutcome> = Vec::new();
        for cell in cells_spec {
            cells.push(self.measure_cell(cell).await?);
        }

        self.check_cells(&cells)?;
        Ok(self.summarize(&cells))
    }
}

impl CloneLruChurn {
    #[instrument(skip_all, err, fields(cell = %cell.name))]
    async fn measure_cell(&self, cell: Cell) -> Result<CellOutcome> {
        let private = topology::private_er(
            &self.base,
            ErOptions {
                label: format!("s5-{}", cell.name),
                settings: Box::new(move |params| {
                    params.engine.accountsdb.lru_capacity = cell.cap;
                    params.chainlink.resubscription_delay = RESUBSCRIBE;
                }),

                ..Default::default()
            },
        )
        .await?;
        let cell_er = private.ctx();
        if cell.closure {
            cell_er.prewarm(&self.pool, ACCOUNT_SPACE as usize).await?;
        } else {
            cell_er.touch(&self.pool).await;
        }

        let request = |id: u64| {
            let account = pick_account(&self.pool, id);
            let api = cell_er.api().clone();
            future::ready(Ok(move |_: Instant| async move {
                api.get_account(&account).await.map(|_| ())
            }))
        };

        let warmup = execute(
            RunConfig {
                iterations: self.profile.warmup,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            request,
        )
        .await?;
        ensure!(warmup.failed == 0);

        let before = cell_er.scrape_metrics().await?;
        let offset = self.profile.warmup;
        let outcome = execute(
            RunConfig {
                iterations: self.profile.iterations,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            |iteration| request(offset + iteration),
        )
        .await?;
        let after = settled_scrape(cell_er).await?;
        let delta = MetricsDelta::new(before, after);

        let evictions = delta.counter(EVICTED_COUNTER).unwrap_or(0.0);
        let cell_outcome = CellOutcome {
            name: cell.name.clone(),
            evictions,
            eviction_rate: evictions / outcome.wall.as_secs_f64(),
            fetches_found: delta.counter_all(FETCHES_FOUND_COUNTER),
            ensure_avg_s: delta.histogram_avg(ENSURE_HISTOGRAM),
            monitored_end: delta.gauge(MONITORED_GAUGE).unwrap_or(0.0),
            outcome,
        };
        debug!(
            %cell_outcome.name,
            cell_outcome.outcome.failed,
            cell_outcome.evictions,
            "cache churn cell finished",
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
            .setting("account space", ACCOUNT_SPACE)
            .setting("closure", cell.closure)
            .setting("warmup reads", self.profile.warmup)
            .setting("measured reads", self.profile.iterations)
            .setting("offered rate /s", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .setting("request timeout s", NETWORK_TIMEOUT.as_secs())
            .observe(
                "read latency us",
                Unit::Micros,
                cell_outcome.outcome.delivery,
            )
            .client_side(&cell_outcome.outcome)
            .metric(
                "achieved reads /s",
                Unit::PerSecond,
                cell_outcome.outcome.achieved_rps(),
            )
            .metric(
                "delivered",
                Unit::Count,
                cell_outcome.outcome.delivered as f64,
            )
            .metric("failed", Unit::Count, cell_outcome.outcome.failed as f64)
            .metric("evictions in window", Unit::Count, cell_outcome.evictions)
            .metric(
                "eviction rate /s",
                Unit::PerSecond,
                cell_outcome.eviction_rate,
            )
            .metric_if(
                "fetches found in window",
                Unit::Count,
                cell_outcome.fetches_found,
            )
            .metric_if(
                "validator ensure avg s",
                Unit::Seconds,
                cell_outcome.ensure_avg_s,
            )
            .metric(
                "monitored accounts (end)",
                Unit::Count,
                cell_outcome.monitored_end,
            );
        self.base.report_cell(&cell.name, &cell_report)
    }

    #[instrument(skip_all, err)]
    fn check_cells(&self, cells: &[CellOutcome]) -> Result<()> {
        let closure = &cells[0];
        ensure!(closure.outcome.failed == 0);
        // The engine's account cache is a per-bucket sampled LRU (scc
        // HashCache), so a handful of early evictions below capacity are
        // normal; only systematic eviction under the cap marks a broken knob.
        let closure_tolerance = (self.profile.working_set as f64) * 0.05;
        ensure!(closure.evictions <= closure_tolerance);
        if closure.outcome.delivery.median >= 1_000_000 {
            warn!(
                closure.outcome.delivery.median,
                "cache-fit cell median exceeds latency bound",
            );
        }

        for churn_cell in &cells[1..] {
            ensure!(churn_cell.outcome.delivered > 0);
            ensure!(churn_cell.evictions > 0.0);
            if let Some(fetches_found) = churn_cell.fetches_found {
                let deviation = (fetches_found - churn_cell.evictions).abs();
                let tolerance = (churn_cell.evictions * 0.15).max(self.profile.concurrency as f64);
                ensure!(deviation <= tolerance);
            }
        }

        Ok(())
    }

    fn summarize(&self, cells: &[CellOutcome]) -> ScenarioReport {
        let mut summary = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("working set", self.profile.working_set)
            .setting("account space", ACCOUNT_SPACE)
            .setting("closure cap", self.profile.closure_cap)
            .setting(
                "cap ladder",
                self.profile
                    .ladder
                    .iter()
                    .map(|cap| cap.to_string())
                    .collect::<Vec<_>>()
                    .join("/"),
            )
            .setting("offered rate /s", self.profile.rate)
            .setting("concurrency", self.profile.concurrency);
        for cell in cells {
            summary = summary
                .metric(
                    format!("{} eviction rate /s", cell.name),
                    Unit::PerSecond,
                    cell.eviction_rate,
                )
                .metric(
                    format!("{} evictions", cell.name),
                    Unit::Count,
                    cell.evictions,
                )
                .metric(
                    format!("{} achieved reads /s", cell.name),
                    Unit::PerSecond,
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
                    format!("{} failed", cell.name),
                    Unit::Count,
                    cell.outcome.failed as f64,
                )
                .metric_if(
                    format!("{} fetches found", cell.name),
                    Unit::Count,
                    cell.fetches_found,
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
