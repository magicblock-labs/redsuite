use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::redline::Accounts;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, prep,
    profile::ProfileValues,
    report,
    runner::{execute, Pacing, RunConfig, RunOutcome},
    topology, BaseCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
};

use crate::program::instruction::build;

const ACCOUNT_SPACE: u32 = 2048;
const READ_WIDTH: usize = 4;
const PAYER_LAMPORTS: u64 = 2_000_000_000;

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;

const STALL_REQUEST_TIMEOUT: Duration = Duration::from_secs(75);
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);

const MONITORED_GAUGE: &str = "engine_keeper_account_cache_entries";
const EVICTED_COUNTER: &str = "engine_keeper_account_cache_evictions";
const ENSURE_HISTOGRAM: &str =
    r#"mbv_ensure_accounts_time{kind="transaction"}"#;

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

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
};

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
    cap: usize,
    outcome: RunOutcome,
    ensure_avg_s: Option<f64>,
    monitored_end: f64,
    evictions: f64,
}

pub struct EnsureGateStall;

#[async_trait(?Send)]
impl Scenario for EnsureGateStall {
    fn name(&self) -> &str {
        "redline/ensure_gate_stall"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers =
            prep::funded_payers(base, profile.prep_payers, PREP_PAYER_LAMPORTS)
                .await?;
        let prep_started = Instant::now();
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base, &prep_payers, profile.working_set, false)
            .await?;
        eprintln!(
            "[redsuite] {}: prepped {} non-delegated 2 KiB accounts in {:.1} s",
            self.name(),
            pool.len(),
            prep_started.elapsed().as_secs_f64(),
        );
        let payer = Rc::new(prep::funded_payer(base, PAYER_LAMPORTS).await?);

        let cells_spec = [
            Cell {
                name: "healthy",
                cap: profile.healthy_cap,
                iterations: profile.healthy_iterations,
                prewarm: true,
            },
            Cell {
                name: "thrash",
                cap: profile.thrash_cap,
                iterations: profile.thrash_iterations,
                prewarm: false,
            },
        ];

        let mut cells: Vec<CellOutcome> = Vec::new();
        for cell in cells_spec {
            let private = topology::private_er(
                base,
                topology::ErOptions {
                    label: format!("s6-{}", cell.name),
                    env: vec![
                        (
                            "MBV_ENGINE__ACCOUNTSDB__LRU_CAPACITY".to_owned(),
                            cell.cap.to_string(),
                        ),
                        // campaign parity: fast resubscription keeps the
                        // churn cadence high
                        (
                            "MBV_CHAINLINK__RESUBSCRIPTION_DELAY".to_owned(),
                            "50ms".to_owned(),
                        ),
                    ],
                    request_timeout: Some(STALL_REQUEST_TIMEOUT),
                    base_endpoints: None,
                },
            )
            .await?;
            let cell_er = private.ctx();
            if cell.prewarm {
                prep::prewarm(
                    cell_er,
                    &pool,
                    ACCOUNT_SPACE as usize,
                    CLONE_TIMEOUT,
                )
                .await?;
            }
            let sender = cell_er.sender(payer.clone());

            let before = cell_er.scrape_metrics().await?;
            let request = {
                let pool = pool.clone();
                move |id: u64| {
                    let accounts = sample_accounts(&pool, id, READ_WIDTH);
                    let ix = build::read_accounts_data(id, &accounts);
                    let sender = sender.clone();
                    async move { sender.submit(&[ix]).await.map(|_| ()) }
                }
            };
            let outcome = execute(
                RunConfig {
                    iterations: cell.iterations,
                    rate: Pacing::PerSecond(profile.rate),
                    concurrency: profile.concurrency,
                },
                request,
            )
            .await?;
            let after = cell_er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);

            let cell_outcome = CellOutcome {
                name: cell.name,
                cap: cell.cap,
                outcome,
                ensure_avg_s: delta.histogram_avg(ENSURE_HISTOGRAM),
                // recorded for context only: the gauge refreshes on the
                // 60 s subscription reconciler, so short windows read stale
                monitored_end: delta.gauge(MONITORED_GAUGE).unwrap_or(0.0),
                evictions: delta.counter(EVICTED_COUNTER).unwrap_or(0.0),
            };
            eprintln!(
                "[redsuite] {}: {} (cap {}): {:.0} tx/s, p50 {} us / p95 {} us, \
                 {} delivered / {} failed, ensure avg {}, monitored {:.0}, evictions {:.0}",
                self.name(),
                cell_outcome.name,
                cell_outcome.cap,
                cell_outcome.outcome.achieved_rps(),
                cell_outcome.outcome.delivery.median,
                cell_outcome.outcome.delivery.quantile95,
                cell_outcome.outcome.delivered,
                cell_outcome.outcome.failed,
                cell_outcome
                    .ensure_avg_s
                    .map(|seconds| format!("{seconds:.6} s"))
                    .unwrap_or_else(|| "n/a".to_owned()),
                cell_outcome.monitored_end,
                cell_outcome.evictions,
            );

            let cell_report =
                ScenarioReport::ok(&format!("{}/{}", self.name(), cell.name))
                    .setting("profile", profile.name)
                    .setting("cap", cell.cap)
                    .setting("working set", profile.working_set)
                    .setting("read width", READ_WIDTH)
                    .setting("account space", ACCOUNT_SPACE)
                    .setting("prewarmed", cell.prewarm)
                    .setting("iterations", cell.iterations)
                    .setting("offered rate /s", profile.rate)
                    .setting("concurrency", profile.concurrency)
                    .setting(
                        "request timeout s",
                        STALL_REQUEST_TIMEOUT.as_secs(),
                    )
                    .observe(
                        "delivery us",
                        Unit::Micros,
                        cell_outcome.outcome.delivery,
                    )
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
                    .metric(
                        "failed",
                        Unit::Count,
                        cell_outcome.outcome.failed as f64,
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
                    )
                    .metric(
                        "evictions in window",
                        Unit::Count,
                        cell_outcome.evictions,
                    );
            report::persist_cell(self.name(), &cell_report);
            cells.push(cell_outcome);
            drop(private);
        }

        let healthy = &cells[0];
        let thrash = &cells[1];

        check_eq!(
            healthy.outcome.failed,
            0,
            "healthy cell requests failed: {:?}",
            healthy.outcome.first_error
        )?;
        if healthy.outcome.delivery.median >= 1_000_000 {
            eprintln!(
                "[redsuite] {}: warning: healthy cell p50 {} us left the \
                 sub-second range",
                self.name(),
                healthy.outcome.delivery.median
            );
        }
        if let Some(ensure_avg) = healthy.ensure_avg_s {
            if ensure_avg >= 0.005 {
                eprintln!(
                    "[redsuite] {}: warning: healthy warm ensure avg \
                     {ensure_avg:.6} s left the µs–ms range",
                    self.name()
                );
            }
        }

        // The engine's account cache is a per-bucket sampled LRU, so a
        // fraction of the working set can evict below capacity; only
        // systematic eviction marks a broken knob.
        let healthy_tolerance = (profile.working_set as f64) * 0.15;
        check!(
            healthy.evictions <= healthy_tolerance,
            "healthy cell (cap ≥ working set) evicted {} accounts",
            healthy.evictions
        )?;

        check!(
            thrash.outcome.delivered > 0,
            "INVALID: the thrash cell delivered nothing"
        )?;
        check!(
            thrash.evictions > 0.0,
            "INVALID: no evictions — the cap knob did not engage"
        )?;

        let slowdown = if healthy.outcome.delivery.median > 0 {
            thrash.outcome.delivery.median as f64
                / healthy.outcome.delivery.median as f64
        } else {
            0.0
        };
        eprintln!(
            "[redsuite] {}: thrash p50 is {slowdown:.0}x the healthy p50",
            self.name()
        );

        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("working set", profile.working_set)
            .setting("read width", READ_WIDTH)
            .setting("account space", ACCOUNT_SPACE)
            .setting(
                "caps",
                format!(
                    "healthy {} / thrash {}",
                    profile.healthy_cap, profile.thrash_cap
                ),
            )
            .setting("offered rate /s", profile.rate)
            .setting("concurrency", profile.concurrency)
            .setting("request timeout s", STALL_REQUEST_TIMEOUT.as_secs())
            .metric("thrash p50 slowdown x", Unit::Ratio, slowdown);
        for cell in &cells {
            summary = summary
                .metric(
                    format!("{} achieved tps", cell.name),
                    Unit::Tps,
                    cell.outcome.achieved_rps(),
                )
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
        Ok(summary)
    }
}
