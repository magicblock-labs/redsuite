use std::time::{Duration, Instant};

use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, prep,
    profile::ProfileValues,
    redline::Accounts,
    report,
    report::Unit,
    runner::{execute, Pacing, RunConfig, RunOutcome},
    topology, BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport,
};
use tracing::{debug, warn};

const ACCOUNT_SPACE: u32 = 2048;
const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);

const MONITORED_GAUGE: &str = "engine_keeper_account_cache_entries";
const EVICTED_COUNTER: &str = "engine_keeper_account_cache_evictions";
const FETCHES_FOUND_COUNTER: &str = "mbv_account_fetches_found_count";
const ENSURE_HISTOGRAM: &str = r#"mbv_ensure_accounts_time{kind="account"}"#;

const SETTLE_POLL: Duration = Duration::from_millis(500);
const RESUBSCRIBE: Duration = Duration::from_millis(50);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);

async fn settled_scrape(er: &ErCtx) -> Result<redsuite_core::Metrics> {
    let mut last = er.scrape_metrics().await?;
    let deadline = tokio::time::Instant::now() + SETTLE_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(SETTLE_POLL).await;
        let next = er.scrape_metrics().await?;
        let stable = next.get(EVICTED_COUNTER) == last.get(EVICTED_COUNTER)
            && next.value_sum(FETCHES_FOUND_COUNTER)
                == last.value_sum(FETCHES_FOUND_COUNTER);
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

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
};

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
    cap: usize,
    outcome: RunOutcome,
    evictions: f64,
    eviction_rate: f64,
    fetches_found: Option<f64>,
    ensure_avg_s: Option<f64>,
    monitored_end: f64,
}

pub struct CloneLruChurn;

#[async_trait(?Send)]
impl Scenario<ScenarioReport> for CloneLruChurn {
    fn name(&self) -> &str {
        "redline/clone_lru_churn"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers =
            prep::funded_payers(base, profile.prep_payers, PREP_PAYER_LAMPORTS)
                .await?;
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base, &prep_payers, profile.working_set, false)
            .await?;

        let mut cells_spec = vec![Cell {
            name: "closure".to_owned(),
            cap: profile.closure_cap,
            closure: true,
        }];
        for &cap in profile.ladder {
            cells_spec.push(Cell {
                name: format!("cap{cap}"),
                cap,
                closure: false,
            });
        }

        let mut cells: Vec<CellOutcome> = Vec::new();
        for cell in cells_spec {
            let private = topology::private_er(
                base,
                topology::ErOptions {
                    label: format!("s5-{}", cell.name),
                    settings: Box::new(move |params| {
                        params.engine.accountsdb.lru_capacity = cell.cap;
                        params.chainlink.resubscription_delay = RESUBSCRIBE;
                    }),
                    request_timeout: Some(REQUEST_TIMEOUT),
                    ..Default::default()
                },
            )
            .await?;
            let cell_er = private.ctx();
            if cell.closure {
                prep::prewarm(
                    cell_er,
                    &pool,
                    ACCOUNT_SPACE as usize,
                    CLONE_TIMEOUT,
                )
                .await?;
            } else {
                prep::touch(cell_er, &pool).await;
            }

            let request = |id: u64| {
                let account = pick_account(&pool, id);
                let api = cell_er.api().clone();
                std::future::ready(Ok(move |_: Instant| async move {
                    api.get_account(&account).await.map(|_| ())
                }))
            };

            let warmup = execute(
                RunConfig {
                    iterations: profile.warmup,
                    rate: Pacing::PerSecond(profile.rate),
                    concurrency: profile.concurrency,
                },
                request,
            )
            .await?;
            check_eq!(
                warmup.failed,
                0,
                "{}: warmup reads failed: {:?}",
                cell.name,
                warmup.first_error
            )?;

            let before = cell_er.scrape_metrics().await?;
            let offset = profile.warmup;
            let outcome = execute(
                RunConfig {
                    iterations: profile.iterations,
                    rate: Pacing::PerSecond(profile.rate),
                    concurrency: profile.concurrency,
                },
                |iteration| request(offset + iteration),
            )
            .await?;
            let after = settled_scrape(cell_er).await?;
            let delta = MetricsDelta::new(before, after);

            let evictions = delta.counter(EVICTED_COUNTER).unwrap_or(0.0);
            let cell_outcome = CellOutcome {
                name: cell.name.clone(),
                cap: cell.cap,
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

            let cell_report =
                ScenarioReport::ok(&format!("{}/{}", self.name(), cell.name))
                    .setting("profile", profile.name)
                    .setting("cap", cell.cap)
                    .setting("working set", profile.working_set)
                    .setting("account space", ACCOUNT_SPACE)
                    .setting("closure", cell.closure)
                    .setting("warmup reads", profile.warmup)
                    .setting("measured reads", profile.iterations)
                    .setting("offered rate /s", profile.rate)
                    .setting("concurrency", profile.concurrency)
                    .setting("request timeout s", REQUEST_TIMEOUT.as_secs())
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
                    .metric(
                        "failed",
                        Unit::Count,
                        cell_outcome.outcome.failed as f64,
                    )
                    .metric(
                        "evictions in window",
                        Unit::Count,
                        cell_outcome.evictions,
                    )
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
            report::persist_cell(self.name(), &cell_report);
            cells.push(cell_outcome);
            drop(private);
        }

        let closure = &cells[0];
        check_eq!(
            closure.outcome.failed,
            0,
            "closure cell reads failed: {:?}",
            closure.outcome.first_error
        )?;
        // The engine's account cache is a per-bucket sampled LRU (scc
        // HashCache), so a handful of early evictions below capacity are
        // normal; only systematic eviction under the cap marks a broken knob.
        let closure_tolerance = (profile.working_set as f64) * 0.05;
        check!(
            closure.evictions <= closure_tolerance,
            "INVALID: closure cell (cap {} ≥ working set {}) evicted {} \
             accounts — the cap knob or the harness is broken",
            closure.cap,
            profile.working_set,
            closure.evictions
        )?;
        if closure.outcome.delivery.median >= 1_000_000 {
            warn!(
                closure.outcome.delivery.median,
                "cache-fit cell median exceeds latency bound",
            );
        }

        for churn_cell in &cells[1..] {
            check!(
                churn_cell.outcome.delivered > 0,
                "INVALID: {} delivered nothing",
                churn_cell.name
            )?;
            check!(
                churn_cell.evictions > 0.0,
                "{}: no evictions with cap {} < working set {} — \
                 churn did not engage",
                churn_cell.name,
                churn_cell.cap,
                profile.working_set
            )?;
            if let Some(fetches_found) = churn_cell.fetches_found {
                let deviation = (fetches_found - churn_cell.evictions).abs();
                let tolerance = (churn_cell.evictions * 0.15)
                    .max(profile.concurrency as f64);
                check!(
                    deviation <= tolerance,
                    "{}: {} refetches vs {} evictions — evict/reload left \
                     lockstep",
                    churn_cell.name,
                    fetches_found,
                    churn_cell.evictions
                )?;
            }
        }

        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("working set", profile.working_set)
            .setting("account space", ACCOUNT_SPACE)
            .setting("closure cap", profile.closure_cap)
            .setting(
                "cap ladder",
                profile
                    .ladder
                    .iter()
                    .map(|cap| cap.to_string())
                    .collect::<Vec<_>>()
                    .join("/"),
            )
            .setting("offered rate /s", profile.rate)
            .setting("concurrency", profile.concurrency);
        for cell in &cells {
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
        Ok(summary)
    }
}
