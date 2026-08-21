use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::future::join_all;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, prep,
    profile::{self, ProfileValues},
    report,
    runner::{execute, RunConfig},
    topology, BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport,
};

const ACCOUNT_SPACE: u32 = 2048;
const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const SWEEP_CONCURRENCY: usize = 16;

const MONITORED_GAUGE: &str = "mbv_monitored_accounts_gauge";
const EVICTED_COUNTER: &str = "mbv_evicted_accounts_count";
const FETCHES_FOUND_COUNTER: &str = "mbv_account_fetches_found_count";
const ENSURE_HISTOGRAM: &str = r#"mbv_ensure_accounts_time{kind="account"}"#;

const SETTLE_POLL: Duration = Duration::from_millis(500);
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
    ladder: [usize; 3],
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    working_set: 400,
    prep_payers: 4,
    closure_cap: 800,
    ladder: [360, 200, 50],
    warmup: 300,
    iterations: 800,
    rate: 200,
    concurrency: 64,
};

const FULL: Profile = Profile {
    name: "full",
    working_set: 1_000,
    prep_payers: 10,
    closure_cap: 1_500,
    ladder: [900, 500, 125],
    warmup: 1_000,
    iterations: 3_000,
    rate: 200,
    concurrency: 64,
};

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
    soak: None,
    deep: None,
};

fn pick_account(pool: &[Pubkey], seed: u64) -> Pubkey {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    pool[(state % pool.len() as u64) as usize]
}

async fn sweep(er: &ErCtx, pool: &[Pubkey]) {
    for window in pool.chunks(SWEEP_CONCURRENCY) {
        let touches = window.iter().map(|account| er.account(account));
        let _ = join_all(touches).await;
    }
}

async fn prewarm_resident(er: &ErCtx, pool: &[Pubkey]) -> Result<()> {
    sweep(er, pool).await;
    for account in pool {
        check::poll(
            &format!("the ER clones the pool account {account}"),
            CLONE_TIMEOUT,
            || async {
                matches!(er.account(account).await, Ok(Some(acc)) if acc.data.len() == ACCOUNT_SPACE as usize)
            },
        )
        .await?;
    }
    Ok(())
}

struct Cell {
    name: String,
    cap: usize,
    closure: bool,
}

struct CellOutcome {
    name: String,
    cap: usize,
    delivered: u64,
    failed: u64,
    first_error: Option<String>,
    p50_us: f64,
    p95_us: f64,
    achieved_rps: f64,
    evictions: f64,
    eviction_rate: f64,
    fetches_found: Option<f64>,
    ensure_avg_s: Option<f64>,
    monitored_end: f64,
}

pub struct CloneLruChurn;

#[async_trait(?Send)]
impl Scenario for CloneLruChurn {
    fn name(&self) -> &str {
        "redline/clone_lru_churn"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);
        let prep_payers =
            prep::funded_payers(base, profile.prep_payers, PREP_PAYER_LAMPORTS)
                .await?;
        let prep_started = Instant::now();
        let pool = crate::init_accounts_batched(
            base,
            &prep_payers,
            profile.working_set,
            ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        eprintln!(
            "[redsuite] {}: prepped {} non-delegated 2 KiB accounts in {:.1} s",
            self.name(),
            pool.len(),
            prep_started.elapsed().as_secs_f64(),
        );

        let mut cells_spec = vec![Cell {
            name: "closure".to_owned(),
            cap: profile.closure_cap,
            closure: true,
        }];
        for cap in profile.ladder {
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
                    env: vec![
                        (
                            "MBV_CHAINLINK__MAX_MONITORED_ACCOUNTS".to_owned(),
                            cell.cap.to_string(),
                        ),
                        (
                            "MBV_CHAINLINK__RESUBSCRIPTION_DELAY".to_owned(),
                            "50ms".to_owned(),
                        ),
                    ],
                    request_timeout: Some(REQUEST_TIMEOUT),
                    ..Default::default()
                },
            )
            .await?;
            let cell_er = private.ctx();
            let sweep_started = Instant::now();
            if cell.closure {
                prewarm_resident(cell_er, &pool).await?;
            } else {
                sweep(cell_er, &pool).await;
            }
            eprintln!(
                "[redsuite] {}: {} swept {} accounts in {:.1} s",
                self.name(),
                cell.name,
                pool.len(),
                sweep_started.elapsed().as_secs_f64(),
            );

            let request = {
                let pool = pool.clone();
                let api = cell_er.api().clone();
                move |id: u64| {
                    let account = pick_account(&pool, id);
                    let api = api.clone();
                    async move { api.get_account(&account).await.map(|_| ()) }
                }
            };

            let warmup = execute(
                RunConfig {
                    iterations: profile.warmup,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                request.clone(),
            )
            .await;
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
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                |iteration| request(offset + iteration),
            )
            .await;
            let after = settled_scrape(cell_er).await?;
            let delta = MetricsDelta::new(before, after);

            let evictions = delta.counter(EVICTED_COUNTER).unwrap_or(0.0);
            let cell_outcome = CellOutcome {
                name: cell.name.clone(),
                cap: cell.cap,
                delivered: outcome.delivered,
                failed: outcome.failed,
                first_error: outcome.first_error.clone(),
                p50_us: outcome.delivery.median as f64,
                p95_us: outcome.delivery.quantile95 as f64,
                achieved_rps: outcome.achieved_rps(),
                evictions,
                eviction_rate: evictions / outcome.wall.as_secs_f64(),
                fetches_found: delta.counter_all(FETCHES_FOUND_COUNTER),
                ensure_avg_s: delta.histogram_avg(ENSURE_HISTOGRAM),
                monitored_end: delta.gauge(MONITORED_GAUGE).unwrap_or(0.0),
            };
            eprintln!(
                "[redsuite] {}: {} (cap {}): {:.0} reads/s, p50 {:.0} us / p95 {:.0} us, \
                 {} delivered / {} failed, evictions {:.0} ({:.1}/s), fetches found {}, ensure avg {}, monitored {:.0}",
                self.name(),
                cell_outcome.name,
                cell_outcome.cap,
                cell_outcome.achieved_rps,
                cell_outcome.p50_us,
                cell_outcome.p95_us,
                cell_outcome.delivered,
                cell_outcome.failed,
                cell_outcome.evictions,
                cell_outcome.eviction_rate,
                cell_outcome
                    .fetches_found
                    .map(|count| format!("{count:.0}"))
                    .unwrap_or_else(|| "n/a".to_owned()),
                cell_outcome
                    .ensure_avg_s
                    .map(|seconds| format!("{seconds:.6} s"))
                    .unwrap_or_else(|| "n/a".to_owned()),
                cell_outcome.monitored_end,
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
                    .observe("read latency us", outcome.delivery)
                    .metric("achieved reads /s", cell_outcome.achieved_rps)
                    .metric("delivered", cell_outcome.delivered as f64)
                    .metric("failed", cell_outcome.failed as f64)
                    .metric("evictions in window", cell_outcome.evictions)
                    .metric("eviction rate /s", cell_outcome.eviction_rate)
                    .metric_if(
                        "fetches found in window",
                        cell_outcome.fetches_found,
                    )
                    .metric_if(
                        "validator ensure avg s",
                        cell_outcome.ensure_avg_s,
                    )
                    .metric(
                        "monitored accounts (end)",
                        cell_outcome.monitored_end,
                    );
            match report::persist(&cell_report) {
                Ok(path) => {
                    eprintln!("[redsuite]   cell report: {}", path.display())
                }
                Err(e) => eprintln!(
                    "[redsuite]   warning: cell report not persisted: {e}"
                ),
            }
            cells.push(cell_outcome);
            drop(private);
        }

        let closure = &cells[0];
        check_eq!(
            closure.failed,
            0,
            "closure cell reads failed: {:?}",
            closure.first_error
        )?;
        check_eq!(
            closure.evictions,
            0.0,
            "INVALID: closure cell (cap {} ≥ working set {}) evicted — \
             the cap knob or the harness is broken",
            closure.cap,
            profile.working_set
        )?;
        if closure.p50_us >= 1_000_000.0 {
            eprintln!(
                "[redsuite] {}: warning: closure cell p50 {:.0} us left the \
                 sub-second range",
                self.name(),
                closure.p50_us
            );
        }

        for churn_cell in &cells[1..] {
            check!(
                churn_cell.delivered > 0,
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
                    cell.eviction_rate,
                )
                .metric(format!("{} evictions", cell.name), cell.evictions)
                .metric(
                    format!("{} achieved reads /s", cell.name),
                    cell.achieved_rps,
                )
                .metric(format!("{} p50 us", cell.name), cell.p50_us)
                .metric(format!("{} p95 us", cell.name), cell.p95_us)
                .metric(format!("{} failed", cell.name), cell.failed as f64)
                .metric_if(
                    format!("{} fetches found", cell.name),
                    cell.fetches_found,
                )
                .metric_if(
                    format!("{} ensure avg s", cell.name),
                    cell.ensure_avg_s,
                );
        }
        Ok(summary)
    }
}
