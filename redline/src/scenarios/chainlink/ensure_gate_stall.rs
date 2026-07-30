use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures_util::future::join_all;
use pubkey::Pubkey;
use redsuite_core::{
    assert::poll_until,
    prep,
    profile::select as select_profile,
    report,
    runner::{drive, RunConfig},
    topology, BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport,
};

use crate::program::instruction::build;

const ACCOUNT_SPACE: u32 = 2048;
const READ_WIDTH: usize = 4;
const PAYER_LAMPORTS: u64 = 2_000_000_000;

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;

const STALL_REQUEST_TIMEOUT: Duration = Duration::from_secs(75);
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const PREWARM_CONCURRENCY: usize = 16;

const MONITORED_GAUGE: &str = "mbv_monitored_accounts_gauge";
const EVICTED_COUNTER: &str = "mbv_evicted_accounts_count";
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

fn profile(scenario: &str) -> &'static Profile {
    match select_profile(scenario, &["lite", "full"]) {
        "full" => &FULL,
        _ => &LITE,
    }
}

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

async fn prewarm(er: &ErCtx, pool: &[Pubkey]) -> Result<()> {
    for window in pool.chunks(PREWARM_CONCURRENCY) {
        let touches = window.iter().map(|pda| er.account(pda));
        let _ = join_all(touches).await;
    }
    for pda in pool {
        poll_until(CLONE_TIMEOUT, || async {
            matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == ACCOUNT_SPACE as usize)
        })
        .await;
    }
    Ok(())
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
    delivered: u64,
    failed: u64,
    first_error: Option<String>,
    p50_us: f64,
    p95_us: f64,
    max_us: f64,
    achieved_tps: f64,
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
        let profile = profile(self.name());
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
                            "MBV_CHAINLINK__MAX_MONITORED_ACCOUNTS".to_owned(),
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
                    ..Default::default()
                },
            )
            .await?;
            let cell_er = private.ctx();
            if cell.prewarm {
                prewarm(cell_er, &pool).await?;
            }
            let sender = cell_er.sender(payer.clone());

            let before = cell_er.scrape_metrics().await?;
            let request = {
                let pool = pool.clone();
                move |id: u64| {
                    let accounts = sample_accounts(&pool, id, READ_WIDTH);
                    let ix = build::read_accounts_data(id, &accounts);
                    let sender = sender.clone();
                    async move { sender.send(&[ix]).await.map(|_| ()) }
                }
            };
            let outcome = drive(
                RunConfig {
                    iterations: cell.iterations,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                request,
            )
            .await;
            let after = cell_er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);

            let cell_outcome = CellOutcome {
                name: cell.name,
                cap: cell.cap,
                delivered: outcome.delivered,
                failed: outcome.failed,
                first_error: outcome.first_error.clone(),
                p50_us: outcome.delivery.median as f64,
                p95_us: outcome.delivery.quantile95 as f64,
                max_us: outcome.delivery.max as f64,
                achieved_tps: outcome.achieved_rps(),
                ensure_avg_s: delta.histogram_avg(ENSURE_HISTOGRAM),
                // recorded for context only: the gauge refreshes on the
                // 60 s subscription reconciler, so short windows read stale
                monitored_end: delta.gauge(MONITORED_GAUGE).unwrap_or(0.0),
                evictions: delta.counter(EVICTED_COUNTER).unwrap_or(0.0),
            };
            eprintln!(
                "[redsuite] {}: {} (cap {}): {:.0} tx/s, p50 {:.0} us / p95 {:.0} us, \
                 {} delivered / {} failed, ensure avg {}, monitored {:.0}, evictions {:.0}",
                self.name(),
                cell_outcome.name,
                cell_outcome.cap,
                cell_outcome.achieved_tps,
                cell_outcome.p50_us,
                cell_outcome.p95_us,
                cell_outcome.delivered,
                cell_outcome.failed,
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
                    .observe("delivery us", outcome.delivery)
                    .metric("achieved tps", cell_outcome.achieved_tps)
                    .metric("delivered", cell_outcome.delivered as f64)
                    .metric("failed", cell_outcome.failed as f64)
                    .metric_if(
                        "validator ensure avg s",
                        cell_outcome.ensure_avg_s,
                    )
                    .metric(
                        "monitored accounts (end)",
                        cell_outcome.monitored_end,
                    )
                    .metric("evictions in window", cell_outcome.evictions);
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

        let healthy = &cells[0];
        let thrash = &cells[1];

        assert_eq!(
            healthy.failed, 0,
            "healthy cell requests failed: {:?}",
            healthy.first_error
        );
        if healthy.p50_us >= 1_000_000.0 {
            eprintln!(
                "[redsuite] {}: warning: healthy cell p50 {:.0} us left the \
                 sub-second range",
                self.name(),
                healthy.p50_us
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

        assert_eq!(
            healthy.evictions, 0.0,
            "healthy cell (cap ≥ working set) must not evict"
        );

        assert!(
            thrash.delivered > 0,
            "INVALID: the thrash cell delivered nothing"
        );
        assert!(
            thrash.evictions > 0.0,
            "INVALID: no evictions — the cap knob did not engage"
        );

        let slowdown = if healthy.p50_us > 0.0 {
            thrash.p50_us / healthy.p50_us
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
            .metric("thrash p50 slowdown x", slowdown);
        for cell in &cells {
            summary = summary
                .metric(
                    format!("{} achieved tps", cell.name),
                    cell.achieved_tps,
                )
                .metric(format!("{} p50 us", cell.name), cell.p50_us)
                .metric(format!("{} p95 us", cell.name), cell.p95_us)
                .metric(format!("{} max us", cell.name), cell.max_us)
                .metric(format!("{} failed", cell.name), cell.failed as f64)
                .metric(format!("{} evictions", cell.name), cell.evictions)
                .metric_if(
                    format!("{} ensure avg s", cell.name),
                    cell.ensure_avg_s,
                );
        }
        Ok(summary)
    }
}
