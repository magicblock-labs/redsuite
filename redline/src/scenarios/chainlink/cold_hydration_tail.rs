use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    prep, report,
    runner::{drive, RunConfig},
    stats::{ObservationsStats, StreamingStats},
    topology, BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport, TxSender,
};

const ACCOUNT_SPACE: u32 = 2048;
const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const PAYER_LAMPORTS: u64 = 2_000_000_000;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const SLOW_THRESHOLD: Duration = Duration::from_millis(100);
const MIN_COLD_WARM_RATIO: f64 = 10.0;

const ENSURE_ACCOUNT_HISTOGRAM: &str =
    r#"mbv_ensure_accounts_time{kind="account"}"#;
const ENSURE_TX_HISTOGRAM: &str =
    r#"mbv_ensure_accounts_time{kind="transaction"}"#;

struct Profile {
    name: &'static str,
    cold_accounts: usize,
    burst_accounts: usize,
    burst_payers: usize,
    burst_concurrency: usize,
    prep_payers: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    cold_accounts: 32,
    burst_accounts: 128,
    burst_payers: 8,
    burst_concurrency: 128,
    prep_payers: 4,
};

const FULL: Profile = Profile {
    name: "full",
    cold_accounts: 64,
    burst_accounts: 512,
    burst_payers: 16,
    burst_concurrency: 256,
    prep_payers: 8,
};

fn profile() -> &'static Profile {
    match std::env::var("REDSUITE_PROFILE") {
        Ok(name) if name == "full" => &FULL,
        Ok(name) if name == "lite" => &LITE,
        Ok(name) => panic!("unknown REDSUITE_PROFILE `{name}` (lite|full)"),
        Err(_) => &LITE,
    }
}

async fn touch_pass(er: &ErCtx, pool: &[Pubkey]) -> Result<ObservationsStats> {
    let mut latencies = StreamingStats::new();
    for account in pool {
        let started = Instant::now();
        let fetched = er.account(account).await?;
        latencies.push(started.elapsed().as_micros() as u32);
        let data_len = fetched.map(|account| account.data.len()).unwrap_or(0);
        if data_len != ACCOUNT_SPACE as usize {
            return Err(format!(
                "hydration broken: {account} read back {data_len} bytes"
            )
            .into());
        }
    }
    Ok(latencies.finalize(false))
}

struct BurstOutcome {
    delivered: u64,
    failed: u64,
    first_error: Option<String>,
    delivery: ObservationsStats,
    slow: u64,
    wall_s: f64,
    ensure_avg_s: Option<f64>,
}

async fn burst_cell(
    base: &BaseCtx,
    label: &str,
    pool: &[Pubkey],
    payers: &[Rc<Keypair>],
    concurrency: usize,
    prewarm_deps: bool,
    prewarm_account: Pubkey,
) -> Result<BurstOutcome> {
    let private = topology::private_er(
        base,
        topology::ErOptions {
            label: label.to_owned(),
            env: Vec::new(),
            request_timeout: Some(REQUEST_TIMEOUT),
        },
    )
    .await?;
    let cell_er = private.ctx();
    let senders: Vec<TxSender> = payers
        .iter()
        .map(|payer| cell_er.sender(payer.clone()))
        .collect();

    if prewarm_deps {
        for sender in &senders {
            let warm_ix =
                crate::program::instruction::build::read_accounts_data(
                    0,
                    &[prewarm_account],
                );
            sender.send(&[warm_ix]).await?;
        }
    }

    let before = cell_er.scrape_metrics().await?;
    let slow = Rc::new(Cell::new(0u64));
    let request = {
        let slow = slow.clone();
        let senders = senders.clone();
        let pool = pool.to_vec();
        move |id: u64| {
            let sender = senders[(id as usize) % senders.len()].clone();
            let account = pool[(id as usize - 1) % pool.len()];
            let ix = crate::program::instruction::build::read_accounts_data(
                id,
                &[account],
            );
            let slow = slow.clone();
            async move {
                let started = Instant::now();
                let delivery = sender.send(&[ix]).await.map(|_| ());
                if started.elapsed() > SLOW_THRESHOLD {
                    slow.set(slow.get() + 1);
                }
                delivery
            }
        }
    };
    let started = Instant::now();
    let outcome = drive(
        RunConfig {
            iterations: pool.len() as u64,
            rate: pool.len() as u32,
            concurrency,
        },
        request,
    )
    .await;
    let wall_s = started.elapsed().as_secs_f64();
    let after = cell_er.scrape_metrics().await?;
    let delta = MetricsDelta::new(before, after);

    Ok(BurstOutcome {
        delivered: outcome.delivered,
        failed: outcome.failed,
        first_error: outcome.first_error,
        delivery: outcome.delivery,
        slow: slow.get(),
        wall_s,
        ensure_avg_s: delta.histogram_avg(ENSURE_TX_HISTOGRAM),
    })
}

pub struct ColdHydrationTail;

#[async_trait(?Send)]
impl Scenario for ColdHydrationTail {
    fn name(&self) -> &str {
        "redline/cold_hydration_tail"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = profile();
        let prep_payers =
            prep::funded_payers(base, profile.prep_payers, PREP_PAYER_LAMPORTS)
                .await?;
        let all_accounts = crate::init_accounts_batched(
            base,
            &prep_payers,
            profile.cold_accounts + profile.burst_accounts + 1,
            ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        let cold_pool = &all_accounts[..profile.cold_accounts];
        let prewarm_account = all_accounts[profile.cold_accounts];
        let burst_accounts = &all_accounts[profile.cold_accounts + 1..];

        let touch_er = topology::private_er(
            base,
            topology::ErOptions {
                label: "s7-touch".to_owned(),
                env: Vec::new(),
                request_timeout: Some(REQUEST_TIMEOUT),
            },
        )
        .await?;
        let touch_before = touch_er.ctx().scrape_metrics().await?;
        let cold = touch_pass(touch_er.ctx(), cold_pool).await?;
        let warm = touch_pass(touch_er.ctx(), cold_pool).await?;
        let touch_after = touch_er.ctx().scrape_metrics().await?;
        let touch_delta = MetricsDelta::new(touch_before, touch_after);
        drop(touch_er);

        let cold_median_us = cold.median.max(1) as f64;
        let warm_median_us = warm.median.max(1) as f64;
        let collapse_ratio = cold_median_us / warm_median_us;
        eprintln!(
            "[redsuite] {}: cold p50 {:.0} us / p95 {} us / max {} us vs warm \
             p50 {:.0} us / p95 {} us — collapse ratio {:.0}x",
            self.name(),
            cold_median_us,
            cold.quantile95,
            cold.max,
            warm_median_us,
            warm.quantile95,
            collapse_ratio,
        );
        assert!(
            collapse_ratio >= MIN_COLD_WARM_RATIO,
            "warm repeats must collapse at least {MIN_COLD_WARM_RATIO}x \
             below cold first touches (got {collapse_ratio:.1}x)"
        );
        assert!(
            warm.quantile95 < 100_000,
            "warm read p95 {} us left the sub-100ms range",
            warm.quantile95
        );

        let touch_report =
            ScenarioReport::ok(&format!("{}/cold_vs_warm", self.name()))
                .setting("profile", profile.name)
                .setting("accounts", profile.cold_accounts)
                .setting("account space", ACCOUNT_SPACE)
                .observe("cold first-touch us", cold)
                .observe("warm repeat us", warm)
                .metric("collapse ratio", collapse_ratio)
                .metric_if(
                    "validator ensure (account) avg s",
                    touch_delta.histogram_avg(ENSURE_ACCOUNT_HISTOGRAM),
                );
        match report::persist(&touch_report) {
            Ok(path) => {
                eprintln!("[redsuite]   cell report: {}", path.display())
            }
            Err(e) => eprintln!(
                "[redsuite]   warning: cell report not persisted: {e}"
            ),
        }

        let burst_payers: Vec<Rc<Keypair>> =
            prep::funded_payers(base, profile.burst_payers, PAYER_LAMPORTS)
                .await?
                .into_iter()
                .map(Rc::new)
                .collect();

        let half = burst_accounts.len() / 2;
        let cold_deps = burst_cell(
            base,
            "s7-burst-cold",
            &burst_accounts[..half],
            &burst_payers,
            profile.burst_concurrency,
            false,
            prewarm_account,
        )
        .await?;
        let warm_deps = burst_cell(
            base,
            "s7-burst-warm",
            &burst_accounts[half..],
            &burst_payers,
            profile.burst_concurrency,
            true,
            prewarm_account,
        )
        .await?;

        for (cell_name, outcome) in [
            ("burst cold-deps", &cold_deps),
            ("burst warm-deps", &warm_deps),
        ] {
            eprintln!(
                "[redsuite] {}: {} — {} delivered / {} failed in {:.1} s, \
                 p50 {} us / p95 {} us / max {} us, >100ms {} of {}, ensure avg {}",
                self.name(),
                cell_name,
                outcome.delivered,
                outcome.failed,
                outcome.wall_s,
                outcome.delivery.median,
                outcome.delivery.quantile95,
                outcome.delivery.max,
                outcome.slow,
                outcome.delivered + outcome.failed,
                outcome
                    .ensure_avg_s
                    .map(|seconds| format!("{seconds:.6} s"))
                    .unwrap_or_else(|| "n/a".to_owned()),
            );
            assert_eq!(
                outcome.failed, 0,
                "{cell_name}: deliveries failed: {:?}",
                outcome.first_error
            );
        }
        if warm_deps.slow > cold_deps.slow {
            eprintln!(
                "[redsuite] {}: warning: warm-deps >100ms tail ({}) exceeded \
                 cold-deps ({})",
                self.name(),
                warm_deps.slow,
                cold_deps.slow
            );
        }

        for (slug, outcome) in [
            ("burst_cold_deps", &cold_deps),
            ("burst_warm_deps", &warm_deps),
        ] {
            let cell_report =
                ScenarioReport::ok(&format!("{}/{slug}", self.name()))
                    .setting("profile", profile.name)
                    .setting("burst accounts", half)
                    .setting("payers", profile.burst_payers)
                    .setting("concurrency", profile.burst_concurrency)
                    .setting("account space", ACCOUNT_SPACE)
                    .setting("slow threshold ms", SLOW_THRESHOLD.as_millis())
                    .observe("delivery us", outcome.delivery)
                    .metric("delivered", outcome.delivered as f64)
                    .metric("slow (>100ms)", outcome.slow as f64)
                    .metric("burst wall s", outcome.wall_s)
                    .metric_if(
                        "validator ensure (tx) avg s",
                        outcome.ensure_avg_s,
                    );
            match report::persist(&cell_report) {
                Ok(path) => {
                    eprintln!("[redsuite]   cell report: {}", path.display())
                }
                Err(e) => eprintln!(
                    "[redsuite]   warning: cell report not persisted: {e}"
                ),
            }
        }

        Ok(ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("cold accounts", profile.cold_accounts)
            .setting("burst accounts per cell", half)
            .setting("burst payers", profile.burst_payers)
            .setting("burst concurrency", profile.burst_concurrency)
            .setting("account space", ACCOUNT_SPACE)
            .metric("cold p50 us", cold_median_us)
            .metric("cold p95 us", cold.quantile95 as f64)
            .metric("warm p50 us", warm_median_us)
            .metric("collapse ratio", collapse_ratio)
            .metric(
                "burst cold-deps p95 us",
                cold_deps.delivery.quantile95 as f64,
            )
            .metric(
                "burst warm-deps p95 us",
                warm_deps.delivery.quantile95 as f64,
            )
            .metric("burst cold-deps >100ms", cold_deps.slow as f64)
            .metric("burst warm-deps >100ms", warm_deps.slow as f64))
    }
}
