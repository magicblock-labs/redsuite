use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    profile::ProfileValues,
    programs::REDLINE,
    redline::Accounts,
    report::Unit,
    runner::{execute, Pacing, RunConfig, RunOutcome, SendTransaction, REQUEST_ORIGIN},
    stats::{ObservationsStats, StreamingStats},
    topology::{self, ErOptions},
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport, TxSender,
};
use tracing::{debug, warn};

const ACCOUNT_SPACE: u32 = 2048;
const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const PAYER_LAMPORTS: u64 = 2_000_000_000;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const SLOW_THRESHOLD: Duration = Duration::from_millis(100);
const MIN_COLD_WARM_RATIO: f64 = 10.0;

const ENSURE_ACCOUNT_HISTOGRAM: &str = r#"mbv_ensure_accounts_time{kind="account"}"#;
const ENSURE_TX_HISTOGRAM: &str = r#"mbv_ensure_accounts_time{kind="transaction"}"#;

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

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

async fn touch_pass(er: &ErCtx, pool: &[Pubkey]) -> Result<ObservationsStats> {
    let mut latencies = StreamingStats::new();
    for account in pool {
        let started = Instant::now();
        let fetched = er.account(account).await?;
        latencies.push(started.elapsed().as_micros() as u32);
        let data_len = fetched.map(|account| account.data.len()).unwrap_or(0);
        if data_len != ACCOUNT_SPACE as usize {
            return Err(anyhow!(
                "hydration broken: {account} read back {data_len} bytes"
            ));
        }
    }
    Ok(latencies.finalize(false))
}

struct BurstOutcome {
    outcome: RunOutcome,
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
        ErOptions {
            label: label.to_owned(),
            request_timeout: Some(REQUEST_TIMEOUT),
            ..Default::default()
        },
    )
    .await?;
    let cell_er = private.ctx();
    let senders: Vec<TxSender> = payers.iter().map(|payer| cell_er.sender(payer.clone())).collect();

    if prewarm_deps {
        for sender in &senders {
            let warm_ix = REDLINE.inspect(&[prewarm_account]);
            sender.submit(&[warm_ix]).await?;
        }
    }

    let before = cell_er.scrape_metrics().await?;
    let slow = Rc::new(Cell::new(0u64));
    let request = |id: u64| {
        let sender = senders[(id as usize) % senders.len()].clone();
        let account = pool[(id as usize - 1) % pool.len()];
        let ix = REDLINE.inspect(&[account]);
        let slow = slow.clone();
        async move {
            let tx = sender.prepare(&[ix]).await?;
            Ok(SendTransaction(move |started: Instant| async move {
                let delivery = sender.submit_prepared(&tx).await.map(|_| ());
                if started.elapsed() > SLOW_THRESHOLD {
                    slow.set(slow.get() + 1);
                }
                delivery
            }))
        }
    };
    let started = Instant::now();
    let outcome = execute(
        RunConfig {
            iterations: pool.len() as u64,
            rate: Pacing::PerSecond(pool.len() as u32),
            concurrency,
        },
        request,
    )
    .await?;
    let wall_s = started.elapsed().as_secs_f64();
    let after = cell_er.scrape_metrics().await?;
    let delta = MetricsDelta::new(before, after);

    Ok(BurstOutcome {
        outcome,
        slow: slow.get(),
        wall_s,
        ensure_avg_s: delta.histogram_avg(ENSURE_TX_HISTOGRAM),
    })
}

pub struct ColdHydrationTail {
    base: Rc<BaseCtx>,
    profile: &'static Profile,
    all_accounts: Vec<Pubkey>,
}

impl Scenario<ScenarioReport> for ColdHydrationTail {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers = base.funded_payers(profile.prep_payers, PREP_PAYER_LAMPORTS).await?;
        let all_accounts = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(
                base.as_ref(),
                &prep_payers,
                profile.cold_accounts + profile.burst_accounts + 1,
                false,
            )
            .await?;
        Ok(Self { base, profile, all_accounts })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let Self { base, profile, all_accounts } = self;
        let cold_pool = &all_accounts[..profile.cold_accounts];
        let prewarm_account = all_accounts[profile.cold_accounts];
        let burst_accounts = &all_accounts[profile.cold_accounts + 1..];

        let touch_er = topology::private_er(
            &base,
            ErOptions {
                label: "s7-touch".to_owned(),
                request_timeout: Some(REQUEST_TIMEOUT),
                ..Default::default()
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
        if collapse_ratio < MIN_COLD_WARM_RATIO {
            warn!(collapse_ratio, "cold-to-warm median speedup below minimum",);
        }
        if warm.quantile95 >= 100_000 {
            warn!(warm.quantile95, "warm read p95 exceeds latency bound");
        }

        let touch_report = ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting("accounts", profile.cold_accounts)
            .setting("account space", ACCOUNT_SPACE)
            .latency_origin(REQUEST_ORIGIN)
            .observe("cold first-touch us", Unit::Micros, cold)
            .observe("warm repeat us", Unit::Micros, warm)
            .metric("collapse ratio", Unit::Ratio, collapse_ratio)
            .metric_if(
                "validator ensure (account) avg s",
                Unit::Seconds,
                touch_delta.histogram_avg(ENSURE_ACCOUNT_HISTOGRAM),
            );
        base.report_cell("cold_vs_warm", &touch_report)?;

        let burst_payers: Vec<Rc<Keypair>> = base
            .funded_payers(profile.burst_payers, PAYER_LAMPORTS)
            .await?
            .into_iter()
            .map(Rc::new)
            .collect();

        let half = burst_accounts.len() / 2;
        let cold_deps = burst_cell(
            &base,
            "s7-burst-cold",
            &burst_accounts[..half],
            &burst_payers,
            profile.burst_concurrency,
            false,
            prewarm_account,
        )
        .await?;
        let warm_deps = burst_cell(
            &base,
            "s7-burst-warm",
            &burst_accounts[half..],
            &burst_payers,
            profile.burst_concurrency,
            true,
            prewarm_account,
        )
        .await?;

        for (cell_name, outcome) in
            [("burst cold-deps", &cold_deps), ("burst warm-deps", &warm_deps)]
        {
            debug!(
                cell_name,
                outcome.outcome.failed, outcome.slow, "dependency hydration cell finished",
            );
            ensure!(
                outcome.outcome.failed == 0,
                "failures={:?}; first error={:?}",
                outcome.outcome.failed,
                outcome.outcome.first_error
            );
        }
        if warm_deps.slow > cold_deps.slow {
            warn!(
                warm_deps.slow,
                cold_deps.slow, "warm dependency tail exceeded cold dependency tail",
            );
        }

        for (slug, outcome) in [("burst_cold_deps", &cold_deps), ("burst_warm_deps", &warm_deps)] {
            let cell_report = ScenarioReport::ok()
                .setting("profile", profile.name)
                .setting("burst accounts", half)
                .setting("payers", profile.burst_payers)
                .setting("concurrency", profile.burst_concurrency)
                .setting("account space", ACCOUNT_SPACE)
                .setting("slow threshold ms", SLOW_THRESHOLD.as_millis())
                .observe("delivery us", Unit::Micros, outcome.outcome.delivery)
                .client_side(&outcome.outcome)
                .metric("delivered", Unit::Count, outcome.outcome.delivered as f64)
                .metric("slow (>100ms)", Unit::Count, outcome.slow as f64)
                .metric("burst wall s", Unit::Seconds, outcome.wall_s)
                .metric_if(
                    "validator ensure (tx) avg s",
                    Unit::Seconds,
                    outcome.ensure_avg_s,
                );
            base.report_cell(slug, &cell_report)?;
        }

        Ok(ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting("cold accounts", profile.cold_accounts)
            .setting("burst accounts per cell", half)
            .setting("burst payers", profile.burst_payers)
            .setting("burst concurrency", profile.burst_concurrency)
            .setting("account space", ACCOUNT_SPACE)
            .latency_origin(REQUEST_ORIGIN)
            .metric("cold p50 us", Unit::Micros, cold_median_us)
            .metric("cold p95 us", Unit::Micros, cold.quantile95 as f64)
            .metric("warm p50 us", Unit::Micros, warm_median_us)
            .metric("collapse ratio", Unit::Ratio, collapse_ratio)
            .with_flags(&cold_deps.outcome)
            .with_flags(&warm_deps.outcome)
            .metric(
                "burst cold-deps p95 us",
                Unit::Micros,
                cold_deps.outcome.delivery.quantile95 as f64,
            )
            .metric(
                "burst warm-deps p95 us",
                Unit::Micros,
                warm_deps.outcome.delivery.quantile95 as f64,
            )
            .metric("burst cold-deps >100ms", Unit::Count, cold_deps.slow as f64)
            .metric("burst warm-deps >100ms", Unit::Count, warm_deps.slow as f64))
    }
}
