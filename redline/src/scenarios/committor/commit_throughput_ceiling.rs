use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use pubkey::Pubkey;
use redsuite_core::{
    monitor::{self, MonitorSpec},
    profile::ProfileValues,
    programs::{SettlementMode, REDLINE},
    redline::Accounts,
    report::Unit,
    runner::{execute, Pacing, RunConfig, RunOutcome, SendTransaction},
    wait, BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport, TxSender,
};
use signature::Signature;
use signer::Signer;
use tokio::time;
use tracing::{debug, warn};

const COMMIT_WIDTH: usize = 8;
const ACCOUNT_SPACE: u32 = 40;
const PAYER_LAMPORTS: u64 = 2_000_000_000;
const PREP_PAYER_LAMPORTS: u64 = 2_000_000_000;
const INTENT_GATE: Duration = Duration::from_secs(90);
const DRAIN_POLL: Duration = Duration::from_secs(2);
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(120);

const INTENTS_COUNTER: &str = "mbv_committor_intents_count";
const EXECUTED_COUNTER: &str = "mbv_committor_intent_execution_time_histogram_v2_count";
const BACKLOG_GAUGE: &str = "mbv_committor_intent_backlog_count";
const BUSY_GAUGE: &str = "mbv_committor_executors_busy_count";

struct Profile {
    name: &'static str,
    // wide commits over never-committed keys
    fresh_commits: u64,
    prep_payers: usize,
    rate: u32,
    concurrency: usize,
    drain_cap: Duration,
    monitor_window: Duration,
}

const LITE: Profile = Profile {
    name: "lite",
    fresh_commits: 12,
    prep_payers: 6,
    rate: 2,
    concurrency: 8,
    drain_cap: Duration::from_secs(240),
    monitor_window: Duration::from_secs(5),
};

const FULL: Profile = Profile {
    name: "full",
    fresh_commits: 25,
    prep_payers: 6,
    rate: 2,
    concurrency: 8,
    drain_cap: Duration::from_secs(180),
    monitor_window: Duration::from_secs(5),
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

async fn deliver_commits(
    sender: &TxSender,
    payer_pubkey: Pubkey,
    sets: Vec<Vec<Pubkey>>,
    first_id: u64,
    rate: u32,
    concurrency: usize,
) -> Result<(Vec<Signature>, RunOutcome)> {
    let delivered: Rc<RefCell<Vec<Signature>>> =
        Rc::new(RefCell::new(Vec::with_capacity(sets.len())));
    let request = |iteration: u64| {
        let id = first_id + iteration;
        let accounts = &sets[(iteration - 1) as usize];
        let ix = REDLINE
            .batch()
            .nonce(id)
            .settle(payer_pubkey, accounts, SettlementMode::Commit)
            .build();
        let sender = sender.clone();
        let delivered = delivered.clone();
        async move {
            let tx = sender.prepare(&[ix]).await?;
            Ok(SendTransaction(move |_: Instant| async move {
                let commit_signature = sender.submit_prepared(&tx).await?;
                delivered.borrow_mut().push(commit_signature);
                Ok(())
            }))
        }
    };
    let outcome = execute(
        RunConfig {
            iterations: sets.len() as u64,
            rate: Pacing::PerSecond(rate),
            concurrency,
        },
        request,
    )
    .await?;
    ensure!(outcome.failed == 0);
    let delivered = Rc::try_unwrap(delivered)
        .unwrap_or_else(|_| panic!("delivery tasks still hold the list"))
        .into_inner();
    Ok((delivered, outcome))
}

async fn quiesce_committor(er: &ErCtx) -> Result<()> {
    wait::retry("committor backlog drained", QUIESCE_TIMEOUT, async || {
        let metrics = er.scrape_metrics().await?;
        let backlog = metrics.value_sum(BACKLOG_GAUGE).unwrap_or(0.0);
        let intents = metrics.value_sum(INTENTS_COUNTER).unwrap_or(0.0);
        let executed = metrics.value_sum(EXECUTED_COUNTER).unwrap_or(0.0);
        ensure!(backlog == 0.0 && intents <= executed);
        Ok(())
    })
    .await
}

struct DrainResult {
    fully_drained: bool,
    drained: f64,
    drain_wall: Duration,
}

async fn await_drain(
    er: &ErCtx,
    executed_before: f64,
    expected: u64,
    cap: Duration,
) -> Result<DrainResult> {
    let target = executed_before + expected as f64;
    let started = Instant::now();
    let deadline = time::Instant::now() + cap;
    loop {
        let executed_now = er.scrape_metrics().await?.value_sum(EXECUTED_COUNTER).unwrap_or(0.0);
        let fully_drained = executed_now >= target;
        if fully_drained || time::Instant::now() >= deadline {
            return Ok(DrainResult {
                fully_drained,
                drained: if fully_drained {
                    expected as f64
                } else {
                    (executed_now - executed_before).max(0.0)
                },
                drain_wall: started.elapsed(),
            });
        }
        time::sleep(DRAIN_POLL).await;
    }
}

pub struct CommitThroughputCeiling {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    pool: Vec<Pubkey>,
    sender: TxSender,
}

impl Scenario<ScenarioReport> for CommitThroughputCeiling {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let pool_size = profile.fresh_commits as usize * COMMIT_WIDTH;

        let prep_payers = base.funded_payers(profile.prep_payers, PREP_PAYER_LAMPORTS).await?;
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base.as_ref(), &prep_payers, pool_size, true)
            .await?;

        er.prewarm(&pool, ACCOUNT_SPACE as usize).await?;

        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let sender = er.sender(Rc::new(payer));

        Ok(Self { base, er, profile, pool, sender })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let Self { base, er, profile, pool, sender } = self;
        let pool_size = pool.len();
        let payer_pubkey = sender.payer().pubkey();
        let fresh_sets: Vec<Vec<Pubkey>> =
            pool.chunks_exact(COMMIT_WIDTH).map(|window| window.to_vec()).collect();

        quiesce_committor(&er).await?;
        let before = er.scrape_metrics().await?;
        let intents_before = before.value_sum(INTENTS_COUNTER).unwrap_or(0.0);
        let executed_before = before.value_sum(EXECUTED_COUNTER).unwrap_or(0.0);
        let monitor = monitor::start(
            er.metrics().clone(),
            MonitorSpec {
                arrival_counter: INTENTS_COUNTER.to_owned(),
                drain_counter: EXECUTED_COUNTER.to_owned(),
                backlog_gauge: BACKLOG_GAUGE.to_owned(),
                busy_gauge: Some(BUSY_GAUGE.to_owned()),
                window: profile.monitor_window,
            },
        );

        let span_started = Instant::now();
        let (delivered, delivery_outcome) = deliver_commits(
            &sender,
            payer_pubkey,
            fresh_sets,
            0,
            profile.rate,
            profile.concurrency,
        )
        .await?;

        if before.get(INTENTS_COUNTER).is_some() {
            let target = intents_before + profile.fresh_commits as f64;
            wait::retry("intents accepted", INTENT_GATE, async || {
                let metrics = er.scrape_metrics().await?;
                let count =
                    metrics.value_sum(INTENTS_COUNTER).context("intents counter missing")?;
                ensure!(count >= target);
                Ok(())
            })
            .await?;
        }

        let drain = await_drain(
            &er,
            executed_before,
            profile.fresh_commits,
            profile.drain_cap,
        )
        .await?;
        let span_wall = span_started.elapsed();
        let sampled = monitor.finish().await?;
        let coverage = sampled.to_string();
        let steady_state = monitor::judge(sampled.value);
        let after = er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);

        ensure!(drain.drained > 0.0);
        let failed_intents = delta.counter_all("mbv_committor_failed_intents_count").unwrap_or(0.0);
        ensure!(failed_intents == 0.0);
        let drain_rate = drain.drained / span_wall.as_secs_f64();
        debug!(
            drain.drained,
            profile.fresh_commits,
            %steady_state.verdict,
            "fresh commit drain finished",
        );
        // drain measured a second, independent way: every scheduling tx's
        // receipt exists, succeeded, and its base txs confirm on chain
        let mut receipt_base_txs = 0usize;
        if drain.fully_drained {
            for commit_signature in &delivered {
                let commit_receipt = super::settled_receipt(er.api(), commit_signature).await?;
                commit_receipt.confirm(base.api()).await?;
                receipt_base_txs += commit_receipt.base_signatures.len();
            }
        } else {
            warn!(
                drain.drained,
                profile.fresh_commits,
                drain_cap_secs = profile.drain_cap.as_secs_f64(),
                "commit drain incomplete; receipt checks skipped",
            );
        }

        let mut summary = ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting("width", COMMIT_WIDTH)
            .setting("account space", ACCOUNT_SPACE)
            .setting("fresh commits", profile.fresh_commits)
            .setting("pool", pool_size)
            .setting("offered rate /s", profile.rate)
            .setting("drain cap s", profile.drain_cap.as_secs())
            .setting("prewarmed", true)
            .setting("verdict", steady_state.verdict)
            .setting("steady-state scrapes", coverage)
            .setting("fully drained", drain.fully_drained)
            .observe("delivery us", Unit::Micros, delivery_outcome.delivery)
            .client_side(&delivery_outcome)
            .metric("fresh drain intents/s", Unit::PerSecond, drain_rate)
            .metric(
                "delivery+drain span s",
                Unit::Seconds,
                span_wall.as_secs_f64(),
            )
            .metric(
                "drain wall s",
                Unit::Seconds,
                drain.drain_wall.as_secs_f64(),
            )
            .metric(
                "monitor arrival /s",
                Unit::PerSecond,
                steady_state.arrival_rate,
            )
            .metric("monitor drain /s", Unit::PerSecond, steady_state.drain_rate)
            .metric(
                "outstanding peak",
                Unit::Count,
                steady_state.outstanding_peak,
            )
            .metric("backlog gauge peak", Unit::Count, steady_state.backlog_peak)
            .metric_if("busy peak", Unit::Count, steady_state.busy_peak)
            .metric_if(
                "validator intent exec avg s",
                Unit::Seconds,
                delta.histogram_avg_all("mbv_committor_intent_execution_time_histogram_v2"),
            )
            .metric_if(
                "alt tables used",
                Unit::Count,
                delta.counter("mbv_committor_intent_alt_count_sum"),
            )
            .metric_if(
                "alt preparation avg s",
                Unit::Seconds,
                delta.histogram_avg("mbv_committor_intent_alt_preparation_time"),
            );
        if drain.fully_drained {
            summary = summary.metric(
                "receipt base txs per commit",
                Unit::Count,
                receipt_base_txs as f64 / profile.fresh_commits as f64,
            );
        }

        Ok(summary)
    }
}
