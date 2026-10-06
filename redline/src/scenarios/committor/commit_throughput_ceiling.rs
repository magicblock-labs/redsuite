use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures_util::future::join_all;
use pubkey::Pubkey;
use redsuite_core::redline::Accounts;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq,
    monitor::{self, MonitorSpec},
    prep,
    profile::ProfileValues,
    receipt,
    runner::{execute, Pacing, RunConfig},
    BaseCtx, ChainCtx, CheckError, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport,
};
use signature::Signature;
use signer::Signer;

use crate::program::instruction::build;

// The widest fresh-key commit the >= 0.13.7 scheduling-time intent gate
// admits with margin (it estimates commits at full inline size; probed on
// v0.13.7: w10 x 40 B schedules, w12 x 40 B is refused). Wide enough that
// every intent still needs ALTs on base — the TableMania convoy trigger.
const COMMIT_WIDTH: usize = 8;
const ACCOUNT_SPACE: u32 = 40;
const PAYER_LAMPORTS: u64 = 2_000_000_000;
const PREP_PAYER_LAMPORTS: u64 = 2_000_000_000;
const INTENT_GATE: Duration = Duration::from_secs(90);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(20);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);
const DRAIN_POLL: Duration = Duration::from_secs(2);
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const PREWARM_CONCURRENCY: usize = 16;
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(120);

const INTENTS_COUNTER: &str = "mbv_committor_intents_count";
const EXECUTED_COUNTER: &str =
    "mbv_committor_intent_execution_time_histogram_v2_count";
const BACKLOG_GAUGE: &str = "mbv_committor_intent_backlog_count";
const BUSY_GAUGE: &str = "mbv_committor_executors_busy_count";

struct Profile {
    name: &'static str,
    // wide commits over never-committed keys — the convoy trigger
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

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
};

async fn deliver_commits(
    sender: &redsuite_core::TxSender,
    payer_pubkey: Pubkey,
    sets: Vec<Vec<Pubkey>>,
    first_id: u64,
    rate: u32,
    concurrency: usize,
) -> Result<(Vec<(u64, Signature)>, redsuite_core::runner::RunOutcome)> {
    let delivered: Rc<RefCell<Vec<(u64, Signature)>>> =
        Rc::new(RefCell::new(Vec::with_capacity(sets.len())));
    let sets = Rc::new(sets);
    let request = {
        let delivered = delivered.clone();
        let sender = sender.clone();
        let sets = sets.clone();
        move |iteration: u64| {
            let id = first_id + iteration;
            let accounts = sets[(iteration - 1) as usize].clone();
            let ix = build::commit_accounts(id, payer_pubkey, &accounts);
            let sender = sender.clone();
            let delivered = delivered.clone();
            async move {
                let tx = sender.prepare(&[ix]).await?;
                let commit_signature = sender.submit_prepared(&tx).await?;
                delivered.borrow_mut().push((id, commit_signature));
                Ok(())
            }
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
    if outcome.failed > 0 {
        return Err(format!(
            "commit deliveries failed: {:?}",
            outcome.first_error
        )
        .into());
    }
    let delivered = Rc::try_unwrap(delivered)
        .unwrap_or_else(|_| panic!("delivery tasks still hold the list"))
        .into_inner();
    Ok((delivered, outcome))
}

async fn prewarm(er: &ErCtx, pool: &[Pubkey]) -> Result<()> {
    for window in pool.chunks(PREWARM_CONCURRENCY) {
        let touches = window.iter().map(|pda| er.account(pda));
        let _ = join_all(touches).await;
    }
    prep::await_clones(er, pool, ACCOUNT_SPACE as usize, CLONE_TIMEOUT).await?;
    Ok(())
}

async fn quiesce_committor(er: &ErCtx) -> Result<()> {
    check::poll(
        "the committor drains its backlog before the measured window",
        QUIESCE_TIMEOUT,
        || async {
            match er.scrape_metrics().await {
                Ok(metrics) => {
                    let backlog =
                        metrics.value_sum(BACKLOG_GAUGE).unwrap_or(0.0);
                    let intents =
                        metrics.value_sum(INTENTS_COUNTER).unwrap_or(0.0);
                    let executed =
                        metrics.value_sum(EXECUTED_COUNTER).unwrap_or(0.0);
                    backlog == 0.0 && intents <= executed
                }
                Err(_) => false,
            }
        },
    )
    .await?;
    Ok(())
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
    let deadline = tokio::time::Instant::now() + cap;
    loop {
        let executed_now = er
            .scrape_metrics()
            .await?
            .value_sum(EXECUTED_COUNTER)
            .unwrap_or(0.0);
        if executed_now >= target {
            return Ok(DrainResult {
                fully_drained: true,
                drained: expected as f64,
                drain_wall: started.elapsed(),
            });
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(DrainResult {
                fully_drained: false,
                drained: (executed_now - executed_before).max(0.0),
                drain_wall: started.elapsed(),
            });
        }
        tokio::time::sleep(DRAIN_POLL).await;
    }
}

pub struct CommitThroughputCeiling;

#[async_trait(?Send)]
impl Scenario for CommitThroughputCeiling {
    fn name(&self) -> &str {
        "redline/commit_throughput_ceiling"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = PROFILES.select(base.config().profile);
        let pool_size = profile.fresh_commits as usize * COMMIT_WIDTH;

        let prep_payers =
            prep::funded_payers(base, profile.prep_payers, PREP_PAYER_LAMPORTS)
                .await?;
        let prep_started = Instant::now();
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base, &prep_payers, pool_size, true)
            .await?;
        eprintln!(
            "[redsuite] {}: prepped {} fresh delegated accounts in {:.1} s",
            self.name(),
            pool.len(),
            prep_started.elapsed().as_secs_f64(),
        );

        prewarm(er, &pool).await?;

        let payer = prep::funded_payer(base, PAYER_LAMPORTS).await?;
        let payer_pubkey = payer.pubkey();
        let sender = er.sender(Rc::new(payer));

        let fresh_sets: Vec<Vec<Pubkey>> = pool
            .chunks_exact(COMMIT_WIDTH)
            .map(|window| window.to_vec())
            .collect();

        quiesce_committor(er).await?;
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
            check::poll(
                &format!("the intents counter reaches {target}"),
                INTENT_GATE,
                || async {
                    matches!(
                        er.scrape_metrics().await.ok().and_then(|metrics| metrics.value_sum(INTENTS_COUNTER)),
                        Some(count) if count >= target
                    )
                },
            )
            .await?;
        }

        let drain = await_drain(
            er,
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

        check!(
            drain.drained > 0.0,
            "INVALID: no intent drained within {:?} — the commit pipeline \
             executed nothing",
            profile.drain_cap,
        )?;
        let failed_intents = delta
            .counter_all("mbv_committor_failed_intents_count")
            .unwrap_or(0.0);
        check_eq!(failed_intents, 0.0, "fresh-key intents failed")?;
        if let Some(alt_tables_used) =
            delta.counter("mbv_committor_intent_alt_count_sum")
        {
            check!(
                alt_tables_used >= 1.0,
                "wide fresh-key commits should ride ALTs"
            )?;
        }

        let drain_rate = drain.drained / span_wall.as_secs_f64();
        eprintln!(
            "[redsuite] {}: fresh drain {:.2} intents/s ({}/{} over {:.1} s \
             span), verdict {}, outstanding peak {:.0} (backlog gauge peak \
             {:.0}), busy peak {:.0}",
            self.name(),
            drain_rate,
            drain.drained,
            profile.fresh_commits,
            span_wall.as_secs_f64(),
            steady_state.verdict,
            steady_state.outstanding_peak,
            steady_state.backlog_peak,
            steady_state.busy_peak.unwrap_or(f64::NAN),
        );
        // drain measured a second, independent way: every scheduling tx's
        // receipt exists, succeeded, and its base txs confirm on chain
        let mut receipt_base_txs = 0usize;
        if drain.fully_drained {
            for (id, commit_signature) in &delivered {
                let commit_receipt = receipt::fetch_commit_receipt(
                    er.api(),
                    commit_signature,
                    RECEIPT_TIMEOUT,
                )
                .await?;
                if let Some(message) = &commit_receipt.error_message {
                    return Err(CheckError::new(format!(
                        "fresh commit {id} intent succeeds"
                    ))
                    .actual(message)
                    .into());
                }
                receipt::confirm_base_signatures(
                    base.api(),
                    &commit_receipt,
                    BASE_CONFIRM_TIMEOUT,
                )
                .await?;
                receipt_base_txs += commit_receipt.base_signatures.len();
            }
        } else {
            eprintln!(
                "[redsuite] {}: warning: only {:.0} of {} intents drained \
                 within {:?} — receipts skipped, drain rate is partial",
                self.name(),
                drain.drained,
                profile.fresh_commits,
                profile.drain_cap,
            );
        }

        let mut summary = ScenarioReport::ok(self.name())
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
            .metric(
                "monitor drain /s",
                Unit::PerSecond,
                steady_state.drain_rate,
            )
            .metric(
                "outstanding peak",
                Unit::Count,
                steady_state.outstanding_peak,
            )
            .metric(
                "backlog gauge peak",
                Unit::Count,
                steady_state.backlog_peak,
            )
            .metric_if("busy peak", Unit::Count, steady_state.busy_peak)
            .metric_if(
                "validator intent exec avg s",
                Unit::Seconds,
                delta.histogram_avg_all(
                    "mbv_committor_intent_execution_time_histogram_v2",
                ),
            )
            .metric_if(
                "alt tables used",
                Unit::Count,
                delta.counter("mbv_committor_intent_alt_count_sum"),
            )
            .metric_if(
                "alt preparation avg s",
                Unit::Seconds,
                delta
                    .histogram_avg("mbv_committor_intent_alt_preparation_time"),
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
