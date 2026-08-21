use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures_util::future::join_all;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq,
    monitor::{MonitorSpec, SteadyStateMonitor},
    prep,
    profile::{self, ProfileValues},
    receipt, report,
    runner::{execute, RunConfig},
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
    // reused-pool cell: hot ALTs, pins the fresh-key attribution
    contrast: bool,
    deep_backlog: bool,
}

const LITE: Profile = Profile {
    name: "lite",
    fresh_commits: 12,
    prep_payers: 6,
    rate: 2,
    concurrency: 8,
    drain_cap: Duration::from_secs(240),
    monitor_window: Duration::from_secs(5),
    contrast: false,
    deep_backlog: false,
};

const FULL: Profile = Profile {
    name: "full",
    fresh_commits: 25,
    prep_payers: 6,
    rate: 2,
    concurrency: 8,
    drain_cap: Duration::from_secs(180),
    monitor_window: Duration::from_secs(5),
    contrast: false,
    deep_backlog: false,
};

const DEEP: Profile = Profile {
    name: "deep",
    fresh_commits: 150,
    prep_payers: 12,
    rate: 2,
    concurrency: 8,
    drain_cap: Duration::from_secs(900),
    monitor_window: Duration::from_secs(10),
    contrast: true,
    deep_backlog: true,
};

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
    soak: None,
    deep: Some(DEEP),
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
                let commit_signature = sender.deliver(&tx).await?;
                delivered.borrow_mut().push((id, commit_signature));
                Ok(())
            }
        }
    };
    let outcome = execute(
        RunConfig {
            iterations: sets.len() as u64,
            rate,
            concurrency,
        },
        request,
    )
    .await;
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
    for pda in pool {
        check::poll(
            &format!("the ER clones the delegated pda {pda}"),
            CLONE_TIMEOUT,
            || async {
                matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == ACCOUNT_SPACE as usize)
            },
        )
        .await?;
    }
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
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);
        let pool_size = profile.fresh_commits as usize * COMMIT_WIDTH;

        let prep_payers =
            prep::funded_payers(base, profile.prep_payers, PREP_PAYER_LAMPORTS)
                .await?;
        let prep_started = Instant::now();
        let pool = crate::init_delegated_accounts_batched(
            base,
            &prep_payers,
            pool_size,
            ACCOUNT_SPACE,
            er.identity(),
        )
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
        let monitor = SteadyStateMonitor::start(
            er.metrics_url().to_owned(),
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
        let steady_state = monitor.finish().await?;
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
            steady_state.busy_peak,
        );
        if profile.deep_backlog {
            if drain_rate < 0.8 {
                if steady_state.outstanding_peak <= 50.0 {
                    eprintln!(
                        "[redsuite] {}: warning: deep-backlog cell never \
                         exceeded 50 outstanding intents (peak {:.0}) — the \
                         convoy was not pressured",
                        self.name(),
                        steady_state.outstanding_peak
                    );
                }
                if steady_state.busy_peak < 40.0 {
                    eprintln!(
                        "[redsuite] {}: warning: executor permits never \
                         saturated (busy peak {:.0})",
                        self.name(),
                        steady_state.busy_peak
                    );
                }
                if steady_state.verdict.to_string() != "OVERLOAD" {
                    eprintln!(
                        "[redsuite] {}: warning: arrival outpaced drain with \
                         a deep queue yet the monitor verdict was {}",
                        self.name(),
                        steady_state.verdict
                    );
                }
            } else {
                eprintln!(
                    "[redsuite] {}: fresh drain {drain_rate:.2}/s is past the \
                     convoy band — the committor P0 fix likely landed; move \
                     the asserts to the post-fix drain band",
                    self.name()
                );
            }
        }

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
            .setting("fully drained", drain.fully_drained)
            .observe("delivery us", delivery_outcome.delivery)
            .metric("fresh drain intents/s", drain_rate)
            .metric("delivery+drain span s", span_wall.as_secs_f64())
            .metric("drain wall s", drain.drain_wall.as_secs_f64())
            .metric("monitor arrival /s", steady_state.arrival_rate)
            .metric("monitor drain /s", steady_state.drain_rate)
            .metric("outstanding peak", steady_state.outstanding_peak)
            .metric("backlog gauge peak", steady_state.backlog_peak)
            .metric("busy peak", steady_state.busy_peak)
            .metric_if(
                "validator intent exec avg s",
                delta.histogram_avg_all(
                    "mbv_committor_intent_execution_time_histogram_v2",
                ),
            )
            .metric_if(
                "alt tables used",
                delta.counter("mbv_committor_intent_alt_count_sum"),
            )
            .metric_if(
                "alt preparation avg s",
                delta
                    .histogram_avg("mbv_committor_intent_alt_preparation_time"),
            );
        if drain.fully_drained {
            summary = summary.metric(
                "receipt base txs per commit",
                receipt_base_txs as f64 / profile.fresh_commits as f64,
            );
        }

        if profile.contrast {
            summary = self
                .contrast_cell(
                    base,
                    er,
                    &sender,
                    payer_pubkey,
                    drain_rate,
                    summary,
                )
                .await?;
        }
        Ok(summary)
    }
}

impl CommitThroughputCeiling {
    async fn contrast_cell(
        &self,
        base: &BaseCtx,
        er: &ErCtx,
        sender: &redsuite_core::TxSender,
        payer_pubkey: Pubkey,
        fresh_drain_rate: f64,
        summary: ScenarioReport,
    ) -> Result<ScenarioReport> {
        const CONTRAST_SETS: usize = 30;
        const ROUNDS: u64 = 3;
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);
        let commits: u64 = ROUNDS * CONTRAST_SETS as u64;
        let contrast_payers =
            prep::funded_payers(base, 4, PREP_PAYER_LAMPORTS).await?;
        let contrast_pool = crate::init_delegated_accounts_batched(
            base,
            &contrast_payers,
            CONTRAST_SETS * COMMIT_WIDTH,
            ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        prewarm(er, &contrast_pool).await?;
        let set_for = |commit_index: usize| {
            contrast_pool[(commit_index % CONTRAST_SETS) * COMMIT_WIDTH..]
                [..COMMIT_WIDTH]
                .to_vec()
        };

        // warm-up round: one commit per set creates that set's ALTs
        let warmup_sets: Vec<Vec<Pubkey>> =
            (0..CONTRAST_SETS).map(set_for).collect();
        quiesce_committor(er).await?;
        let warmup_executed_before = er
            .scrape_metrics()
            .await?
            .value_sum(EXECUTED_COUNTER)
            .unwrap_or(0.0);
        deliver_commits(
            sender,
            payer_pubkey,
            warmup_sets,
            1_000_000,
            profile.rate,
            profile.concurrency,
        )
        .await?;
        let warmup_drain = await_drain(
            er,
            warmup_executed_before,
            CONTRAST_SETS as u64,
            profile.drain_cap,
        )
        .await?;
        if !warmup_drain.fully_drained {
            return Err(CheckError::new(
                "contrast warm-up round did not drain",
            )
            .into());
        }

        let measured_sets: Vec<Vec<Pubkey>> =
            (0..commits as usize).map(set_for).collect();
        quiesce_committor(er).await?;
        let before = er.scrape_metrics().await?;
        let executed_before = before.value_sum(EXECUTED_COUNTER).unwrap_or(0.0);
        let span_started = Instant::now();
        let (_, delivery_outcome) = deliver_commits(
            sender,
            payer_pubkey,
            measured_sets,
            2_000_000,
            profile.rate,
            profile.concurrency,
        )
        .await?;
        let drain =
            await_drain(er, executed_before, commits, profile.drain_cap)
                .await?;
        let span_wall = span_started.elapsed();
        let after = er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);
        let failed_intents = delta
            .counter_all("mbv_committor_failed_intents_count")
            .unwrap_or(0.0);
        check_eq!(failed_intents, 0.0, "reused-pool intents failed")?;
        check!(
            drain.drained > 0.0,
            "INVALID: the reused-pool cell drained nothing"
        )?;

        let drain_rate = drain.drained / span_wall.as_secs_f64();
        let contrast_ratio = if fresh_drain_rate > 0.0 {
            drain_rate / fresh_drain_rate
        } else {
            0.0
        };
        eprintln!(
            "[redsuite] {}: reused-pool drain {:.2} intents/s ({}/{} over \
             {:.1} s span) — {:.2}x the fresh-key drain",
            self.name(),
            drain_rate,
            drain.drained,
            commits,
            span_wall.as_secs_f64(),
            contrast_ratio,
        );

        let cell_report =
            ScenarioReport::ok(&format!("{}/reused", self.name()))
                .setting("profile", profile.name)
                .setting("width", COMMIT_WIDTH)
                .setting("account space", ACCOUNT_SPACE)
                .setting("commits", commits)
                .setting("sets", CONTRAST_SETS)
                .setting("rounds", ROUNDS)
                .setting("alt warmup round", true)
                .setting("fully drained", drain.fully_drained)
                .observe("delivery us", delivery_outcome.delivery)
                .metric("reused drain intents/s", drain_rate)
                .metric("reused/fresh drain ratio", contrast_ratio)
                .metric("delivery+drain span s", span_wall.as_secs_f64())
                .metric_if(
                    "validator intent exec avg s",
                    delta.histogram_avg_all(
                        "mbv_committor_intent_execution_time_histogram_v2",
                    ),
                );
        match report::persist(&cell_report) {
            Ok(path) => {
                eprintln!("[redsuite]   cell report: {}", path.display())
            }
            Err(e) => eprintln!(
                "[redsuite]   warning: cell report not persisted: {e}"
            ),
        }

        Ok(summary
            .metric("reused drain intents/s", drain_rate)
            .metric("reused/fresh drain ratio", contrast_ratio))
    }
}
