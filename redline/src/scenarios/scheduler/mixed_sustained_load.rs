use std::{
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use instruction::Instruction;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, host, prep,
    profile::ProfileValues,
    redline::{causal, Accounts},
    report::Unit,
    runner::{
        execute_for_raw, execute_raw, merge_outcomes, spawn_workers, split_budget,
        split_iterations, Delivery, Pacing, RawRunOutcome, RunConfig, RunOutcome, SendTransaction,
    },
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
};
use tokio::sync::{watch, Mutex};
use tracing::{debug, info};
use url::Url;

use crate::{
    load::Options,
    metrics,
    program::{instruction::build, layout, utils::hash_chain},
    ACCOUNT_SPACE,
};

const PAYER_LAMPORTS: u64 = 4_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(60);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1_800);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);

const BLOCKED_TRANSACTIONS: &str = "engine_processor_blocked_transactions";
const BUSY_EXECUTORS: &str = "engine_processor_busy_executors";
const PENDING_TRANSACTIONS: &str = "engine_ledger_pending_transactions";
const ORDERING_DEPENDENCIES: &str = "engine_processor_ordering_dependencies";

const MIX_PERIOD: u64 = 20;
const HIGH_CU_SLOTS: u64 = 2;
const SIMPLE_SLOTS: u64 = 9;
const READ_WRITE_SLOTS: u64 = MIX_PERIOD - HIGH_CU_SLOTS - SIMPLE_SLOTS;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Mode {
    HighCu,
    SimpleWrite,
    ReadWrite,
}

impl Mode {
    const ALL: [Mode; 3] = [Mode::HighCu, Mode::SimpleWrite, Mode::ReadWrite];

    fn label(self) -> &'static str {
        match self {
            Mode::HighCu => "high cu",
            Mode::SimpleWrite => "simple write",
            Mode::ReadWrite => "read write",
        }
    }
}

fn mode_for(id: u64) -> Mode {
    let slot = (id - 1) % MIX_PERIOD;
    if slot < HIGH_CU_SLOTS {
        Mode::HighCu
    } else if slot < HIGH_CU_SLOTS + SIMPLE_SLOTS {
        Mode::SimpleWrite
    } else {
        Mode::ReadWrite
    }
}

fn mode_count(mode: Mode, total: u64) -> u64 {
    let (offset, width) = match mode {
        Mode::HighCu => (0, HIGH_CU_SLOTS),
        Mode::SimpleWrite => (HIGH_CU_SLOTS, SIMPLE_SLOTS),
        Mode::ReadWrite => (HIGH_CU_SLOTS + SIMPLE_SLOTS, READ_WRITE_SLOTS),
    };
    let periods = total / MIX_PERIOD;
    let tail = (total % MIX_PERIOD).saturating_sub(offset).min(width);
    periods * width + tail
}

fn lane_of(id: u64, lanes: u64) -> u64 {
    (id - 1) % lanes
}

fn id_for(round: u64, lane: u64, lanes: u64) -> u64 {
    round * lanes + lane + 1
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

/// Worker-owned contiguous lanes; each worker advances its own round counter.
#[derive(Clone, Copy)]
struct LaneSpan {
    lo: u64,
    len: u64,
}

impl LaneSpan {
    fn jobs(&self, lanes: u64, total: u64) -> u64 {
        let full = total / lanes;
        let tail = (total % lanes).saturating_sub(self.lo).min(self.len);
        full * self.len + tail
    }

    fn job_id(&self, job: u64, lanes: u64) -> u64 {
        id_for(job / self.len, self.lo + job % self.len, lanes)
    }
}

struct Profile {
    name: &'static str,
    payers: usize,
    lanes: u64,
    read_span: usize,
    iterations: u64,
    rate: u32,
    threads: usize,
    high_cu_iters: u32,
}

impl Profile {
    fn concurrency_per_thread(&self) -> usize {
        (self.lanes as usize).div_ceil(self.threads.max(1)).max(1)
    }
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 16,
    lanes: 63,
    read_span: 3,
    iterations: 63_000,
    rate: 600,
    threads: 1,
    high_cu_iters: 180,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 48,
    lanes: 511,
    read_span: 4,
    iterations: 1_022_000,
    rate: 4_000,
    threads: 4,
    high_cu_iters: 180,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

fn build_ixs(id: u64, pool: &[Pubkey], read_span: usize, high_cu_iters: u32) -> Vec<Instruction> {
    let lanes = pool.len() as u64;
    let lane = lane_of(id, lanes);
    let target = pool[lane as usize];
    match mode_for(id) {
        Mode::SimpleWrite => vec![build::simple_byte_set(id, &[target])],
        Mode::HighCu => vec![
            causal::compute_unit_limit(causal::CU_LIMIT),
            build::expensive_hash_compute(id, causal::HASH_INIT, high_cu_iters, &[target]),
        ],
        Mode::ReadWrite => {
            let sources: Vec<Pubkey> = (1..=read_span as u64)
                .map(|step| pool[((lane + step) % lanes) as usize])
                .collect();
            vec![build::multi_account_read(id, target, &sources)]
        }
    }
}

#[derive(Clone)]
enum Limit {
    Iterations(u64),
    Until { deadline: Instant, stop: watch::Receiver<bool> },
}

struct Executed {
    outcome: RunOutcome,
    last_ids: Vec<Option<u64>>,
    counts: [u64; 3],
}

struct ExecuteConfig {
    lanes: u64,
    limit: Limit,
    read_span: usize,
    high_cu_iters: u32,
    rate: u32,
    threads: usize,
    concurrency: usize,
}

async fn execute(
    er_rpc_url: Url,
    config: ExecuteConfig,
    pool: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
) -> Result<Executed> {
    let threads = config.threads.max(1);
    let spans: Vec<LaneSpan> = split_iterations(config.lanes, threads)
        .into_iter()
        .map(|(lo, len)| LaneSpan { lo, len })
        .take_while(|span| match config.limit {
            Limit::Iterations(total) => span.jobs(config.lanes, total) > 0,
            Limit::Until { .. } => true,
        })
        .collect();
    let rates = Pacing::PerSecond(config.rate).partition(spans.len())?;
    let lanes = config.lanes;
    let limit = config.limit;
    let concurrency = split_budget(config.concurrency as u64, spans.len());
    let read_span = config.read_span;
    let high_cu_iters = config.high_cu_iters;
    let outcomes = spawn_workers(spans.len(), move |worker| {
        let index = worker.index;
        let span = spans[index];
        let rate = rates[index];
        let concurrency = concurrency[index] as usize;
        let limit = limit.clone();
        let er_rpc_url = er_rpc_url.clone();
        let pool = pool.clone();
        let payer_bytes = payer_bytes.clone();
        async move {
            let senders = prep::worker_senders(&er_rpc_url, &payer_bytes, index, threads);
            let locks: Rc<Vec<Arc<Mutex<()>>>> =
                Rc::new((0..span.len).map(|_| Arc::new(Mutex::new(()))).collect());
            let request = |iteration| {
                let id = span.job_id(iteration - 1, lanes);
                let lane_index = (lane_of(id, lanes) - span.lo) as usize;
                let ixs = build_ixs(id, &pool, read_span, high_cu_iters);
                let sender = senders[(id as usize) % senders.len()].clone();
                let locks = locks.clone();
                async move {
                    let lane = locks[lane_index].clone().lock_owned().await;
                    let send = sender.staged(ixs).await?;
                    Ok(SendTransaction(move |sent: Instant| async move {
                        let _lane = lane;
                        send.deliver(sent).await
                    }))
                }
            };
            let outcome = match limit {
                Limit::Iterations(total) => {
                    execute_raw(
                        RunConfig {
                            iterations: span.jobs(lanes, total),
                            rate,
                            concurrency,
                        },
                        request,
                    )
                    .await?
                }
                Limit::Until { deadline, stop } => {
                    execute_for_raw(rate, concurrency, deadline, stop, request).await?
                }
            };
            Ok((span, outcome))
        }
    })
    .join_async()
    .await?;
    Ok(summarize(lanes, outcomes))
}

// Timed workers stop at different rounds, so their IDs are not one global prefix.
// Count admitted IDs (including failed sends); delivery checks reject failures.
fn summarize(lanes: u64, outcomes: Vec<(LaneSpan, RawRunOutcome)>) -> Executed {
    let mut last_ids = vec![None; lanes as usize];
    let mut counts = [0; 3];
    for (span, outcome) in &outcomes {
        let admitted = outcome.delivered + outcome.failed;
        for offset in 0..span.len {
            let lane = span.lo + offset;
            let rounds = admitted / span.len + u64::from(offset < admitted % span.len);
            if rounds > 0 {
                last_ids[lane as usize] = Some(id_for(rounds - 1, lane, lanes));
            }
            // Coprime lane counts visit every mix slot once per period.
            for round in 0..MIX_PERIOD {
                let count = rounds / MIX_PERIOD + u64::from(round < rounds % MIX_PERIOD);
                counts[mode_for(id_for(round, lane, lanes)) as usize] += count;
            }
        }
    }
    Executed {
        outcome: merge_outcomes(outcomes.into_iter().map(|(_, outcome)| outcome)),
        last_ids,
        counts,
    }
}

struct DrainState {
    elapsed: Duration,
    blocked: f64,
    busy: f64,
    pending: f64,
}

async fn drain(er: &ErCtx, target: f64) -> Result<DrainState> {
    let started = Instant::now();
    crate::await_executed(er, target, DRAIN_TIMEOUT).await?;
    check::poll(
        "the execution pipeline goes idle (nothing blocked, busy or pending)",
        SETTLE_TIMEOUT,
        || async {
            let Ok(metrics) = er.scrape_metrics().await else {
                return false;
            };
            [BLOCKED_TRANSACTIONS, BUSY_EXECUTORS, PENDING_TRANSACTIONS]
                .iter()
                .all(|name| metrics.value_sum(name).unwrap_or_default() == 0.0)
        },
    )
    .await?;
    let metrics = er.scrape_metrics().await?;
    Ok(DrainState {
        elapsed: started.elapsed(),
        blocked: metrics.value_sum(BLOCKED_TRANSACTIONS).unwrap_or_default(),
        busy: metrics.value_sum(BUSY_EXECUTORS).unwrap_or_default(),
        pending: metrics.value_sum(PENDING_TRANSACTIONS).unwrap_or_default(),
    })
}

async fn verify_final_state(
    er: &ErCtx,
    pool: &[Pubkey],
    last_ids: &[Option<u64>],
    high_cu_iters: u32,
    read_span: usize,
) -> Result<()> {
    let expected_hash = hash_chain(causal::HASH_INIT.to_bytes(), high_cu_iters);
    let expected_sum = (read_span as u64) * u64::from(ACCOUNT_SPACE);
    for (index, pda) in pool.iter().enumerate() {
        let Some(last_id) = last_ids[index] else {
            continue;
        };
        let account = er.account(pda).await?.ok_or("pda not on er")?;
        let data = &account.data;
        let mode = mode_for(last_id);
        check_eq!(
            crate::account_update_id(data),
            Some(last_id),
            "lane {index} must hold id {last_id}, the last {} write",
            mode.label()
        )?;
        match mode {
            Mode::SimpleWrite => {
                let filled = data[layout::DATA_OFFSET..]
                    .chunks_exact(layout::ID_SIZE)
                    .all(|chunk| chunk == last_id.to_le_bytes());
                check!(
                    filled,
                    "lane {index} must be filled with the repeated id \
                     {last_id} — the last simple write did not land"
                )?;
            }
            Mode::HighCu => {
                let hash = &data[layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE];
                check_eq!(
                    hash,
                    expected_hash,
                    "lane {index} must hold the {high_cu_iters}-iteration \
                     hash chain from id {last_id}"
                )?;
            }
            Mode::ReadWrite => {
                let offset = layout::ID_OFFSET + layout::ID_SIZE;
                let sum = &data[offset..offset + 8];
                check_eq!(
                    sum,
                    expected_sum.to_le_bytes(),
                    "lane {index} must hold the {read_span}-account read-set \
                     size from id {last_id}"
                )?;
            }
        }
    }
    Ok(())
}

pub struct MixedSustainedLoad {
    er: ErCtx,
    workload: Workload,
    started: Instant,
}

impl Scenario<ScenarioReport> for MixedSustainedLoad {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let started = Instant::now();
        let workload = Workload::setup(&base, &er).await?;
        Ok(Self { er, workload, started })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let (report, outcome) = self.workload.run(&self.er, None, self.started).await?;
        outcome.map(|()| report)
    }
}

pub(crate) struct Workload {
    profile: &'static Profile,
    pool: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
}

impl Workload {
    pub(crate) async fn setup(base: &BaseCtx, er: &ErCtx) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);

        check_eq!(
            gcd(profile.lanes, MIX_PERIOD),
            1,
            "profile {}: lanes must be coprime with the {MIX_PERIOD}-slot mix \
             period so every lane sees every mode",
            profile.name
        )?;
        check!(
            profile.lanes >= profile.threads as u64,
            "profile {}: lanes ({}) must cover every one of the {} driver threads",
            profile.name,
            profile.lanes,
            profile.threads
        )?;
        check!(
            profile.payers >= profile.threads,
            "profile {}: payers ({}) must cover every one of the {} driver \
             threads — a thread with no payer cannot send",
            profile.name,
            profile.payers,
            profile.threads
        )?;
        check!(
            (profile.read_span as u64) < profile.lanes,
            "profile {}: read span ({}) must stay below the lane count ({})",
            profile.name,
            profile.read_span,
            profile.lanes
        )?;

        let payers = base.funded_payers(profile.payers, PAYER_LAMPORTS).await?;
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base, &payers, profile.lanes as usize, true)
            .await?;
        er.await_clones(&pool, ACCOUNT_SPACE as usize, CLONE_TIMEOUT).await?;

        let pool = Arc::new(pool);
        let payer_bytes = prep::payer_bytes(&payers);

        Ok(Self { profile, pool, payer_bytes })
    }

    pub(crate) async fn run(
        self,
        er: &ErCtx,
        load: Option<&Options>,
        started: Instant,
    ) -> Result<(ScenarioReport, Result<()>)> {
        let Self { profile, pool, payer_bytes } = self;
        let er_pid = er.pid();
        let rate = load.and_then(|load| load.rate).map_or(profile.rate, |rate| rate.get());
        let concurrency = load.and_then(|load| load.concurrency).map_or(
            profile.concurrency_per_thread() * profile.threads,
            |concurrency| concurrency.get(),
        );
        let threads = profile.threads.min(rate as usize).min(concurrency);

        let before = er.scrape_metrics().await?;
        let engine_txs_before = before.get(metrics::ENGINE_TRANSACTIONS).ok_or(format!(
            "the ER exposes no {} metric to drain against",
            metrics::ENGINE_TRANSACTIONS
        ))?;
        let cpu_before = host::cpu_sample(er_pid)?;
        let rss_before = host::rss_kb(er_pid)?;

        let setup = started.elapsed();
        let limit = match load {
            Some(load) => {
                check!(!load.duration.is_zero(), "load duration must be positive")?;
                let deadline = Instant::now()
                    .checked_add(load.duration)
                    .ok_or("load duration exceeds monotonic clock range")?;
                info!(rate, concurrency, ?load.duration, "load generation started");
                Limit::Until {
                    deadline,
                    stop: load.stop.clone(),
                }
            }
            None => Limit::Iterations(profile.iterations),
        };
        let executed = execute(
            er.api().url().clone(),
            ExecuteConfig {
                lanes: profile.lanes,
                limit,
                read_span: profile.read_span,
                high_cu_iters: profile.high_cu_iters,
                rate,
                threads,
                concurrency,
            },
            pool.clone(),
            payer_bytes,
        )
        .await?;

        let Executed { outcome, last_ids, counts } = executed;
        let total = outcome.delivered + outcome.failed;
        if load.is_some() {
            info!(
                outcome.delivered,
                outcome.failed,
                delivered_tps = outcome.achieved_rps(),
                "load requests drained"
            );
        }
        let mut report = ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting(
                "mix",
                "10% high cu / 45% simple write / 45% read write, no commits",
            )
            .setting("workload txs", total)
            .setting("target tps", rate)
            .setting("lanes", profile.lanes)
            .setting("read span", profile.read_span)
            .setting("payers", profile.payers)
            .setting("driver threads", threads)
            .setting("total concurrency", concurrency)
            .setting("high cu sha256 iters", profile.high_cu_iters)
            .observe("delivery us", Unit::Micros, outcome.delivery)
            .client_side(&outcome)
            .metric("target tps", Unit::Tps, f64::from(rate))
            .metric("delivered tps", Unit::Tps, outcome.achieved_rps())
            .metric("delivered txs", Unit::Count, outcome.delivered as f64)
            .metric("failed txs", Unit::Count, outcome.failed as f64);
        if let Some(load) = load {
            report = report
                .setting("requested duration s", load.duration.as_secs_f64())
                .metric("setup s", Unit::Seconds, setup.as_secs_f64())
                .metric(
                    "generation s",
                    Unit::Seconds,
                    outcome.admission_wall.as_secs_f64(),
                )
                .metric(
                    "client drain s",
                    Unit::Seconds,
                    outcome.wall.saturating_sub(outcome.admission_wall).as_secs_f64(),
                );
        } else {
            report = report.setting("concurrency per thread", profile.concurrency_per_thread());
        }
        for (mode, count) in Mode::ALL.into_iter().zip(counts) {
            report = report.metric(format!("{} txs", mode.label()), Unit::Count, count as f64);
        }
        let validation = async {
            if load.is_none() {
                check_eq!(
                    total,
                    profile.iterations,
                    "every benchmark iteration must be admitted"
                )?;
                for mode in Mode::ALL {
                    check_eq!(
                        counts[mode as usize],
                        mode_count(mode, total),
                        "worker mix accounting must match the benchmark prefix"
                    )?;
                }
            }
            check_eq!(
                counts.iter().sum::<u64>(),
                total,
                "the per-mode split must account for every transaction"
            )?;
            check_eq!(
                outcome.failed,
                0,
                "deliveries failed: {:?}",
                outcome.first_error
            )?;
            check_eq!(
                outcome.delivered,
                total,
                "every workload transaction must be delivered"
            )?;

            let drained = drain(er, engine_txs_before + total as f64).await?;
            let cpu_after = host::cpu_sample(er_pid)?;
            let rss_after = host::rss_kb(er_pid)?;
            let after = er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);

            let engine_txs = delta.counter(metrics::ENGINE_TRANSACTIONS).unwrap_or_default();
            let excess = engine_txs - total as f64;
            let rpc_handled = delta.counter(metrics::RPC_HANDLED_TRANSACTIONS);
            let rpc_accepted = delta.counter(metrics::RPC_ACCEPTED_TRANSACTIONS);
            let wall = outcome.wall + drained.elapsed;
            let executed_tps = total as f64 / wall.as_secs_f64();
            let thread_cores = cpu_after.thread_cores_since(&cpu_before);
            let observations = ScenarioReport::ok()
                .metric("executed tps", Unit::Tps, executed_tps)
                .metric("engine txs", Unit::Count, engine_txs)
                .metric("engine txs beyond workload", Unit::Count, excess)
                .metric_if("rpc handled txs", Unit::Count, rpc_handled)
                .metric_if("rpc accepted txs", Unit::Count, rpc_accepted)
                .metric("drain s", Unit::Seconds, drained.elapsed.as_secs_f64())
                .metric("blocked at drain", Unit::Count, drained.blocked)
                .metric("busy at drain", Unit::Count, drained.busy)
                .metric("pending at drain", Unit::Count, drained.pending)
                .metric(
                    "validator cores",
                    Unit::Count,
                    cpu_after.cores_since(&cpu_before),
                )
                .metric(
                    "top thread cores",
                    Unit::Count,
                    thread_cores.first().copied().unwrap_or(0.0),
                )
                .metric("rss growth mib", Unit::Count, {
                    let growth = rss_after.saturating_sub(rss_before);
                    growth as f64 / 1024.0
                })
                .metric_if(
                    "ordering dependencies",
                    Unit::Count,
                    delta.counter(ORDERING_DEPENDENCIES),
                );
            report.measurements.extend(observations.measurements);
            check!(
                engine_txs >= total as f64,
                "the engine executed {engine_txs:.0} transactions, fewer than the \
             {total} delivered by the workload"
            )?;

            if let Some(rpc_handled) = rpc_handled {
                check_eq!(
                    rpc_handled,
                    total as f64,
                    "the RPC layer handled {rpc_handled:.0} sendTransaction calls, \
                 not the {total} the workload submitted"
                )?;
            }
            if let Some(rpc_accepted) = rpc_accepted {
                check_eq!(
                    rpc_accepted,
                    total as f64,
                    "the RPC layer accepted {rpc_accepted:.0} of the {total} \
                 transactions the workload submitted — the rest failed decode, \
                 sanitization or account ensure before reaching the engine"
                )?;
            }

            verify_final_state(
                er,
                &pool,
                &last_ids,
                profile.high_cu_iters,
                profile.read_span,
            )
            .await?;

            debug!(
                total,
                delivered_tps = outcome.achieved_rps(),
                executed_tps,
                "sustained workload finished",
            );

            Ok(())
        }
        .await;
        Ok((report, validation))
    }
}
