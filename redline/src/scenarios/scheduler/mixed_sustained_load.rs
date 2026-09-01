use std::{
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, host, prep,
    profile::{self, ProfileValues},
    runner::{
        execute_raw, panic_message, RawRunOutcome, RunConfig, RunOutcome,
    },
    topology, BaseCtx, ChainCtx, ErClient, ErCtx, MetricsDelta, Result,
    Scenario, ScenarioReport, TxSender,
};

use crate::program::{instruction::build, layout, utils::hash_chain};

const PAYER_LAMPORTS: u64 = 4_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(60);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1_800);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);
const HASH_INIT: Pubkey = Pubkey::new_from_array([7u8; 32]);
const CU_LIMIT: u32 = 1_400_000;

use crate::metrics::{ENGINE_TRANSACTIONS, RPC_TRANSACTIONS};

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

fn last_id_for_lane(lane: u64, lanes: u64, total: u64) -> Option<u64> {
    let rounds = total / lanes + u64::from(lane < total % lanes);
    (rounds > 0).then(|| id_for(rounds - 1, lane, lanes))
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

#[derive(Clone, Copy)]
struct LaneSpan {
    lo: u64,
    len: u64,
}

fn lane_spans(lanes: u64, threads: usize) -> Vec<LaneSpan> {
    let threads = threads.max(1) as u64;
    let base = lanes / threads;
    let remainder = lanes % threads;
    let mut lo = 0;
    (0..threads)
        .map(|index| {
            let len = base + u64::from(index < remainder);
            let span = LaneSpan { lo, len };
            lo += len;
            span
        })
        .filter(|span| span.len > 0)
        .collect()
}

impl LaneSpan {
    fn jobs(&self, lanes: u64, total: u64) -> u64 {
        let full = total / lanes;
        let tail = (total % lanes).saturating_sub(self.lo).min(self.len);
        full * self.len + tail
    }

    fn job_id(&self, job: u64, lanes: u64, total: u64) -> u64 {
        let full = total / lanes;
        let body = full * self.len;
        if job < body {
            id_for(job / self.len, self.lo + job % self.len, lanes)
        } else {
            id_for(full, self.lo + (job - body), lanes)
        }
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

const SOAK: Profile = Profile {
    name: "soak",
    payers: 64,
    lanes: 2_047,
    read_span: 4,
    iterations: 40_000_000,
    rate: 12_000,
    threads: 8,
    high_cu_iters: 180,
};

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
    soak: Some(SOAK),
    deep: None,
};

fn compute_unit_limit(limit: u32) -> Instruction {
    let mut data = vec![2u8];
    data.extend_from_slice(&limit.to_le_bytes());
    Instruction {
        program_id: sdk_ids::compute_budget::ID,
        accounts: Vec::new(),
        data,
    }
}

fn build_ixs(
    id: u64,
    pool: &[Pubkey],
    read_span: usize,
    high_cu_iters: u32,
) -> Vec<Instruction> {
    let lanes = pool.len() as u64;
    let lane = lane_of(id, lanes);
    let target = pool[lane as usize];
    match mode_for(id) {
        Mode::SimpleWrite => vec![build::simple_byte_set(id, &[target])],
        Mode::HighCu => vec![
            compute_unit_limit(CU_LIMIT),
            build::expensive_hash_compute(
                id,
                HASH_INIT,
                high_cu_iters,
                &[target],
            ),
        ],
        Mode::ReadWrite => {
            let sources: Vec<Pubkey> = (1..=read_span as u64)
                .map(|step| pool[((lane + step) % lanes) as usize])
                .collect();
            vec![build::multi_account_read(id, target, &sources)]
        }
    }
}

struct ExecuteConfig {
    lanes: u64,
    total: u64,
    read_span: usize,
    high_cu_iters: u32,
    rate: u32,
    threads: usize,
    concurrency: usize,
}

fn execute(
    er_rpc_url: String,
    config: ExecuteConfig,
    pool: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
) -> Result<RunOutcome> {
    let threads = config.threads.max(1);
    let spans = lane_spans(config.lanes, threads);
    let rate = (config.rate / threads as u32).max(1);
    let (sender, receiver) = std::sync::mpsc::channel();

    let mut handles = Vec::with_capacity(spans.len());
    for (index, span) in spans.into_iter().enumerate() {
        let jobs = span.jobs(config.lanes, config.total);
        if jobs == 0 {
            continue;
        }
        let er_rpc_url = er_rpc_url.clone();
        let pool = pool.clone();
        let payer_bytes = payer_bytes.clone();
        let outcome_sender = sender.clone();
        let lanes = config.lanes;
        let total = config.total;
        let read_span = config.read_span;
        let high_cu_iters = config.high_cu_iters;
        let concurrency = config.concurrency;
        handles.push(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("driver runtime build is infallible");
            let local = tokio::task::LocalSet::new();
            let outcome = runtime.block_on(local.run_until(async move {
                let client = ErClient::new(er_rpc_url);
                let senders: Vec<TxSender> = payer_bytes
                    .iter()
                    .enumerate()
                    .filter(|(payer_index, _)| payer_index % threads == index)
                    .map(|(_, bytes)| {
                        let payer = Keypair::try_from(&bytes[..])
                            .expect("payer bytes round-trip");
                        client.sender(Rc::new(payer))
                    })
                    .collect();
                let locks: Rc<Vec<tokio::sync::Mutex<()>>> = Rc::new(
                    (0..span.len)
                        .map(|_| tokio::sync::Mutex::new(()))
                        .collect(),
                );
                execute_raw(
                    RunConfig {
                        iterations: jobs,
                        rate,
                        concurrency,
                    },
                    |iteration| {
                        let id = span.job_id(iteration - 1, lanes, total);
                        let lane_index =
                            (lane_of(id, lanes) - span.lo) as usize;
                        let ixs =
                            build_ixs(id, &pool, read_span, high_cu_iters);
                        let sender =
                            senders[(id as usize) % senders.len()].clone();
                        let locks = locks.clone();
                        async move {
                            let _lane = locks[lane_index].lock().await;
                            sender.submit(&ixs).await.map(|_| ())
                        }
                    },
                )
                .await
            }));
            let _ = outcome_sender.send(outcome);
        }));
    }
    drop(sender);

    let workers = handles.len();
    let mut outcomes: Vec<RawRunOutcome> = Vec::new();
    while let Ok(outcome) = receiver.recv() {
        outcomes.push(outcome);
    }
    let mut first_panic = None;
    for handle in handles {
        if let Err(payload) = handle.join() {
            first_panic.get_or_insert_with(|| panic_message(payload));
        }
    }
    if let Some(panic) = first_panic {
        return Err(format!("driver worker thread panicked: {panic}").into());
    }
    if outcomes.len() != workers {
        return Err("a driver worker thread stopped without an outcome".into());
    }

    Ok(outcomes
        .into_iter()
        .reduce(|mut merged, outcome| {
            merged.merge(outcome);
            merged
        })
        .map(RawRunOutcome::finalize)
        .unwrap_or_default())
}

struct DrainState {
    elapsed: Duration,
    blocked: f64,
    busy: f64,
    pending: f64,
}

async fn drain(er: &ErCtx, target: f64) -> Result<DrainState> {
    let started = Instant::now();
    check::poll(
        &format!("the engine transaction count reaches {target:.0}"),
        DRAIN_TIMEOUT,
        || async {
            matches!(
                er.scrape_metrics()
                    .await
                    .ok()
                    .and_then(|m| m.get(ENGINE_TRANSACTIONS)),
                Some(count) if count >= target
            )
        },
    )
    .await?;
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
    total: u64,
    high_cu_iters: u32,
    read_span: usize,
) -> Result<()> {
    let lanes = pool.len() as u64;
    let expected_hash = hash_chain(HASH_INIT.to_bytes(), high_cu_iters);
    let expected_sum = (read_span as u64) * u64::from(crate::ACCOUNT_SPACE);
    for (index, pda) in pool.iter().enumerate() {
        let Some(last_id) = last_id_for_lane(index as u64, lanes, total) else {
            continue;
        };
        let account = er.account(pda).await?.ok_or("pda not on er")?;
        let data = &account.data;
        let id_bytes =
            &data[layout::ID_OFFSET..layout::ID_OFFSET + layout::ID_SIZE];
        let mode = mode_for(last_id);
        check_eq!(
            id_bytes,
            last_id.to_le_bytes(),
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
                let hash = &data[layout::HASH_OFFSET
                    ..layout::HASH_OFFSET + layout::HASH_SIZE];
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

pub struct MixedSustainedLoad;

#[async_trait(?Send)]
impl Scenario for MixedSustainedLoad {
    fn name(&self) -> &str {
        "redline/mixed_sustained_load"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);

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

        let total = profile.iterations;
        let counts: Vec<(Mode, u64)> = Mode::ALL
            .into_iter()
            .map(|mode| (mode, mode_count(mode, total)))
            .collect();
        let counted: u64 = counts.iter().map(|(_, count)| count).sum();
        check_eq!(
            counted,
            total,
            "the per-mode split must account for every transaction"
        )?;

        let payers =
            prep::funded_payers(base, profile.payers, PAYER_LAMPORTS).await?;
        let prep_started = Instant::now();
        let pool = crate::init_delegated_accounts_batched(
            base,
            &payers,
            profile.lanes as usize,
            crate::ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        for pda in &pool {
            check::poll(
                &format!("the ER clones the delegated pda {pda}"),
                CLONE_TIMEOUT,
                || async {
                    matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                },
            )
            .await?;
        }
        eprintln!(
            "[redsuite] {}: prepped {} lanes in {:.1} s",
            self.name(),
            profile.lanes,
            prep_started.elapsed().as_secs_f64(),
        );

        let pool = Arc::new(pool);
        let payer_bytes: Arc<Vec<[u8; 64]>> =
            Arc::new(payers.iter().map(|payer| payer.to_bytes()).collect());
        let er_pid = topology::current_state()
            .ok_or("no shared stack state")?
            .er_pid;

        let before = er.scrape_metrics().await?;
        let engine_txs_before =
            before.get(ENGINE_TRANSACTIONS).ok_or(format!(
            "the ER exposes no {ENGINE_TRANSACTIONS} metric to drain against"
        ))?;
        let cpu_before = host::cpu_sample(er_pid)?;
        let rss_before = host::rss_kb(er_pid)?;

        let outcome = execute(
            er.api().url().to_owned(),
            ExecuteConfig {
                lanes: profile.lanes,
                total,
                read_span: profile.read_span,
                high_cu_iters: profile.high_cu_iters,
                rate: profile.rate,
                threads: profile.threads,
                concurrency: profile.concurrency_per_thread(),
            },
            pool.clone(),
            payer_bytes,
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

        let engine_txs = delta.counter(ENGINE_TRANSACTIONS).unwrap_or_default();
        check!(
            engine_txs >= total as f64,
            "the engine executed {engine_txs:.0} transactions, fewer than the \
             {total} delivered by the workload"
        )?;
        let excess = engine_txs - total as f64;

        let rpc_txs = delta.counter(RPC_TRANSACTIONS);
        if let Some(rpc_txs) = rpc_txs {
            check_eq!(
                rpc_txs,
                total as f64,
                "the RPC layer accepted {rpc_txs:.0} transactions, not the \
                 {total} the workload submitted"
            )?;
        }

        verify_final_state(
            er,
            &pool,
            total,
            profile.high_cu_iters,
            profile.read_span,
        )
        .await?;

        let wall = outcome.wall + drained.elapsed;
        let executed_tps = total as f64 / wall.as_secs_f64();
        let thread_cores = cpu_after.thread_cores_since(&cpu_before);
        eprintln!(
            "[redsuite] {}: {total} txs at {:.0} tps offered / {:.0} tps \
             delivered / {:.0} tps executed (drain {:.1} s), p50 {} us / \
             p95 {} us, cores {:.2}, rss {} -> {} MiB",
            self.name(),
            profile.rate,
            outcome.achieved_rps(),
            executed_tps,
            drained.elapsed.as_secs_f64(),
            outcome.delivery.median,
            outcome.delivery.quantile95,
            cpu_after.cores_since(&cpu_before),
            rss_before / 1024,
            rss_after / 1024,
        );

        let mut report = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting(
                "mix",
                "10% high cu / 45% simple write / 45% read write, no commits",
            )
            .setting("workload txs", total)
            .setting("target tps", profile.rate)
            .setting("lanes", profile.lanes)
            .setting("read span", profile.read_span)
            .setting("payers", profile.payers)
            .setting("driver threads", profile.threads)
            .setting("concurrency per thread", profile.concurrency_per_thread())
            .setting("high cu sha256 iters", profile.high_cu_iters)
            .observe("delivery us", Unit::Micros, outcome.delivery)
            .metric("target tps", Unit::Tps, f64::from(profile.rate))
            .metric("delivered tps", Unit::Tps, outcome.achieved_rps())
            .metric("executed tps", Unit::Tps, executed_tps)
            .metric("delivered txs", Unit::Count, outcome.delivered as f64)
            .metric("failed txs", Unit::Count, outcome.failed as f64)
            .metric("engine txs", Unit::Count, engine_txs)
            .metric("engine txs beyond workload", Unit::Count, excess)
            .metric_if("rpc accepted txs", Unit::Count, rpc_txs)
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
        for (mode, count) in counts {
            report = report.metric(
                format!("{} txs", mode.label()),
                Unit::Count,
                count as f64,
            );
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mix_holds_the_qualified_split() {
        let total = MIX_PERIOD * 1_000;
        assert_eq!(mode_count(Mode::HighCu, total), total / 10);
        assert_eq!(mode_count(Mode::SimpleWrite, total), total * 45 / 100);
        assert_eq!(mode_count(Mode::ReadWrite, total), total * 45 / 100);
    }

    #[test]
    fn per_mode_counts_reconcile_with_the_total() {
        for total in [1, 7, 20, 21, 63_000, 40_000_000] {
            let counted: u64 =
                Mode::ALL.into_iter().map(|m| mode_count(m, total)).sum();
            assert_eq!(counted, total, "total {total}");
            let walked = (1..=total.min(10_000))
                .filter(|id| mode_for(*id) == Mode::HighCu)
                .count() as u64;
            assert_eq!(walked, mode_count(Mode::HighCu, total.min(10_000)));
        }
    }

    #[test]
    fn every_lane_sees_every_mode() {
        for lanes in [LITE.lanes, FULL.lanes, SOAK.lanes] {
            assert_eq!(gcd(lanes, MIX_PERIOD), 1, "lanes {lanes}");
            let modes: std::collections::HashSet<Mode> = (0..MIX_PERIOD)
                .map(|round| mode_for(id_for(round, 0, lanes)))
                .collect();
            assert_eq!(modes.len(), 3, "lanes {lanes}");
        }
    }

    #[test]
    fn lane_spans_partition_the_workload_exactly() {
        for (lanes, threads, total) in [
            (63u64, 1usize, 63_000u64),
            (511, 4, 1_022_000),
            (2_047, 8, 40_000_000),
            (63, 3, 100),
        ] {
            let spans = lane_spans(lanes, threads);
            assert_eq!(
                spans.iter().map(|span| span.len).sum::<u64>(),
                lanes,
                "spans must cover every lane"
            );
            let jobs: u64 =
                spans.iter().map(|span| span.jobs(lanes, total)).sum();
            assert_eq!(jobs, total, "lanes {lanes} threads {threads}");

            let mut seen = vec![false; total as usize];
            for span in &spans {
                for job in 0..span.jobs(lanes, total) {
                    let id = span.job_id(job, lanes, total);
                    assert!(id >= 1 && id <= total, "id {id} out of range");
                    assert!(!seen[(id - 1) as usize], "id {id} driven twice");
                    seen[(id - 1) as usize] = true;
                    assert!(
                        (span.lo..span.lo + span.len)
                            .contains(&lane_of(id, lanes)),
                        "id {id} left its span"
                    );
                }
            }
            assert!(seen.into_iter().all(|hit| hit), "an id was never driven");
        }
    }

    #[test]
    fn a_lanes_ids_ascend_with_its_jobs() {
        let (lanes, total) = (63u64, 1_000u64);
        let span = LaneSpan { lo: 0, len: lanes };
        let mut last = vec![0u64; lanes as usize];
        for job in 0..span.jobs(lanes, total) {
            let id = span.job_id(job, lanes, total);
            let lane = lane_of(id, lanes) as usize;
            assert!(id > last[lane], "lane {lane} went backwards at job {job}");
            last[lane] = id;
        }
        for (lane, seen) in last.iter().enumerate() {
            assert_eq!(
                Some(*seen),
                last_id_for_lane(lane as u64, lanes, total),
                "lane {lane}"
            );
        }
    }
}
