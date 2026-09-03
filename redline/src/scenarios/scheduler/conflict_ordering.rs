use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures_util::future::join_all;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    api, check, check_eq, prep,
    profile::{self, ProfileValues},
    report,
    runner::{execute_until_raw, panic_message},
    stats::{ObservationsStats, StreamingStats},
    transport::ws::SignatureConfirmations,
    BaseCtx, ChainCtx, CheckError, ErClient, ErCtx, Metrics, MetricsDelta,
    Result, Scenario, ScenarioReport, TxSender,
};
use signature::Signature;
use signer::Signer;

use crate::program::{
    instruction::build,
    layout,
    utils::{fold_hash, hash_chain},
};

const PAYER_LAMPORTS: u64 = 200_000_000;
const PREP_CHUNK: usize = 32;
const CLONE_TIMEOUT: Duration = Duration::from_secs(60);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(600);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(60);
const WARM_IN_TIMEOUT: Duration = Duration::from_secs(15);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const STOP_POLL: Duration = Duration::from_millis(20);
const CALIBRATION: Duration = Duration::from_secs(3);
const STALL_BOUND: Duration = Duration::from_secs(10);
const CU_LIMIT: u32 = 1_400_000;
const HASH_INIT: Pubkey = Pubkey::new_from_array([7u8; 32]);
const OPEN_CONCURRENCY: usize = 128;
const LANES_PER_EXECUTOR: usize = 12;
const MIN_EXECUTORS: f64 = 2.0;
const STEPS: u64 = 3;
const PAYERS_PER_PAIR: usize = 3;

const TX_COUNT: &str = crate::metrics::ENGINE_TRANSACTIONS;
const BUSY_EXECUTORS: &str = "engine_processor_busy_executors";
const BLOCKED_TRANSACTIONS: &str = "engine_processor_blocked_transactions";
const PENDING_TRANSACTIONS: &str = "engine_ledger_pending_transactions";
const ORDERING_DEPENDENCIES: &str = "engine_processor_ordering_dependencies";
const REQUIRED_METRICS: [&str; 5] = [
    TX_COUNT,
    BUSY_EXECUTORS,
    BLOCKED_TRANSACTIONS,
    PENDING_TRANSACTIONS,
    ORDERING_DEPENDENCIES,
];

struct Profile {
    name: &'static str,
    pairs: usize,
    chains: u64,
    batches: usize,
    heavy_iters: u32,
    independent_accounts: usize,
    independent_iters: u32,
    driver_threads: usize,
    phase_span: Duration,
}

const LITE: Profile = Profile {
    name: "lite",
    pairs: 8,
    chains: 40,
    batches: 3,
    heavy_iters: 180,
    independent_accounts: 256,
    independent_iters: 180,
    driver_threads: 4,
    phase_span: Duration::from_secs(4),
};

const FULL: Profile = Profile {
    name: "full",
    pairs: 32,
    chains: 200,
    batches: 5,
    heavy_iters: 180,
    independent_accounts: 512,
    independent_iters: 180,
    driver_threads: 8,
    phase_span: Duration::from_secs(10),
};

const SOAK: Profile = Profile {
    name: "soak",
    pairs: 64,
    chains: 400,
    batches: 12,
    heavy_iters: 180,
    independent_accounts: 1024,
    independent_iters: 180,
    driver_threads: 8,
    phase_span: Duration::from_secs(20),
};

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
    soak: Some(SOAK),
    deep: None,
};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Step {
    X,
    Y,
    Z,
}

impl Step {
    const ALL: [Step; 3] = [Step::X, Step::Y, Step::Z];

    fn index(self) -> usize {
        match self {
            Step::X => 0,
            Step::Y => 1,
            Step::Z => 2,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Step::X => "X(A)",
            Step::Y => "Y(A,B)",
            Step::Z => "Z(B)",
        }
    }
}

fn heavy_step(chain: u64) -> Step {
    Step::ALL[(chain % STEPS) as usize]
}

#[derive(Clone, Copy)]
struct ChainSpace {
    pairs: u64,
    chains: u64,
}

impl ChainSpace {
    fn step_id(&self, batch: u64, pair: u64, chain: u64, step: Step) -> u64 {
        let chain_ordinal = (batch * self.pairs + pair) * self.chains + chain;
        chain_ordinal * STEPS + step.index() as u64 + 1
    }
}

fn phase_ranges(chains: u64) -> [(u64, u64); 2] {
    let split = chains.div_ceil(2);
    [(0, split), (split, chains)]
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct PairModel {
    a: [u8; 32],
    b: [u8; 32],
    a_id: u64,
    b_id: u64,
}

impl PairModel {
    fn apply(&mut self, step: Step, id: u64, iters: u32) {
        match step {
            Step::X => {
                self.a = fold_hash(id, &[self.a], iters);
                self.a_id = id;
            }
            Step::Y => {
                let merged = fold_hash(id, &[self.a, self.b], iters);
                self.a = merged;
                self.b = merged;
                self.a_id = id;
                self.b_id = id;
            }
            Step::Z => {
                self.b = fold_hash(id, &[self.b], iters);
                self.b_id = id;
            }
        }
    }
}

struct Pair {
    a: Pubkey,
    b: Pubkey,
    senders: Vec<TxSender>,
    model: PairModel,
    steps: Vec<(u64, Step, Signature)>,
}

impl Pair {
    fn accounts(&self, step: Step) -> Vec<Pubkey> {
        match step {
            Step::X => vec![self.a],
            Step::Y => vec![self.a, self.b],
            Step::Z => vec![self.b],
        }
    }
}

fn compute_unit_limit(limit: u32) -> Instruction {
    let mut data = vec![2u8];
    data.extend_from_slice(&limit.to_le_bytes());
    Instruction {
        program_id: sdk_ids::compute_budget::ID,
        accounts: Vec::new(),
        data,
    }
}

fn chain_ixs(id: u64, iters: u32, accounts: &[Pubkey]) -> Vec<Instruction> {
    let fold = build::hash_fold(id, iters, accounts);
    if iters > 0 {
        vec![compute_unit_limit(CU_LIMIT), fold]
    } else {
        vec![fold]
    }
}

fn independent_ixs(id: u64, account: Pubkey, iters: u32) -> Vec<Instruction> {
    vec![
        compute_unit_limit(CU_LIMIT),
        build::expensive_hash_compute(id, HASH_INIT, iters, &[account]),
    ]
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LoadMode {
    Bounded { lanes: usize },
    Open { concurrency: usize },
}

impl LoadMode {
    fn label(self) -> &'static str {
        match self {
            LoadMode::Bounded { .. } => "contention",
            LoadMode::Open { .. } => "backpressure",
        }
    }

    fn shape(self) -> String {
        match self {
            LoadMode::Bounded { lanes } => format!(
                "{lanes} lanes, one unconfirmed high-cu tx each (ready queue \
                 deep, sequencer intake never throttled)"
            ),
            LoadMode::Open { concurrency } => format!(
                "open loop at concurrency {concurrency} (sequencer channel \
                 full, pending-work bound engaged)"
            ),
        }
    }
}

struct LoadOutcome {
    delivered: u64,
    failed: u64,
    first_error: Option<String>,
    delivery: ObservationsStats,
    wall: Duration,
}

struct LoadTally {
    delivered: u64,
    failed: u64,
    first_error: Option<String>,
    delivery: StreamingStats,
    wall: Duration,
}

impl LoadTally {
    fn merge(&mut self, other: LoadTally) {
        self.delivered += other.delivered;
        self.failed += other.failed;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
        self.delivery.merge(other.delivery);
        self.wall = self.wall.max(other.wall);
    }

    fn finalize(self) -> LoadOutcome {
        LoadOutcome {
            delivered: self.delivered,
            failed: self.failed,
            first_error: self.first_error,
            delivery: self.delivery.finalize(false),
            wall: self.wall,
        }
    }
}

struct LoadHandles {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<Result<LoadTally>>>,
}

fn spawn_load(
    er_rpc_url: String,
    er_ws_url: String,
    mode: LoadMode,
    threads: usize,
    accounts: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
    iters: u32,
) -> LoadHandles {
    let stop = Arc::new(AtomicBool::new(false));
    let threads = threads.clamp(1, accounts.len().max(1));
    let handles = (0..threads)
        .map(|thread| {
            let er_rpc_url = er_rpc_url.clone();
            let er_ws_url = er_ws_url.clone();
            let accounts = accounts.clone();
            let payer_bytes = payer_bytes.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("driver runtime build is infallible");
                let local = tokio::task::LocalSet::new();
                runtime.block_on(local.run_until(async move {
                    let client = ErClient::new(er_rpc_url);
                    let (lane_limit, concurrency) = match mode {
                        LoadMode::Bounded { lanes } => (lanes, 0),
                        LoadMode::Open { concurrency } => {
                            (accounts.len(), (concurrency / threads).max(1))
                        }
                    };
                    let mut lanes = Vec::new();
                    for index in (thread..lane_limit.min(accounts.len()))
                        .step_by(threads)
                    {
                        let payer = Keypair::try_from(&payer_bytes[index][..])
                            .expect("payer bytes round-trip");
                        lanes.push((
                            accounts[index],
                            client.sender(Rc::new(payer)),
                        ));
                    }
                    if lanes.is_empty() {
                        return Ok(LoadTally {
                            delivered: 0,
                            failed: 0,
                            first_error: None,
                            delivery: StreamingStats::new(),
                            wall: Duration::ZERO,
                        });
                    }
                    match mode {
                        LoadMode::Open { .. } => {
                            open_load(lanes, iters, concurrency, stop).await
                        }
                        LoadMode::Bounded { .. } => {
                            bounded_load(&er_ws_url, lanes, iters, stop).await
                        }
                    }
                }))
            })
        })
        .collect();
    LoadHandles {
        stop,
        threads: handles,
    }
}

async fn open_load(
    lanes: Vec<(Pubkey, TxSender)>,
    iters: u32,
    concurrency: usize,
    stop: Arc<AtomicBool>,
) -> Result<LoadTally> {
    let stop_cell = Rc::new(Cell::new(false));
    let bridge = {
        let stop_cell = stop_cell.clone();
        tokio::task::spawn_local(async move {
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(STOP_POLL).await;
            }
            stop_cell.set(true);
        })
    };
    let outcome = execute_until_raw(u32::MAX, concurrency, stop_cell, |id| {
        let (account, sender) = &lanes[((id - 1) as usize) % lanes.len()];
        let ixs = independent_ixs(id, *account, iters);
        let sender = sender.clone();
        async move { sender.submit(&ixs).await.map(|_| ()) }
    })
    .await;
    bridge.abort();
    Ok(LoadTally {
        delivered: outcome.delivered,
        failed: outcome.failed,
        first_error: outcome.first_error,
        delivery: outcome.delivery,
        wall: outcome.wall,
    })
}

#[derive(Default)]
struct LaneTally {
    delivered: u64,
    failed: u64,
    first_error: Option<String>,
    delivery: Option<StreamingStats>,
}

async fn bounded_load(
    ws_url: &str,
    lanes: Vec<(Pubkey, TxSender)>,
    iters: u32,
    stop: Arc<AtomicBool>,
) -> Result<LoadTally> {
    let sigs = Rc::new(SignatureConfirmations::connect(ws_url).await?);
    let tally = Rc::new(RefCell::new(LaneTally {
        delivery: Some(StreamingStats::new()),
        ..LaneTally::default()
    }));
    let next_id = Rc::new(Cell::new(0u64));
    let started = Instant::now();
    let tasks: Vec<_> = lanes
        .into_iter()
        .map(|(account, sender)| {
            let sigs = sigs.clone();
            let tally = tally.clone();
            let stop = stop.clone();
            let next_id = next_id.clone();
            tokio::task::spawn_local(async move {
                while !stop.load(Ordering::Relaxed) {
                    let id = next_id.get() + 1;
                    next_id.set(id);
                    let ixs = independent_ixs(id, account, iters);
                    let sent = Instant::now();
                    let result: Result<()> = async {
                        let tx = sender.prepare(&ixs).await?;
                        sigs.subscribe(id, &tx.signatures[0]).await?;
                        sender.submit_prepared(&tx).await?;
                        if let Some(delivery) =
                            tally.borrow_mut().delivery.as_mut()
                        {
                            delivery.push(sent.elapsed().as_micros() as u32);
                        }
                        tokio::time::timeout(
                            CONFIRM_TIMEOUT,
                            sigs.await_id(id),
                        )
                        .await
                        .map_err(|_| {
                            format!(
                                "confirmation for independent tx {id} not \
                                     within {CONFIRM_TIMEOUT:?}"
                            )
                        })??;
                        Ok(())
                    }
                    .await;
                    let mut tally = tally.borrow_mut();
                    match result {
                        Ok(()) => tally.delivered += 1,
                        Err(error) => {
                            tally.failed += 1;
                            tally.first_error.get_or_insert(error.to_string());
                            break;
                        }
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await
            .map_err(|error| format!("independent lane panicked: {error}"))?;
    }
    let confirmations = sigs.finalize();
    let mut tally = tally.borrow_mut();
    Ok(LoadTally {
        delivered: tally.delivered,
        failed: tally.failed + confirmations.failed as u64,
        first_error: tally.first_error.take().or(confirmations.first_failure),
        delivery: tally.delivery.take().unwrap_or_default(),
        wall: started.elapsed(),
    })
}

async fn join_load(handles: LoadHandles) -> Result<LoadOutcome> {
    handles.stop.store(true, Ordering::Relaxed);
    let mut merged: Option<LoadTally> = None;
    for handle in handles.threads {
        let tally = tokio::task::spawn_blocking(move || handle.join())
            .await
            .map_err(|error| format!("load driver join failed: {error}"))?
            .map_err(|payload| {
                format!(
                    "load driver thread panicked: {}",
                    panic_message(payload)
                )
            })??;
        match merged.as_mut() {
            Some(merged) => merged.merge(tally),
            None => merged = Some(tally),
        }
    }
    Ok(merged
        .map(LoadTally::finalize)
        .ok_or("no load driver thread was started")?)
}

struct Samples {
    busy_mean: f64,
    busy_max: f64,
    blocked_max: f64,
    max_stall: Duration,
    count: usize,
}

struct Sampler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Samples>>,
}

impl Sampler {
    fn spawn(metrics_url: String) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("sampler runtime build is infallible");
            runtime.block_on(async move {
                let mut busy_sum = 0.0f64;
                let mut busy_max = 0.0f64;
                let mut blocked_max = 0.0f64;
                let mut count = 0usize;
                let mut last_txs: Option<f64> = None;
                let mut last_progress = Instant::now();
                let mut max_stall = Duration::ZERO;
                while !flag.load(Ordering::Relaxed) {
                    if let Ok(metrics) = api::scrape_metrics(&metrics_url).await
                    {
                        if let Some(busy) = metrics.get(BUSY_EXECUTORS) {
                            busy_sum += busy;
                            busy_max = busy_max.max(busy);
                            count += 1;
                        }
                        if let Some(blocked) = metrics.get(BLOCKED_TRANSACTIONS)
                        {
                            blocked_max = blocked_max.max(blocked);
                        }
                        if let Some(txs) = metrics.get(TX_COUNT) {
                            if last_txs.is_none_or(|last| txs > last) {
                                last_progress = Instant::now();
                            }
                            last_txs = Some(txs);
                            max_stall = max_stall.max(last_progress.elapsed());
                        }
                    }
                    tokio::time::sleep(SAMPLE_INTERVAL).await;
                }
                Samples {
                    busy_mean: if count > 0 {
                        busy_sum / count as f64
                    } else {
                        0.0
                    },
                    busy_max,
                    blocked_max,
                    max_stall,
                    count,
                }
            })
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn finish(mut self) -> Samples {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .take()
            .and_then(|handle| handle.join().ok())
            .unwrap_or(Samples {
                busy_mean: 0.0,
                busy_max: 0.0,
                blocked_max: 0.0,
                max_stall: Duration::ZERO,
                count: 0,
            })
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

struct DrainState {
    elapsed: Duration,
    blocked: f64,
    busy: f64,
    pending: f64,
}

fn idle_values(metrics: &Metrics) -> (f64, f64, f64) {
    (
        metrics.get(BLOCKED_TRANSACTIONS).unwrap_or(f64::NAN),
        metrics.get(BUSY_EXECUTORS).unwrap_or(f64::NAN),
        metrics.get(PENDING_TRANSACTIONS).unwrap_or(f64::NAN),
    )
}

fn is_idle(metrics: &Metrics) -> bool {
    idle_values(metrics) == (0.0, 0.0, 0.0)
}

async fn drain(er: &ErCtx, target: f64) -> Result<DrainState> {
    let started = Instant::now();
    check::poll(
        &format!("the engine transaction count reaches {target:.0}"),
        DRAIN_TIMEOUT,
        || async {
            matches!(
                er.scrape_metrics().await.ok().and_then(|m| m.get(TX_COUNT)),
                Some(count) if count >= target
            )
        },
    )
    .await?;
    check::poll(
        "the execution pipeline goes idle (nothing blocked, busy or pending)",
        SETTLE_TIMEOUT,
        || async {
            er.scrape_metrics()
                .await
                .is_ok_and(|metrics| is_idle(&metrics))
        },
    )
    .await?;
    let (blocked, busy, pending) = idle_values(&er.scrape_metrics().await?);
    Ok(DrainState {
        elapsed: started.elapsed(),
        blocked,
        busy,
        pending,
    })
}

async fn await_load_reaching_executors(
    er: &ErCtx,
    handles: &mut Option<LoadHandles>,
) -> Result<()> {
    let reached = check::poll(
        "the independent load reaches the executors",
        WARM_IN_TIMEOUT,
        || async {
            er.scrape_metrics()
                .await
                .ok()
                .and_then(|metrics| metrics.get(BUSY_EXECUTORS))
                .is_some_and(|busy| busy >= 1.0)
        },
    )
    .await;
    if reached.is_err() {
        if let Some(handles) = handles.take() {
            let load = join_load(handles).await?;
            if let Some(error) = load.first_error {
                return Err(format!(
                    "independent load failed before reaching the executors: \
                     {error}"
                )
                .into());
            }
        }
    }
    reached.map_err(Into::into)
}

struct PhasePlan {
    batch: u64,
    space: ChainSpace,
    range: (u64, u64),
    heavy_iters: u32,
    span: Duration,
    mode: LoadMode,
    independent_iters: u32,
    driver_threads: usize,
}

struct PhaseOutcome {
    label: &'static str,
    chains: u64,
    chain_txs: u64,
    send: ObservationsStats,
    load: LoadOutcome,
    samples: Samples,
    wall: Duration,
}

async fn drive_pair(
    pair_index: u64,
    pair: &mut Pair,
    plan: &PhasePlan,
    phase_started: tokio::time::Instant,
    gap: Duration,
    send_stats: &RefCell<StreamingStats>,
) -> Result<()> {
    let (first, last) = plan.range;
    for chain in first..last {
        tokio::time::sleep_until(phase_started + gap * (chain - first) as u32)
            .await;
        let heavy = heavy_step(chain);
        for step in Step::ALL {
            let id = plan.space.step_id(plan.batch, pair_index, chain, step);
            let iters = if step == heavy { plan.heavy_iters } else { 0 };
            let ixs = chain_ixs(id, iters, &pair.accounts(step));
            let sent = Instant::now();
            let signature = pair.senders[step.index()]
                .submit(&ixs)
                .await
                .map_err(|error| {
                    format!(
                        "pair {pair_index} chain {chain} step {} (id {id}) \
                             was not accepted: {error}",
                        step.label()
                    )
                })?;
            send_stats
                .borrow_mut()
                .push(sent.elapsed().as_micros() as u32);
            pair.model.apply(step, id, iters);
            pair.steps.push((id, step, signature));
        }
    }
    Ok(())
}

async fn run_phase(
    er: &ErCtx,
    pairs: &mut [Pair],
    independent: &Arc<Vec<Pubkey>>,
    independent_payers: &Arc<Vec<[u8; 64]>>,
    plan: PhasePlan,
) -> Result<PhaseOutcome> {
    let mut handles = Some(spawn_load(
        er.api().url().to_owned(),
        er.ws_url().to_owned(),
        plan.mode,
        plan.driver_threads,
        independent.clone(),
        independent_payers.clone(),
        plan.independent_iters,
    ));
    await_load_reaching_executors(er, &mut handles).await?;
    let sampler = Sampler::spawn(er.metrics_url().to_owned());

    let phase_started = tokio::time::Instant::now();
    let (first, last) = plan.range;
    let chains = last.saturating_sub(first);
    let gap = plan
        .span
        .checked_div(chains.max(1) as u32)
        .unwrap_or_default();
    let send_stats = RefCell::new(StreamingStats::new());
    let results =
        join_all(pairs.iter_mut().enumerate().map(|(pair_index, pair)| {
            drive_pair(
                pair_index as u64,
                pair,
                &plan,
                phase_started,
                gap,
                &send_stats,
            )
        }))
        .await;
    let chain_result: Result<()> = results.into_iter().collect();
    tokio::time::sleep_until(phase_started + plan.span).await;
    let load = match handles.take() {
        Some(handles) => join_load(handles).await?,
        None => return Err("independent load driver already joined".into()),
    };
    let samples = sampler.finish();
    chain_result?;
    Ok(PhaseOutcome {
        label: plan.mode.label(),
        chains,
        chain_txs: chains * STEPS * pairs.len() as u64,
        send: send_stats.into_inner().finalize(false),
        load,
        samples,
        wall: phase_started.elapsed(),
    })
}

async fn first_failed_step(
    er: &ErCtx,
    steps: &[(u64, Step, Signature)],
) -> Option<String> {
    for (id, step, signature) in steps {
        match er.api().get_transaction(signature).await {
            Ok(None) => {
                return Some(format!(
                    "step {} id {id} ({signature}) never reached the ledger",
                    step.label()
                ))
            }
            Ok(Some(info)) => {
                if let Some(err) = info.err {
                    return Some(format!(
                        "step {} id {id} ({signature}) failed in slot {}: \
                         {err}; logs: {:?}",
                        step.label(),
                        info.slot,
                        info.logs
                    ));
                }
            }
            Err(_) => continue,
        }
    }
    None
}

async fn verify_pairs(
    er: &ErCtx,
    pairs: &mut [Pair],
    batch: usize,
) -> Result<()> {
    for (index, pair) in pairs.iter_mut().enumerate() {
        let expectations = [
            ("A", pair.a, pair.model.a, pair.model.a_id),
            ("B", pair.b, pair.model.b, pair.model.b_id),
        ];
        for (label, address, expected_hash, expected_id) in expectations {
            let account = er.account(&address).await?.ok_or(format!(
                "pair {index} account {label} {address} is not on the ER"
            ))?;
            let data = &account.data;
            if data.len() < layout::HASH_OFFSET + layout::HASH_SIZE {
                return Err(format!(
                    "pair {index} account {label} holds {} bytes, fewer than \
                     the {} the layout needs",
                    data.len(),
                    layout::HASH_OFFSET + layout::HASH_SIZE
                )
                .into());
            }
            let hash = &data
                [layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE];
            let id_bytes =
                &data[layout::ID_OFFSET..layout::ID_OFFSET + layout::ID_SIZE];
            let id = u64::from_le_bytes(
                id_bytes.try_into().expect("id slice is 8 bytes"),
            );
            if hash == expected_hash && id == expected_id {
                continue;
            }
            let failed = first_failed_step(er, &pair.steps).await;
            let error = CheckError::new(format!(
                "batch {batch}: pair {index} account {label} must hold the \
                 fold of its accepted X/Y/Z history — a conflicting \
                 transaction executed out of accepted order, was skipped, \
                 or failed"
            ))
            .expected(format!("id {expected_id}, hash {}", hex(&expected_hash)))
            .actual(format!("id {id}, hash {}", hex(hash)))
            .context(
                "first failed step of this batch",
                failed.unwrap_or_else(|| {
                    "none — every step executed successfully".to_owned()
                }),
            );
            return Err(error.into());
        }
        pair.steps.clear();
    }
    Ok(())
}

async fn verify_independent(
    er: &ErCtx,
    accounts: &[Pubkey],
    iters: u32,
) -> Result<()> {
    let expected = hash_chain(HASH_INIT.to_bytes(), iters);
    for (index, address) in accounts.iter().enumerate() {
        let account = er
            .account(address)
            .await?
            .ok_or(format!("independent account {index} is not on the ER"))?;
        let hash = &account.data
            [layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE];
        check_eq!(
            hash,
            expected,
            "independent account {index} must hold the {iters}-iteration \
             hash chain — the high-cu work beside the chains was not executed"
        )?;
    }
    Ok(())
}

async fn prime_payers(er: &ErCtx, payers: &[Pubkey]) -> Result<()> {
    for chunk in payers.chunks(PREP_CHUNK) {
        let results = join_all(chunk.iter().map(|payer| async move {
            check::poll(
                &format!("the ER clones payer {payer}"),
                CLONE_TIMEOUT,
                || async {
                    matches!(er.account(payer).await, Ok(Some(acc)) if acc.lamports > 0)
                },
            )
            .await
        }))
        .await;
        for result in results {
            result?;
        }
    }
    Ok(())
}

async fn await_clones(er: &ErCtx, pdas: &[Pubkey]) -> Result<()> {
    for pda in pdas {
        check::poll(
            &format!("the ER clones the delegated pda {pda}"),
            CLONE_TIMEOUT,
            || async {
                matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
            },
        )
        .await?;
    }
    Ok(())
}

async fn funded_payers(base: &BaseCtx, count: usize) -> Result<Vec<Keypair>> {
    let mut payers = Vec::with_capacity(count);
    while payers.len() < count {
        let chunk = PREP_CHUNK.min(count - payers.len());
        let funded = join_all(
            (0..chunk).map(|_| prep::funded_payer(base, PAYER_LAMPORTS)),
        )
        .await;
        for payer in funded {
            payers.push(payer?);
        }
    }
    Ok(payers)
}

fn clone_keypair(payer: &Keypair) -> Keypair {
    Keypair::try_from(&payer.to_bytes()[..]).expect("payer bytes round-trip")
}

struct BatchOutcome {
    phases: Vec<PhaseOutcome>,
    drained: DrainState,
    engine_txs: f64,
    dependencies: f64,
    dropped: Option<f64>,
    execution_failed: Option<f64>,
}

impl BatchOutcome {
    fn chain_txs(&self) -> u64 {
        self.phases.iter().map(|phase| phase.chain_txs).sum()
    }

    fn independent_txs(&self) -> u64 {
        self.phases.iter().map(|phase| phase.load.delivered).sum()
    }

    fn chains(&self) -> u64 {
        self.phases.iter().map(|phase| phase.chains).sum()
    }
}

pub struct ConflictOrdering;

#[async_trait(?Send)]
impl Scenario for ConflictOrdering {
    fn name(&self) -> &str {
        "redline/conflict_ordering"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);
        check!(
            profile.chains >= 2,
            "profile {}: at least two chains per pair are needed to split a \
             batch across the contention and backpressure phases",
            profile.name
        )?;

        let probe = er.scrape_metrics().await?;
        for name in REQUIRED_METRICS {
            check!(
                probe.get(name).is_some(),
                "the ER exposes no {name} metric — this scenario reads the \
                 engine's sequencer and ledger gauges to prove ordering"
            )?;
        }

        let prep_started = Instant::now();
        let chain_payers =
            funded_payers(base, profile.pairs * PAYERS_PER_PAIR).await?;
        let independent_payers =
            funded_payers(base, profile.independent_accounts).await?;

        let prep_payers: Vec<Keypair> = chain_payers
            .iter()
            .step_by(PAYERS_PER_PAIR)
            .map(clone_keypair)
            .collect();
        let mut pair_accounts = Vec::with_capacity(profile.pairs * 2);
        for chunk in prep_payers.chunks(PREP_CHUNK) {
            let pdas = crate::init_delegated_accounts_batched(
                base,
                chunk,
                chunk.len() * 2,
                crate::ACCOUNT_SPACE,
                er.identity(),
            )
            .await?;
            await_clones(er, &pdas).await?;
            pair_accounts.extend(pdas);
        }
        let mut independent = Vec::with_capacity(profile.independent_accounts);
        for chunk in independent_payers.chunks(PREP_CHUNK) {
            let pdas = crate::init_delegated_accounts_batched(
                base,
                chunk,
                chunk.len(),
                crate::ACCOUNT_SPACE,
                er.identity(),
            )
            .await?;
            await_clones(er, &pdas).await?;
            independent.extend(pdas);
        }
        let payer_keys: Vec<Pubkey> = chain_payers
            .iter()
            .chain(independent_payers.iter())
            .map(|payer| payer.pubkey())
            .collect();
        prime_payers(er, &payer_keys).await?;
        eprintln!(
            "[redsuite] {}: prepped {} pairs x 2 accounts x 3 payers and {} \
             independent lanes in {:.1} s",
            self.name(),
            profile.pairs,
            profile.independent_accounts,
            prep_started.elapsed().as_secs_f64(),
        );

        let mut pairs: Vec<Pair> = pair_accounts
            .chunks_exact(2)
            .enumerate()
            .map(|(index, accounts)| Pair {
                a: accounts[0],
                b: accounts[1],
                senders: chain_payers
                    [index * PAYERS_PER_PAIR..(index + 1) * PAYERS_PER_PAIR]
                    .iter()
                    .map(|payer| er.sender(Rc::new(clone_keypair(payer))))
                    .collect(),
                model: PairModel::default(),
                steps: Vec::new(),
            })
            .collect();
        let independent = Arc::new(independent);
        let independent_payer_bytes: Arc<Vec<[u8; 64]>> = Arc::new(
            independent_payers
                .iter()
                .map(|payer| payer.to_bytes())
                .collect(),
        );

        let calibration_before = er.scrape_metrics().await?;
        let mut handles = Some(spawn_load(
            er.api().url().to_owned(),
            er.ws_url().to_owned(),
            LoadMode::Open {
                concurrency: OPEN_CONCURRENCY,
            },
            profile.driver_threads,
            independent.clone(),
            independent_payer_bytes.clone(),
            profile.independent_iters,
        ));
        await_load_reaching_executors(er, &mut handles).await?;
        let sampler = Sampler::spawn(er.metrics_url().to_owned());
        tokio::time::sleep(CALIBRATION).await;
        let calibration = match handles.take() {
            Some(handles) => join_load(handles).await?,
            None => return Err("calibration load already joined".into()),
        };
        let calibration_samples = sampler.finish();
        check_eq!(
            calibration.failed,
            0,
            "calibration deliveries failed: {:?}",
            calibration.first_error
        )?;
        let executors = calibration_samples.busy_max;
        check!(
            executors >= MIN_EXECUTORS,
            "saturating open-loop high-cu load kept at most {executors:.0} \
             executor busy — this scenario needs at least {MIN_EXECUTORS:.0} \
             (the engine sizes its pool from the host's cpus)"
        )?;
        let contention_lanes = (executors as usize * LANES_PER_EXECUTOR)
            .min(profile.independent_accounts)
            .max(1);
        let calibration_target =
            calibration_before.get(TX_COUNT).unwrap_or_default()
                + calibration.delivered as f64;
        let calibration_drain = drain(er, calibration_target).await?;
        eprintln!(
            "[redsuite] {}: calibration: {} executors busy at peak (mean \
             {:.1}), {} independent txs at {:.0} tps, drained in {:.1} s; \
             contention phases use {contention_lanes} lanes",
            self.name(),
            executors,
            calibration_samples.busy_mean,
            calibration.delivered,
            calibration.delivered as f64 / calibration.wall.as_secs_f64(),
            calibration_drain.elapsed.as_secs_f64(),
        );

        let space = ChainSpace {
            pairs: profile.pairs as u64,
            chains: profile.chains,
        };
        let modes = [
            LoadMode::Bounded {
                lanes: contention_lanes,
            },
            LoadMode::Open {
                concurrency: OPEN_CONCURRENCY,
            },
        ];
        let ranges = phase_ranges(profile.chains);

        let mut batches: Vec<BatchOutcome> =
            Vec::with_capacity(profile.batches);
        for batch in 0..profile.batches {
            let before = er.scrape_metrics().await?;
            check!(
                is_idle(&before),
                "batch {batch} must start on an idle pipeline, observed \
                 (blocked, busy, pending) = {:?}",
                idle_values(&before)
            )?;
            let txs_before = before.get(TX_COUNT).unwrap_or_default();

            let mut phases = Vec::with_capacity(modes.len());
            for (mode, range) in modes.iter().zip(ranges) {
                let plan = PhasePlan {
                    batch: batch as u64,
                    space,
                    range,
                    heavy_iters: profile.heavy_iters,
                    span: profile.phase_span,
                    mode: *mode,
                    independent_iters: profile.independent_iters,
                    driver_threads: profile.driver_threads,
                };
                let phase = run_phase(
                    er,
                    &mut pairs,
                    &independent,
                    &independent_payer_bytes,
                    plan,
                )
                .await?;
                check_eq!(
                    phase.load.failed,
                    0,
                    "batch {batch} {}: independent deliveries failed: {:?}",
                    phase.label,
                    phase.load.first_error
                )?;
                eprintln!(
                    "[redsuite] {}: batch {batch} {}: {} chain txs (send p50 \
                     {} us / p95 {} us) beside {} independent txs in {:.1} s; \
                     busy executors mean {:.1} / max {:.0} over {} samples, \
                     blocked max {:.0}, longest progress stall {:.2} s",
                    self.name(),
                    phase.label,
                    phase.chain_txs,
                    phase.send.median,
                    phase.send.quantile95,
                    phase.load.delivered,
                    phase.wall.as_secs_f64(),
                    phase.samples.busy_mean,
                    phase.samples.busy_max,
                    phase.samples.count,
                    phase.samples.blocked_max,
                    phase.samples.max_stall.as_secs_f64(),
                );
                phases.push(phase);
            }

            let workload: u64 = phases
                .iter()
                .map(|phase| phase.chain_txs + phase.load.delivered)
                .sum();
            let drained = drain(er, txs_before + workload as f64).await?;
            let after = er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);
            let engine_txs = delta.counter(TX_COUNT).unwrap_or_default();
            let dependencies =
                delta.counter(ORDERING_DEPENDENCIES).unwrap_or_default();
            let failed_kind = |kind: &str| {
                delta.counter(&format!(
                    "{}{{kind=\"{kind}\"}}",
                    crate::metrics::FAILED_TRANSACTIONS
                ))
            };
            let outcome = BatchOutcome {
                phases,
                drained,
                engine_txs,
                dependencies,
                dropped: failed_kind("dropped"),
                execution_failed: failed_kind("execution"),
            };

            for phase in &outcome.phases {
                check!(
                    phase.samples.busy_max >= MIN_EXECUTORS,
                    "batch {batch} {}: at most {:.0} executor was busy at \
                     once — independent work did not spread across executors",
                    phase.label,
                    phase.samples.busy_max
                )?;
                check!(
                    phase.samples.busy_mean > 1.0,
                    "batch {batch} {}: busy executors averaged {:.2} — \
                     independent work ran effectively serially",
                    phase.label,
                    phase.samples.busy_mean
                )?;
                check!(
                    phase.samples.max_stall <= STALL_BOUND,
                    "batch {batch} {}: the engine transaction count stood \
                     still for {:.1} s under load, longer than the {:.0} s \
                     progress bound",
                    phase.label,
                    phase.samples.max_stall.as_secs_f64(),
                    STALL_BOUND.as_secs_f64()
                )?;
            }
            for (label, value) in [
                ("blocked", outcome.drained.blocked),
                ("busy", outcome.drained.busy),
                ("pending", outcome.drained.pending),
            ] {
                check_eq!(
                    value,
                    0.0,
                    "batch {batch}: {label} work must return to zero after \
                     the drain"
                )?;
            }
            check!(
                engine_txs >= workload as f64,
                "batch {batch}: the engine executed {engine_txs:.0} \
                 transactions, fewer than the {workload} the workload \
                 delivered"
            )?;
            if let Some(dropped) = outcome.dropped {
                check_eq!(
                    dropped,
                    0.0,
                    "batch {batch}: the sequencer dropped {dropped:.0} \
                     transactions before execution"
                )?;
            }
            if let Some(execution_failed) = outcome.execution_failed {
                check_eq!(
                    execution_failed,
                    0.0,
                    "batch {batch}: {execution_failed:.0} transactions \
                     reached the SVM and failed there"
                )?;
            }
            let contention_chains =
                outcome.phases[0].chains * profile.pairs as u64;
            check!(
                dependencies >= contention_chains as f64,
                "batch {batch}: the engine registered {dependencies:.0} \
                 ordering dependencies, fewer than one per contention-phase \
                 chain ({contention_chains}) — the chain steps never met an \
                 unfinished predecessor, so executor timing was not exercised"
            )?;
            verify_pairs(er, &mut pairs, batch).await?;
            eprintln!(
                "[redsuite] {}: batch {batch}: {} chains ({} chain txs, {} \
                 independent txs) drained in {:.1} s, {dependencies:.0} \
                 ordering dependencies, every pair matches its accepted \
                 X/Y/Z fold",
                self.name(),
                outcome.chains() * profile.pairs as u64,
                outcome.chain_txs(),
                outcome.independent_txs(),
                outcome.drained.elapsed.as_secs_f64(),
            );

            let mut cell =
                ScenarioReport::ok(&format!("{}/batch{batch}", self.name()))
                    .setting("profile", profile.name)
                    .setting("batch", batch)
                    .setting("pairs", profile.pairs)
                    .setting("chains per pair", profile.chains)
                    .setting("heavy step iters", profile.heavy_iters)
                    .setting("independent iters", profile.independent_iters)
                    .setting("executors", executors)
                    .setting("contention lanes", contention_lanes)
                    .setting("open concurrency", OPEN_CONCURRENCY)
                    .metric(
                        "chain txs",
                        Unit::Count,
                        outcome.chain_txs() as f64,
                    )
                    .metric(
                        "independent txs",
                        Unit::Count,
                        outcome.independent_txs() as f64,
                    )
                    .metric("engine txs in window", Unit::Count, engine_txs)
                    .metric(
                        "engine txs beyond workload",
                        Unit::Count,
                        engine_txs - workload as f64,
                    )
                    .metric("ordering dependencies", Unit::Count, dependencies)
                    .metric(
                        "dependencies per chain",
                        Unit::Ratio,
                        dependencies
                            / (outcome.chains() * profile.pairs as u64) as f64,
                    )
                    .metric(
                        "drain s",
                        Unit::Seconds,
                        outcome.drained.elapsed.as_secs_f64(),
                    )
                    .metric(
                        "blocked at drain",
                        Unit::Count,
                        outcome.drained.blocked,
                    )
                    .metric("busy at drain", Unit::Count, outcome.drained.busy)
                    .metric(
                        "pending at drain",
                        Unit::Count,
                        outcome.drained.pending,
                    )
                    .metric_if("dropped txs", Unit::Count, outcome.dropped)
                    .metric_if(
                        "execution failed txs",
                        Unit::Count,
                        outcome.execution_failed,
                    );
            for phase in &outcome.phases {
                cell = cell
                    .observe(
                        format!("{} chain send us", phase.label),
                        Unit::Micros,
                        phase.send,
                    )
                    .observe(
                        format!("{} independent delivery us", phase.label),
                        Unit::Micros,
                        phase.load.delivery,
                    )
                    .metric(
                        format!("{} chain txs", phase.label),
                        Unit::Count,
                        phase.chain_txs as f64,
                    )
                    .metric(
                        format!("{} independent txs", phase.label),
                        Unit::Count,
                        phase.load.delivered as f64,
                    )
                    .metric(
                        format!("{} independent tps", phase.label),
                        Unit::Tps,
                        phase.load.delivered as f64
                            / phase.load.wall.as_secs_f64().max(1e-9),
                    )
                    .metric(
                        format!("{} busy executors mean", phase.label),
                        Unit::Count,
                        phase.samples.busy_mean,
                    )
                    .metric(
                        format!("{} busy executors max", phase.label),
                        Unit::Count,
                        phase.samples.busy_max,
                    )
                    .metric(
                        format!("{} blocked max", phase.label),
                        Unit::Count,
                        phase.samples.blocked_max,
                    )
                    .metric(
                        format!("{} longest stall s", phase.label),
                        Unit::Seconds,
                        phase.samples.max_stall.as_secs_f64(),
                    )
                    .metric(
                        format!("{} wall s", phase.label),
                        Unit::Seconds,
                        phase.wall.as_secs_f64(),
                    );
            }
            match report::persist_cell(self.name(), &cell) {
                Ok(path) => {
                    eprintln!("[redsuite]   cell report: {}", path.display())
                }
                Err(err) => eprintln!(
                    "[redsuite]   warning: cell report not persisted: {err}"
                ),
            }
            batches.push(outcome);
        }

        verify_independent(er, &independent, profile.independent_iters).await?;

        let chain_txs: u64 = batches.iter().map(BatchOutcome::chain_txs).sum();
        let independent_txs: u64 =
            batches.iter().map(BatchOutcome::independent_txs).sum();
        let chains: u64 = batches
            .iter()
            .map(|batch| batch.chains() * profile.pairs as u64)
            .sum();
        let dependencies: f64 =
            batches.iter().map(|batch| batch.dependencies).sum();
        let engine_txs: f64 =
            batches.iter().map(|batch| batch.engine_txs).sum();
        let longest_stall = batches
            .iter()
            .flat_map(|batch| batch.phases.iter())
            .map(|phase| phase.samples.max_stall)
            .max()
            .unwrap_or_default();
        let slowest_drain = batches
            .iter()
            .map(|batch| batch.drained.elapsed)
            .max()
            .unwrap_or_default();
        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting(
                "shape",
                "X(A) -> Y(A,B) -> Z(B) hash-fold chains, one payer per \
                 step, beside high-cu work on disjoint accounts",
            )
            .setting("pairs", profile.pairs)
            .setting("chains per pair per batch", profile.chains)
            .setting("batches", profile.batches)
            .setting("heavy step iters", profile.heavy_iters)
            .setting("independent accounts", profile.independent_accounts)
            .setting("independent iters", profile.independent_iters)
            .setting("driver threads", profile.driver_threads)
            .setting("phase span s", profile.phase_span.as_secs())
            .setting("executors", executors)
            .setting("contention lanes", contention_lanes)
            .setting(
                "contention load",
                LoadMode::Bounded {
                    lanes: contention_lanes,
                }
                .shape(),
            )
            .setting(
                "backpressure load",
                LoadMode::Open {
                    concurrency: OPEN_CONCURRENCY,
                }
                .shape(),
            )
            .metric("chains", Unit::Count, chains as f64)
            .metric("chain txs", Unit::Count, chain_txs as f64)
            .metric("independent txs", Unit::Count, independent_txs as f64)
            .metric("engine txs", Unit::Count, engine_txs)
            .metric("ordering dependencies", Unit::Count, dependencies)
            .metric(
                "dependencies per chain",
                Unit::Ratio,
                dependencies / chains.max(1) as f64,
            )
            .metric(
                "longest stall s",
                Unit::Seconds,
                longest_stall.as_secs_f64(),
            )
            .metric(
                "slowest drain s",
                Unit::Seconds,
                slowest_drain.as_secs_f64(),
            );
        for (index, batch) in batches.iter().enumerate() {
            summary = summary
                .metric(
                    format!("batch{index} ordering dependencies"),
                    Unit::Count,
                    batch.dependencies,
                )
                .metric(
                    format!("batch{index} drain s"),
                    Unit::Seconds,
                    batch.drained.elapsed.as_secs_f64(),
                );
            for phase in &batch.phases {
                summary = summary
                    .metric(
                        format!(
                            "batch{index} {} chain send p50 us",
                            phase.label
                        ),
                        Unit::Micros,
                        phase.send.median as f64,
                    )
                    .metric(
                        format!(
                            "batch{index} {} chain send p95 us",
                            phase.label
                        ),
                        Unit::Micros,
                        phase.send.quantile95 as f64,
                    )
                    .metric(
                        format!(
                            "batch{index} {} busy executors mean",
                            phase.label
                        ),
                        Unit::Count,
                        phase.samples.busy_mean,
                    )
                    .metric(
                        format!("batch{index} {} independent tps", phase.label),
                        Unit::Tps,
                        phase.load.delivered as f64
                            / phase.load.wall.as_secs_f64().max(1e-9),
                    );
            }
        }
        Ok(summary)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn step_ids_are_unique_and_follow_the_accepted_order() {
        let space = ChainSpace {
            pairs: 3,
            chains: 5,
        };
        let mut seen = HashSet::new();
        for batch in 0..2 {
            for pair in 0..space.pairs {
                let mut last = 0;
                for chain in 0..space.chains {
                    for step in Step::ALL {
                        let id = space.step_id(batch, pair, chain, step);
                        assert!(id > last, "ids must ascend within a pair");
                        assert!(seen.insert(id), "id {id} issued twice");
                        last = id;
                    }
                }
            }
        }
        assert_eq!(seen.len(), 2 * 3 * 5 * 3);
    }

    #[test]
    fn heavy_step_rotates_through_every_position() {
        let heavy: HashSet<Step> = (0..3).map(heavy_step).collect();
        assert_eq!(heavy.len(), 3);
    }

    #[test]
    fn phase_ranges_partition_the_chains() {
        for chains in [2u64, 3, 40, 41, 200] {
            let [(first_lo, first_hi), (second_lo, second_hi)] =
                phase_ranges(chains);
            assert_eq!(first_lo, 0);
            assert_eq!(first_hi, second_lo);
            assert_eq!(second_hi, chains);
            assert!(first_hi - first_lo >= 1);
            assert!(second_hi - second_lo >= 1);
        }
    }

    fn replay(sequence: &[(Step, u64)]) -> PairModel {
        let mut model = PairModel::default();
        for &(step, id) in sequence {
            model.apply(step, id, 0);
        }
        model
    }

    #[test]
    fn the_model_pins_causal_order_and_nothing_more() {
        let accepted = [(Step::X, 1), (Step::Y, 2), (Step::Z, 3), (Step::X, 4)];
        let z_after_next_x =
            [(Step::X, 1), (Step::Y, 2), (Step::X, 4), (Step::Z, 3)];
        assert_eq!(replay(&accepted), replay(&z_after_next_x));

        let y_before_x =
            [(Step::Y, 2), (Step::X, 1), (Step::Z, 3), (Step::X, 4)];
        assert_ne!(replay(&accepted), replay(&y_before_x));
        let z_before_y =
            [(Step::X, 1), (Step::Z, 3), (Step::Y, 2), (Step::X, 4)];
        assert_ne!(replay(&accepted), replay(&z_before_y));
        let next_x_before_y =
            [(Step::X, 1), (Step::X, 4), (Step::Y, 2), (Step::Z, 3)];
        assert_ne!(replay(&accepted), replay(&next_x_before_y));

        let mut heavy = PairModel::default();
        heavy.apply(Step::X, 1, 1);
        assert_ne!(heavy, replay(&[(Step::X, 1)]));
    }
}
