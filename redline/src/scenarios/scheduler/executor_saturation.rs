use std::{
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
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
    api, check, check_eq, host, prep,
    profile::{self, ProfileValues},
    report,
    runner::{
        execute_raw, panic_message, RawRunOutcome, RunConfig, RunOutcome,
    },
    topology, Api, BaseCtx, BatchBody, ChainCtx, CheckError, ErClient, ErCtx,
    MetricsDelta, Result, Scenario, ScenarioReport, TxSender,
};
use signature::Signature;

use crate::program::{instruction::build, layout, utils::hash_chain};

const PAYER_LAMPORTS: u64 = 200_000_000;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(900);
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const CLONE_TIMEOUT: Duration = Duration::from_secs(60);
const PREP_CHUNK: usize = 32;
const BUSY_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const TX_COUNT: &str = crate::metrics::ENGINE_TRANSACTIONS;
const BUSY_EXECUTORS: &str = "engine_processor_busy_executors";
const ORDERING_DEPENDENCIES: &str = "engine_processor_ordering_dependencies";
const BLOCKED_TRANSACTIONS: &str = "engine_processor_blocked_transactions";
const PROGRAM: Pubkey = crate::program::ID;
const HASH_INIT: Pubkey = Pubkey::new_from_array([7u8; 32]);
const LIGHT_ITERS: u32 = 1;
const CU_LIMIT: u32 = 1_400_000;
const CU_CONTRAST_FLOOR: f64 = 10.0;
const BUSY_THREAD_CORES: f64 = 0.5;

struct Profile {
    name: &'static str,
    accounts: usize,
    heavy_iters: u32,
    threads: usize,
    warmup: u64,
    iterations: u64,
    heavy_iterations: u64,
    batch: usize,
    rpc_batch: usize,
    concurrency: usize,
    heavy_concurrency: usize,
}

impl Profile {
    fn cell_iterations(&self, label: &str) -> u64 {
        if label == "heavy" {
            self.heavy_iterations
        } else {
            self.iterations
        }
    }

    fn cell_concurrency(&self, label: &str) -> usize {
        if label == "heavy" {
            self.heavy_concurrency
        } else {
            self.concurrency
        }
    }

    fn mode(&self) -> &'static str {
        if self.rpc_batch > 0 {
            "staged backlog, batched rpc"
        } else {
            "pre-signed burst"
        }
    }
}

const LITE: Profile = Profile {
    name: "lite",
    accounts: 256,
    heavy_iters: 180,
    threads: 8,
    warmup: 5_000,
    iterations: 60_000,
    heavy_iterations: 60_000,
    batch: 2_500,
    rpc_batch: 0,
    concurrency: 2_048,
    heavy_concurrency: 2_048,
};

const FULL: Profile = Profile {
    name: "full",
    accounts: 512,
    heavy_iters: 180,
    threads: 8,
    warmup: 25_000,
    iterations: 300_000,
    heavy_iterations: 300_000,
    batch: 2_500,
    rpc_batch: 0,
    concurrency: 2_048,
    heavy_concurrency: 2_048,
};

const SOAK: Profile = Profile {
    name: "soak",
    accounts: 512,
    heavy_iters: 180,
    threads: 16,
    warmup: 50_000,
    iterations: 1_000_000,
    heavy_iterations: 100_000,
    batch: u64::MAX as usize,
    rpc_batch: 500,
    concurrency: 512,
    heavy_concurrency: 8,
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

fn consumed_cus(logs: &[String]) -> Option<f64> {
    logs.iter().find_map(|line| {
        let (_, rest) = line.split_once(" consumed ")?;
        let (cus, tail) = rest.split_once(" of ")?;
        if !tail.contains("compute units") {
            return None;
        }
        cus.parse().ok()
    })
}

fn slot_of(global_id: u64, len: usize) -> usize {
    (global_id - 1) as usize % len
}

struct BurstConfig {
    threads: usize,
    iterations: u64,
    batch: usize,
    rpc_batch: usize,
    concurrency: usize,
}

struct BurstOutcome {
    outcome: RunOutcome,
    sign_s: f64,
    blast_s: f64,
    staged: u64,
}

fn build_ixs(
    accounts: &[Pubkey],
    global_id: u64,
    iters: u32,
    raise_budget: bool,
) -> Vec<Instruction> {
    let pda = accounts[slot_of(global_id, accounts.len())];
    let compute = build::expensive_hash_compute_at(
        PROGRAM,
        global_id,
        HASH_INIT,
        iters,
        &[pda],
    );
    if raise_budget {
        vec![compute_unit_limit(CU_LIMIT), compute]
    } else {
        vec![compute]
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_cell_burst(
    er_rpc_url: String,
    config: BurstConfig,
    id_offset: u64,
    accounts: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
    iters: u32,
    raise_budget: bool,
    probe: Arc<OnceLock<Signature>>,
) -> Result<BurstOutcome> {
    let threads = config.threads.max(1);
    let base_iterations = config.iterations / threads as u64;
    let remainder = config.iterations % threads as u64;
    let concurrency = (config.concurrency / threads).max(1);
    let (outcome_sender, outcome_receiver) = std::sync::mpsc::channel();

    let mut first_id = id_offset;
    let mut handles = Vec::with_capacity(threads);
    for thread_index in 0..threads {
        let iterations =
            base_iterations + u64::from((thread_index as u64) < remainder);
        if iterations == 0 {
            continue;
        }
        let thread_first_id = first_id;
        first_id += iterations;
        let er_rpc_url = er_rpc_url.clone();
        let accounts = accounts.clone();
        let payer_bytes = payer_bytes.clone();
        let probe = probe.clone();
        let batch = config.batch.max(1);
        let rpc_batch = config.rpc_batch;
        let outcome_sender = outcome_sender.clone();
        handles.push(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("driver runtime build is infallible");
            let local = tokio::task::LocalSet::new();
            let result = runtime.block_on(local.run_until(async move {
                let client = ErClient::new(er_rpc_url);
                let senders: Vec<TxSender> = payer_bytes
                    .iter()
                    .map(|bytes| {
                        let payer = Keypair::try_from(&bytes[..])
                            .expect("payer bytes round-trip");
                        client.sender(Rc::new(payer))
                    })
                    .collect();
                let api = client.api().clone();

                let mut outcomes = Vec::new();
                let mut sign_s = 0.0f64;
                let mut blast_s = 0.0f64;
                let mut staged = 0u64;
                let ids: Vec<u64> = (1..=iterations)
                    .map(|iteration| thread_first_id + iteration)
                    .collect();
                for chunk in ids.chunks(batch) {
                    let sign_started = Instant::now();
                    let mut signed = Vec::with_capacity(chunk.len());
                    for &global_id in chunk {
                        let ixs = build_ixs(
                            &accounts,
                            global_id,
                            iters,
                            raise_budget,
                        );
                        let sender =
                            &senders[slot_of(global_id, senders.len())];
                        let tx = sender
                            .prepare(&ixs)
                            .await
                            .expect("pre-signing must not fail");
                        let _ = probe.set(tx.signatures[0]);
                        signed.push(tx);
                    }
                    staged += signed.len() as u64;
                    let bodies: Vec<Rc<BatchBody>> =
                        if rpc_batch > 0 {
                            signed
                                .chunks(rpc_batch)
                                .map(|chunk| {
                                    Rc::new(Api::batch_send_body(chunk).expect(
                                        "batch body build is infallible",
                                    ))
                                })
                                .collect()
                        } else {
                            Vec::new()
                        };
                    sign_s += sign_started.elapsed().as_secs_f64();

                    let blast_started = Instant::now();
                    let outcome = if rpc_batch > 0 {
                        execute_raw(
                            RunConfig {
                                iterations: bodies.len() as u64,
                                rate: u32::MAX,
                                concurrency,
                            },
                            |index| {
                                let body = bodies[(index - 1) as usize].clone();
                                let api = api.clone();
                                async move {
                                    match api.send_batch(&body).await {
                                        Ok(0) => Ok(()),
                                        Ok(rejected) => Err(format!(
                                            "{rejected} batch entries rejected"
                                        )
                                        .into()),
                                        Err(error) => Err(error),
                                    }
                                }
                            },
                        )
                        .await
                    } else {
                        execute_raw(
                            RunConfig {
                                iterations: signed.len() as u64,
                                rate: u32::MAX,
                                concurrency,
                            },
                            |batch_index| {
                                let tx =
                                    signed[(batch_index - 1) as usize].clone();
                                let api = api.clone();
                                async move {
                                    api.send_transaction(&tx).await.map(|_| ())
                                }
                            },
                        )
                        .await
                    };
                    blast_s += blast_started.elapsed().as_secs_f64();
                    outcomes.push(outcome);
                }
                (outcomes, sign_s, blast_s, staged)
            }));
            let _ = outcome_sender.send(result);
        }));
    }
    drop(outcome_sender);

    let workers = handles.len();
    let mut received = 0usize;
    let mut all_outcomes: Vec<RawRunOutcome> = Vec::new();
    let mut sign_s = 0.0f64;
    let mut blast_s = 0.0f64;
    let mut staged = 0u64;
    while let Ok((outcomes, thread_sign_s, thread_blast_s, thread_staged)) =
        outcome_receiver.recv()
    {
        received += 1;
        all_outcomes.extend(outcomes);
        sign_s = sign_s.max(thread_sign_s);
        blast_s = blast_s.max(thread_blast_s);
        staged += thread_staged;
    }
    let mut first_worker_panic = None;
    for handle in handles {
        if let Err(payload) = handle.join() {
            first_worker_panic.get_or_insert_with(|| panic_message(payload));
        }
    }
    if let Some(panic) = first_worker_panic {
        return Err(format!("driver worker thread panicked: {panic}").into());
    }
    if received != workers {
        return Err("a driver worker thread stopped without an outcome".into());
    }

    let mut outcome = all_outcomes
        .into_iter()
        .reduce(|mut merged, chunk_outcome| {
            merged.merge(chunk_outcome);
            merged
        })
        .map(RawRunOutcome::finalize)
        .unwrap_or_default();
    outcome.wall = Duration::from_secs_f64(blast_s.max(1e-9));
    Ok(BurstOutcome {
        outcome,
        sign_s,
        blast_s,
        staged,
    })
}

async fn drain_processed(er: &ErCtx, target: f64) -> Result<Duration> {
    let started = Instant::now();
    check::poll(
        &format!("the validator transaction count reaches {target:.0}"),
        DRAIN_TIMEOUT,
        || async {
            matches!(
                er.scrape_metrics().await.ok().and_then(|metrics| metrics.get(TX_COUNT)),
                Some(count) if count >= target
            )
        },
    )
    .await?;
    Ok(started.elapsed())
}

struct BusySampler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Vec<f64>>>,
}

struct BusySamples {
    mean: f64,
    max: f64,
    count: usize,
}

impl BusySampler {
    fn spawn(metrics_url: String) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("sampler runtime build is infallible");
            runtime.block_on(async move {
                let mut samples = Vec::new();
                while !flag.load(Ordering::Relaxed) {
                    if let Ok(metrics) = api::scrape_metrics(&metrics_url).await
                    {
                        if let Some(busy) = metrics.get(BUSY_EXECUTORS) {
                            samples.push(busy);
                        }
                    }
                    tokio::time::sleep(BUSY_SAMPLE_INTERVAL).await;
                }
                samples
            })
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn finish(mut self) -> BusySamples {
        self.stop.store(true, Ordering::Relaxed);
        let samples = self
            .handle
            .take()
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default();
        let count = samples.len();
        let mean = if count > 0 {
            samples.iter().sum::<f64>() / count as f64
        } else {
            0.0
        };
        let max = samples.iter().copied().fold(0.0, f64::max);
        BusySamples { mean, max, count }
    }
}

impl Drop for BusySampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

struct Cell {
    label: &'static str,
    iters: u32,
    outcome: RunOutcome,
    sign_s: f64,
    blast_s: f64,
    drain: Duration,
    probe_cus: f64,
    cores: f64,
    top_thread_cores: f64,
    busy_threads: usize,
    busy: BusySamples,
    validator_txs: Option<f64>,
    dependencies: Option<f64>,
    blocked: Option<f64>,
    staged: u64,
    iterations: u64,
    dropped: Option<f64>,
    execution_failed: Option<f64>,
}

impl Cell {
    fn delivered_tps(&self) -> f64 {
        self.staged as f64 / self.blast_s.max(1e-9)
    }

    fn executed_tps(&self, iterations: u64) -> f64 {
        iterations as f64 / (self.outcome.wall + self.drain).as_secs_f64()
    }

    fn dependency_ratio(&self) -> Option<f64> {
        self.dependencies
            .map(|dependencies| dependencies / self.iterations.max(1) as f64)
    }
}

pub struct ExecutorSaturation;

#[async_trait(?Send)]
impl Scenario for ExecutorSaturation {
    fn name(&self) -> &str {
        "redline/executor_saturation"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);

        let prep_started = Instant::now();
        let mut payers: Vec<Keypair> = Vec::with_capacity(profile.accounts);
        while payers.len() < profile.accounts {
            let count = PREP_CHUNK.min(profile.accounts - payers.len());
            let funded = join_all(
                (0..count).map(|_| prep::funded_payer(base, PAYER_LAMPORTS)),
            )
            .await;
            for payer in funded {
                payers.push(payer?);
            }
        }
        let mut accounts: Vec<Pubkey> = Vec::with_capacity(profile.accounts);
        for chunk in payers.chunks(PREP_CHUNK) {
            let pdas = crate::init_delegated_accounts_batched_at(
                PROGRAM,
                base,
                chunk,
                chunk.len(),
                crate::ACCOUNT_SPACE,
                er.identity(),
            )
            .await?;
            for pda in &pdas {
                check::poll(
                    &format!("the ER clones the delegated pda {pda}"),
                    CLONE_TIMEOUT,
                    || async {
                        matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                    },
                )
                .await?;
            }
            accounts.extend(pdas);
        }
        eprintln!(
            "[redsuite] {}: prepped {} payers x 1 delegated account in {:.1} s",
            self.name(),
            profile.accounts,
            prep_started.elapsed().as_secs_f64(),
        );
        let accounts = Arc::new(accounts);
        let payer_bytes: Arc<Vec<[u8; 64]>> =
            Arc::new(payers.iter().map(|payer| payer.to_bytes()).collect());
        let er_rpc_url = er.api().url().to_owned();
        let er_pid = topology::current_state()
            .ok_or("no shared stack state")?
            .er_pid;

        let count_before_warmup = er.scrape_metrics().await?.get(TX_COUNT);
        let warm = execute_cell_burst(
            er_rpc_url.clone(),
            BurstConfig {
                threads: profile.threads,
                iterations: profile.warmup,
                batch: profile.batch,
                rpc_batch: profile.rpc_batch,
                concurrency: profile.concurrency,
            },
            0,
            accounts.clone(),
            payer_bytes.clone(),
            LIGHT_ITERS,
            false,
            Arc::new(OnceLock::new()),
        )?;
        check_eq!(
            warm.outcome.failed,
            0,
            "warmup deliveries failed: {:?}",
            warm.outcome.first_error
        )?;
        if let Some(seen) = count_before_warmup {
            drain_processed(er, seen + profile.warmup as f64).await?;
        }

        let mut offset = profile.warmup;
        let mut cells: Vec<Cell> = Vec::new();
        for (label, iters, raise_budget) in [
            ("light", LIGHT_ITERS, false),
            ("heavy", profile.heavy_iters, true),
        ] {
            let cell_iterations = profile.cell_iterations(label);
            let probe: Arc<OnceLock<Signature>> = Arc::new(OnceLock::new());
            let before = er.scrape_metrics().await?;
            let cpu_before = host::cpu_sample(er_pid)?;
            let sampler = BusySampler::spawn(er.metrics_url().to_owned());
            let burst = execute_cell_burst(
                er_rpc_url.clone(),
                BurstConfig {
                    threads: profile.threads,
                    iterations: cell_iterations,
                    batch: profile.batch,
                    rpc_batch: profile.rpc_batch,
                    concurrency: profile.cell_concurrency(label),
                },
                offset,
                accounts.clone(),
                payer_bytes.clone(),
                iters,
                raise_budget,
                probe.clone(),
            )?;
            let outcome = burst.outcome;
            offset += cell_iterations;
            check_eq!(
                outcome.failed,
                0,
                "{label}: measured deliveries failed: {:?}",
                outcome.first_error
            )?;
            let drain = match before.get(TX_COUNT) {
                Some(seen) => {
                    drain_processed(er, seen + cell_iterations as f64).await?
                }
                None => Duration::ZERO,
            };
            let busy = sampler.finish();
            let cpu_after = host::cpu_sample(er_pid)?;
            let after = er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);

            let failed_kind = |kind: &str| {
                delta.counter(&format!(
                    "{}{{kind=\"{kind}\"}}",
                    crate::metrics::FAILED_TRANSACTIONS
                ))
            };
            let dropped = failed_kind("dropped");
            let execution_failed = failed_kind("execution");
            if let Some(dropped) = dropped {
                check_eq!(
                    dropped,
                    0.0,
                    "{label}: the sequencer dropped {dropped:.0} of \
                     {cell_iterations} transactions before execution — either \
                     a duplicate signature or a blockhash already outside the \
                     60 s window. Staging plus blasting took longer than that \
                     window, or ids collided across cells"
                )?;
            }
            if let Some(execution_failed) = execution_failed {
                check_eq!(
                    execution_failed,
                    0.0,
                    "{label}: {execution_failed:.0} of {cell_iterations} \
                     transactions reached the SVM and failed there — compare \
                     the probe's consumed CUs against the {CU_LIMIT} CU limit"
                )?;
            }

            let probe_sig =
                probe.get().copied().ok_or("no probe signature captured")?;
            let probe_tx = er
                .api()
                .await_transaction(&probe_sig, PROBE_TIMEOUT)
                .await?;
            check!(
                probe_tx.err.is_none(),
                "{label}: probe tx failed on-chain (sha256 iters {iters} \
                 over the compute budget?): {:?}\nlogs: {:#?}",
                probe_tx.err,
                probe_tx.logs
            )?;
            let probe_cus = consumed_cus(&probe_tx.logs).ok_or_else(|| {
                CheckError::new(format!(
                    "{label}: probe logs carry no `consumed .. compute \
                     units` line"
                ))
                .actual(format!("{:#?}", probe_tx.logs))
            })?;

            let thread_cores = cpu_after.thread_cores_since(&cpu_before);
            let cell = Cell {
                label,
                iters,
                outcome,
                sign_s: burst.sign_s,
                blast_s: burst.blast_s,
                drain,
                probe_cus,
                cores: cpu_after.cores_since(&cpu_before),
                top_thread_cores: thread_cores.first().copied().unwrap_or(0.0),
                busy_threads: thread_cores
                    .iter()
                    .filter(|cores| **cores >= BUSY_THREAD_CORES)
                    .count(),
                busy,
                validator_txs: delta.counter(TX_COUNT),
                dependencies: delta.counter(ORDERING_DEPENDENCIES),
                blocked: delta.counter(BLOCKED_TRANSACTIONS),
                staged: burst.staged,
                iterations: cell_iterations,
                dropped,
                execution_failed,
            };
            eprintln!(
                "[redsuite] {}: {label} (sha256 iters {iters}): signed in {:.1} s, \
                 blasted in {:.1} s ({:.0} tps delivered), {:.0} tps executed \
                 (drain {:.1} s), p50 {} us / p95 {} us, probe {:.0} cus, \
                 busy executors mean {:.1} / max {:.0} over {} samples, \
                 dependency ratio {:.3}, validator cores {:.2} (top thread \
                 {:.2}, {} threads >= {:.1})",
                self.name(),
                cell.sign_s,
                cell.blast_s,
                cell.delivered_tps(),
                cell.executed_tps(cell_iterations),
                cell.drain.as_secs_f64(),
                cell.outcome.delivery.median,
                cell.outcome.delivery.quantile95,
                cell.probe_cus,
                cell.busy.mean,
                cell.busy.max,
                cell.busy.count,
                cell.dependency_ratio().unwrap_or(f64::NAN),
                cell.cores,
                cell.top_thread_cores,
                cell.busy_threads,
                BUSY_THREAD_CORES,
            );

            let cell_report =
                ScenarioReport::ok(&format!("{}/{label}", self.name()))
                    .setting("profile", profile.name)
                    .setting("sha256 iters", cell.iters)
                    .setting(
                        "shape",
                        "width-1 sha256 hash-chain, one payer per account",
                    )
                    .setting("accounts", profile.accounts)
                    .setting("driver threads", profile.threads)
                    .setting("measured iters", cell_iterations)
                    .setting("mode", profile.mode())
                    .setting("rpc batch", profile.rpc_batch)
                    .setting("batch per thread", profile.batch)
                    .setting("concurrency", profile.cell_concurrency(label))
                    .observe("delivery us", Unit::Micros, cell.outcome.delivery)
                    .metric(
                        "achieved tps",
                        Unit::Tps,
                        cell.outcome.achieved_rps(),
                    )
                    .metric(
                        "executed tps",
                        Unit::Tps,
                        cell.executed_tps(cell_iterations),
                    )
                    .metric("sign s", Unit::Seconds, cell.sign_s)
                    .metric("blast s", Unit::Seconds, cell.blast_s)
                    .metric("drain s", Unit::Seconds, cell.drain.as_secs_f64())
                    .metric("probe consumed cus", Unit::Count, cell.probe_cus)
                    .metric("busy executors mean", Unit::Count, cell.busy.mean)
                    .metric("busy executors max", Unit::Count, cell.busy.max)
                    .metric_if(
                        "dependency ratio",
                        Unit::Ratio,
                        cell.dependency_ratio(),
                    )
                    .metric_if(
                        "ordering dependencies",
                        Unit::Count,
                        cell.dependencies,
                    )
                    .metric_if("blocked txs", Unit::Count, cell.blocked)
                    .metric("validator cores", Unit::Count, cell.cores)
                    .metric(
                        "top thread cores",
                        Unit::Count,
                        cell.top_thread_cores,
                    )
                    .metric(
                        "busy threads",
                        Unit::Count,
                        cell.busy_threads as f64,
                    )
                    .metric_if(
                        "validator txs in window",
                        Unit::Count,
                        cell.validator_txs,
                    )
                    .metric_if("dropped txs", Unit::Count, cell.dropped)
                    .metric_if(
                        "execution failed txs",
                        Unit::Count,
                        cell.execution_failed,
                    );
            match report::persist_cell(self.name(), &cell_report) {
                Ok(path) => {
                    eprintln!("[redsuite]   cell report: {}", path.display())
                }
                Err(err) => eprintln!(
                    "[redsuite]   warning: cell report not persisted: {err}"
                ),
            }
            cells.push(cell);
        }

        let expected_hash =
            hash_chain(HASH_INIT.to_bytes(), profile.heavy_iters);
        for (index, pda) in accounts.iter().enumerate() {
            let on_er = er.account(pda).await?.ok_or("pda not on er")?;
            let hash_bytes = &on_er.data
                [layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE];
            check_eq!(
                hash_bytes,
                expected_hash,
                "account {index} must hold the {}-iteration hash chain — \
                 heavy work was not executed",
                profile.heavy_iters
            )?;
        }

        let light = &cells[0];
        let heavy = &cells[1];
        let cu_ratio = heavy.probe_cus / light.probe_cus;
        check!(
            cu_ratio >= CU_CONTRAST_FLOOR,
            "heavy cell consumed {:.0} cus per tx vs light {:.0} — not the \
             >= {CU_CONTRAST_FLOOR}x compute contrast this scenario is about",
            heavy.probe_cus,
            light.probe_cus,
        )?;

        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting(
                "shape",
                "width-1 sha256 hash-chain, one payer per account",
            )
            .setting("accounts", profile.accounts)
            .setting("light iters", LIGHT_ITERS)
            .setting("heavy iters", profile.heavy_iters)
            .setting("driver threads", profile.threads)
            .setting("measured iters", profile.iterations)
            .setting("heavy measured iters", profile.heavy_iterations)
            .setting("mode", profile.mode())
            .setting("rpc batch", profile.rpc_batch)
            .setting("batch per thread", profile.batch)
            .setting("concurrency", profile.concurrency)
            .setting("heavy concurrency", profile.heavy_concurrency)
            .metric("heavy/light probe cu ratio", Unit::Ratio, cu_ratio)
            .metric(
                "heavy/light cores ratio",
                Unit::Ratio,
                if light.cores > 0.0 {
                    heavy.cores / light.cores
                } else {
                    0.0
                },
            );
        for cell in &cells {
            summary = summary
                .metric(
                    format!("{} achieved tps", cell.label),
                    Unit::Tps,
                    cell.delivered_tps(),
                )
                .metric(
                    format!("{} executed tps", cell.label),
                    Unit::Tps,
                    cell.executed_tps(cell.iterations),
                )
                .metric(
                    format!("{} staged txs", cell.label),
                    Unit::Count,
                    cell.staged as f64,
                )
                .metric(
                    format!("{} delivery p50 us", cell.label),
                    Unit::Micros,
                    cell.outcome.delivery.median as f64,
                )
                .metric(
                    format!("{} delivery p95 us", cell.label),
                    Unit::Micros,
                    cell.outcome.delivery.quantile95 as f64,
                )
                .metric(
                    format!("{} sign s", cell.label),
                    Unit::Seconds,
                    cell.sign_s,
                )
                .metric(
                    format!("{} blast s", cell.label),
                    Unit::Seconds,
                    cell.blast_s,
                )
                .metric(
                    format!("{} drain s", cell.label),
                    Unit::Seconds,
                    cell.drain.as_secs_f64(),
                )
                .metric(
                    format!("{} probe consumed cus", cell.label),
                    Unit::Count,
                    cell.probe_cus,
                )
                .metric(
                    format!("{} busy executors mean", cell.label),
                    Unit::Count,
                    cell.busy.mean,
                )
                .metric(
                    format!("{} busy executors max", cell.label),
                    Unit::Count,
                    cell.busy.max,
                )
                .metric_if(
                    format!("{} dependency ratio", cell.label),
                    Unit::Ratio,
                    cell.dependency_ratio(),
                )
                .metric(
                    format!("{} validator cores", cell.label),
                    Unit::Count,
                    cell.cores,
                )
                .metric(
                    format!("{} top thread cores", cell.label),
                    Unit::Count,
                    cell.top_thread_cores,
                )
                .metric(
                    format!("{} busy threads", cell.label),
                    Unit::Count,
                    cell.busy_threads as f64,
                );
        }
        Ok(summary)
    }
}
