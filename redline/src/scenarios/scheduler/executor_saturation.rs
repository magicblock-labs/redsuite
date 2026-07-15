use std::{
    rc::Rc,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    assert::poll_until,
    host, prep, report,
    runner::{drive, RunConfig, RunOutcome},
    stats::ObservationsStats,
    topology, BaseCtx, ChainCtx, ErClient, ErCtx, MetricsDelta, Result,
    Scenario, ScenarioReport, TxSender,
};
use signature::Signature;

use crate::program::{instruction::build, layout, utils::hash_chain};

const PAYER_LAMPORTS: u64 = 2_000_000_000;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(900);
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const CLONE_TIMEOUT: Duration = Duration::from_secs(60);
const TX_COUNT: &str = "mbv_transaction_count";
const HASH_INIT: Pubkey = Pubkey::new_from_array([7u8; 32]);
const LIGHT_ITERS: u32 = 1;
const CU_LIMIT: u32 = 1_400_000;
const CU_CONTRAST_FLOOR: f64 = 10.0;
const BUSY_THREAD_CORES: f64 = 0.5;

struct Profile {
    name: &'static str,
    programs: usize,
    payers: usize,
    accounts_per_program: usize,
    heavy_iters: u32,
    threads: usize,
    warmup: u64,
    iterations: u64,
    batch: usize,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    programs: 8,
    payers: 32,
    accounts_per_program: 12,
    heavy_iters: 180,
    threads: 8,
    warmup: 5_000,
    iterations: 60_000,
    batch: 2_500,
    concurrency: 2_048,
};

const FULL: Profile = Profile {
    name: "full",
    programs: 8,
    payers: 32,
    accounts_per_program: 32,
    heavy_iters: 180,
    threads: 8,
    warmup: 25_000,
    iterations: 300_000,
    batch: 2_500,
    concurrency: 2_048,
};

fn profile() -> &'static Profile {
    match std::env::var("REDSUITE_PROFILE") {
        Ok(name) if name == "full" => &FULL,
        Ok(name) if name == "lite" => &LITE,
        Ok(name) => panic!("unknown REDSUITE_PROFILE `{name}` (lite|full)"),
        Err(_) => &LITE,
    }
}

fn compute_unit_limit(limit: u32) -> Instruction {
    let mut data = vec![2u8];
    data.extend_from_slice(&limit.to_le_bytes());
    Instruction {
        program_id: "ComputeBudget111111111111111111111111111111"
            .parse()
            .expect("compute budget id parses"),
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

struct BurstConfig {
    threads: usize,
    iterations: u64,
    batch: usize,
    concurrency: usize,
}

struct BurstOutcome {
    outcome: RunOutcome,
    sign_s: f64,
    blast_s: f64,
}

fn build_ixs(
    programs: &[Pubkey],
    pools: &[Vec<Pubkey>],
    global_id: u64,
    iters: u32,
    raise_budget: bool,
) -> Vec<Instruction> {
    let index = (global_id - 1) as usize;
    let program_index = index % programs.len();
    let pda = pools[program_index]
        [(index / programs.len()) % pools[program_index].len()];
    let compute = build::expensive_hash_compute_at(
        programs[program_index],
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
fn drive_cell_burst(
    er_rpc_url: String,
    config: BurstConfig,
    id_offset: u64,
    programs: Arc<Vec<Pubkey>>,
    pools: Arc<Vec<Vec<Pubkey>>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
    iters: u32,
    raise_budget: bool,
    probe: Arc<OnceLock<Signature>>,
) -> BurstOutcome {
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
        let programs = programs.clone();
        let pools = pools.clone();
        let payer_bytes = payer_bytes.clone();
        let probe = probe.clone();
        let batch = config.batch.max(1);
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
                    .enumerate()
                    .filter(|(payer_index, _)| {
                        payer_index % threads == thread_index
                    })
                    .map(|(_, bytes)| {
                        let payer = Keypair::try_from(&bytes[..])
                            .expect("payer bytes round-trip");
                        client.sender(Rc::new(payer))
                    })
                    .collect();
                let api = client.api().clone();

                let mut outcomes = Vec::new();
                let mut sign_s = 0.0f64;
                let mut blast_s = 0.0f64;
                let ids: Vec<u64> = (1..=iterations)
                    .map(|iteration| thread_first_id + iteration)
                    .collect();
                for chunk in ids.chunks(batch) {
                    let sign_started = Instant::now();
                    let mut signed = Vec::with_capacity(chunk.len());
                    for &global_id in chunk {
                        let ixs = build_ixs(
                            &programs,
                            &pools,
                            global_id,
                            iters,
                            raise_budget,
                        );
                        let sender =
                            &senders[(global_id as usize) % senders.len()];
                        let tx = sender
                            .prepare(&ixs)
                            .await
                            .expect("pre-signing must not fail");
                        let _ = probe.set(tx.signatures[0]);
                        signed.push(tx);
                    }
                    sign_s += sign_started.elapsed().as_secs_f64();

                    let blast_started = Instant::now();
                    let outcome = drive(
                        RunConfig {
                            iterations: signed.len() as u64,
                            rate: u32::MAX,
                            concurrency,
                        },
                        |batch_index| {
                            let tx = signed[(batch_index - 1) as usize].clone();
                            let api = api.clone();
                            async move {
                                api.send_transaction(&tx).await.map(|_| ())
                            }
                        },
                    )
                    .await;
                    blast_s += blast_started.elapsed().as_secs_f64();
                    outcomes.push(outcome);
                }
                (outcomes, sign_s, blast_s)
            }));
            let _ = outcome_sender.send(result);
        }));
    }
    drop(outcome_sender);

    let mut all_outcomes: Vec<RunOutcome> = Vec::new();
    let mut sign_s = 0.0f64;
    let mut blast_s = 0.0f64;
    while let Ok((outcomes, thread_sign_s, thread_blast_s)) =
        outcome_receiver.recv()
    {
        all_outcomes.extend(outcomes);
        sign_s = sign_s.max(thread_sign_s);
        blast_s = blast_s.max(thread_blast_s);
    }
    for handle in handles {
        let _ = handle.join();
    }

    let outcome = RunOutcome {
        delivered: all_outcomes.iter().map(|outcome| outcome.delivered).sum(),
        failed: all_outcomes.iter().map(|outcome| outcome.failed).sum(),
        first_error: all_outcomes
            .iter()
            .find_map(|outcome| outcome.first_error.clone()),
        delivery: ObservationsStats::merge(
            all_outcomes
                .iter()
                .map(|outcome| outcome.delivery)
                .collect(),
            true,
        ),
        sync: None,
        rps: ObservationsStats::merge(
            all_outcomes.iter().map(|outcome| outcome.rps).collect(),
            false,
        ),
        wall: Duration::from_secs_f64(blast_s.max(1e-9)),
    };
    BurstOutcome {
        outcome,
        sign_s,
        blast_s,
    }
}

async fn drain_processed(er: &ErCtx, target: f64) -> Result<Duration> {
    let started = Instant::now();
    poll_until(DRAIN_TIMEOUT, || async {
        matches!(
            er.scrape_metrics().await.ok().and_then(|metrics| metrics.get(TX_COUNT)),
            Some(count) if count >= target
        )
    })
    .await;
    let drained = er.scrape_metrics().await?.get(TX_COUNT).unwrap_or(0.0);
    if drained < target {
        return Err(format!(
            "intake never drained: {drained:.0} < {target:.0} after \
             {DRAIN_TIMEOUT:?}"
        )
        .into());
    }
    Ok(started.elapsed())
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
    validator_txs: Option<f64>,
}

impl Cell {
    fn executed_tps(&self, iterations: u64) -> f64 {
        iterations as f64 / (self.outcome.wall + self.drain).as_secs_f64()
    }
}

pub struct ExecutorSaturation;

#[async_trait(?Send)]
impl Scenario for ExecutorSaturation {
    fn name(&self) -> &str {
        "redline/executor_saturation"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = profile();
        let programs: Arc<Vec<Pubkey>> =
            Arc::new(topology::redline_alias_ids(profile.programs));
        for program in programs.iter() {
            let deployed = base.account(program).await?;
            if !deployed.map(|account| account.executable).unwrap_or(false) {
                return Err(format!(
                    "alias program {program} is not on the base chain — the \
                     running stack predates program aliases; run `stack down` \
                     and retry"
                )
                .into());
            }
        }

        let payers =
            prep::funded_payers(base, profile.payers, PAYER_LAMPORTS).await?;
        let prep_started = Instant::now();
        let mut pools: Vec<Vec<Pubkey>> = Vec::with_capacity(profile.programs);
        for program in programs.iter() {
            let pool = crate::init_delegated_accounts_batched_at(
                *program,
                base,
                &payers,
                profile.accounts_per_program,
                crate::ACCOUNT_SPACE,
                er.identity(),
            )
            .await?;
            for pda in &pool {
                poll_until(CLONE_TIMEOUT, || async {
                    matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                })
                .await;
            }
            pools.push(pool);
        }
        eprintln!(
            "[redsuite] {}: prepped {} programs x {} delegated accounts in {:.1} s",
            self.name(),
            profile.programs,
            profile.accounts_per_program,
            prep_started.elapsed().as_secs_f64(),
        );
        let pools = Arc::new(pools);
        let payer_bytes: Arc<Vec<[u8; 64]>> =
            Arc::new(payers.iter().map(|payer| payer.to_bytes()).collect());
        let er_rpc_url = er.api().url().to_owned();
        let er_pid = topology::current_state()
            .ok_or("no shared stack state")?
            .er_pid;

        let count_before_warmup = er.scrape_metrics().await?.get(TX_COUNT);
        let warm = drive_cell_burst(
            er_rpc_url.clone(),
            BurstConfig {
                threads: profile.threads,
                iterations: profile.warmup,
                batch: profile.batch,
                concurrency: profile.concurrency,
            },
            0,
            programs.clone(),
            pools.clone(),
            payer_bytes.clone(),
            LIGHT_ITERS,
            false,
            Arc::new(OnceLock::new()),
        );
        assert_eq!(
            warm.outcome.failed, 0,
            "warmup deliveries failed: {:?}",
            warm.outcome.first_error
        );
        if let Some(seen) = count_before_warmup {
            drain_processed(er, seen + profile.warmup as f64).await?;
        }

        let mut offset = profile.warmup;
        let mut cells: Vec<Cell> = Vec::new();
        for (label, iters, raise_budget) in [
            ("light", LIGHT_ITERS, false),
            ("heavy", profile.heavy_iters, true),
        ] {
            let probe: Arc<OnceLock<Signature>> = Arc::new(OnceLock::new());
            let before = er.scrape_metrics().await?;
            let cpu_before = host::cpu_sample(er_pid)?;
            let burst = drive_cell_burst(
                er_rpc_url.clone(),
                BurstConfig {
                    threads: profile.threads,
                    iterations: profile.iterations,
                    batch: profile.batch,
                    concurrency: profile.concurrency,
                },
                offset,
                programs.clone(),
                pools.clone(),
                payer_bytes.clone(),
                iters,
                raise_budget,
                probe.clone(),
            );
            let outcome = burst.outcome;
            offset += profile.iterations;
            assert_eq!(
                outcome.failed, 0,
                "{label}: measured deliveries failed: {:?}",
                outcome.first_error
            );
            let drain = match before.get(TX_COUNT) {
                Some(seen) => {
                    drain_processed(er, seen + profile.iterations as f64)
                        .await?
                }
                None => Duration::ZERO,
            };
            let cpu_after = host::cpu_sample(er_pid)?;
            let after = er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);

            let probe_sig =
                probe.get().copied().ok_or("no probe signature captured")?;
            let probe_tx = er
                .api()
                .await_transaction(&probe_sig, PROBE_TIMEOUT)
                .await?;
            assert!(
                probe_tx.err.is_none(),
                "{label}: probe tx failed on-chain (sha256 iters {iters} \
                 over the compute budget?): {:?}\nlogs: {:#?}",
                probe_tx.err,
                probe_tx.logs
            );
            let probe_cus = consumed_cus(&probe_tx.logs).ok_or_else(|| {
                format!(
                    "{label}: probe logs carry no `consumed .. compute \
                     units` line: {:#?}",
                    probe_tx.logs
                )
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
                validator_txs: delta.counter(TX_COUNT),
            };
            eprintln!(
                "[redsuite] {}: {label} (sha256 iters {iters}): signed in {:.1} s, \
                 blasted in {:.1} s ({:.0} tps delivered), {:.0} tps executed \
                 (drain {:.1} s), p50 {} us / p95 {} us, probe {:.0} cus, \
                 validator cores {:.2} (top thread {:.2}, {} threads >= {:.1})",
                self.name(),
                cell.sign_s,
                cell.blast_s,
                cell.outcome.achieved_rps(),
                cell.executed_tps(profile.iterations),
                cell.drain.as_secs_f64(),
                cell.outcome.delivery.median,
                cell.outcome.delivery.quantile95,
                cell.probe_cus,
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
                        "width-1 sha256 hash-chain, programs round-robin",
                    )
                    .setting("programs", profile.programs)
                    .setting(
                        "accounts per program",
                        profile.accounts_per_program,
                    )
                    .setting("payers", profile.payers)
                    .setting("driver threads", profile.threads)
                    .setting("measured iters", profile.iterations)
                    .setting("mode", "pre-signed burst")
                    .setting("batch per thread", profile.batch)
                    .setting("concurrency", profile.concurrency)
                    .observe("delivery us", cell.outcome.delivery)
                    .metric("achieved tps", cell.outcome.achieved_rps())
                    .metric(
                        "executed tps",
                        cell.executed_tps(profile.iterations),
                    )
                    .metric("sign s", cell.sign_s)
                    .metric("blast s", cell.blast_s)
                    .metric("drain s", cell.drain.as_secs_f64())
                    .metric("probe consumed cus", cell.probe_cus)
                    .metric("validator cores", cell.cores)
                    .metric("top thread cores", cell.top_thread_cores)
                    .metric("busy threads", cell.busy_threads as f64)
                    .metric_if("validator txs in window", cell.validator_txs);
            match report::persist(&cell_report) {
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
        for (program_index, pool) in pools.iter().enumerate() {
            for (pda_index, pda) in pool.iter().enumerate() {
                let on_er = er.account(pda).await?.ok_or("pda not on er")?;
                let hash_bytes = &on_er.data[layout::HASH_OFFSET
                    ..layout::HASH_OFFSET + layout::HASH_SIZE];
                assert_eq!(
                    hash_bytes, expected_hash,
                    "program {program_index} pda {pda_index} must hold the \
                     {}-iteration hash chain — heavy work was not executed",
                    profile.heavy_iters
                );
            }
        }

        let light = &cells[0];
        let heavy = &cells[1];
        let cu_ratio = heavy.probe_cus / light.probe_cus;
        assert!(
            cu_ratio >= CU_CONTRAST_FLOOR,
            "heavy cell consumed {:.0} cus per tx vs light {:.0} — not the \
             >= {CU_CONTRAST_FLOOR}x compute contrast this scenario is about",
            heavy.probe_cus,
            light.probe_cus,
        );

        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("shape", "width-1 sha256 hash-chain, programs round-robin")
            .setting("programs", profile.programs)
            .setting("accounts per program", profile.accounts_per_program)
            .setting("light iters", LIGHT_ITERS)
            .setting("heavy iters", profile.heavy_iters)
            .setting("payers", profile.payers)
            .setting("driver threads", profile.threads)
            .setting("measured iters per cell", profile.iterations)
            .setting("mode", "pre-signed burst")
            .setting("batch per thread", profile.batch)
            .setting("concurrency", profile.concurrency)
            .metric("heavy/light probe cu ratio", cu_ratio)
            .metric(
                "heavy/light cores ratio",
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
                    cell.outcome.achieved_rps(),
                )
                .metric(
                    format!("{} executed tps", cell.label),
                    cell.executed_tps(profile.iterations),
                )
                .metric(
                    format!("{} delivery p50 us", cell.label),
                    cell.outcome.delivery.median as f64,
                )
                .metric(
                    format!("{} delivery p95 us", cell.label),
                    cell.outcome.delivery.quantile95 as f64,
                )
                .metric(format!("{} sign s", cell.label), cell.sign_s)
                .metric(format!("{} blast s", cell.label), cell.blast_s)
                .metric(
                    format!("{} drain s", cell.label),
                    cell.drain.as_secs_f64(),
                )
                .metric(
                    format!("{} probe consumed cus", cell.label),
                    cell.probe_cus,
                )
                .metric(format!("{} validator cores", cell.label), cell.cores)
                .metric(
                    format!("{} top thread cores", cell.label),
                    cell.top_thread_cores,
                )
                .metric(
                    format!("{} busy threads", cell.label),
                    cell.busy_threads as f64,
                );
        }
        Ok(summary)
    }
}
