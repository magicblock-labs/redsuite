use std::{
    rc::Rc,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use instruction::Instruction;
use pubkey::Pubkey;
use redsuite_core::redline::Accounts;
use redsuite_core::report::Unit::{Count, Micros, Ratio, Seconds, Tps};
use redsuite_core::{
    check, check_eq, host, prep,
    profile::ProfileValues,
    redline::causal::{compute_unit_limit, CU_LIMIT, HASH_INIT},
    report,
    runner::{
        execute_raw, merge_outcomes, spawn_workers, Pacing, RawRunOutcome,
        RunConfig, RunOutcome, WorkerBudgets,
    },
    sampler::{MeanMax, Sampled, Sampler, Trailing},
    topology, Api, BaseCtx, ChainCtx, ErClient, ErCtx, MetricsDelta, Result,
    Scenario, ScenarioReport, SendBody, TxSender,
};
use signature::Signature;

use crate::program::{instruction::build, layout, utils::hash_chain};

const PAYER_LAMPORTS: u64 = 200_000_000;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(900);
const CLONE_TIMEOUT: Duration = Duration::from_secs(60);
const BUSY_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const TX_COUNT: &str = crate::metrics::ENGINE_TRANSACTIONS;
const BUSY_EXECUTORS: &str = "engine_processor_busy_executors";
const ORDERING_DEPENDENCIES: &str = "engine_processor_ordering_dependencies";
const BLOCKED_TRANSACTIONS: &str = "engine_processor_blocked_transactions";
const PROGRAM: Pubkey = crate::program::ID;
const LIGHT_ITERS: u32 = 1;
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
            "staged backlog, pre-encoded batch transport"
        } else {
            "pre-encoded individual transport burst"
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

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
};

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

struct WorkerBurst {
    outcomes: Vec<RawRunOutcome>,
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
async fn execute_cell_burst(
    er_rpc_url: String,
    config: BurstConfig,
    id_offset: u64,
    accounts: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
    iters: u32,
    raise_budget: bool,
    probe: Arc<OnceLock<Signature>>,
) -> Result<BurstOutcome> {
    let budgets = WorkerBudgets::partition(
        config.threads,
        config.iterations,
        Pacing::Unlimited,
        config.concurrency,
    )?;
    let batch = config.batch.max(1);
    let rpc_batch = config.rpc_batch;
    let bursts = spawn_workers(budgets.workers(), move |worker| {
        let (first_id, iterations) = budgets.iterations[worker.index];
        let concurrency = budgets.concurrency[worker.index];
        let thread_first_id = id_offset + first_id;
        let er_rpc_url = er_rpc_url.clone();
        let accounts = accounts.clone();
        let payer_bytes = payer_bytes.clone();
        let probe = probe.clone();
        async move {
            let client = ErClient::new(er_rpc_url);
            let senders: Vec<TxSender> = payer_bytes
                .iter()
                .map(|bytes| {
                    let payer = prep::payer_from_bytes(bytes);
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
                    let ixs =
                        build_ixs(&accounts, global_id, iters, raise_budget);
                    let sender = &senders[slot_of(global_id, senders.len())];
                    let tx = sender
                        .prepare(&ixs)
                        .await
                        .expect("pre-signing must not fail");
                    let _ = probe.set(tx.signatures[0]);
                    signed.push(tx);
                }
                staged += signed.len() as u64;
                let bodies: Vec<Rc<SendBody>> = signed
                    .chunks(rpc_batch.max(1))
                    .map(|chunk| {
                        Rc::new(
                            Api::send_body(chunk)
                                .expect("send body build is infallible"),
                        )
                    })
                    .collect();
                sign_s += sign_started.elapsed().as_secs_f64();

                let blast_started = Instant::now();
                let outcome = execute_raw(
                    RunConfig {
                        iterations: bodies.len() as u64,
                        rate: Pacing::Unlimited,
                        concurrency,
                    },
                    |index| {
                        let body = bodies[(index - 1) as usize].clone();
                        let api = api.clone();
                        async move {
                            match api.send_prepared(&body).await {
                                Ok(0) => Ok(()),
                                Ok(rejected) => {
                                    Err(format!("{rejected} entries rejected")
                                        .into())
                                }
                                Err(error) => Err(error),
                            }
                        }
                    },
                )
                .await?;
                blast_s += blast_started.elapsed().as_secs_f64();
                outcomes.push(outcome);
            }
            Ok(WorkerBurst {
                outcomes,
                sign_s,
                blast_s,
                staged,
            })
        }
    })
    .join_async()
    .await?;

    let mut all_outcomes: Vec<RawRunOutcome> = Vec::new();
    let mut sign_s = 0.0f64;
    let mut blast_s = 0.0f64;
    let mut staged = 0u64;
    for burst in bursts {
        all_outcomes.extend(burst.outcomes);
        sign_s = sign_s.max(burst.sign_s);
        blast_s = blast_s.max(burst.blast_s);
        staged += burst.staged;
    }
    let mut outcome = merge_outcomes(all_outcomes);
    outcome.wall = Duration::from_secs_f64(blast_s.max(1e-9));
    Ok(BurstOutcome {
        outcome,
        sign_s,
        blast_s,
        staged,
    })
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
    busy: Sampled<MeanMax>,
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
impl Scenario<ScenarioReport> for ExecutorSaturation {
    fn name(&self) -> &str {
        "redline/executor_saturation"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = PROFILES.select(base.config().profile);

        let prep_started = Instant::now();
        let payers =
            prep::funded_payers(base, profile.accounts, PAYER_LAMPORTS).await?;
        let accounts = Accounts {
            program_id: PROGRAM,
            space: crate::ACCOUNT_SPACE,
            authority: er.identity(),
        }
        .init_batched(base, &payers, profile.accounts, true)
        .await?;
        prep::await_clones(
            er,
            &accounts,
            crate::ACCOUNT_SPACE as usize,
            CLONE_TIMEOUT,
        )
        .await?;
        eprintln!(
            "[redsuite] {}: prepped {} payers x 1 delegated account in {:.1} s",
            self.name(),
            profile.accounts,
            prep_started.elapsed().as_secs_f64(),
        );
        let accounts = Arc::new(accounts);
        let payer_bytes = prep::payer_bytes(&payers);
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
        )
        .await?;
        check_eq!(
            warm.outcome.failed,
            0,
            "warmup deliveries failed: {:?}",
            warm.outcome.first_error
        )?;
        if let Some(seen) = count_before_warmup {
            crate::await_executed(
                er,
                seen + profile.warmup as f64,
                DRAIN_TIMEOUT,
            )
            .await?;
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
            let sampler = Sampler::spawn(
                er.metrics().clone(),
                BUSY_SAMPLE_INTERVAL,
                Trailing::Skip,
                |metrics, busy: &mut MeanMax| {
                    busy.push(metrics.get(BUSY_EXECUTORS)?);
                    Some(())
                },
            );
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
            )
            .await?;
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
                    crate::await_executed(
                        er,
                        seen + cell_iterations as f64,
                        DRAIN_TIMEOUT,
                    )
                    .await?
                }
                None => Duration::ZERO,
            };
            let busy = sampler.finish_complete(BUSY_EXECUTORS).await?;
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
            let probe_cus =
                crate::probe_cus(er, &probe_sig, label, iters).await?;

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
            let busy = &cell.busy.value;
            eprintln!(
                "[redsuite] {}: {label} (sha256 iters {iters}): sign+encode {:.1} s, \
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
                busy.mean(),
                busy.max,
                busy.count,
                cell.dependency_ratio().unwrap_or(f64::NAN),
                cell.cores,
                cell.top_thread_cores,
                cell.busy_threads,
                BUSY_THREAD_CORES,
            );

            let mut cell_report =
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
                    .observe("delivery us", Micros, cell.outcome.delivery)
                    .setting("metrics sampling", &cell.busy);
            for (name, unit, value) in [
                ("achieved tps", Tps, Some(cell.outcome.achieved_rps())),
                (
                    "executed tps",
                    Tps,
                    Some(cell.executed_tps(cell_iterations)),
                ),
                ("sign+encode s", Seconds, Some(cell.sign_s)),
                ("blast s", Seconds, Some(cell.blast_s)),
                ("drain s", Seconds, Some(cell.drain.as_secs_f64())),
                ("probe consumed cus", Count, Some(cell.probe_cus)),
                ("busy executors mean", Count, Some(busy.mean())),
                ("busy executors max", Count, Some(busy.max)),
                ("dependency ratio", Ratio, cell.dependency_ratio()),
                ("ordering dependencies", Count, cell.dependencies),
                ("blocked txs", Count, cell.blocked),
                ("validator cores", Count, Some(cell.cores)),
                ("top thread cores", Count, Some(cell.top_thread_cores)),
                ("busy threads", Count, Some(cell.busy_threads as f64)),
                ("validator txs in window", Count, cell.validator_txs),
                ("dropped txs", Count, cell.dropped),
                ("execution failed txs", Count, cell.execution_failed),
            ] {
                cell_report = cell_report.metric_if(name, unit, value);
            }
            report::persist_cell(self.name(), &cell_report);
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
            .metric("heavy/light probe cu ratio", Ratio, cu_ratio)
            .metric(
                "heavy/light cores ratio",
                Ratio,
                if light.cores > 0.0 {
                    heavy.cores / light.cores
                } else {
                    0.0
                },
            );
        for cell in &cells {
            let busy = &cell.busy.value;
            for (name, unit, value) in [
                ("achieved tps", Tps, Some(cell.delivered_tps())),
                (
                    "executed tps",
                    Tps,
                    Some(cell.executed_tps(cell.iterations)),
                ),
                ("staged txs", Count, Some(cell.staged as f64)),
                (
                    "delivery p50 us",
                    Micros,
                    Some(cell.outcome.delivery.median as f64),
                ),
                (
                    "delivery p95 us",
                    Micros,
                    Some(cell.outcome.delivery.quantile95 as f64),
                ),
                ("sign+encode s", Seconds, Some(cell.sign_s)),
                ("blast s", Seconds, Some(cell.blast_s)),
                ("drain s", Seconds, Some(cell.drain.as_secs_f64())),
                ("probe consumed cus", Count, Some(cell.probe_cus)),
                ("busy executors mean", Count, Some(busy.mean())),
                ("busy executors max", Count, Some(busy.max)),
                ("dependency ratio", Ratio, cell.dependency_ratio()),
                ("validator cores", Count, Some(cell.cores)),
                ("top thread cores", Count, Some(cell.top_thread_cores)),
                ("busy threads", Count, Some(cell.busy_threads as f64)),
            ] {
                summary = summary.metric_if(
                    format!("{} {name}", cell.label),
                    unit,
                    value,
                );
            }
        }
        Ok(summary)
    }
}
