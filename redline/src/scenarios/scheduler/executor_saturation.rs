use std::{
    future,
    rc::Rc,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use instruction::Instruction;
use pubkey::Pubkey;
use redsuite_core::{
    host, prep,
    profile::ProfileValues,
    programs::{HashState, REDLINE},
    redline::{causal, Accounts},
    report::Unit::{Count, Micros, Ratio, Seconds, Tps},
    runner::{
        execute_raw, merge_outcomes, spawn_workers, Pacing, RawRunOutcome, RunConfig, RunOutcome,
        SendTransaction, WorkerBudgets,
    },
    sampler::{MeanMax, Sampled, Sampler, Trailing},
    Api, BaseCtx, ChainCtx, ErClient, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
    SendBody, TxSender,
};
use signature::Signature;
use tracing::debug;
use url::Url;

use crate::{metrics, program::layout, ACCOUNT_SPACE};

const PAYER_LAMPORTS: u64 = 200_000_000;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(900);
const BUSY_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const TX_COUNT: &str = metrics::ENGINE_TRANSACTIONS;
const BUSY_EXECUTORS: &str = "engine_processor_busy_executors";
const ORDERING_DEPENDENCIES: &str = "engine_processor_ordering_dependencies";
const BLOCKED_TRANSACTIONS: &str = "engine_processor_blocked_transactions";
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

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

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
    let compute = REDLINE.hash(global_id, causal::HASH_INIT.to_bytes(), iters, &[pda]);
    if raise_budget {
        vec![causal::compute_unit_limit(causal::CU_LIMIT), compute]
    } else {
        vec![compute]
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_cell_burst(
    er_rpc_url: Url,
    er_ws_url: Url,
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
        let er_ws_url = er_ws_url.clone();
        let accounts = accounts.clone();
        let payer_bytes = payer_bytes.clone();
        let probe = probe.clone();
        async move {
            let client = ErClient::new(er_rpc_url, er_ws_url);
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
            let ids: Vec<u64> =
                (1..=iterations).map(|iteration| thread_first_id + iteration).collect();
            for chunk in ids.chunks(batch) {
                let sign_started = Instant::now();
                let mut signed = Vec::with_capacity(chunk.len());
                for &global_id in chunk {
                    let ixs = build_ixs(&accounts, global_id, iters, raise_budget);
                    let sender = &senders[slot_of(global_id, senders.len())];
                    let tx = sender.prepare(&ixs).await.expect("pre-signing must not fail");
                    let _ = probe.set(tx.signatures[0]);
                    signed.push(tx);
                }
                staged += signed.len() as u64;
                let bodies: Vec<Rc<SendBody>> = signed
                    .chunks(rpc_batch.max(1))
                    .map(|chunk| {
                        Rc::new(Api::send_body(chunk).expect("send body build is infallible"))
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
                        future::ready(Ok(SendTransaction(move |_: Instant| async move {
                            let rejected = api.send_prepared(&body).await?;
                            ensure!(rejected == 0);
                            Ok(())
                        })))
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
    Ok(BurstOutcome { outcome, sign_s, blast_s, staged })
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

pub struct ExecutorSaturation {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    accounts: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
}

impl Scenario<ScenarioReport> for ExecutorSaturation {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);

        let payers = base.funded_payers(profile.accounts, PAYER_LAMPORTS).await?;
        let accounts = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base.as_ref(), &payers, profile.accounts, true)
            .await?;
        er.await_clones(&accounts, ACCOUNT_SPACE as usize).await?;
        let accounts = Arc::new(accounts);
        let payer_bytes = prep::payer_bytes(&payers);

        Ok(Self {
            base,
            er,
            profile,
            accounts,
            payer_bytes,
        })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let Self {
            base,
            er,
            profile,
            accounts,
            payer_bytes,
        } = self;
        let er_rpc_url = er.api().url().clone();
        let er_pid = er.pid();
        let count_before_warmup = er.scrape_metrics().await?.get(TX_COUNT);
        let warm = execute_cell_burst(
            er_rpc_url.clone(),
            er.ws_url().clone(),
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
        ensure!(warm.outcome.failed == 0);
        if let Some(seen) = count_before_warmup {
            crate::await_executed(&er, seen + profile.warmup as f64, DRAIN_TIMEOUT).await?;
        }

        let mut offset = profile.warmup;
        let mut cells: Vec<Cell> = Vec::new();
        for (label, iters, raise_budget) in
            [("light", LIGHT_ITERS, false), ("heavy", profile.heavy_iters, true)]
        {
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
                er.ws_url().clone(),
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
            ensure!(outcome.failed == 0);
            let drain = match before.get(TX_COUNT) {
                Some(seen) => {
                    crate::await_executed(&er, seen + cell_iterations as f64, DRAIN_TIMEOUT).await?
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
                    metrics::FAILED_TRANSACTIONS
                ))
            };
            let dropped = failed_kind("dropped");
            let execution_failed = failed_kind("execution");
            if let Some(dropped) = dropped {
                ensure!(dropped == 0.0);
            }
            if let Some(execution_failed) = execution_failed {
                ensure!(execution_failed == 0.0);
            }

            let probe_sig = probe.get().copied().context("no probe signature captured")?;
            let probe_cus = crate::probe_cus(&er, &probe_sig, label, iters).await?;

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
            debug!(
                label,
                busy_mean = busy.mean(),
                busy.max,
                "executor saturation cell finished",
            );

            let mut cell_report = ScenarioReport::ok()
                .setting("profile", profile.name)
                .setting("sha256 iters", cell.iters)
                .setting("shape", "width-1 sha256 hash-chain, one payer per account")
                .setting("accounts", profile.accounts)
                .setting("driver threads", profile.threads)
                .setting("measured iters", cell_iterations)
                .setting("mode", profile.mode())
                .setting("rpc batch", profile.rpc_batch)
                .setting("batch per thread", profile.batch)
                .setting("concurrency", profile.cell_concurrency(label))
                .observe("delivery us", Micros, cell.outcome.delivery)
                .client_side(&cell.outcome)
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
            base.report_cell(label, &cell_report)?;
            cells.push(cell);
        }

        let expected_hash = HashState::chain(causal::HASH_INIT.to_bytes(), profile.heavy_iters);
        for pda in accounts.iter() {
            let on_er = er.account(pda).await?.context("pda not on er")?;
            let hash_bytes =
                &on_er.data[layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE];
            ensure!(hash_bytes == expected_hash);
        }

        let light = &cells[0];
        let heavy = &cells[1];
        let cu_ratio = heavy.probe_cus / light.probe_cus;
        ensure!(cu_ratio >= CU_CONTRAST_FLOOR);

        let mut summary = ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting("shape", "width-1 sha256 hash-chain, one payer per account")
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
                if light.cores > 0.0 { heavy.cores / light.cores } else { 0.0 },
            );
        for cell in &cells {
            summary = summary.with_flags(&cell.outcome);
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
                summary = summary.metric_if(format!("{} {name}", cell.label), unit, value);
            }
        }
        Ok(summary)
    }
}
