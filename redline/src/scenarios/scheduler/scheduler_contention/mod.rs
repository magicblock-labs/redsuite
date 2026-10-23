mod background;
mod report;
use background::{await_load_reaching_executors, join_load, spawn_load, LoadMode};

use std::{
    cell::RefCell,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure, Context};
use futures_util::future::join_all;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    config::ProfileValues,
    prep,
    programs::HashState,
    redline::{
        causal::{self, CausalPair, Step},
        Accounts,
    },
    runner::RunOutcome,
    sampler::{MeanMax, Sampled, Sampler, Trailing},
    stats::{ObservationsStats, StreamingStats},
    wait, BaseCtx, ChainCtx, ErCtx, Metrics, MetricsDelta, Result, Scenario, ScenarioReport,
};
use signature::Signature;
use signer::Signer;
use tokio::time;
use tracing::{debug, instrument};

use crate::{metrics, program::layout, ACCOUNT_SPACE};

const PAYER_LAMPORTS: u64 = 200_000_000;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(600);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);
const WARM_IN_TIMEOUT: Duration = Duration::from_secs(15);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const CALIBRATION: Duration = Duration::from_secs(3);
const STALL_BOUND: Duration = Duration::from_secs(10);
const OPEN_CONCURRENCY: usize = 128;
const LANES_PER_EXECUTOR: usize = 12;
const MIN_EXECUTORS: f64 = 2.0;
const PAYERS_PER_PAIR: usize = 3;

const TX_COUNT: &str = metrics::ENGINE_TRANSACTIONS;
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
    heavy_iters: 180,
    independent_accounts: 512,
    independent_iters: 180,
    driver_threads: 8,
    phase_span: Duration::from_secs(10),
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

fn heavy_step(chain: u64) -> Step {
    Step::ALL[(chain % causal::STEPS) as usize]
}

#[derive(Clone, Copy)]
struct ChainSpace {
    pairs: u64,
    chains: u64,
}

impl ChainSpace {
    fn step_id(&self, batch: u64, pair: u64, chain: u64, step: Step) -> u64 {
        let chain_ordinal = (batch * self.pairs + pair) * self.chains + chain;
        chain_ordinal * causal::STEPS + step.index() as u64 + 1
    }
}

fn phase_ranges(chains: u64) -> [(u64, u64); 2] {
    let split = chains.div_ceil(2);
    [(0, split), (split, chains)]
}

struct Pair {
    chain: CausalPair,
    steps: Vec<(u64, Step, Signature)>,
}

#[derive(Default)]
struct Samples {
    busy: MeanMax,
    blocked_max: f64,
    max_stall: Duration,
}

fn spawn_sampler(er: &ErCtx) -> Sampler<Samples> {
    let mut last_txs: Option<f64> = None;
    let mut last_progress = Instant::now();
    Sampler::spawn(
        er.metrics().clone(),
        SAMPLE_INTERVAL,
        Trailing::Skip,
        move |metrics: &Metrics, samples: &mut Samples| {
            let busy = metrics.get(BUSY_EXECUTORS)?;
            let blocked = metrics.get(BLOCKED_TRANSACTIONS)?;
            let txs = metrics.get(TX_COUNT)?;
            samples.busy.push(busy);
            samples.blocked_max = samples.blocked_max.max(blocked);
            if last_txs.is_none_or(|last| txs > last) {
                last_progress = Instant::now();
            }
            last_txs = Some(txs);
            samples.max_stall = samples.max_stall.max(last_progress.elapsed());
            Some(())
        },
    )
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
    crate::await_executed(er, target, DRAIN_TIMEOUT).await?;
    let (blocked, busy, pending) =
        wait::retry("execution pipeline idle", SETTLE_TIMEOUT, async || {
            let metrics = er.scrape_metrics().await?;
            ensure!(is_idle(&metrics));
            Ok(idle_values(&metrics))
        })
        .await?;
    Ok(DrainState {
        elapsed: started.elapsed(),
        blocked,
        busy,
        pending,
    })
}

struct PhasePlan {
    batch: u64,
    space: ChainSpace,
    range: (u64, u64),
    heavy_iters: u32,
    span: Duration,
    deadline: Instant,
    stop: tokio::sync::watch::Receiver<bool>,
    mode: LoadMode,
    independent_iters: u32,
    driver_threads: usize,
}

struct PhaseOutcome {
    label: &'static str,
    chains: u64,
    chain_txs: u64,
    send: ObservationsStats,
    load: RunOutcome,
    samples: Sampled<Samples>,
}

async fn drive_pair(
    pair_index: u64,
    pair: &mut Pair,
    plan: &PhasePlan,
    phase_started: time::Instant,
    gap: Duration,
    send_stats: &RefCell<StreamingStats>,
) -> Result<()> {
    let (first, last) = plan.range;
    for chain in first..last {
        time::sleep_until(phase_started + gap * (chain - first) as u32).await;
        if Instant::now() >= plan.deadline || *plan.stop.borrow() {
            break;
        }
        let heavy = heavy_step(chain);
        for step in Step::ALL {
            let id = plan.space.step_id(plan.batch, pair_index, chain, step);
            let iters = if step == heavy { plan.heavy_iters } else { 0 };
            let (signature, elapsed) = pair.chain.send(step, id, iters).await?;
            send_stats.borrow_mut().push(elapsed.as_micros() as u32);
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
        er.api().url().clone(),
        er.ws_url().clone(),
        plan.mode,
        plan.driver_threads,
        independent.clone(),
        independent_payers.clone(),
        plan.independent_iters,
    ));
    await_load_reaching_executors(er, &mut handles).await?;
    let sampler = spawn_sampler(er);

    let stop_task = handles.as_ref().unwrap().stop_at(plan.deadline, plan.stop.clone());
    let phase_started = time::Instant::now();
    let (first, last) = plan.range;
    let chains = last.saturating_sub(first);
    let gap = plan.span.checked_div(chains.max(1) as u32).unwrap_or_default();
    let send_stats = RefCell::new(StreamingStats::new());
    let results = join_all(pairs.iter_mut().enumerate().map(|(pair_index, pair)| {
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
    time::sleep_until(phase_started + plan.span).await;
    let load = match handles.take() {
        Some(handles) => join_load(handles).await?,
        None => return Err(anyhow!("independent load driver already joined")),
    };
    stop_task.abort();
    let samples = sampler.finish_complete("scheduler").await?;
    chain_result?;
    let sent = send_stats.into_inner().finalize(false);
    Ok(PhaseOutcome {
        label: plan.mode.label(),
        chains: sent.count as u64 / (causal::STEPS * pairs.len() as u64),
        chain_txs: sent.count as u64,
        send: sent,
        load,
        samples,
    })
}

async fn first_failed_step(er: &ErCtx, steps: &[(u64, Step, Signature)]) -> Option<String> {
    for (id, step, signature) in steps {
        match er.api().get_transaction(signature).await {
            Ok(None) => return Some(format!("step {} id {id} not published", step.label())),
            Ok(Some(info)) => {
                let meta = match transaction_meta(&info) {
                    Ok(meta) => meta,
                    Err(error) => return Some(format!("{error:#}")),
                };
                if let Some(err) = &meta.err {
                    return Some(format!("step {} id {id} failed: {err}", step.label()));
                }
            }
            Err(_) => continue,
        }
    }
    None
}

async fn verify_pairs(er: &ErCtx, pairs: &mut [Pair], batch: usize) -> Result<()> {
    for (index, pair) in pairs.iter_mut().enumerate() {
        if let Err(error) =
            pair.chain.verify(er, &format!("batch {batch}: pair {index} on the ER")).await
        {
            let step = first_failed_step(er, &pair.steps)
                .await
                .unwrap_or_else(|| "none — every step executed successfully".to_owned());
            return Err(error.context(format!("first failed step of this batch: {step}")));
        }
        pair.steps.clear();
    }
    Ok(())
}

async fn verify_independent(er: &ErCtx, accounts: &[Pubkey], iters: u32) -> Result<()> {
    let expected = HashState::chain(causal::HASH_INIT.to_bytes(), iters);
    for (index, address) in accounts.iter().enumerate() {
        let account = er
            .account(address)
            .await?
            .with_context(|| format!("independent account {index} is not on the ER"))?;
        let hash = &account.data[layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE];
        ensure!(hash == expected);
    }
    Ok(())
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

/// Checks causal ordering and progress under sustained competing execution.
pub struct SchedulerContention {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    pairs: Vec<Pair>,
    independent: Arc<Vec<Pubkey>>,
    independent_payer_bytes: Arc<Vec<[u8; 64]>>,
}

impl Scenario<(ScenarioReport, Result<()>)> for SchedulerContention {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);

        let probe = er.scrape_metrics().await?;
        for name in REQUIRED_METRICS {
            ensure!(probe.get(name).is_some());
        }

        let payers = base
            .funded_payers(
                profile.pairs * PAYERS_PER_PAIR + profile.independent_accounts,
                PAYER_LAMPORTS,
            )
            .await?;
        let (chain_payers, independent_payers) = payers.split_at(profile.pairs * PAYERS_PER_PAIR);

        let prep_payers: Vec<Keypair> = chain_payers
            .iter()
            .step_by(PAYERS_PER_PAIR)
            .map(Keypair::insecure_clone)
            .collect();
        let pair_accounts = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base.as_ref(), &prep_payers, profile.pairs * 2, true)
            .await?;
        er.await_clones(&pair_accounts, ACCOUNT_SPACE as usize).await?;
        let independent = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(
                base.as_ref(),
                independent_payers,
                profile.independent_accounts,
                true,
            )
            .await?;
        er.await_clones(&independent, ACCOUNT_SPACE as usize).await?;
        let payer_keys: Vec<Pubkey> = payers.iter().map(Signer::pubkey).collect();
        er.await_cloned_payers(&payer_keys).await?;

        let pairs: Vec<Pair> = pair_accounts
            .chunks_exact(2)
            .enumerate()
            .map(|(index, accounts)| Pair {
                chain: CausalPair::new(
                    [accounts[0], accounts[1]],
                    chain_payers[index * PAYERS_PER_PAIR..(index + 1) * PAYERS_PER_PAIR]
                        .iter()
                        .map(|payer| er.sender(Rc::new(payer.insecure_clone())))
                        .collect(),
                ),
                steps: Vec::new(),
            })
            .collect();
        let independent = Arc::new(independent);
        let independent_payer_bytes = prep::payer_bytes(independent_payers);

        Ok(Self {
            base,
            er,
            profile,
            pairs,
            independent,
            independent_payer_bytes,
        })
    }

    async fn run(mut self) -> Result<(ScenarioReport, Result<()>)> {
        let (executors, contention_lanes) = self.calibrate().await?;
        let options = crate::RunControl::from_config(&self.base.config())?;
        let deadline = options.deadline()?;
        let mut batches = Vec::new();
        let mut batch = 0;
        let result: Result<()> = async {
            while !options.stopped(deadline) {
                let outcome = self
                    .measure_batch(batch, contention_lanes, deadline, options.stop.clone())
                    .await?;
                self.check_batch(batch, &outcome).await?;
                tracing::info!(
                    batch,
                    chain_txs = outcome.phases.iter().map(|phase| phase.chain_txs).sum::<u64>(),
                    "verified scheduler window"
                );
                batches.clear();
                batches.push(outcome);
                batch += 1;
            }
            verify_independent(&self.er, &self.independent, self.profile.independent_iters).await?;
            Ok(())
        }
        .await;
        let report = self
            .summarize(&batches, executors, contention_lanes)
            .setting("scope", "last verified batch")
            .setting("completion", options.completion(&result));
        Ok((report, result))
    }
}

impl SchedulerContention {
    #[instrument(skip_all, err)]
    async fn calibrate(&self) -> Result<(f64, usize)> {
        let calibration_before = self.er.scrape_metrics().await?;
        let mut handles = Some(spawn_load(
            self.er.api().url().clone(),
            self.er.ws_url().clone(),
            LoadMode::Open { concurrency: OPEN_CONCURRENCY },
            self.profile.driver_threads,
            self.independent.clone(),
            self.independent_payer_bytes.clone(),
            self.profile.independent_iters,
        ));
        await_load_reaching_executors(&self.er, &mut handles).await?;
        let sampler = spawn_sampler(&self.er);
        time::sleep(CALIBRATION).await;
        let calibration = match handles.take() {
            Some(handles) => join_load(handles).await?,
            None => return Err(anyhow!("calibration load already joined")),
        };
        let calibration_samples = sampler.finish_complete("scheduler").await?;
        ensure!(calibration.failed == 0);
        let executors = calibration_samples.value.busy.max;
        ensure!(executors >= MIN_EXECUTORS);
        let contention_lanes = (executors as usize * LANES_PER_EXECUTOR)
            .min(self.profile.independent_accounts)
            .max(1);
        let calibration_target =
            calibration_before.get(TX_COUNT).unwrap_or_default() + calibration.delivered as f64;
        drain(&self.er, calibration_target).await?;
        debug!(
            executors,
            contention_lanes, "contention calibration finished"
        );

        Ok((executors, contention_lanes))
    }

    #[instrument(skip_all, err, fields(batch = batch))]
    async fn measure_batch(
        &mut self,
        batch: usize,
        contention_lanes: usize,
        deadline: Instant,
        stop: tokio::sync::watch::Receiver<bool>,
    ) -> Result<BatchOutcome> {
        let space = ChainSpace {
            pairs: self.profile.pairs as u64,
            chains: self.profile.chains,
        };
        let modes = [
            LoadMode::Bounded { lanes: contention_lanes },
            LoadMode::Open { concurrency: OPEN_CONCURRENCY },
        ];
        let ranges = phase_ranges(self.profile.chains);

        let before = self.er.scrape_metrics().await?;
        ensure!(is_idle(&before));
        let txs_before = before.get(TX_COUNT).unwrap_or_default();

        let mut phases = Vec::with_capacity(modes.len());
        for (mode, range) in modes.iter().zip(ranges) {
            if Instant::now() >= deadline || *stop.borrow() {
                break;
            }
            let plan = PhasePlan {
                batch: batch as u64,
                space,
                range,
                heavy_iters: self.profile.heavy_iters,
                span: self
                    .profile
                    .phase_span
                    .min(deadline.saturating_duration_since(Instant::now())),
                deadline,
                stop: stop.clone(),
                mode: *mode,
                independent_iters: self.profile.independent_iters,
                driver_threads: self.profile.driver_threads,
            };
            let phase = run_phase(
                &self.er,
                &mut self.pairs,
                &self.independent,
                &self.independent_payer_bytes,
                plan,
            )
            .await?;
            ensure!(phase.load.failed == 0);
            debug!(
                batch,
                phase.label, phase.chain_txs, "contention phase finished",
            );
            phases.push(phase);
        }

        let workload: u64 = phases.iter().map(|phase| phase.chain_txs + phase.load.delivered).sum();
        let drained = drain(&self.er, txs_before + workload as f64).await?;
        let after = self.er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);
        let engine_txs = delta.counter(TX_COUNT).unwrap_or_default();
        let dependencies = delta.counter(ORDERING_DEPENDENCIES).unwrap_or_default();
        let failed_kind = |kind: &str| {
            delta.counter(&format!(
                "{}{{kind=\"{kind}\"}}",
                metrics::FAILED_TRANSACTIONS
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

        Ok(outcome)
    }

    #[instrument(skip_all, err, fields(batch = batch))]
    async fn check_batch(&mut self, batch: usize, outcome: &BatchOutcome) -> Result<()> {
        let workload: u64 =
            outcome.phases.iter().map(|phase| phase.chain_txs + phase.load.delivered).sum();
        for phase in &outcome.phases {
            let samples = &phase.samples.value;
            ensure!(samples.busy.max >= MIN_EXECUTORS);
            ensure!(samples.busy.mean() > 1.0);
            ensure!(samples.max_stall <= STALL_BOUND);
        }
        for (_label, value) in [
            ("blocked", outcome.drained.blocked),
            ("busy", outcome.drained.busy),
            ("pending", outcome.drained.pending),
        ] {
            ensure!(value == 0.0);
        }
        ensure!(outcome.engine_txs >= workload as f64);
        if let Some(dropped) = outcome.dropped {
            ensure!(dropped == 0.0);
        }
        if let Some(execution_failed) = outcome.execution_failed {
            ensure!(execution_failed == 0.0);
        }
        let contention_chains =
            outcome.phases.first().context("no contention phase observed")?.chains
                * self.profile.pairs as u64;
        ensure!(outcome.dependencies >= contention_chains as f64);
        verify_pairs(&self.er, &mut self.pairs, batch).await?;
        debug!(
            batch,
            outcome.dependencies, "contention batch drained; all pairs match accepted fold",
        );

        Ok(())
    }
}
