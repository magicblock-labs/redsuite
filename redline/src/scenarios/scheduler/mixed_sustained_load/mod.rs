mod driver;
use driver::{execute, ExecuteConfig, Executed};

use std::{
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use instruction::Instruction;
use pubkey::Pubkey;
use redsuite_core::{
    check,
    config::{ExecutionConfig, ProfileValues},
    host, prep,
    programs::{CellHeader, HashState, REDLINE},
    redline::{causal, check_ok, Accounts},
    report::Unit,
    wait, Account, BaseCtx, ChainCtx, ErCtx, Metrics, MetricsDelta, Result, Scenario,
    ScenarioReport,
};
use tracing::{debug, info, instrument};

use crate::{metrics, program::layout, RunControl, ACCOUNT_SPACE};

const PAYER_LAMPORTS: u64 = 4_000_000_000;
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);

const BLOCKED_TRANSACTIONS: &str = "engine_processor_blocked_transactions";
const BUSY_EXECUTORS: &str = "engine_processor_busy_executors";
const PENDING_TRANSACTIONS: &str = "engine_ledger_pending_transactions";
const ORDERING_DEPENDENCIES: &str = "engine_processor_ordering_dependencies";

const MIX_PERIOD: u64 = 20;
const HASH_COMPUTE_SLOTS: u64 = 2;
const PATTERN_WRITE_SLOTS: u64 = 9;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Mode {
    HashCompute,
    PatternWrite,
    AccountSizeSum,
}

impl Mode {
    const ALL: [Mode; 3] = [Mode::HashCompute, Mode::PatternWrite, Mode::AccountSizeSum];

    fn label(self) -> &'static str {
        match self {
            Mode::HashCompute => "hash compute",
            Mode::PatternWrite => "pattern write",
            Mode::AccountSizeSum => "account size sum",
        }
    }
}

fn mode_for(id: u64) -> Mode {
    let slot = (id - 1) % MIX_PERIOD;
    if slot < HASH_COMPUTE_SLOTS {
        Mode::HashCompute
    } else if slot < HASH_COMPUTE_SLOTS + PATTERN_WRITE_SLOTS {
        Mode::PatternWrite
    } else {
        Mode::AccountSizeSum
    }
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

struct Profile {
    name: &'static str,
    payers: usize,
    lanes: u64,
    read_span: usize,
    rate: u32,
    threads: usize,
    hash_iters: u32,
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
    rate: 600,
    threads: 1,
    hash_iters: 180,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 48,
    lanes: 511,
    read_span: 4,
    rate: 4_000,
    threads: 4,
    hash_iters: 180,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

fn build_ixs(id: u64, pool: &[Pubkey], read_span: usize, hash_iters: u32) -> Vec<Instruction> {
    let lanes = pool.len() as u64;
    let lane = lane_of(id, lanes);
    let target = pool[lane as usize];
    match mode_for(id) {
        Mode::PatternWrite => vec![REDLINE.write(id, &[target])],
        Mode::HashCompute => vec![
            causal::compute_unit_limit(causal::CU_LIMIT),
            REDLINE.hash(id, causal::HASH_INIT.to_bytes(), hash_iters, &[target]),
        ],
        Mode::AccountSizeSum => {
            let sources: Vec<Pubkey> = (1..=read_span as u64)
                .map(|step| pool[((lane + step) % lanes) as usize])
                .collect();
            vec![REDLINE.sum_sizes(id, target, &sources)]
        }
    }
}

struct DrainState {
    elapsed: Duration,
    blocked: f64,
    busy: f64,
    pending: f64,
}

async fn read_final_state(
    er: &ErCtx,
    pool: &[Pubkey],
    last_ids: &[Option<u64>],
) -> Result<Vec<Option<Account>>> {
    let mut accounts = Vec::with_capacity(pool.len());
    for (pda, last_id) in pool.iter().zip(last_ids) {
        accounts.push(if last_id.is_some() { er.account(pda).await? } else { None });
    }
    Ok(accounts)
}

fn verify_final_state(
    accounts: &[Option<Account>],
    last_ids: &[Option<u64>],
    hash_iters: u32,
    read_span: usize,
) -> Result<()> {
    let expected_hash = HashState::chain(causal::HASH_INIT.to_bytes(), hash_iters);
    let expected_sum = (read_span as u64) * u64::from(ACCOUNT_SPACE);
    for (account, last_id) in accounts.iter().zip(last_ids) {
        let Some(last_id) = *last_id else {
            continue;
        };
        let account = account.as_ref().context("pda not on er")?;
        let data = &account.data;
        let mode = mode_for(last_id);
        ensure!(CellHeader::written_id(data) == Some(last_id));
        match mode {
            Mode::PatternWrite => {
                let filled = data[layout::DATA_OFFSET..]
                    .chunks_exact(layout::ID_SIZE)
                    .all(|chunk| chunk == last_id.to_le_bytes());
                ensure!(filled);
            }
            Mode::HashCompute => {
                let hash = &data[layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE];
                ensure!(hash == expected_hash);
            }
            Mode::AccountSizeSum => {
                let offset = layout::DATA_OFFSET;
                let sum = &data[offset..offset + 8];
                ensure!(sum == expected_sum.to_le_bytes());
            }
        }
    }
    Ok(())
}

/// Measures sustained mixed execution and verifies its final workload-owned state.
pub struct MixedSustainedLoad {
    er: ErCtx,
    profile: &'static Profile,
    pool: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
    base_config: ExecutionConfig,
    started: Instant,
}

impl Scenario<(ScenarioReport, Result<()>)> for MixedSustainedLoad {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let started = Instant::now();
        let profile = PROFILES.select(base.config().profile);

        ensure!(gcd(profile.lanes, MIX_PERIOD) == 1);
        ensure!(profile.lanes >= profile.threads as u64);
        ensure!(profile.payers >= profile.threads);
        ensure!((profile.read_span as u64) < profile.lanes);

        let payers = base.funded_payers(profile.payers, PAYER_LAMPORTS).await?;
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base.as_ref(), &payers, profile.lanes as usize, true)
            .await?;
        er.await_clones(&pool, ACCOUNT_SPACE as usize).await?;
        // Use disjoint ids so warmup cannot deduplicate a measured transaction.
        for ix in [
            REDLINE.hash(
                u64::MAX,
                causal::HASH_INIT.to_bytes(),
                profile.hash_iters,
                &pool[..1],
            ),
            REDLINE.write(u64::MAX - 1, &pool[..1]),
            REDLINE.sum_sizes(u64::MAX - 2, pool[0], &pool[1..=profile.read_span]),
        ] {
            er.submit_and_confirm(
                &payers[0],
                &[causal::compute_unit_limit(causal::CU_LIMIT), ix],
            )
            .await?;
        }

        let pool = Arc::new(pool);
        let payer_bytes = prep::payer_bytes(&payers);

        Ok(Self {
            er,
            profile,
            pool,
            payer_bytes,
            base_config: base.config(),
            started,
        })
    }

    async fn run(self) -> Result<(ScenarioReport, Result<()>)> {
        let load = RunControl::from_config(&self.base_config)?;
        let er = &self.er;
        let rate = self.profile.rate;
        let concurrency = self.profile.concurrency_per_thread() * self.profile.threads;
        let threads = self.profile.threads.min(rate as usize).min(concurrency);

        let metrics = er.scrape_metrics().await?;
        check!(
            metrics.get(metrics::ENGINE_TRANSACTIONS).is_some(),
            "the ER exposes no {} metric for execution observations",
            metrics::ENGINE_TRANSACTIONS
        );
        let baseline = Baseline {
            metrics,
            cpu: er.local_pid().map(host::cpu_sample).transpose()?,
            rss: er.local_pid().map(host::rss_kb).transpose()?,
        };

        let setup = self.started.elapsed();
        let deadline = load.deadline()?;
        info!(rate, concurrency, ?load.duration, "load generation started");
        let executed = execute(
            er.api().url().clone(),
            er.ws_url().clone(),
            ExecuteConfig {
                lanes: self.profile.lanes,
                until: (deadline, load.stop.clone()),
                read_span: self.profile.read_span,
                hash_iters: self.profile.hash_iters,
                rate,
                threads,
                concurrency,
            },
            self.pool.clone(),
            self.payer_bytes,
        )
        .await?;

        let outcome = &executed.outcome;
        info!(
            outcome.delivered,
            outcome.failed,
            delivered_tps = outcome.achieved_rps(),
            "load requests drained"
        );
        let mut report = executed.report(self.profile, &load, setup, (rate, concurrency, threads));
        let validation = executed.check(er, self.profile, &self.pool, baseline, &mut report).await;
        let completion = load.completion(&validation);
        Ok((report.setting("completion", completion), validation))
    }
}

impl Executed {
    fn report(
        &self,
        profile: &Profile,
        load: &RunControl,
        setup: Duration,
        settings: (u32, usize, usize),
    ) -> ScenarioReport {
        let outcome = &self.outcome;
        let (rate, concurrency, threads) = settings;
        let mut report = ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting(
                "mix",
                "10% hash compute / 45% pattern write / 45% account size sum, no commits",
            )
            .setting("workload txs", outcome.delivered + outcome.failed)
            .setting("target tps", rate)
            .setting("lanes", profile.lanes)
            .setting("read span", profile.read_span)
            .setting("payers", profile.payers)
            .setting("driver threads", threads)
            .setting("total concurrency", concurrency)
            .setting("hash-chain iterations", profile.hash_iters)
            .observe("delivery us", Unit::Micros, outcome.delivery)
            .client_side(outcome)
            .metric("target tps", Unit::Tps, f64::from(rate))
            .metric("delivered tps", Unit::Tps, outcome.achieved_rps())
            .metric("delivered txs", Unit::Count, outcome.delivered as f64)
            .metric("failed txs", Unit::Count, outcome.failed as f64);
        {
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
        }
        for (mode, count) in Mode::ALL.into_iter().zip(self.counts) {
            report = report.metric(format!("{} txs", mode.label()), Unit::Count, count as f64);
        }

        report
    }

    #[instrument(skip_all, err)]
    async fn check(
        &self,
        er: &ErCtx,
        profile: &Profile,
        pool: &[Pubkey],
        baseline: Baseline,
        report: &mut ScenarioReport,
    ) -> Result<()> {
        let outcome = &self.outcome;
        let counts = self.counts;
        let total = outcome.delivered + outcome.failed;

        check!(counts.iter().sum::<u64>() == total);
        check!(total > 0, "no measured mixed work completed");
        outcome.check_failures();

        let state_started = Instant::now();
        let settled = wait::retry("workload final state", SETTLE_TIMEOUT, async || {
            let accounts = read_final_state(er, pool, &self.last_ids).await?;
            verify_final_state(
                &accounts,
                &self.last_ids,
                profile.hash_iters,
                profile.read_span,
            )
        })
        .await;
        check_ok(settled, "workload final state did not settle");
        let metrics = er.scrape_metrics().await?;
        let drained = DrainState {
            elapsed: state_started.elapsed(),
            blocked: metrics.value_sum(BLOCKED_TRANSACTIONS).unwrap_or_default(),
            busy: metrics.value_sum(BUSY_EXECUTORS).unwrap_or_default(),
            pending: metrics.value_sum(PENDING_TRANSACTIONS).unwrap_or_default(),
        };
        let cpu_after = er.local_pid().map(host::cpu_sample).transpose()?;
        let rss_after = er.local_pid().map(host::rss_kb).transpose()?;
        let after = er.scrape_metrics().await?;
        let delta = MetricsDelta::new(baseline.metrics, after);

        let engine_txs = delta.counter(metrics::ENGINE_TRANSACTIONS).unwrap_or_default();
        let excess = engine_txs - total as f64;
        let rpc_handled = delta.counter(metrics::RPC_HANDLED_TRANSACTIONS);
        let rpc_accepted = delta.counter(metrics::RPC_ACCEPTED_TRANSACTIONS);
        let wall = outcome.wall + drained.elapsed;
        let executed_tps = total as f64 / wall.as_secs_f64();
        let thread_cores = cpu_after
            .as_ref()
            .zip(baseline.cpu.as_ref())
            .map(|(after, before)| after.thread_cores_since(before));
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
            .metric_if(
                "validator cores",
                Unit::Count,
                cpu_after
                    .as_ref()
                    .zip(baseline.cpu.as_ref())
                    .map(|(after, before)| after.cores_since(before)),
            )
            .metric_if(
                "top thread cores",
                Unit::Count,
                thread_cores.as_ref().and_then(|cores| cores.first().copied()),
            )
            .metric_if(
                "rss growth mib",
                Unit::Count,
                rss_after
                    .zip(baseline.rss)
                    .map(|(after, before)| after.saturating_sub(before) as f64 / 1024.0),
            )
            .metric_if(
                "ordering dependencies",
                Unit::Count,
                delta.counter(ORDERING_DEPENDENCIES),
            );
        report.measurements.extend(observations.measurements);
        for op in ["snapshot", "checksum"] {
            let metric = format!("engine_accountsdb_operation_duration_micros{{op=\"{op}\"}}");
            if let Some(value) = delta.histogram_avg(&metric) {
                report.measurements.push(redsuite_core::report::Measurement {
                    label: format!("accountsdb {op} mean us"),
                    unit: Unit::Micros,
                    value: redsuite_core::report::MeasureValue::Scalar(value),
                });
            }
        }
        check!(engine_txs >= total as f64);

        if let Some(rpc_handled) = rpc_handled {
            debug!(rpc_handled, total, "RPC processing window");
        }
        if let Some(rpc_accepted) = rpc_accepted {
            debug!(rpc_accepted, total, "RPC acceptance window");
        }

        let accounts = read_final_state(er, pool, &self.last_ids).await?;
        let verified = verify_final_state(
            &accounts,
            &self.last_ids,
            profile.hash_iters,
            profile.read_span,
        );
        check_ok(verified, "workload final state changed after settling");

        debug!(
            total,
            delivered_tps = outcome.achieved_rps(),
            executed_tps,
            "sustained workload finished",
        );

        Ok(())
    }
}

struct Baseline {
    metrics: Metrics,
    cpu: Option<host::CpuSample>,
    rss: Option<u64>,
}
