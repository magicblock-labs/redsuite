use std::{
    cell::Cell,
    fs,
    path::Path,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, prep,
    profile::{self, ProfileValues},
    topology::{self, ReplicatedOptions, ReplicatedTopology, Verifier},
    BaseCtx, ChainCtx, CheckError, PrivateErScenario, Result, ScenarioReport,
    TxSender,
};
use signer::Signer;

use crate::program::{instruction::build, layout, utils::fold_hash};

const LABEL: &str = "replication-recovery";
const PAYERS_PER_PAIR: usize = 3;
const STEPS: u64 = 3;
const SUPERBLOCK_SLOTS: u64 = 128;
const LEDGER_SIZE_LIMIT_BYTES: u64 = 1;
const TRUNCATIONS_TO_OUTRUN: f64 = 2.0;
const CU_LIMIT: u32 = 1_400_000;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(90);
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(120);
const RETENTION_TIMEOUT: Duration = Duration::from_secs(180);
const CLONE_TIMEOUT: Duration = Duration::from_secs(60);
const SEAL_TIMEOUT: Duration = Duration::from_secs(60);
const LAG_SAMPLE: Duration = Duration::from_millis(500);
const RESTART_AFTER_SEAL_BUDGET: Duration = Duration::from_secs(3);

const TRANSACTIONS: &str = "engine_ledger_transactions";
const BLOCKS: &str = "engine_ledger_blocks";
const SUPERBLOCKS: &str = "engine_ledger_superblocks";
const STATE_MISMATCHES: &str = "engine_replicator_client_state_mismatches";
const CLIENT_SNAPSHOTS: &str = r#"engine_replicator_operation_duration_micros_count{op="client_stage_snapshot"}"#;
const SERVER_SNAPSHOTS: &str = r#"engine_replicator_operation_duration_micros_count{op="server_send_snapshot"}"#;
const TRUNCATIONS: &str =
    r#"engine_ledger_operation_duration_micros_count{op="truncate"}"#;

struct Profile {
    name: &'static str,
    pairs: usize,
    chain_gap: Duration,
    heavy_iters: u32,
    steady: Duration,
    snapshot_recovery: bool,
}

const LITE: Profile = Profile {
    name: "lite",
    pairs: 4,
    chain_gap: Duration::from_millis(100),
    heavy_iters: 30,
    steady: Duration::from_secs(8),
    snapshot_recovery: false,
};

const FULL: Profile = Profile {
    name: "full",
    pairs: 8,
    chain_gap: Duration::from_millis(50),
    heavy_iters: 60,
    steady: Duration::from_secs(15),
    snapshot_recovery: true,
};

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
    soak: None,
    deep: None,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
    chains: u64,
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct Workload {
    stop: Rc<Cell<bool>>,
    sent: Rc<Cell<u64>>,
    tasks: Vec<tokio::task::JoinHandle<Result<Pair>>>,
}

impl Workload {
    fn start(pairs: Vec<Pair>, gap: Duration, heavy_iters: u32) -> Self {
        let stop = Rc::new(Cell::new(false));
        let sent = Rc::new(Cell::new(0u64));
        let next_id = Rc::new(Cell::new(1u64));
        let tasks = pairs
            .into_iter()
            .map(|mut pair| {
                let stop = stop.clone();
                let sent = sent.clone();
                let next_id = next_id.clone();
                tokio::task::spawn_local(async move {
                    while !stop.get() {
                        let heavy = Step::ALL[(pair.chains % STEPS) as usize];
                        for step in Step::ALL {
                            let id = next_id.get();
                            next_id.set(id + 1);
                            let iters =
                                if step == heavy { heavy_iters } else { 0 };
                            let ixs =
                                chain_ixs(id, iters, &pair.accounts(step));
                            pair.senders[step.index()]
                                .submit(&ixs)
                                .await
                                .map_err(|error| {
                                    format!(
                                        "chain step {step:?} (id {id}) was \
                                         not accepted by the leader: {error}"
                                    )
                                })?;
                            pair.model.apply(step, id, iters);
                            sent.set(sent.get() + 1);
                        }
                        pair.chains += 1;
                        tokio::time::sleep(gap).await;
                    }
                    Ok(pair)
                })
            })
            .collect();
        Self { stop, sent, tasks }
    }

    fn sent(&self) -> u64 {
        self.sent.get()
    }

    async fn stop(self) -> Result<Vec<Pair>> {
        self.stop.set(true);
        let mut pairs = Vec::with_capacity(self.tasks.len());
        for task in self.tasks {
            pairs.push(
                task.await
                    .map_err(|error| format!("workload task: {error}"))??,
            );
        }
        Ok(pairs)
    }
}

async fn leader_metric(
    topology: &ReplicatedTopology,
    name: &str,
) -> Result<f64> {
    topology
        .leader()
        .ctx()
        .scrape_metrics()
        .await?
        .get(name)
        .ok_or_else(|| format!("the leader exposes no {name} metric").into())
}

async fn leader_count(
    topology: &ReplicatedTopology,
    name: &str,
) -> Result<f64> {
    Ok(topology
        .leader()
        .ctx()
        .scrape_metrics()
        .await?
        .get(name)
        .unwrap_or(0.0))
}

async fn verifier_metric(verifier: &Verifier, name: &str) -> Result<f64> {
    verifier.scrape_metrics().await?.get(name).ok_or_else(|| {
        format!("verifier `{}` exposes no {name} metric", verifier.label())
            .into()
    })
}

async fn verifier_count(verifier: &Verifier, name: &str) -> Result<f64> {
    Ok(verifier.scrape_metrics().await?.get(name).unwrap_or(0.0))
}

async fn await_catch_up(
    verifier: &Verifier,
    name: &str,
    target: f64,
    moment: &str,
) -> Result<Duration> {
    let started = Instant::now();
    check::poll(
        &format!(
            "{moment}: verifier `{}` {name} reaching the leader's {target:.0}",
            verifier.label()
        ),
        CATCH_UP_TIMEOUT,
        || async {
            verifier_metric(verifier, name)
                .await
                .is_ok_and(|value| value >= target)
        },
    )
    .await?;
    Ok(started.elapsed())
}

async fn await_leader_advance(
    topology: &ReplicatedTopology,
    name: &str,
    by: f64,
    what: &str,
) -> Result<f64> {
    let from = leader_count(topology, name).await?;
    let target = from + by;
    check::poll(
        &format!("{what} (leader {name} reaching {target:.0})"),
        SEAL_TIMEOUT.max(RETENTION_TIMEOUT),
        || async {
            leader_count(topology, name)
                .await
                .is_ok_and(|value| value >= target)
        },
    )
    .await?;
    Ok(target)
}

fn strip_ansi(line: &str) -> String {
    let mut cleaned = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            if chars.peek() == Some(&'[') {
                for next in chars.by_ref() {
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        cleaned.push(ch);
    }
    cleaned
}

fn executors_in_log(log: &Path) -> Result<u64> {
    let text = fs::read_to_string(log)?;
    let line = text
        .lines()
        .rfind(|line| line.contains("sequencer started"))
        .ok_or_else(|| {
            format!("{} never logged `sequencer started`", log.display())
        })?;
    let cleaned = strip_ansi(line);
    let digits: String = cleaned
        .split("executors=")
        .nth(1)
        .unwrap_or_default()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().map_err(|_| {
        format!(
            "{} logged an unparseable executor count: {cleaned}",
            log.display()
        )
        .into()
    })
}

fn log_mentions_mismatch(log: &Path) -> Result<Option<String>> {
    let text = fs::read_to_string(log)?;
    Ok(text
        .lines()
        .find(|line| line.to_ascii_lowercase().contains("mismatch"))
        .map(strip_ansi))
}

fn cpu_sets() -> (String, String, usize) {
    let total = std::thread::available_parallelism()
        .map(|cpus| cpus.get())
        .unwrap_or(2);
    let narrow = (total / 4).clamp(4, total.max(1));
    let wide = (total / 2).clamp(6, total.max(1)).max(narrow);
    (
        format!("0-{}", narrow - 1),
        format!("0-{}", wide - 1),
        total,
    )
}

struct LagSample {
    max_lag: Vec<f64>,
    samples: usize,
}

async fn sample_lag(
    topology: &ReplicatedTopology,
    window: Duration,
) -> Result<LagSample> {
    let deadline = tokio::time::Instant::now() + window;
    let mut max_lag = vec![0.0f64; topology.verifiers().len()];
    let mut samples = 0usize;
    while tokio::time::Instant::now() < deadline {
        let leader = leader_metric(topology, TRANSACTIONS).await?;
        for (index, verifier) in topology.verifiers().iter().enumerate() {
            let seen = verifier_metric(verifier, TRANSACTIONS).await?;
            max_lag[index] = max_lag[index].max(leader - seen);
        }
        samples += 1;
        tokio::time::sleep(LAG_SAMPLE).await;
    }
    Ok(LagSample { max_lag, samples })
}

async fn verify_pairs(
    topology: &ReplicatedTopology,
    pairs: &[Pair],
) -> Result<()> {
    let leader = topology.leader().ctx();
    for (index, pair) in pairs.iter().enumerate() {
        let expectations = [
            ("A", pair.a, pair.model.a, pair.model.a_id),
            ("B", pair.b, pair.model.b, pair.model.b_id),
        ];
        for (label, address, expected_hash, expected_id) in expectations {
            let account = leader.account(&address).await?.ok_or(format!(
                "pair {index} account {label} {address} is not on the leader"
            ))?;
            let data = &account.data;
            let hash = &data
                [layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE];
            let id = u64::from_le_bytes(
                data[layout::ID_OFFSET..layout::ID_OFFSET + layout::ID_SIZE]
                    .try_into()
                    .expect("id slice is 8 bytes"),
            );
            if hash == expected_hash && id == expected_id {
                continue;
            }
            return Err(CheckError::new(format!(
                "pair {index} account {label} on the leader must hold the \
                 fold of its accepted X/Y/Z history"
            ))
            .expected(format!("id {expected_id}, hash {}", hex(&expected_hash)))
            .actual(format!("id {id}, hash {}", hex(hash)))
            .into());
        }
    }
    Ok(())
}

async fn verify_no_mismatch(topology: &ReplicatedTopology) -> Result<()> {
    for verifier in topology.verifiers() {
        check!(
            verifier.is_running(),
            "verifier `{}` is no longer running — a replication error ended it",
            verifier.label()
        )?;
        check!(
            verifier.stream_connected().await,
            "verifier `{}` lost its replication stream",
            verifier.label()
        )?;
        check_eq!(
            verifier_count(verifier, STATE_MISMATCHES).await?,
            0.0,
            "verifier `{}` detected sealed-state checksum mismatches",
            verifier.label()
        )?;
        if let Some(line) = log_mentions_mismatch(verifier.log())? {
            return Err(CheckError::new(format!(
                "verifier `{}` logged a replication mismatch",
                verifier.label()
            ))
            .actual(line)
            .into());
        }
    }
    Ok(())
}

async fn prepare_pairs(
    base: &BaseCtx,
    topology: &ReplicatedTopology,
    count: usize,
) -> Result<Vec<Pair>> {
    let leader = topology.leader();
    let payers = prep::funded_payers(
        base,
        count * PAYERS_PER_PAIR,
        crate::PAYER_LAMPORTS,
    )
    .await?;
    let mut pairs = Vec::with_capacity(count);
    for index in 0..count {
        let owner = &payers[index * PAYERS_PER_PAIR];
        let a =
            crate::init_delegated_account(base, owner, 0, leader.identity())
                .await?;
        let b =
            crate::init_delegated_account(base, owner, 1, leader.identity())
                .await?;
        for pda in [a, b] {
            check::poll(
                &format!("the leader clones the delegated pda {pda}"),
                CLONE_TIMEOUT,
                || async {
                    matches!(leader.ctx().account(&pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                },
            )
            .await?;
        }
        let senders = payers
            [index * PAYERS_PER_PAIR..(index + 1) * PAYERS_PER_PAIR]
            .iter()
            .map(|payer| {
                leader.ctx().sender(Rc::new(
                    Keypair::try_from(&payer.to_bytes()[..])
                        .expect("payer bytes round-trip"),
                ))
            })
            .collect();
        pairs.push(Pair {
            a,
            b,
            senders,
            model: PairModel::default(),
            chains: 0,
        });
    }
    for payer in &payers {
        let address = payer.pubkey();
        check::poll(
            &format!("the leader clones payer {address}"),
            CLONE_TIMEOUT,
            || async {
                matches!(leader.ctx().account(&address).await, Ok(Some(acc)) if acc.lamports > 0)
            },
        )
        .await?;
    }
    Ok(pairs)
}

struct RestartOutcome {
    offline: Duration,
    reconnect: Duration,
    catch_up: Duration,
    snapshots: f64,
}

async fn restart_with_retained_cursor(
    topology: &mut ReplicatedTopology,
    index: usize,
    prune_active: bool,
) -> Result<RestartOutcome> {
    if prune_active {
        await_leader_advance(
            topology,
            BLOCKS,
            SUPERBLOCK_SLOTS as f64,
            "a fresh superblock boundary before the retained-cursor restart",
        )
        .await?;
    }
    let stopped_at = Instant::now();
    let stop = topology.verifier_mut(index).stop(false).await?;
    check_eq!(
        stop.exit_code,
        Some(0),
        "verifier {index} must stop cleanly so its cursor is durable"
    )?;
    topology.verifier_mut(index).start(READY_TIMEOUT).await?;
    let offline = stopped_at.elapsed();
    if prune_active {
        check!(
            offline <= RESTART_AFTER_SEAL_BUDGET,
            "verifier {index} was offline {offline:?}, too long to be sure \
             its cursor survived the next retention check"
        )?;
    }
    let reconnect = topology
        .verifier(index)
        .wait_connected(CONNECT_TIMEOUT)
        .await?;
    let snapshots =
        verifier_count(topology.verifier(index), CLIENT_SNAPSHOTS).await?;
    check_eq!(
        snapshots,
        0.0,
        "verifier {index} must resume from its retained cursor, not a snapshot"
    )?;
    let target = leader_metric(topology, TRANSACTIONS).await?;
    let catch_up = await_catch_up(
        topology.verifier(index),
        TRANSACTIONS,
        target,
        "after the retained-cursor restart",
    )
    .await?;
    Ok(RestartOutcome {
        offline,
        reconnect,
        catch_up,
        snapshots,
    })
}

struct RecoveryOutcome {
    offline: Duration,
    truncations: f64,
    reconnect: Duration,
    catch_up: Duration,
    client_snapshots: f64,
    server_snapshots: f64,
}

async fn recover_from_snapshot(
    topology: &mut ReplicatedTopology,
    index: usize,
) -> Result<RecoveryOutcome> {
    let server_snapshots_before =
        leader_count(topology, SERVER_SNAPSHOTS).await?;
    let stopped_at = Instant::now();
    let stop = topology.verifier_mut(index).stop(false).await?;
    check_eq!(
        stop.exit_code,
        Some(0),
        "verifier {index} must stop cleanly before falling behind retention"
    )?;
    let truncations_target = await_leader_advance(
        topology,
        TRUNCATIONS,
        TRUNCATIONS_TO_OUTRUN,
        "retention purging the history behind the offline verifier",
    )
    .await?;
    let offline = stopped_at.elapsed();
    topology.verifier_mut(index).start(READY_TIMEOUT).await?;
    let reconnect = topology
        .verifier(index)
        .wait_connected(CONNECT_TIMEOUT)
        .await?;
    let client_snapshots =
        verifier_count(topology.verifier(index), CLIENT_SNAPSHOTS).await?;
    check!(
        client_snapshots >= 1.0,
        "verifier {index} rejoined after {truncations_target:.0} retention \
         purges without installing a snapshot"
    )?;
    let server_snapshots = leader_count(topology, SERVER_SNAPSHOTS).await?
        - server_snapshots_before;
    check!(
        server_snapshots >= 1.0,
        "the leader served no snapshot to the out-of-retention verifier"
    )?;
    let target = leader_metric(topology, TRANSACTIONS).await?;
    let catch_up = await_catch_up(
        topology.verifier(index),
        TRANSACTIONS,
        target,
        "after the snapshot recovery",
    )
    .await?;
    Ok(RecoveryOutcome {
        offline,
        truncations: truncations_target,
        reconnect,
        catch_up,
        client_snapshots,
        server_snapshots,
    })
}

pub struct ReplicationRecovery;

#[async_trait(?Send)]
impl PrivateErScenario for ReplicationRecovery {
    fn name(&self) -> &str {
        "redshift/replication_recovery"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);
        let (narrow_cpus, wide_cpus, host_cpus) = cpu_sets();

        let mut leader_env = vec![(
            "MBV_ENGINE__BLOCKSTORE__SUPERBLOCK".to_owned(),
            SUPERBLOCK_SLOTS.to_string(),
        )];
        if profile.snapshot_recovery {
            leader_env.push((
                "MBV_ENGINE__LEDGER__SIZE_LIMIT".to_owned(),
                LEDGER_SIZE_LIMIT_BYTES.to_string(),
            ));
        }
        let boot_started = Instant::now();
        let mut topology = topology::replicated(
            base,
            ReplicatedOptions {
                label: LABEL.to_owned(),
                verifiers: 2,
                leader_env,
                verifier_env: Vec::new(),
                verifier_cpu_sets: vec![narrow_cpus.clone(), wide_cpus.clone()],
                request_timeout: None,
            },
        )
        .await?;
        topology.leader().wait_ready(READY_TIMEOUT).await?;
        topology.wait_verifiers_connected(CONNECT_TIMEOUT).await?;
        let boot = boot_started.elapsed();

        let leader_executors = executors_in_log(topology.leader().log())?;
        let executors: Vec<u64> = topology
            .verifiers()
            .iter()
            .map(|verifier| executors_in_log(verifier.log()))
            .collect::<Result<_>>()?;
        check!(
            executors[0] != executors[1],
            "the verifiers must run different executor counts, both report \
             {} (host cpus {host_cpus}, cpu sets {narrow_cpus} / {wide_cpus})",
            executors[0]
        )?;
        eprintln!(
            "[redsuite] {}: leader ({leader_executors} executors) + verifier 0 \
             ({} executors, cpus {narrow_cpus}) + verifier 1 ({} executors, \
             cpus {wide_cpus}) up in {:.1} s",
            self.name(),
            executors[0],
            executors[1],
            boot.as_secs_f64(),
        );

        let pairs = prepare_pairs(base, &topology, profile.pairs).await?;
        let workload =
            Workload::start(pairs, profile.chain_gap, profile.heavy_iters);
        let lag = sample_lag(&topology, profile.steady).await?;
        let steady_target = leader_metric(&topology, TRANSACTIONS).await?;
        let mut steady_catch_up = Duration::ZERO;
        for verifier in topology.verifiers() {
            steady_catch_up = steady_catch_up.max(
                await_catch_up(
                    verifier,
                    TRANSACTIONS,
                    steady_target,
                    "under steady load",
                )
                .await?,
            );
        }
        verify_no_mismatch(&topology).await?;
        eprintln!(
            "[redsuite] {}: steady load for {:.0} s: {} chain txs sent, max \
             lag {:.0} / {:.0} txs over {} samples, both verifiers within \
             {} ms of the leader's {steady_target:.0}",
            self.name(),
            profile.steady.as_secs_f64(),
            workload.sent(),
            lag.max_lag[0],
            lag.max_lag[1],
            lag.samples,
            steady_catch_up.as_millis(),
        );

        let restart = restart_with_retained_cursor(
            &mut topology,
            0,
            profile.snapshot_recovery,
        )
        .await?;
        verify_no_mismatch(&topology).await?;
        eprintln!(
            "[redsuite] {}: verifier 0 restarted on its retained cursor: \
             offline {} ms, reconnected after {} ms, caught up under load in \
             {} ms, snapshots {}",
            self.name(),
            restart.offline.as_millis(),
            restart.reconnect.as_millis(),
            restart.catch_up.as_millis(),
            restart.snapshots,
        );

        let recovery = if profile.snapshot_recovery {
            let recovery = recover_from_snapshot(&mut topology, 1).await?;
            verify_no_mismatch(&topology).await?;
            eprintln!(
                "[redsuite] {}: verifier 1 fell behind retention ({:.0} \
                 purges, offline {:.1} s), installed {} snapshot(s) (leader \
                 served {}), reconnected after {} ms and replayed the tail in \
                 {} ms",
                self.name(),
                recovery.truncations,
                recovery.offline.as_secs_f64(),
                recovery.client_snapshots,
                recovery.server_snapshots,
                recovery.reconnect.as_millis(),
                recovery.catch_up.as_millis(),
            );
            Some(recovery)
        } else {
            None
        };

        let chain_txs = workload.sent();
        let pairs = workload.stop().await?;
        let final_txs = leader_metric(&topology, TRANSACTIONS).await?;
        await_leader_advance(
            &topology,
            BLOCKS,
            SUPERBLOCK_SLOTS as f64 + 1.0,
            "the final sealed boundary after the load",
        )
        .await?;
        let leader_blocks = leader_metric(&topology, BLOCKS).await?;
        let leader_txs = leader_metric(&topology, TRANSACTIONS).await?;
        check_eq!(
            leader_txs,
            final_txs,
            "the leader appended transactions after the workload stopped"
        )?;
        let mut drain = Duration::ZERO;
        for verifier in topology.verifiers() {
            drain = drain.max(
                await_catch_up(
                    verifier,
                    TRANSACTIONS,
                    leader_txs,
                    "at the final sealed boundary",
                )
                .await?,
            );
            await_catch_up(
                verifier,
                BLOCKS,
                leader_blocks,
                "at the final sealed boundary",
            )
            .await?;
            check_eq!(
                verifier_metric(verifier, TRANSACTIONS).await?,
                leader_txs,
                "verifier `{}` must hold exactly the leader's transactions at \
                 the final sealed boundary",
                verifier.label()
            )?;
        }
        verify_no_mismatch(&topology).await?;
        verify_pairs(&topology, &pairs).await?;
        let leader_superblocks = leader_metric(&topology, SUPERBLOCKS).await?;
        let verifier_superblocks: Vec<f64> = {
            let mut counts = Vec::new();
            for verifier in topology.verifiers() {
                counts.push(verifier_metric(verifier, SUPERBLOCKS).await?);
            }
            counts
        };
        let chains: u64 = pairs.iter().map(|pair| pair.chains).sum();
        eprintln!(
            "[redsuite] {}: final boundary: {chains} chains ({chain_txs} \
             chain txs) over {} pairs; leader {leader_txs:.0} txs / \
             {leader_blocks:.0} blocks / {leader_superblocks:.0} superblocks, \
             verifiers drained to zero lag in {} ms with superblocks {:?}, no \
             mismatches, every pair matches its fold on the leader",
            self.name(),
            pairs.len(),
            drain.as_millis(),
            verifier_superblocks,
        );

        topology.finish().await?;

        let mut report = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("pairs", profile.pairs)
            .setting("chain gap ms", profile.chain_gap.as_millis())
            .setting("heavy step iters", profile.heavy_iters)
            .setting("superblock slots", SUPERBLOCK_SLOTS)
            .setting(
                "ledger size limit bytes",
                if profile.snapshot_recovery {
                    LEDGER_SIZE_LIMIT_BYTES.to_string()
                } else {
                    "default".to_owned()
                },
            )
            .setting("host cpus", host_cpus)
            .setting("verifier0 cpus", narrow_cpus)
            .setting("verifier1 cpus", wide_cpus)
            .setting("leader executors", leader_executors)
            .setting("verifier0 executors", executors[0])
            .setting("verifier1 executors", executors[1])
            .setting(
                "verifier state equality",
                "the follower recomputes every sealed superblock checksum and \
                 aborts on a mismatch; zero mismatches over the run's seals is \
                 the account-equality evidence, the leader's accounts are \
                 compared against the client-side model directly",
            )
            .metric("boot s", Unit::Seconds, boot.as_secs_f64())
            .metric("chains", Unit::Count, chains as f64)
            .metric("chain txs", Unit::Count, chain_txs as f64)
            .metric("leader txs", Unit::Count, leader_txs)
            .metric("leader blocks", Unit::Count, leader_blocks)
            .metric("leader superblocks", Unit::Count, leader_superblocks)
            .metric("steady verifier0 max lag txs", Unit::Count, lag.max_lag[0])
            .metric("steady verifier1 max lag txs", Unit::Count, lag.max_lag[1])
            .metric(
                "steady catch up ms",
                Unit::Millis,
                steady_catch_up.as_secs_f64() * 1e3,
            )
            .metric(
                "cursor restart offline ms",
                Unit::Millis,
                restart.offline.as_secs_f64() * 1e3,
            )
            .metric(
                "cursor restart reconnect ms",
                Unit::Millis,
                restart.reconnect.as_secs_f64() * 1e3,
            )
            .metric(
                "cursor restart catch up ms",
                Unit::Millis,
                restart.catch_up.as_secs_f64() * 1e3,
            )
            .metric("final drain ms", Unit::Millis, drain.as_secs_f64() * 1e3)
            .metric(
                "verifier0 superblocks",
                Unit::Count,
                verifier_superblocks[0],
            )
            .metric(
                "verifier1 superblocks",
                Unit::Count,
                verifier_superblocks[1],
            );
        if let Some(recovery) = recovery {
            report = report
                .metric(
                    "snapshot recovery offline s",
                    Unit::Seconds,
                    recovery.offline.as_secs_f64(),
                )
                .metric(
                    "snapshot recovery purges",
                    Unit::Count,
                    recovery.truncations,
                )
                .metric(
                    "snapshot recovery reconnect ms",
                    Unit::Millis,
                    recovery.reconnect.as_secs_f64() * 1e3,
                )
                .metric(
                    "snapshot recovery tail replay ms",
                    Unit::Millis,
                    recovery.catch_up.as_secs_f64() * 1e3,
                )
                .metric(
                    "snapshots installed",
                    Unit::Count,
                    recovery.client_snapshots,
                )
                .metric(
                    "snapshots served",
                    Unit::Count,
                    recovery.server_snapshots,
                );
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_lines_parse_through_colour_codes() {
        let dir = std::env::temp_dir().join(format!(
            "redsuite-replication-recovery-{}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("verifier.log");
        fs::write(
            &log,
            "boot\n\u{1b}[2m2026-09-03T09:04:12Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \
             processor::sequencer: sequencer started \u{1b}[3mexecutors\u{1b}[0m\
             \u{1b}[2m=\u{1b}[0m7 \u{1b}[3mreplay\u{1b}[0m\u{1b}[2m=\u{1b}[0mfalse\n",
        )
        .unwrap();
        assert_eq!(executors_in_log(&log).unwrap(), 7);
        fs::write(&log, "nothing here\n").unwrap();
        assert!(executors_in_log(&log).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn cpu_sets_differ_in_executor_count_on_six_or_more_cpus() {
        let executors = |cpus: usize| cpus.saturating_sub(2).max(2) / 2;
        for total in [6usize, 8, 16, 32, 64] {
            let narrow = (total / 4).clamp(4, total);
            let wide = (total / 2).clamp(6, total).max(narrow);
            assert_ne!(executors(narrow), executors(wide), "{total} cpus");
        }
    }

    #[test]
    fn the_model_pins_causal_order() {
        let mut expected = PairModel::default();
        for (step, id) in [(Step::X, 1), (Step::Y, 2), (Step::Z, 3)] {
            expected.apply(step, id, 0);
        }
        let mut swapped = PairModel::default();
        for (step, id) in [(Step::Y, 2), (Step::X, 1), (Step::Z, 3)] {
            swapped.apply(step, id, 0);
        }
        assert_ne!(expected, swapped);
    }
}
