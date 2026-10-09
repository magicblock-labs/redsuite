use super::{await_catch_up, leader_metric, verifier_metric};

use std::{
    cell::Cell,
    fs,
    path::Path,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, prep,
    redline::causal::{chain_ixs, PairModel, Step, STEPS},
    topology::{self, ReplicatedOptions, ReplicatedTopology, Verifier},
    BaseCtx, ChainCtx, CheckError, PrivateErScenario, Result, TxSender,
};
use signature::Signature;
use signer::Signer;

const LABEL: &str = "replication-recovery";
const PAIRS: usize = 4;
const CHAIN_GAP: Duration = Duration::from_millis(100);
const HEAVY_ITERS: u32 = 30;
const STEADY: Duration = Duration::from_secs(8);
const PAYERS_PER_PAIR: usize = 3;
const SUPERBLOCK_SLOTS: u64 = 128;
const LEDGER_SIZE_LIMIT_BYTES: u64 = 1;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(90);
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(120);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(120);
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

struct Pair {
    a: Pubkey,
    b: Pubkey,
    senders: Vec<TxSender>,
    model: PairModel,
    chains: u64,
    last_signature: Option<Signature>,
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
                            let ixs = chain_ixs(
                                id,
                                iters,
                                &step.accounts(pair.a, pair.b),
                            );
                            let signature = pair.senders[step.index()]
                                .submit(&ixs)
                                .await
                                .map_err(|error| {
                                    format!(
                                        "chain step {step:?} (id {id}) was \
                                         not accepted by the leader: {error}"
                                    )
                                })?;
                            pair.last_signature = Some(signature);
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

    async fn stop(
        mut self,
        topology: &ReplicatedTopology,
    ) -> Result<(Vec<Pair>, u64)> {
        self.stop.set(true);
        let stopped = tokio::time::timeout(DRAIN_TIMEOUT, async {
            let mut pairs = Vec::with_capacity(self.tasks.len());
            for task in &mut self.tasks {
                let pair = task
                    .await
                    .map_err(|error| format!("workload task: {error}"))??;
                if let Some(signature) = pair.last_signature {
                    let tx = topology
                        .leader()
                        .ctx()
                        .api()
                        .await_transaction(&signature, DRAIN_TIMEOUT)
                        .await?;
                    check!(
                        tx.err.is_none(),
                        "final transaction {signature} failed: {:?}",
                        tx.err
                    )?;
                }
                pairs.push(pair);
            }
            await_leader_advance(
                topology,
                BLOCKS,
                1.0,
                "ledger metrics after the workload drains",
            )
            .await?;
            Ok((pairs, self.sent()))
        })
        .await;
        // On timeout or a producer error, don't leave detached producers behind.
        for task in self.tasks {
            task.abort();
        }
        stopped.map_err(|_| {
            format!("workload did not drain within {DRAIN_TIMEOUT:?}")
        })?
    }
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

async fn verifier_count(verifier: &Verifier, name: &str) -> Result<f64> {
    Ok(verifier.scrape_metrics().await?.get(name).unwrap_or(0.0))
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
        pair.model
            .verify(
                leader,
                [pair.a, pair.b],
                &format!("pair {index} on the leader"),
            )
            .await?;
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
    let accounts = redsuite_core::redline::Accounts::new(
        crate::ACCOUNT_SPACE,
        leader.identity(),
    );
    let mut pairs = Vec::with_capacity(count);
    for index in 0..count {
        let owner = &payers[index * PAYERS_PER_PAIR];
        let pdas = accounts.init_delegated(base, owner, 2).await?;
        prep::await_clones(
            leader.ctx(),
            &pdas,
            crate::ACCOUNT_SPACE as usize,
            CLONE_TIMEOUT,
        )
        .await?;
        let senders = payers
            [index * PAYERS_PER_PAIR..(index + 1) * PAYERS_PER_PAIR]
            .iter()
            .map(|payer| leader.ctx().sender(Rc::new(payer.insecure_clone())))
            .collect();
        pairs.push(Pair {
            a: pdas[0],
            b: pdas[1],
            senders,
            model: PairModel::default(),
            chains: 0,
            last_signature: None,
        });
    }
    let payer_keys: Vec<Pubkey> =
        payers.iter().map(|payer| payer.pubkey()).collect();
    prep::await_cloned_payers(leader.ctx(), &payer_keys, CLONE_TIMEOUT).await?;
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
) -> Result<RestartOutcome> {
    await_leader_advance(
        topology,
        BLOCKS,
        SUPERBLOCK_SLOTS as f64,
        "a fresh superblock boundary before the retained-cursor restart",
    )
    .await?;
    let stopped_at = Instant::now();
    let stop = topology.verifier_mut(index).stop(false).await?;
    check_eq!(
        stop.exit_code,
        Some(0),
        "verifier {index} must stop cleanly so its cursor is durable"
    )?;
    topology.verifier_mut(index).start(READY_TIMEOUT).await?;
    let offline = stopped_at.elapsed();
    check!(
        offline <= RESTART_AFTER_SEAL_BUDGET,
        "verifier {index} was offline {offline:?}, too long to be sure \
         its cursor survived the next retention check"
    )?;
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
        CATCH_UP_TIMEOUT,
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
    let truncations_before = leader_count(topology, TRUNCATIONS).await?;
    let stopped_at = Instant::now();
    let stop = topology.verifier_mut(index).stop(false).await?;
    check_eq!(
        stop.exit_code,
        Some(0),
        "verifier {index} must stop cleanly before falling behind retention"
    )?;
    // A block produced after shutdown is beyond the stopped cursor. Observe
    // it before waiting for retention to remove its superblock.
    let slot = topology.leader().ctx().api().get_slot().await? + 1;
    for (retained, timeout) in
        [(true, SEAL_TIMEOUT), (false, RETENTION_TIMEOUT)]
    {
        check::poll(
            &format!("leader block {slot} retained={retained} after verifier {index} stopped"),
            timeout,
            || async {
                topology.leader().ctx().api().get_block(slot).await
                    .is_ok_and(|block| block.is_some() == retained)
            },
        )
        .await?;
    }
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
        "verifier {index} rejoined after leader block {slot} was pruned \
         without installing a snapshot"
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
        CATCH_UP_TIMEOUT,
    )
    .await?;
    Ok(RecoveryOutcome {
        offline,
        truncations: leader_count(topology, TRUNCATIONS).await?
            - truncations_before,
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

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let leader_env = vec![
            (
                "MBV_ENGINE__BLOCKSTORE__SUPERBLOCK".to_owned(),
                SUPERBLOCK_SLOTS.to_string(),
            ),
            (
                "MBV_ENGINE__LEDGER__SIZE_LIMIT".to_owned(),
                LEDGER_SIZE_LIMIT_BYTES.to_string(),
            ),
        ];
        let boot_started = Instant::now();
        let mut topology = topology::replicated(
            base,
            ReplicatedOptions {
                label: LABEL.to_owned(),
                verifiers: 2,
                leader_env,
                verifier_env: Vec::new(),
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
        eprintln!(
            "[redsuite] {}: leader ({leader_executors} executors) + verifier 0 \
             ({} executors) + verifier 1 ({} executors) up in {:.1} s",
            self.name(),
            executors[0],
            executors[1],
            boot.as_secs_f64(),
        );

        let pairs = prepare_pairs(base, &topology, PAIRS).await?;
        let workload = Workload::start(pairs, CHAIN_GAP, HEAVY_ITERS);
        let lag = sample_lag(&topology, STEADY).await?;
        let steady_target = leader_metric(&topology, TRANSACTIONS).await?;
        let mut steady_catch_up = Duration::ZERO;
        for verifier in topology.verifiers() {
            steady_catch_up = steady_catch_up.max(
                await_catch_up(
                    verifier,
                    TRANSACTIONS,
                    steady_target,
                    "under steady load",
                    CATCH_UP_TIMEOUT,
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
            STEADY.as_secs_f64(),
            workload.sent(),
            lag.max_lag[0],
            lag.max_lag[1],
            lag.samples,
            steady_catch_up.as_millis(),
        );

        let restart = restart_with_retained_cursor(&mut topology, 0).await?;
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

        let (pairs, chain_txs) = workload.stop(&topology).await?;
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
                    CATCH_UP_TIMEOUT,
                )
                .await?,
            );
            await_catch_up(
                verifier,
                BLOCKS,
                leader_blocks,
                "at the final sealed boundary",
                CATCH_UP_TIMEOUT,
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
        Ok(())
    }
}
