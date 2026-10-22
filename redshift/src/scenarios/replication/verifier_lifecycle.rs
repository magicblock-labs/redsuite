use std::{
    cell::Cell,
    fs,
    path::Path,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure, Context};
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    redline::{
        causal,
        causal::{CausalPair, Step},
        Accounts,
    },
    topology,
    topology::{ReplicatedOptions, ReplicatedTopology, Verifier, VerifierStop},
    wait, BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use signer::Signer;
use tokio::{task::JoinSet, time};
use tracing::{debug, instrument};

use crate::{ACCOUNT_SPACE, PAYER_LAMPORTS};

const LABEL: &str = "verifier-lifecycle";
const VERIFIERS: usize = 2;
const PAIRS: usize = 4;
const PAYERS_PER_PAIR: usize = 3;
const PROBES: usize = 4;
const PROBE_LAMPORTS: u64 = 1_000_000;
const CHAIN_GAP: Duration = Duration::from_millis(100);
const HEAVY_ITERS: u32 = 30;
const STEADY: Duration = Duration::from_secs(8);
const SUPERBLOCK_SLOTS: u64 = 128;
const LEDGER_SIZE_LIMIT_BYTES: u64 = 1;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(90);
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(120);
const DETACH_TIMEOUT: Duration = Duration::from_secs(30);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(120);
const RETENTION_TIMEOUT: Duration = Duration::from_secs(180);
const SEAL_TIMEOUT: Duration = Duration::from_secs(60);
const ADVANCE_WINDOW: Duration = Duration::from_secs(2);
const LAG_SAMPLE: Duration = Duration::from_millis(500);
const RESTART_BUDGET: Duration = Duration::from_secs(16);

const TRANSACTIONS: &str = "engine_ledger_transactions";
const BLOCKS: &str = "engine_ledger_blocks";
const SUPERBLOCKS: &str = "engine_ledger_superblocks";
const STATE_MISMATCHES: &str = "engine_replicator_client_state_mismatches";
const CLIENT_SNAPSHOTS: &str =
    r#"engine_replicator_operation_duration_micros_count{op="client_stage_snapshot"}"#;
const SERVER_SNAPSHOTS: &str =
    r#"engine_replicator_operation_duration_micros_count{op="server_send_snapshot"}"#;
const TRUNCATIONS: &str = r#"engine_ledger_operation_duration_micros_count{op="truncate"}"#;

#[derive(Default)]
struct Progress {
    stop: Cell<bool>,
    sent: Cell<u64>,
    next_id: Cell<u64>,
}

impl Progress {
    async fn run(self: Rc<Self>, mut pair: CausalPair) -> Result<CausalPair> {
        let mut chains = 0;
        while !self.stop.get() {
            let heavy = Step::ALL[(chains % causal::STEPS) as usize];
            for step in Step::ALL {
                let id = self.next_id.get() + 1;
                self.next_id.set(id);
                let iters = if step == heavy { HEAVY_ITERS } else { 0 };
                pair.send(step, id, iters).await?;
                self.sent.set(self.sent.get() + 1);
            }
            chains += 1;
            time::sleep(CHAIN_GAP).await;
        }
        Ok(pair)
    }
}

struct Workload {
    progress: Rc<Progress>,
    tasks: JoinSet<Result<CausalPair>>,
}

impl Workload {
    fn start(pairs: Vec<CausalPair>) -> Self {
        let progress = Rc::new(Progress::default());
        let mut tasks = JoinSet::new();
        for pair in pairs {
            tasks.spawn_local(progress.clone().run(pair));
        }
        Self { progress, tasks }
    }

    fn sent(&self) -> u64 {
        self.progress.sent.get()
    }

    async fn stop(self, topology: &ReplicatedTopology) -> Result<(Vec<CausalPair>, u64)> {
        self.progress.stop.set(true);
        time::timeout(DRAIN_TIMEOUT, self.drain(topology))
            .await
            .with_context(|| format!("workload did not drain within {DRAIN_TIMEOUT:?}"))?
    }

    async fn drain(self, topology: &ReplicatedTopology) -> Result<(Vec<CausalPair>, u64)> {
        let pairs: Vec<_> = self.tasks.join_all().await.into_iter().collect::<Result<_>>()?;
        for pair in &pairs {
            if let Some(signature) = pair.last_signature {
                let tx = topology.leader().ctx().api().await_transaction(&signature).await?;
                let meta = transaction_meta(&tx)?;
                ensure!(meta.err == None);
            }
        }
        await_leader_advance(
            topology,
            BLOCKS,
            1.0,
            "ledger metrics after the workload drains",
        )
        .await?;
        Ok((pairs, self.progress.sent.get()))
    }
}

async fn leader_metric(topology: &ReplicatedTopology, name: &str) -> Result<f64> {
    let metrics = topology.leader().ctx().scrape_metrics().await?;
    metrics
        .get(name)
        .with_context(|| format!("the leader exposes no {name} metric"))
}

async fn verifier_metric(verifier: &Verifier, name: &str) -> Result<f64> {
    verifier
        .scrape_metrics()
        .await?
        .get(name)
        .with_context(|| format!("verifier `{}` exposes no {name} metric", verifier.label()))
}

async fn leader_count(topology: &ReplicatedTopology, name: &str) -> Result<f64> {
    let metrics = topology.leader().ctx().scrape_metrics().await?;
    Ok(metrics.get(name).unwrap_or(0.0))
}

async fn verifier_count(verifier: &Verifier, name: &str) -> Result<f64> {
    Ok(verifier.scrape_metrics().await?.get(name).unwrap_or(0.0))
}

async fn await_catch_up(verifier: &Verifier, name: &str, target: f64) -> Result<Duration> {
    let started = Instant::now();
    wait::retry(
        &format!(
            "verifier `{}` {name} reaching the leader's {target:.0}",
            verifier.label()
        ),
        CATCH_UP_TIMEOUT,
        async || {
            ensure!(verifier_metric(verifier, name).await? >= target);
            Ok(())
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
    wait::retry(
        &format!("{what} (leader {name} reaching {target:.0})"),
        SEAL_TIMEOUT.max(RETENTION_TIMEOUT),
        async || {
            ensure!(leader_count(topology, name).await? >= target);
            Ok(())
        },
    )
    .await?;
    Ok(target)
}

async fn stop_verifier(verifier: &mut Verifier, hard_kill: bool) -> Result<VerifierStop> {
    let stop = verifier.stop(hard_kill).await?;
    if hard_kill {
        ensure!(stop.exit_signal == Some(9));
    } else {
        ensure!(!stop.needed_sigkill);
        ensure!(stop.exit_code == Some(0));
    }
    ensure!(!verifier.is_running());
    ensure!(verifier.scrape_metrics().await.is_err());
    Ok(stop)
}

async fn blocks_advance(verifier: &Verifier) -> Result<f64> {
    let before = verifier_metric(verifier, BLOCKS).await?;
    time::sleep(ADVANCE_WINDOW).await;
    let after = verifier_metric(verifier, BLOCKS).await?;
    ensure!(after > before);
    Ok(after - before)
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
        .with_context(|| format!("{} never logged `sequencer started`", log.display()))?;
    let cleaned = strip_ansi(line);
    let digits: String = cleaned
        .split("executors=")
        .nth(1)
        .unwrap_or_default()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().with_context(|| {
        format!(
            "{} logged an unparseable executor count: {cleaned}",
            log.display()
        )
    })
}

fn log_mentions_mismatch(log: &Path) -> Result<Option<String>> {
    let text = fs::read_to_string(log)?;
    Ok(text
        .lines()
        .find(|line| line.to_ascii_lowercase().contains("mismatch"))
        .map(strip_ansi))
}

pub struct VerifierLifecycle {
    base: Rc<BaseCtx>,
    topology: ReplicatedTopology,
}

impl PrivateErScenario for VerifierLifecycle {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let topology = topology::replicated(
            &base,
            ReplicatedOptions {
                label: LABEL.to_owned(),
                verifiers: VERIFIERS,
                settings: Box::new(|params| {
                    params.engine.blockstore.superblock = SUPERBLOCK_SLOTS;
                    params.engine.ledger.size_limit = LEDGER_SIZE_LIMIT_BYTES;
                }),
            },
        )
        .await?;

        Ok(Self { base, topology })
    }

    async fn run(mut self) -> Result<()> {
        self.check_ready().await?;

        self.verify_connections().await?;
        self.catch_up_fleet(&[TRANSACTIONS, BLOCKS]).await?;

        let pairs = self.prepare_pairs(PAIRS).await?;
        let workload = Workload::start(pairs);

        self.check_steady_load(&workload).await?;

        self.restart_retained_cursor().await?;

        let server_snapshots_before = leader_count(&self.topology, SERVER_SNAPSHOTS).await?;
        let truncations_before = leader_count(&self.topology, TRUNCATIONS).await?;
        stop_verifier(self.topology.verifier_mut(1), false).await?;

        self.check_retention_gap().await?;

        self.recover_snapshot(server_snapshots_before, truncations_before).await?;

        self.kill_verifier().await?;

        self.rejoin_wiped().await?;

        let (pairs, _) = workload.stop(&self.topology).await?;
        self.check_final_boundary(&pairs).await?;

        self.topology.finish().await
    }
}

impl VerifierLifecycle {
    #[instrument(skip_all, err)]
    async fn check_ready(&self) -> Result<()> {
        self.topology.leader().wait_ready(READY_TIMEOUT).await?;
        self.topology.wait_verifiers_connected(CONNECT_TIMEOUT).await?;
        let leader_executors = executors_in_log(self.topology.leader().log())?;
        let mut verifier_executors = Vec::with_capacity(VERIFIERS);
        for verifier in self.topology.verifiers() {
            verifier_executors.push(executors_in_log(verifier.log())?);
        }
        debug!(
            leader_executors,
            ?verifier_executors,
            "replicated topology ready",
        );
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_steady_load(&self, workload: &Workload) -> Result<()> {
        let (max_lag, _) = self.sample_lag(STEADY).await?;
        self.catch_up_fleet(&[TRANSACTIONS]).await?;
        self.verify_no_mismatch().await?;
        debug!(
            sent = workload.sent(),
            ?max_lag,
            "steady replication load finished",
        );
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn restart_retained_cursor(&mut self) -> Result<()> {
        await_leader_advance(
            &self.topology,
            BLOCKS,
            SUPERBLOCK_SLOTS as f64,
            "a fresh superblock boundary before the retained-cursor \
                 restart",
        )
        .await?;
        let restart_started = Instant::now();
        stop_verifier(self.topology.verifier_mut(0), false).await?;
        self.topology.verifier_mut(0).start(READY_TIMEOUT).await?;
        let restart_elapsed = restart_started.elapsed();
        ensure!(restart_elapsed <= RESTART_BUDGET);
        self.topology.verifier(0).wait_connected(CONNECT_TIMEOUT).await?;
        let snapshots = verifier_count(self.topology.verifier(0), CLIENT_SNAPSHOTS).await?;
        ensure!(snapshots == 0.0);
        let target = leader_metric(&self.topology, TRANSACTIONS).await?;
        let catch_up = await_catch_up(self.topology.verifier(0), TRANSACTIONS, target).await?;
        self.await_leader_connections(VERIFIERS).await?;
        self.verify_no_mismatch().await?;
        debug!(
            catch_up_secs = catch_up.as_secs_f64(),
            snapshots, "verifier restarted on retained cursor",
        );
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_retention_gap(&self) -> Result<()> {
        self.await_leader_connections(1).await?;
        // A block produced after shutdown is beyond the stopped
        // cursor. Observe it before waiting for retention to remove
        // its superblock.
        let slot = self.topology.leader().ctx().api().get_slot().await? + 1;
        let pruned = async {
            for retained in [true, false] {
                self.topology
                    .leader()
                    .ctx()
                    .api()
                    .await_query(
                        &format!(
                            "leader block {slot} retained={retained} \
                                 after verifier 1 stopped"
                        ),
                        async || {
                            let block = self.topology.leader().ctx().api().get_block(slot).await?;
                            ensure!(block.is_some() == retained);
                            Ok(())
                        },
                    )
                    .await?;
            }
            Result::Ok(())
        };
        let survivor = async {
            let slot_before = self.topology.leader().ctx().api().get_slot().await?;
            blocks_advance(self.topology.verifier(0)).await?;
            let slot_after = self.topology.leader().ctx().api().get_slot().await?;
            ensure!(slot_after > slot_before);
            let target = leader_metric(&self.topology, TRANSACTIONS).await?;
            await_catch_up(self.topology.verifier(0), TRANSACTIONS, target).await?;
            Result::Ok(())
        };
        tokio::try_join!(pruned, survivor)?;
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn recover_snapshot(
        &mut self,
        server_snapshots_before: f64,
        truncations_before: f64,
    ) -> Result<()> {
        self.topology.verifier_mut(1).start(READY_TIMEOUT).await?;
        self.topology.verifier(1).wait_connected(CONNECT_TIMEOUT).await?;
        let client_snapshots = verifier_count(self.topology.verifier(1), CLIENT_SNAPSHOTS).await?;
        ensure!(client_snapshots >= 1.0);
        let server_snapshots =
            leader_count(&self.topology, SERVER_SNAPSHOTS).await? - server_snapshots_before;
        ensure!(server_snapshots >= 1.0);
        let target = leader_metric(&self.topology, TRANSACTIONS).await?;
        await_catch_up(self.topology.verifier(1), TRANSACTIONS, target).await?;
        self.await_leader_connections(VERIFIERS).await?;
        let sealed = await_leader_advance(
            &self.topology,
            SUPERBLOCKS,
            1.0,
            "a superblock sealed after the snapshot recovery",
        )
        .await?;
        await_catch_up(self.topology.verifier(1), SUPERBLOCKS, sealed).await?;
        self.verify_no_mismatch().await?;
        let truncations = leader_count(&self.topology, TRUNCATIONS).await? - truncations_before;
        debug!(
            truncations,
            client_snapshots, server_snapshots, "verifier recovered beyond retention via snapshot",
        );
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn kill_verifier(&mut self) -> Result<()> {
        stop_verifier(self.topology.verifier_mut(1), true).await?;
        self.await_leader_connections(1).await?;
        let slot_before = self.topology.leader().ctx().api().get_slot().await?;
        let lone_blocks = blocks_advance(self.topology.verifier(0)).await?;
        let slot_after = self.topology.leader().ctx().api().get_slot().await?;
        ensure!(slot_after > slot_before);
        self.probe_clones().await?;
        let target = leader_metric(&self.topology, TRANSACTIONS).await?;
        await_catch_up(self.topology.verifier(0), TRANSACTIONS, target).await?;
        ensure!(!self.topology.verifier(1).is_running());
        debug!(
            slots_advanced = slot_after - slot_before,
            lone_blocks, target, "surviving verifier kept up while peer offline",
        );
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn rejoin_wiped(&mut self) -> Result<()> {
        let served_before = leader_count(&self.topology, SERVER_SNAPSHOTS).await?;
        self.topology.verifier_mut(1).reset_storage()?;
        self.topology.verifier_mut(1).start(READY_TIMEOUT).await?;
        self.topology.verifier(1).wait_connected(CONNECT_TIMEOUT).await?;
        let client_snapshots = verifier_count(self.topology.verifier(1), CLIENT_SNAPSHOTS).await?;
        ensure!(client_snapshots >= 1.0);
        let server_snapshots =
            leader_count(&self.topology, SERVER_SNAPSHOTS).await? - served_before;
        ensure!(server_snapshots >= 1.0);
        let target = leader_metric(&self.topology, TRANSACTIONS).await?;
        await_catch_up(self.topology.verifier(1), TRANSACTIONS, target).await?;
        let blocks = blocks_advance(self.topology.verifier(1)).await?;
        self.await_leader_connections(VERIFIERS).await?;
        self.verify_no_mismatch().await?;
        debug!(
            client_snapshots,
            blocks, "verifier rejoined from wiped storage",
        );
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_final_boundary(&self, pairs: &[CausalPair]) -> Result<()> {
        let final_txs = leader_metric(&self.topology, TRANSACTIONS).await?;
        await_leader_advance(
            &self.topology,
            BLOCKS,
            SUPERBLOCK_SLOTS as f64 + 1.0,
            "the final sealed boundary after the load",
        )
        .await?;
        let leader_blocks = leader_metric(&self.topology, BLOCKS).await?;
        let leader_txs = leader_metric(&self.topology, TRANSACTIONS).await?;
        ensure!(leader_txs == final_txs);
        self.catch_up_fleet(&[TRANSACTIONS, SUPERBLOCKS]).await?;
        await_catch_up(self.topology.verifier(0), BLOCKS, leader_blocks).await?;
        for verifier in self.topology.verifiers() {
            ensure!(verifier_metric(verifier, TRANSACTIONS).await? == leader_txs);
        }
        self.verify_no_mismatch().await?;
        self.verify_pairs(pairs).await?;
        leader_metric(&self.topology, SUPERBLOCKS).await?;
        let mut verifier_superblocks = Vec::with_capacity(VERIFIERS);
        for verifier in self.topology.verifiers() {
            verifier_superblocks.push(verifier_metric(verifier, SUPERBLOCKS).await?);
        }
        debug!(
            leader_txs,
            ?verifier_superblocks,
            "final replicated account state verified",
        );
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn catch_up_fleet(&self, names: &[&str]) -> Result<Duration> {
        let mut slowest = Duration::ZERO;
        for name in names {
            let target = leader_metric(&self.topology, name).await?;
            for verifier in self.topology.verifiers() {
                slowest = slowest.max(await_catch_up(verifier, name, target).await?);
            }
        }
        Ok(slowest)
    }

    async fn await_leader_connections(&self, expected: usize) -> Result<()> {
        wait::retry(
            &format!("the leader serving {expected} replication connection(s)"),
            DETACH_TIMEOUT,
            async || {
                ensure!(self.topology.leader_connections().await? == expected as f64);
                Ok(())
            },
        )
        .await?;
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn verify_connections(&self) -> Result<()> {
        for verifier in self.topology.verifiers() {
            ensure!(verifier.stream_connected().await);
        }
        ensure!(self.topology.leader_connections().await? == (VERIFIERS as f64));
        Ok(())
    }

    async fn sample_lag(&self, window: Duration) -> Result<(Vec<f64>, usize)> {
        let deadline = time::Instant::now() + window;
        let mut max_lag = vec![0.0f64; self.topology.verifiers().len()];
        let mut samples = 0usize;
        while time::Instant::now() < deadline {
            let leader = leader_metric(&self.topology, TRANSACTIONS).await?;
            for (index, verifier) in self.topology.verifiers().iter().enumerate() {
                let seen = verifier_metric(verifier, TRANSACTIONS).await?;
                max_lag[index] = max_lag[index].max(leader - seen);
            }
            samples += 1;
            time::sleep(LAG_SAMPLE).await;
        }
        Ok((max_lag, samples))
    }

    async fn verify_pairs(&self, pairs: &[CausalPair]) -> Result<()> {
        let leader = self.topology.leader().ctx();
        for (index, pair) in pairs.iter().enumerate() {
            pair.verify(leader, &format!("pair {index} on the leader")).await?;
        }
        Ok(())
    }

    async fn verify_no_mismatch(&self) -> Result<()> {
        for verifier in self.topology.verifiers() {
            ensure!(verifier.is_running());
            ensure!(verifier.stream_connected().await);
            ensure!(verifier_count(verifier, STATE_MISMATCHES).await? == 0.0);
            if let Some(line) = log_mentions_mismatch(verifier.log())? {
                return Err(anyhow!(
                    "verifier {}: replication mismatch: {line}",
                    verifier.label()
                ));
            }
        }
        Ok(())
    }

    async fn probe_clones(&self) -> Result<()> {
        let leader = self.topology.leader().ctx();
        for _ in 0..PROBES {
            let probe = self.base.funded_payer(PROBE_LAMPORTS).await?;
            let address = probe.pubkey();
            leader
                .api()
                .await_accounts(&[address], "leader clones the probe", async || {
                    let account =
                        leader.account(&address).await?.context("probe account missing")?;
                    ensure!(account.lamports > 0);
                    Ok(())
                })
                .await?;
        }
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn prepare_pairs(&self, count: usize) -> Result<Vec<CausalPair>> {
        let leader = self.topology.leader();
        let payers = self.base.funded_payers(count * PAYERS_PER_PAIR, PAYER_LAMPORTS).await?;
        let accounts = Accounts::new(ACCOUNT_SPACE, leader.identity());
        let mut pairs = Vec::with_capacity(count);
        for index in 0..count {
            let owner = &payers[index * PAYERS_PER_PAIR];
            let pdas = accounts.init_delegated(self.base.as_ref(), owner, 2).await?;
            leader.ctx().await_clones(&pdas, ACCOUNT_SPACE as usize).await?;
            let senders = payers[index * PAYERS_PER_PAIR..(index + 1) * PAYERS_PER_PAIR]
                .iter()
                .map(|payer| leader.ctx().sender(Rc::new(payer.insecure_clone())))
                .collect();
            pairs.push(CausalPair::new([pdas[0], pdas[1]], senders));
        }
        let payer_keys: Vec<Pubkey> = payers.iter().map(|payer| payer.pubkey()).collect();
        leader.ctx().await_cloned_payers(&payer_keys).await?;
        Ok(pairs)
    }
}
