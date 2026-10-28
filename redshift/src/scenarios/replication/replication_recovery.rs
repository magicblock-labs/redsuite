use std::{
    cell::Cell,
    fs,
    path::Path,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure, Context};
use pubkey::Pubkey;
use signer::Signer;

use redsuite_core::{
    api::transaction_meta,
    dlp,
    programs::{
        Action, CellHeader, Commit, IntentBundle, ResultCallback, SettlementMode, REDSHIFT,
    },
    random::{self, Rng},
    redline::{
        causal,
        causal::{CausalPair, Step},
        Accounts,
    },
    signed::audit_signatures,
    topology,
    topology::{ReplicatedOptions, ReplicatedTopology, RestartConfig, Verifier},
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use tokio::{task::JoinSet, time};
use tracing::{debug, instrument};

use crate::{ACCOUNT_SPACE, PAYER_LAMPORTS};

const VERIFIERS: usize = 2;
const PAIRS: usize = 4;
const PAYERS_PER_PAIR: usize = 3;
const PROBES: usize = 4;
const PROBE_LAMPORTS: u64 = 1_000_000;
const CHAIN_GAP: Duration = Duration::from_millis(100);
const HEAVY_ITERS: u32 = 30;
const STEADY: Duration = Duration::from_secs(8);
const BLOCKTIME: Duration = Duration::from_millis(40);
// Retention removes one superblock every 512 slots; avoid producing a backlog.
const SUPERBLOCK_SLOTS: u64 = 512;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(90);
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(120);
const DETACH_TIMEOUT: Duration = Duration::from_secs(30);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(120);
const SEAL_TIMEOUT: Duration = Duration::from_secs(60);
const ADVANCE_WINDOW: Duration = Duration::from_secs(2);
const LAG_SAMPLE: Duration = Duration::from_millis(500);
const RESTART_BUDGET: Duration = Duration::from_secs(16);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(60);

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

    async fn stop(self, topology: &ReplicatedTopology) -> Result<Vec<CausalPair>> {
        self.progress.stop.set(true);
        time::timeout(DRAIN_TIMEOUT, self.drain(topology))
            .await
            .with_context(|| format!("workload did not drain within {DRAIN_TIMEOUT:?}"))?
    }

    async fn drain(self, topology: &ReplicatedTopology) -> Result<Vec<CausalPair>> {
        let pairs: Vec<_> = self.tasks.join_all().await.into_iter().collect::<Result<_>>()?;
        for pair in &pairs {
            if let Some(signature) = pair.last_signature {
                let tx = topology.leader().api().await_transaction(&signature).await?;
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
        Ok(pairs)
    }
}

async fn leader_metric(topology: &ReplicatedTopology, name: &str) -> Result<f64> {
    let metrics = topology.leader().scrape_metrics().await?;
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
    let metrics = topology.leader().scrape_metrics().await?;
    Ok(metrics.get(name).unwrap_or(0.0))
}

async fn verifier_count(verifier: &Verifier, name: &str) -> Result<f64> {
    Ok(verifier.scrape_metrics().await?.get(name).unwrap_or(0.0))
}

async fn await_catch_up(verifier: &Verifier, name: &str, target: f64) -> Result<Duration> {
    let started = Instant::now();
    let caught = wait::retry(
        &format!(
            "verifier `{}` {name} reaching the leader's {target:.0}",
            verifier.label()
        ),
        CATCH_UP_TIMEOUT,
        async || {
            if !verifier.is_running() {
                return Ok(false);
            }
            ensure!(verifier_metric(verifier, name).await? >= target);
            Ok(true)
        },
    )
    .await;
    if !matches!(caught, Ok(true)) {
        if let Some(line) = log_mentions_mismatch(verifier.log())? {
            return Err(anyhow!(
                "verifier `{}` stopped below the leader's {name} {target:.0}: {line}",
                verifier.label()
            ));
        }
        ensure!(
            verifier.is_running(),
            "verifier `{}` exited before reaching the leader's {name} {target:.0}",
            verifier.label()
        );
    }
    caught?;
    Ok(started.elapsed())
}

async fn await_leader_advance(
    topology: &ReplicatedTopology,
    name: &str,
    by: f64,
    what: &str,
) -> Result<()> {
    let from = leader_metric(topology, name).await?;
    let target = from + by;
    wait::retry(
        &format!("{what} (leader {name} reaching {target:.0})"),
        SEAL_TIMEOUT,
        async || {
            ensure!(leader_metric(topology, name).await? >= target);
            Ok(())
        },
    )
    .await
}

async fn stop_verifier(verifier: &mut Verifier, hard_kill: bool) -> Result<()> {
    let stop = verifier.stop(hard_kill).await?;
    if hard_kill {
        ensure!(stop.exit_signal == Some(9));
    } else {
        ensure!(!stop.needed_sigkill);
        ensure!(stop.exit_code == Some(0));
    }
    ensure!(verifier.scrape_metrics().await.is_err());
    Ok(())
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

fn log_mentions_mismatch(log: &Path) -> Result<Option<String>> {
    let text = fs::read_to_string(log)?;
    Ok(text
        .lines()
        .find(|line| line.to_ascii_lowercase().contains("mismatch"))
        .map(strip_ansi))
}

/// Checks incremental/snapshot recovery, surviving peers, and wiped follower rejoin.
#[derive(Clone, Copy)]
pub enum Coverage {
    Recovery,
    Settlement,
}

pub struct ReplicationRecovery {
    base: Rc<BaseCtx>,
    topology: ReplicatedTopology,
    coverage: Coverage,
}

impl Scenario<(), Coverage> for ReplicationRecovery {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, coverage: Coverage) -> Result<Self> {
        let topology = topology::replicated(
            &base,
            er,
            ReplicatedOptions {
                label: match coverage {
                    Coverage::Recovery => "retention",
                    Coverage::Settlement => "replication-security",
                }
                .to_owned(),
                verifiers: VERIFIERS,
            },
        )
        .await?;

        Ok(Self { base, topology, coverage })
    }

    async fn run(mut self) -> Result<()> {
        self.check_ready().await?;

        // Snapshot bootstrap preserves transaction/seal cursors, not the local block count.
        self.catch_up_fleet(&[TRANSACTIONS, SUPERBLOCKS]).await?;

        let pairs = self.prepare_pairs(PAIRS).await?;
        let workload = Workload::start(pairs);

        self.check_steady_load(&workload).await?;

        time::timeout(RECOVERY_TIMEOUT, self.restart_retained_cursor())
            .await
            .context("retained-cursor recovery exceeded 60s")??;

        let server_snapshots_before = leader_count(&self.topology, SERVER_SNAPSHOTS).await?;
        let truncations_before = leader_count(&self.topology, TRUNCATIONS).await?;
        stop_verifier(self.topology.verifier_mut(1), false).await?;
        self.check_retention_gap().await?;
        time::timeout(
            RECOVERY_TIMEOUT,
            self.recover_snapshot(server_snapshots_before, truncations_before),
        )
        .await
        .context("pruned-cursor recovery exceeded 60s")??;

        self.kill_verifier().await?;

        time::timeout(RECOVERY_TIMEOUT, self.rejoin_wiped())
            .await
            .context("wiped verifier recovery exceeded 60s")??;

        let pairs = workload.stop(&self.topology).await?;
        if matches!(self.coverage, Coverage::Settlement) {
            self.check_settlement_replay().await?;
        }
        self.check_final_boundary(&pairs).await?;

        self.topology.finish().await
    }
}

impl ReplicationRecovery {
    async fn check_settlement_replay(&mut self) -> Result<()> {
        let mut rng = random::rng(
            self.base.config().seed,
            &["redhat", "replication_settlement"],
            0,
        );
        let marker = rng.gen_range(1..1_000_000u64);
        let funder = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let payer = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let er = self.topology.leader().clone();
        let cell = self
            .base
            .cells(REDSHIFT, &payer)
            .create_primary(er.identity(), u32::MAX)
            .await?;
        self.base
            .submit_and_confirm(
                &payer,
                &[dlp::top_up_ephemeral_balance(&payer.pubkey(), 500_000_000, 1)],
            )
            .await?;
        self.base.delegate_payer(&funder, &payer, er.identity()).await?;
        er.await_delegated(&cell, "replicated settlement cell").await?;
        er.await_delegated(&payer.pubkey(), "replicated settlement payer").await?;
        let before = er
            .local_accounts(&[REDSHIFT.id()], &[cell])
            .await?
            .pop()
            .flatten()
            .context("replicated cell missing")?;
        let mut expected = before.clone();
        let mut header = CellHeader::try_decode(&expected.data)?;
        header.value = marker;
        header.revision += 2;
        header.write(&mut expected.data)?;
        let mut commit = Commit::new(SettlementMode::Commit, &[cell]);
        commit.after_commit.push(
            Action::new(
                payer.pubkey(),
                REDSHIFT.batch().nonce(marker).action(cell, dlp::dlp_id()),
                100_000,
            )
            .then(ResultCallback {
                cell,
                refund: None,
                compute_units: 20_000,
            }),
        );
        let mut bundle = IntentBundle::new(payer.pubkey());
        bundle.validator = Some(er.identity());
        bundle.commits.push(commit);
        let start = er.api().get_slot().await?;
        let scheduling = er
            .submit_and_confirm(
                &payer,
                &[REDSHIFT.batch().nonce(marker).set(&[cell], marker).intents(bundle).build()],
            )
            .await?;
        let receipt = er.check_commit(&self.base, &scheduling, &[cell], false).await?;
        receipt.check_success("replicated callback settlement")?;
        er.api()
            .await_account(&cell, |account| {
                ensure!(account == &expected);
                Ok(())
            })
            .await?;
        let failure = er
            .sender(Rc::new(payer.insecure_clone()))
            .submit(&[REDSHIFT.batch().nonce(marker + 1).add(&[cell], 99).fail(0x247).build()])
            .await?;
        let failed = er.api().await_transaction(&failure).await?;
        ensure!(
            transaction_meta(&failed)?.err
                == Some(
                    redsuite_core::rpc_types::response::TransactionError::InstructionError(
                        0,
                        solana_instruction_error::InstructionError::Custom(0x247)
                    )
                    .into()
                )
        );
        ensure!(
            er.local_accounts(&[REDSHIFT.id()], &[cell]).await? == vec![Some(expected.clone())]
        );
        let settled = er
            .submit_and_confirm(
                &payer,
                &[REDSHIFT
                    .batch()
                    .nonce(marker + 2)
                    .invoke(redsuite_core::programs::MAGIC.commit(
                        payer.pubkey(),
                        Some(dlp::magic_fee_vault_pda(&er.identity())),
                        &[cell],
                        SettlementMode::Commit,
                    ))
                    .build()],
            )
            .await?;
        let final_receipt = er.check_commit(&self.base, &settled, &[cell], false).await?;
        final_receipt.check_success("replicated final snapshot")?;
        let base_image =
            self.base.account(&cell).await?.context("settled replication cell missing")?;
        ensure!(
            base_image.data == expected.data
                && base_image.lamports == expected.lamports
                && base_image.owner == dlp::dlp_id()
        );
        let signatures = [scheduling, receipt.signature, failure, settled, final_receipt.signature];
        let mut records = Vec::with_capacity(signatures.len());
        for signature in signatures {
            records.push(serde_json::to_value(
                er.api().await_transaction(&signature).await?,
            )?);
        }
        self.check_final_boundary(&[]).await?;
        let restart = er.restart(RestartConfig::default()).await?;
        ensure!(restart.exit_code == Some(0) && !restart.needed_sigkill);
        self.topology.wait_verifiers_connected(CONNECT_TIMEOUT).await?;
        self.check_final_boundary(&[]).await?;
        ensure!(
            er.local_accounts(&[REDSHIFT.id()], &[cell]).await? == vec![Some(expected.clone())]
        );
        ensure!(self.base.account(&cell).await? == Some(base_image));
        for (signature, record) in signatures.iter().zip(records) {
            ensure!(serde_json::to_value(er.api().await_transaction(signature).await?)? == record);
            ensure!(er.api().get_signature_status(signature).await?.is_some());
        }
        audit_signatures(
            &er,
            start,
            &signatures.iter().map(|signature| (signature, 1)).collect::<Vec<_>>(),
        )
        .await?;
        let next = er
            .submit_and_confirm(
                &payer,
                &[REDSHIFT
                    .batch()
                    .nonce(marker + 3)
                    .invoke(redsuite_core::programs::MAGIC.commit(
                        payer.pubkey(),
                        Some(dlp::magic_fee_vault_pda(&er.identity())),
                        &[cell],
                        SettlementMode::Commit,
                    ))
                    .build()],
            )
            .await?;
        er.check_commit(&self.base, &next, &[cell], false)
            .await?
            .check_success("owner settlement after replicated recovery")?;
        ensure!(self
            .base
            .account(&cell)
            .await?
            .is_some_and(|account| account.data == expected.data));
        Ok(())
    }
    #[instrument(skip_all, err)]
    async fn check_ready(&self) -> Result<()> {
        self.topology.wait_verifiers_connected(CONNECT_TIMEOUT).await?;
        for verifier in self.topology.verifiers() {
            ensure!(verifier.stream_connected().await);
        }
        ensure!(self.topology.leader_connections().await? == (VERIFIERS as f64));
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_steady_load(&self, workload: &Workload) -> Result<()> {
        let max_lag = self.sample_lag(STEADY).await?;
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
        let events = self.topology.leader().api().events().await?;
        let mut slots = events.slots().await?;
        loop {
            let head = leader_metric(&self.topology, SUPERBLOCKS).await?;
            let sealed = slots
                .query("new leader superblock", async || {
                    let current = leader_metric(&self.topology, SUPERBLOCKS).await?;
                    ensure!(current > head);
                    Ok(current)
                })
                .await?;
            await_catch_up(self.topology.verifier(0), SUPERBLOCKS, sealed).await?;
            let slot = self.topology.leader().api().get_slot().await?;
            let headroom = BLOCKTIME * (SUPERBLOCK_SLOTS - slot % SUPERBLOCK_SLOTS) as u32;
            if headroom > RESTART_BUDGET + Duration::from_millis(500) {
                break;
            }
        }
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
        let slot = self.topology.leader().api().get_slot().await? + 1;
        let pruned = async {
            for retained in [true, false] {
                self.topology
                    .leader()
                    .api()
                    .await_query(
                        &format!(
                            "leader block {slot} retained={retained} \
                                 after verifier 1 stopped"
                        ),
                        async || {
                            let block = self.topology.leader().api().get_block(slot).await?;
                            ensure!(block.is_some() == retained);
                            Ok(())
                        },
                    )
                    .await?;
            }
            Result::Ok(())
        };
        let survivor = async {
            let slot_before = self.topology.leader().api().get_slot().await?;
            blocks_advance(self.topology.verifier(0)).await?;
            let slot_after = self.topology.leader().api().get_slot().await?;
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
        debug!(phase = "snapshot connected", "verifier recovery progress");
        let client_snapshots = verifier_count(self.topology.verifier(1), CLIENT_SNAPSHOTS).await?;
        ensure!(client_snapshots >= 1.0);
        let server_snapshots =
            leader_count(&self.topology, SERVER_SNAPSHOTS).await? - server_snapshots_before;
        ensure!(server_snapshots >= 1.0);
        let target = leader_metric(&self.topology, TRANSACTIONS).await?;
        debug!(target, "waiting for recovered verifier transactions");
        await_catch_up(self.topology.verifier(1), TRANSACTIONS, target).await?;
        self.await_leader_connections(VERIFIERS).await?;
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
        let slot_before = self.topology.leader().api().get_slot().await?;
        let lone_blocks = blocks_advance(self.topology.verifier(0)).await?;
        let slot_after = self.topology.leader().api().get_slot().await?;
        ensure!(slot_after > slot_before);
        self.probe_clones().await?;
        let target = leader_metric(&self.topology, TRANSACTIONS).await?;
        await_catch_up(self.topology.verifier(0), TRANSACTIONS, target).await?;
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
        // Background services can append transactions after the workload drains.
        // Replicate this fixed cutoff through a sealed boundary, not a moving head.
        let final_txs = leader_metric(&self.topology, TRANSACTIONS).await?;
        await_leader_advance(
            &self.topology,
            SUPERBLOCKS,
            1.0,
            "the final sealed boundary after the load",
        )
        .await?;
        self.catch_up_fleet(&[SUPERBLOCKS]).await?;
        for verifier in self.topology.verifiers() {
            await_catch_up(verifier, TRANSACTIONS, final_txs).await?;
        }
        self.verify_no_mismatch().await?;
        self.verify_pairs(pairs).await?;
        let mut verifier_superblocks = Vec::with_capacity(VERIFIERS);
        for verifier in self.topology.verifiers() {
            verifier_superblocks.push(verifier_metric(verifier, SUPERBLOCKS).await?);
        }
        debug!(
            final_txs,
            ?verifier_superblocks,
            "final replicated account state verified",
        );
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn catch_up_fleet(&self, names: &[&str]) -> Result<()> {
        for name in names {
            let target = leader_metric(&self.topology, name).await?;
            for verifier in self.topology.verifiers() {
                await_catch_up(verifier, name, target).await?;
            }
        }
        Ok(())
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
        .await
    }

    async fn sample_lag(&self, window: Duration) -> Result<Vec<f64>> {
        let deadline = time::Instant::now() + window;
        let mut max_lag = vec![0.0f64; self.topology.verifiers().len()];
        while time::Instant::now() < deadline {
            let leader = leader_metric(&self.topology, TRANSACTIONS).await?;
            for (index, verifier) in self.topology.verifiers().iter().enumerate() {
                let seen = verifier_metric(verifier, TRANSACTIONS).await?;
                max_lag[index] = max_lag[index].max(leader - seen);
            }
            time::sleep(LAG_SAMPLE).await;
        }
        Ok(max_lag)
    }

    async fn verify_pairs(&self, pairs: &[CausalPair]) -> Result<()> {
        let leader = self.topology.leader();
        for (index, pair) in pairs.iter().enumerate() {
            pair.verify(leader, &format!("pair {index} on the leader")).await?;
        }
        Ok(())
    }

    async fn verify_no_mismatch(&self) -> Result<()> {
        if matches!(self.coverage, Coverage::Settlement) {
            if leader_metric(&self.topology, SUPERBLOCKS).await? < 1.0 {
                await_leader_advance(&self.topology, SUPERBLOCKS, 1.0, "authenticated state seal")
                    .await?;
            }
            self.catch_up_fleet(&[SUPERBLOCKS]).await?;
        }
        for verifier in self.topology.verifiers() {
            ensure!(verifier.is_running());
            ensure!(verifier.stream_connected().await);
            match self.coverage {
                Coverage::Recovery => {
                    ensure!(verifier_count(verifier, STATE_MISMATCHES).await? == 0.0)
                }
                Coverage::Settlement => {
                    if let Some(mismatches) = verifier.scrape_metrics().await?.get(STATE_MISMATCHES)
                    {
                        ensure!(mismatches == 0.0);
                    }
                }
            }
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
        let leader = self.topology.leader();
        for _ in 0..PROBES {
            let probe = self.base.funded_payer(PROBE_LAMPORTS).await?;
            let address = probe.pubkey();
            leader
                .api()
                .await_accounts(&[address], "leader clones the probe", |accounts| {
                    let account = accounts[0].as_ref().context("probe account missing")?;
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
            leader.await_clones(&pdas, ACCOUNT_SPACE as usize).await?;
            let senders = payers[index * PAYERS_PER_PAIR..(index + 1) * PAYERS_PER_PAIR]
                .iter()
                .map(|payer| leader.sender(Rc::new(payer.insecure_clone())))
                .collect();
            pairs.push(CausalPair::new([pdas[0], pdas[1]], senders));
        }
        let payer_keys: Vec<Pubkey> = payers.iter().map(|payer| payer.pubkey()).collect();
        leader.await_cloned_payers(&payer_keys).await?;
        Ok(pairs)
    }
}
