use std::{
    collections::HashSet,
    path::PathBuf,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, prep,
    topology::{self, ReplicatedOptions, ReplicatedTopology, Verifier},
    BaseCtx, ChainCtx, PrivateErScenario, Result, ScenarioReport,
};
use signer::Signer;

const VERIFIERS: usize = 2;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(90);
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(60);
const DETACH_TIMEOUT: Duration = Duration::from_secs(30);
const ADVANCE_WINDOW: Duration = Duration::from_secs(2);
const PROBE_LAMPORTS: u64 = 1_000_000;
const PROBES: usize = 4;
const BLOCKS: &str = "engine_ledger_blocks";
const TRANSACTIONS: &str = "engine_ledger_transactions";

async fn verifier_gauge(verifier: &Verifier, name: &str) -> Result<f64> {
    verifier.scrape_metrics().await?.get(name).ok_or_else(|| {
        format!("verifier `{}` exposes no {name} metric", verifier.label())
            .into()
    })
}

async fn leader_gauge(
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

async fn await_catch_up(
    verifier: &Verifier,
    name: &str,
    target: f64,
) -> Result<Duration> {
    let started = Instant::now();
    check::poll(
        &format!(
            "verifier `{}` {name} reaching the leader's {target:.0}",
            verifier.label()
        ),
        CATCH_UP_TIMEOUT,
        || async {
            verifier_gauge(verifier, name)
                .await
                .is_ok_and(|value| value >= target)
        },
    )
    .await?;
    Ok(started.elapsed())
}

async fn await_leader_connections(
    topology: &ReplicatedTopology,
    expected: usize,
) -> Result<()> {
    check::poll(
        &format!("the leader serving {expected} replication connection(s)"),
        DETACH_TIMEOUT,
        || async {
            topology
                .leader_connections()
                .await
                .is_ok_and(|connections| connections == expected as f64)
        },
    )
    .await?;
    Ok(())
}

async fn blocks_advance(verifier: &Verifier, moment: &str) -> Result<f64> {
    let before = verifier_gauge(verifier, BLOCKS).await?;
    tokio::time::sleep(ADVANCE_WINDOW).await;
    let after = verifier_gauge(verifier, BLOCKS).await?;
    check!(
        after > before,
        "{moment}: verifier `{}` stayed at {before:.0} blocks for \
         {ADVANCE_WINDOW:?} — it stopped following the leader",
        verifier.label()
    )?;
    Ok(after - before)
}

async fn probe_clones(
    base: &BaseCtx,
    topology: &ReplicatedTopology,
) -> Result<()> {
    let leader = topology.leader().ctx();
    for _ in 0..PROBES {
        let probe = prep::funded_payer(base, PROBE_LAMPORTS).await?;
        let address = probe.pubkey();
        check::poll(
            &format!("the leader clones probe {address}"),
            READY_TIMEOUT,
            || async {
                matches!(leader.account(&address).await, Ok(Some(acc)) if acc.lamports > 0)
            },
        )
        .await?;
    }
    Ok(())
}

async fn verify_isolation(topology: &ReplicatedTopology) -> Result<()> {
    let leader = topology.leader();
    let mut identities: HashSet<Pubkey> = HashSet::from([leader.identity()]);
    let mut metrics_ports: HashSet<u16> =
        HashSet::from([leader.metrics_port()]);
    let mut storage: HashSet<PathBuf> =
        HashSet::from([leader.storage_dir().to_path_buf()]);
    let mut logs: HashSet<PathBuf> =
        HashSet::from([leader.log().to_path_buf()]);
    for verifier in topology.verifiers() {
        let label = verifier.label();
        check!(
            identities.insert(verifier.identity()),
            "verifier `{label}` shares its identity with another node"
        )?;
        check!(
            metrics_ports.insert(verifier.metrics_port()),
            "verifier `{label}` shares its metrics port with another node"
        )?;
        check!(
            storage.insert(verifier.storage_dir().to_path_buf()),
            "verifier `{label}` shares its storage directory with another node"
        )?;
        check!(
            logs.insert(verifier.log().to_path_buf()),
            "verifier `{label}` shares its log with another node"
        )?;
        check!(
            verifier.storage_dir().is_dir(),
            "verifier `{label}` has no storage directory at {}",
            verifier.storage_dir().display()
        )?;
        check!(
            verifier.config_path().is_file(),
            "verifier `{label}` has no configuration at {}",
            verifier.config_path().display()
        )?;
        check!(
            verifier.log().is_file(),
            "verifier `{label}` has no log at {}",
            verifier.log().display()
        )?;
        check!(
            leader.allowed_followers().contains(&verifier.identity()),
            "the leader's follower allowlist lacks verifier `{label}`"
        )?;
        check_eq!(
            verifier.upstream_address(),
            format!("127.0.0.1:{}", leader.replication_port()),
            "verifier `{label}` must follow the leader's replication listener"
        )?;
        check!(
            verifier.stream_connected().await,
            "verifier `{label}` reports no live replication stream"
        )?;
    }
    check_eq!(
        topology.leader_connections().await?,
        VERIFIERS as f64,
        "the leader must serve one replication connection per verifier"
    )?;
    Ok(())
}

pub struct VerifierLifecycle;

#[async_trait(?Send)]
impl PrivateErScenario for VerifierLifecycle {
    fn name(&self) -> &str {
        "redshift/verifier_lifecycle"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let boot_started = Instant::now();
        let mut topology = topology::replicated(
            base,
            ReplicatedOptions {
                label: "verifier-lifecycle".to_owned(),
                verifiers: VERIFIERS,
                ..ReplicatedOptions::default()
            },
        )
        .await?;
        topology.leader().wait_ready(READY_TIMEOUT).await?;
        let boot = boot_started.elapsed();
        let connect =
            topology.wait_verifiers_connected(CONNECT_TIMEOUT).await?;
        verify_isolation(&topology).await?;

        probe_clones(base, &topology).await?;
        let leader_txs = leader_gauge(&topology, TRANSACTIONS).await?;
        let leader_blocks = leader_gauge(&topology, BLOCKS).await?;
        let mut initial_catch_up = Duration::ZERO;
        for verifier in topology.verifiers() {
            await_catch_up(verifier, TRANSACTIONS, leader_txs).await?;
            initial_catch_up = initial_catch_up
                .max(await_catch_up(verifier, BLOCKS, leader_blocks).await?);
        }
        eprintln!(
            "[redsuite] {}: leader + {VERIFIERS} verifiers up in {:.1} s, \
             streams connected after {:.1} s, both verifiers caught up with \
             {leader_txs:.0} transactions / {leader_blocks:.0} blocks",
            self.name(),
            boot.as_secs_f64(),
            connect.as_secs_f64(),
        );

        let stopped = topology.verifier_mut(0).stop(false).await?;
        check!(
            !stopped.needed_sigkill,
            "verifier 0 ignored SIGTERM and had to be killed"
        )?;
        check_eq!(
            stopped.exit_code,
            Some(0),
            "verifier 0 must exit cleanly on SIGTERM"
        )?;
        check!(
            !topology.verifier(0).is_running(),
            "verifier 0 still counts as running after its stop"
        )?;
        check!(
            topology.verifier(0).scrape_metrics().await.is_err(),
            "verifier 0's metrics endpoint still answers after the stop"
        )?;
        await_leader_connections(&topology, 1).await?;
        let survivor_blocks =
            blocks_advance(topology.verifier(1), "with verifier 0 stopped")
                .await?;

        let startup = topology.verifier_mut(0).start(READY_TIMEOUT).await?;
        let reconnect =
            topology.verifier(0).wait_connected(CONNECT_TIMEOUT).await?;
        let leader_blocks_at_restart = leader_gauge(&topology, BLOCKS).await?;
        let catch_up = await_catch_up(
            topology.verifier(0),
            BLOCKS,
            leader_blocks_at_restart,
        )
        .await?;
        await_leader_connections(&topology, VERIFIERS).await?;
        eprintln!(
            "[redsuite] {}: verifier 0 stopped in {} ms (exit {:?}), \
             restarted in {} ms, reconnected after {} ms and caught up \
             {leader_blocks_at_restart:.0} blocks in {} ms while verifier 1 \
             advanced {survivor_blocks:.0} blocks alone",
            self.name(),
            stopped.shutdown.as_millis(),
            stopped.exit_code,
            startup.as_millis(),
            reconnect.as_millis(),
            catch_up.as_millis(),
        );

        let killed = topology.verifier_mut(1).stop(true).await?;
        check_eq!(
            killed.exit_signal,
            Some(9),
            "verifier 1 must die by SIGKILL when hard-killed"
        )?;
        await_leader_connections(&topology, 1).await?;
        let slot_before = topology.leader().ctx().api().get_slot().await?;
        let lone_blocks = blocks_advance(
            topology.verifier(0),
            "with verifier 1 kept offline",
        )
        .await?;
        let slot_after = topology.leader().ctx().api().get_slot().await?;
        check!(
            slot_after > slot_before,
            "the leader's slot stood at {slot_before} with a verifier offline"
        )?;
        probe_clones(base, &topology).await?;
        let leader_txs_final = leader_gauge(&topology, TRANSACTIONS).await?;
        let final_catch_up = await_catch_up(
            topology.verifier(0),
            TRANSACTIONS,
            leader_txs_final,
        )
        .await?;
        check!(
            !topology.verifier(1).is_running(),
            "verifier 1 must stay offline while the others are observed"
        )?;
        eprintln!(
            "[redsuite] {}: verifier 1 kept offline (exit signal {:?}); the \
             leader advanced {} slots and verifier 0 followed with \
             {lone_blocks:.0} blocks, catching up to {leader_txs_final:.0} \
             transactions in {} ms",
            self.name(),
            killed.exit_signal,
            slot_after - slot_before,
            final_catch_up.as_millis(),
        );

        let rejoin_started = Instant::now();
        topology.verifier_mut(1).reset_storage()?;
        topology.verifier_mut(1).start(READY_TIMEOUT).await?;
        topology.verifier(1).wait_connected(CONNECT_TIMEOUT).await?;
        let leader_blocks_at_rejoin = leader_gauge(&topology, BLOCKS).await?;
        await_catch_up(topology.verifier(1), BLOCKS, leader_blocks_at_rejoin)
            .await?;
        await_catch_up(topology.verifier(1), TRANSACTIONS, leader_txs_final)
            .await?;
        let rejoin = rejoin_started.elapsed();
        await_leader_connections(&topology, VERIFIERS).await?;
        eprintln!(
            "[redsuite] {}: verifier 1 rejoined from wiped storage in {} ms \
             and caught up {leader_blocks_at_rejoin:.0} blocks",
            self.name(),
            rejoin.as_millis(),
        );

        let leader_identity = topology.leader().identity().to_string();
        let leader_rpc_port = topology.leader().rpc_port();
        let verifier_summary: Vec<String> = topology
            .verifiers()
            .iter()
            .map(|verifier| {
                format!(
                    "{} identity {} metrics 127.0.0.1:{}",
                    verifier.label(),
                    verifier.identity(),
                    verifier.metrics_port()
                )
            })
            .collect();
        topology.finish().await?;

        Ok(ScenarioReport::ok(self.name())
            .setting("verifiers", VERIFIERS)
            .setting("leader identity", leader_identity)
            .setting("leader rpc port", leader_rpc_port)
            .setting("verifier nodes", verifier_summary.join("; "))
            .metric("boot s", Unit::Seconds, boot.as_secs_f64())
            .metric("connect s", Unit::Seconds, connect.as_secs_f64())
            .metric(
                "initial catch up ms",
                Unit::Millis,
                initial_catch_up.as_secs_f64() * 1e3,
            )
            .metric(
                "verifier0 stop ms",
                Unit::Millis,
                stopped.shutdown.as_secs_f64() * 1e3,
            )
            .metric(
                "verifier0 restart ms",
                Unit::Millis,
                startup.as_secs_f64() * 1e3,
            )
            .metric(
                "verifier0 reconnect ms",
                Unit::Millis,
                reconnect.as_secs_f64() * 1e3,
            )
            .metric(
                "verifier0 catch up ms",
                Unit::Millis,
                catch_up.as_secs_f64() * 1e3,
            )
            .metric(
                "verifier1 blocks while alone",
                Unit::Count,
                survivor_blocks,
            )
            .metric("verifier0 blocks while alone", Unit::Count, lone_blocks)
            .metric(
                "verifier1 kill ms",
                Unit::Millis,
                killed.shutdown.as_secs_f64() * 1e3,
            )
            .metric(
                "final catch up ms",
                Unit::Millis,
                final_catch_up.as_secs_f64() * 1e3,
            )
            .metric(
                "verifier1 rejoin ms",
                Unit::Millis,
                rejoin.as_secs_f64() * 1e3,
            )
            .metric("leader txs", Unit::Count, leader_txs_final))
    }
}
