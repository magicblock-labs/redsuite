use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure, Context};
use futures_util::future::try_join_all;
use redsuite_core::{
    logging::strip_ansi,
    topology::{ReplicatedTopology, Verifier},
    wait, Result,
};

/// Checkpoint cadence for scenarios that verify live replay without sealing.
pub const CHECKPOINT_SLOTS: u64 = 16;
const ADVANCE_TIMEOUT: Duration = Duration::from_secs(60);
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(120);

pub const TRANSACTIONS: &str = "engine_ledger_transactions";
pub(super) const BLOCKS: &str = "engine_ledger_blocks";
pub(super) const SUPERBLOCKS: &str = "engine_ledger_superblocks";
pub(super) const STATE_MISMATCHES: &str = "engine_replicator_client_state_mismatches";
const CLIENT_SNAPSHOTS: &str =
    r#"engine_replicator_operation_duration_micros_count{op="client_stage_snapshot"}"#;

pub async fn leader_metric(topology: &ReplicatedTopology, name: &str) -> Result<f64> {
    let metrics = topology.leader().scrape_metrics().await?;
    metrics
        .get(name)
        .with_context(|| format!("the leader exposes no {name} metric"))
}

pub(super) async fn verifier_metric(verifier: &Verifier, name: &str) -> Result<f64> {
    verifier
        .scrape_metrics()
        .await?
        .get(name)
        .with_context(|| format!("verifier `{}` exposes no {name} metric", verifier.label()))
}

pub(super) async fn leader_count(topology: &ReplicatedTopology, name: &str) -> Result<f64> {
    let metrics = topology.leader().scrape_metrics().await?;
    Ok(metrics.get(name).unwrap_or(0.0))
}

pub(super) async fn verifier_count(verifier: &Verifier, name: &str) -> Result<f64> {
    Ok(verifier.scrape_metrics().await?.get(name).unwrap_or(0.0))
}

/// Waits for a fixed leader cutoff, surfacing mismatch log evidence if the verifier exits first.
pub async fn await_catch_up(verifier: &Verifier, name: &str, target: f64) -> Result<Duration> {
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
            let observed = verifier_metric(verifier, name).await?;
            ensure!(
                observed >= target,
                "observed {observed:.0}, need at least {target:.0}"
            );
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

pub(super) async fn await_leader_advance(
    topology: &ReplicatedTopology,
    name: &str,
    by: f64,
    what: &str,
) -> Result<()> {
    let from = leader_metric(topology, name).await?;
    let target = from + by;
    wait::retry(
        &format!("{what} (leader {name} reaching {target:.0})"),
        ADVANCE_TIMEOUT,
        async || {
            let observed = leader_metric(topology, name).await?;
            ensure!(
                observed >= target,
                "observed {observed:.0}, need at least {target:.0}"
            );
            Ok(())
        },
    )
    .await
}

pub(super) async fn snapshots(verifier: &Verifier) -> Result<f64> {
    verifier_count(verifier, CLIENT_SNAPSHOTS).await
}

pub(super) async fn fence(topology: &ReplicatedTopology) -> Result<()> {
    await_leader_advance(topology, BLOCKS, 1.0, "runtime writes entering the ledger").await?;
    let target = leader_metric(topology, TRANSACTIONS).await?;
    try_join_all(topology.verifiers().iter().map(|verifier| async move {
        await_catch_up(verifier, TRANSACTIONS, target).await?;
        let before = verifier_metric(verifier, BLOCKS).await?;
        await_catch_up(verifier, BLOCKS, before + (CHECKPOINT_SLOTS + 1) as f64).await?;
        ensure!(
            verifier.stream_connected().await,
            "{} disconnected",
            verifier.label()
        );
        let mismatches = verifier_count(verifier, STATE_MISMATCHES).await?;
        ensure!(
            mismatches == 0.0,
            "verifier {} reported {mismatches:.0} state mismatches",
            verifier.label()
        );
        Ok::<_, anyhow::Error>(())
    }))
    .await?;
    Ok(())
}

/// Preserves the first mismatch diagnostic, including when verifier shutdown prevents metric reads.
pub fn log_mentions_mismatch(log: &Path) -> Result<Option<String>> {
    for line in BufReader::new(File::open(log)?).lines() {
        let line = line?;
        if line.to_ascii_lowercase().contains("mismatch") {
            return Ok(Some(strip_ansi(&line).into_owned()));
        }
    }
    Ok(None)
}
