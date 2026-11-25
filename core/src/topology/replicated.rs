use std::{
    fs,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use futures_util::future::try_join_all;
use signer::Signer;
use tokio::time;

use super::{
    config,
    config::VerifierPlan,
    identity, process,
    process::PortLease,
    state::{self, Storage},
    RestartConfig,
};
use crate::{
    api::{Metrics, MetricsCollector},
    context::{BaseCtx, ErCtx},
    report,
    resources::LaunchRecord,
    Result,
};

pub const STREAM_CONNECTED: &str = "engine_replicator_client_stream_connected";
pub const SERVER_CONNECTIONS: &str = "engine_replicator_server_connections";

const VERIFIER_READY_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_POLL: Duration = Duration::from_millis(100);
const SNAPSHOT_ARCHIVE: &str = "accountsdb.tar.zst";

#[derive(Debug, Clone)]
pub struct VerifierStop {
    pub shutdown: Duration,
    pub needed_sigkill: bool,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct VerifierTiming {
    pub stop: VerifierStop,
    pub startup: Duration,
    pub connect: Duration,
    pub total: Duration,
}

pub struct Verifier {
    proc: process::Owned,
    plan: VerifierPlan,
    log: PathBuf,
    metrics: MetricsCollector,
    ports: PortLease,
}

impl Verifier {
    pub fn label(&self) -> &str {
        self.proc.label()
    }

    pub fn storage_dir(&self) -> &Path {
        self.plan.storage_dir()
    }

    pub fn accountsdb_dir(&self) -> &Path {
        &self.plan.params.engine.accountsdb.directory
    }

    pub fn log(&self) -> &Path {
        &self.log
    }

    pub fn is_running(&self) -> bool {
        self.proc.is_running()
    }

    pub fn exit_status(&mut self) -> Result<Option<std::process::ExitStatus>> {
        self.proc.exit_status()
    }

    pub async fn scrape_metrics(&self) -> Result<Metrics> {
        self.metrics.scrape().await
    }

    pub async fn stream_connected(&self) -> bool {
        self.scrape_metrics()
            .await
            .ok()
            .and_then(|metrics| metrics.get(STREAM_CONNECTED))
            .is_some_and(|connected| connected >= 1.0)
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        process::wait_until(
            timeout,
            &format!("verifier `{}` metrics answering", self.proc.label()),
            &self.log,
            self.proc.pid(),
            || async { self.scrape_metrics().await.is_ok() },
        )
        .await
        .map_err(|error| self.ports.failure(error))
    }

    pub async fn wait_connected(&self, timeout: Duration) -> Result<Duration> {
        let started = Instant::now();
        process::wait_until_every(
            CONNECT_POLL,
            timeout,
            &format!(
                "verifier `{}` holding a live replication stream",
                self.proc.label()
            ),
            &self.log,
            self.proc.pid(),
            || self.stream_connected(),
        )
        .await?;
        Ok(started.elapsed())
    }

    pub async fn stop(&mut self, hard_kill: bool) -> Result<VerifierStop> {
        let started = Instant::now();
        let (exit_status, needed_sigkill) =
            self.proc.terminate(hard_kill).await?.with_context(|| {
                format!(
                    "verifier `{}` has no running process to stop",
                    self.proc.label()
                )
            })?;
        Ok(VerifierStop {
            shutdown: started.elapsed(),
            needed_sigkill,
            exit_code: exit_status.code(),
            exit_signal: ExitStatusExt::signal(&exit_status),
        })
    }

    /// Requires a clean exit without escalating to SIGKILL.
    pub async fn stop_gracefully(&mut self) -> Result<()> {
        let stop = self.stop(false).await?;
        ensure!(
            stop.exit_code == Some(0) && !stop.needed_sigkill,
            "verifier `{}` did not stop gracefully: {stop:?}",
            self.label()
        );
        Ok(())
    }

    // A verifier killed before it archived a snapshot cannot reopen its
    // storage; wiping it lets the next start rejoin from the leader's
    // snapshot under the same identity, ports and configuration.
    pub fn reset_storage(&mut self) -> Result<()> {
        self.proc
            .clear_storage()
            .with_context(|| format!("removing the storage of verifier `{}`", self.proc.label()))
    }

    pub async fn start(&mut self, ready_timeout: Duration) -> Result<Duration> {
        let started = Instant::now();
        self.proc.relaunch(&self.plan, &self.log)?;
        self.wait_ready(ready_timeout).await?;
        Ok(started.elapsed())
    }

    pub async fn restart(
        &mut self,
        hard_kill: bool,
        ready_timeout: Duration,
        connect_timeout: Duration,
    ) -> Result<VerifierTiming> {
        let started = Instant::now();
        let stop = self.stop(hard_kill).await?;
        let startup = self.start(ready_timeout).await?;
        let connect = self.wait_connected(connect_timeout).await?;
        Ok(VerifierTiming {
            stop,
            startup,
            connect,
            total: started.elapsed(),
        })
    }

    async fn finish(&mut self) -> Result<()> {
        self.proc.terminate(false).await?;
        self.proc.discard_storage()
    }
}

pub struct ReplicatedTopology {
    verifiers: Vec<Verifier>,
    leader: ErCtx,
}

impl ReplicatedTopology {
    pub fn leader(&self) -> &ErCtx {
        &self.leader
    }

    pub fn verifiers(&self) -> &[Verifier] {
        &self.verifiers
    }

    pub fn verifier(&self, index: usize) -> &Verifier {
        &self.verifiers[index]
    }

    pub fn verifier_mut(&mut self, index: usize) -> &mut Verifier {
        &mut self.verifiers[index]
    }

    pub fn running_verifiers(&self) -> usize {
        self.verifiers.iter().filter(|verifier| verifier.is_running()).count()
    }

    pub async fn stop_leader(&self) -> Result<()> {
        self.leader
            .owner()
            .context("replication requires a group-owned leader")?
            .lock()
            .await
            .stop(false)
            .await
    }

    pub async fn promote(&mut self, index: usize) -> Result<()> {
        let owner = self.leader.owner().context("replication requires a group-owned leader")?;
        let mut leader = owner.lock().await;
        leader.adopt_storage(&mut self.verifiers[index].proc)?;
        self.verifiers.remove(index);
        leader.start(RestartConfig::default()).await
    }

    pub async fn leader_connections(&self) -> Result<f64> {
        let metrics = self.leader.scrape_metrics().await?;
        metrics
            .get(SERVER_CONNECTIONS)
            .with_context(|| format!("the leader exposes no {SERVER_CONNECTIONS} metric"))
    }

    pub async fn wait_verifiers_connected(&self, timeout: Duration) -> Result<Duration> {
        let started = Instant::now();
        let deadline = time::Instant::now() + timeout;
        for verifier in self.verifiers.iter().filter(|v| v.is_running()) {
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            verifier.wait_connected(remaining).await?;
        }
        let expected = self.running_verifiers() as f64;
        loop {
            if self.leader_connections().await? >= expected {
                return Ok(started.elapsed());
            }
            ensure!(
                time::Instant::now() < deadline,
                "the leader did not report {expected:.0} replication \
                     connections within {timeout:?}"
            );
            time::sleep(CONNECT_POLL).await;
        }
    }

    pub async fn finish(self) -> Result<()> {
        let Self { mut verifiers, .. } = self;
        let mut first_error = None;
        for verifier in verifiers.iter_mut() {
            if let Err(error) = verifier.finish().await {
                first_error.get_or_insert(error);
            }
        }
        drop(verifiers);
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

pub(super) fn follower_label(leader: &str, index: usize) -> String {
    format!("{leader}-verifier{index}")
}

/// Starts the followers admitted by this leader. Callers with imported initial
/// state must first wait for a leader snapshot that includes that state.
pub async fn replicated(base: &BaseCtx, leader: ErCtx) -> Result<ReplicatedTopology> {
    let owner = leader.owner().context("replication requires a group-owned leader")?;
    let (label, leader_params) = {
        let owner = owner.lock().await;
        (owner.label().to_owned(), owner.params().clone())
    };
    let allowed = &leader_params.engine.replication.allowed_followers;
    ensure!(!allowed.is_empty(), "{label} admits no followers");
    let verifier_bin = config::find_verifier_bin()?;

    let dir = state::stack_dir();
    let (bin_version, bin_fingerprint) = report::binary_provenance(&verifier_bin);

    let mut verifiers = Vec::with_capacity(allowed.len());
    for (index, expected) in allowed.iter().enumerate() {
        let label = follower_label(&label, index);
        let identity = identity::identity_for_label(&label)?;
        ensure!(
            identity.pubkey() == expected.0,
            "unexpected admitted identity for {label}"
        );
        let storage = Storage::new(dir.join(format!("er-{label}")));
        storage.clear()?;
        let mut ports = PortLease::default();
        let metrics_port = ports.single()?;
        let plan = VerifierPlan {
            bin: verifier_bin.clone(),
            config_path: config::config_path(&dir, &label),
            params: config::verifier_params(&leader_params, identity, metrics_port, &storage),
        };
        let log = dir.join(format!("er-{label}.log"));
        ports.release();
        let child = plan.spawn(&log)?;
        let launch = LaunchRecord {
            label,
            role: "verifier".to_owned(),
            bin: plan.bin.display().to_string(),
            bin_version: bin_version.clone(),
            bin_fingerprint: bin_fingerprint.clone(),
            identity: plan.identity().to_string(),
            metrics_port,
            upstream: Some(plan.upstream_address().to_string()),
            log: log.display().to_string(),
            config: Some(plan.config_path.display().to_string()),
            ..LaunchRecord::default()
        };
        let proc = process::Owned::new("verifier", child, storage, base.launches(), launch);
        let metrics_url = format!("http://127.0.0.1:{metrics_port}").parse()?;
        verifiers.push(Verifier {
            proc,
            plan,
            log,
            metrics: MetricsCollector::new(metrics_url),
            ports,
        });
    }

    try_join_all(verifiers.iter().map(|verifier| verifier.wait_ready(VERIFIER_READY_TIMEOUT)))
        .await?;
    Ok(ReplicatedTopology { verifiers, leader })
}

/// Returns the highest completed snapshot's superblock ID and archive path.
pub fn latest_snapshot(storage: &Path) -> Result<Option<(u64, PathBuf)>> {
    let mut latest = None;
    for entry in fs::read_dir(storage)? {
        let path = entry?.path();
        let Some(id) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("superblock-"))
            .and_then(|id| id.parse::<u64>().ok())
        else {
            continue;
        };
        let archive = path.join(SNAPSHOT_ARCHIVE);
        if archive.is_file() {
            latest = latest.max(Some((id, archive)));
        }
    }
    Ok(latest)
}
