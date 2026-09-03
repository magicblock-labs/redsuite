use std::{
    fs,
    path::{Path, PathBuf},
    process::Child,
    rc::Rc,
    time::{Duration, Instant},
};

use keypair::Keypair;
use pubkey::Pubkey;
use signer::Signer;

use super::{
    config,
    config::{ErOptions, VerifierPlan},
    private::{leader_er, PrivateEr},
    process, state,
};
use crate::{
    api::{self, Metrics},
    context::BaseCtx,
    host::proc_running,
    report,
    resources::{LaunchRecord, ResourceRecord},
    Result,
};

pub const STREAM_CONNECTED: &str = "engine_replicator_client_stream_connected";
pub const SERVER_CONNECTIONS: &str = "engine_replicator_server_connections";

const VERIFIER_READY_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_POLL: Duration = Duration::from_millis(100);

#[derive(Default)]
pub struct ReplicatedOptions {
    pub label: String,
    pub verifiers: usize,
    pub leader_env: Vec<(String, String)>,
    pub verifier_env: Vec<(String, String)>,
    pub request_timeout: Option<Duration>,
}

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
    index: usize,
    label: String,
    pid: u32,
    plan: VerifierPlan,
    log: PathBuf,
    child: Option<Child>,
    record: Rc<ResourceRecord>,
    metrics_url: String,
}

impl Verifier {
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn identity(&self) -> Pubkey {
        self.plan.identity.pubkey()
    }

    pub fn metrics_port(&self) -> u16 {
        self.plan.metrics_port
    }

    pub fn metrics_url(&self) -> &str {
        &self.metrics_url
    }

    pub fn upstream_address(&self) -> String {
        self.plan.upstream_address()
    }

    pub fn storage_dir(&self) -> &Path {
        &self.plan.storage_dir
    }

    pub fn config_path(&self) -> PathBuf {
        self.plan.config_path()
    }

    pub fn log(&self) -> &Path {
        &self.log
    }

    pub fn is_running(&self) -> bool {
        self.child.is_some() && proc_running(self.pid)
    }

    pub async fn scrape_metrics(&self) -> Result<Metrics> {
        api::scrape_metrics(&self.metrics_url).await
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
            &format!("verifier `{}` metrics answering", self.label),
            &self.log,
            self.pid,
            || async { self.scrape_metrics().await.is_ok() },
        )
        .await
    }

    pub async fn wait_connected(&self, timeout: Duration) -> Result<Duration> {
        let started = Instant::now();
        process::wait_until_every(
            CONNECT_POLL,
            timeout,
            &format!(
                "verifier `{}` holding a live replication stream",
                self.label
            ),
            &self.log,
            self.pid,
            || self.stream_connected(),
        )
        .await?;
        Ok(started.elapsed())
    }

    pub async fn stop(&mut self, hard_kill: bool) -> Result<VerifierStop> {
        let child = self.child.as_mut().ok_or_else(|| {
            format!("verifier `{}` has no running process to stop", self.label)
        })?;
        let started = Instant::now();
        let (exit_status, needed_sigkill) =
            process::terminate(child, self.pid, hard_kill).await?;
        self.child = None;
        self.record.mark_finished();
        Ok(VerifierStop {
            shutdown: started.elapsed(),
            needed_sigkill,
            exit_code: exit_status.code(),
            exit_signal: std::os::unix::process::ExitStatusExt::signal(
                &exit_status,
            ),
        })
    }

    // A verifier killed before it archived a snapshot cannot reopen its
    // storage; wiping it lets the next start rejoin from the leader's
    // snapshot under the same identity, ports and configuration.
    pub fn reset_storage(&mut self) -> Result<()> {
        if self.child.is_some() {
            return Err(format!(
                "verifier `{}` must be stopped before its storage is reset",
                self.label
            )
            .into());
        }
        if let Err(error) = fs::remove_dir_all(&self.plan.storage_dir) {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error.into());
            }
        }
        self.plan.write_config()?;
        Ok(())
    }

    pub async fn start(&mut self, ready_timeout: Duration) -> Result<Duration> {
        if self.child.is_some() {
            return Err(format!(
                "verifier `{}` is already running (pid {})",
                self.label, self.pid
            )
            .into());
        }
        let started = Instant::now();
        let child = process::spawn_child(self.plan.command(), &self.log)?;
        self.pid = child.id();
        self.child = Some(child);
        self.record.relaunched(self.pid);
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
        if self.child.is_none() {
            return Ok(());
        }
        let outcome = self.stop(false).await.map(|_| ());
        if let Err(error) = &outcome {
            self.record.record_finish_error(error.to_string());
        }
        outcome
    }
}

impl Drop for Verifier {
    fn drop(&mut self) {
        if self.child.is_none() && !proc_running(self.pid) {
            return;
        }
        eprintln!(
            "[redsuite] stopping verifier `{}` (pid {})",
            self.label, self.pid
        );
        process::kill_pid(self.pid);
        if let Some(mut child) = self.child.take() {
            let _ = child.wait();
            self.record.mark_finished();
        }
    }
}

pub struct ReplicatedTopology {
    verifiers: Vec<Verifier>,
    leader: PrivateEr,
}

impl ReplicatedTopology {
    pub fn leader(&self) -> &PrivateEr {
        &self.leader
    }

    pub fn leader_mut(&mut self) -> &mut PrivateEr {
        &mut self.leader
    }

    pub fn verifiers(&self) -> &[Verifier] {
        &self.verifiers
    }

    pub fn verifiers_mut(&mut self) -> &mut [Verifier] {
        &mut self.verifiers
    }

    pub fn verifier(&self, index: usize) -> &Verifier {
        &self.verifiers[index]
    }

    pub fn verifier_mut(&mut self, index: usize) -> &mut Verifier {
        &mut self.verifiers[index]
    }

    pub fn running_verifiers(&self) -> usize {
        self.verifiers
            .iter()
            .filter(|verifier| verifier.is_running())
            .count()
    }

    pub async fn leader_connections(&self) -> Result<f64> {
        let metrics = self.leader.ctx().scrape_metrics().await?;
        metrics.get(SERVER_CONNECTIONS).ok_or_else(|| {
            format!("the leader exposes no {SERVER_CONNECTIONS} metric").into()
        })
    }

    pub async fn wait_verifiers_connected(
        &self,
        timeout: Duration,
    ) -> Result<Duration> {
        let started = Instant::now();
        let deadline = tokio::time::Instant::now() + timeout;
        for verifier in self.verifiers.iter().filter(|v| v.is_running()) {
            let remaining =
                deadline.saturating_duration_since(tokio::time::Instant::now());
            verifier.wait_connected(remaining).await?;
        }
        let expected = self.running_verifiers() as f64;
        loop {
            if self.leader_connections().await? >= expected {
                return Ok(started.elapsed());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "the leader did not report {expected:.0} replication \
                     connections within {timeout:?}"
                )
                .into());
            }
            tokio::time::sleep(CONNECT_POLL).await;
        }
    }

    pub async fn finish(self) -> Result<()> {
        let Self {
            mut verifiers,
            leader,
        } = self;
        let mut first_error = None;
        for verifier in verifiers.iter_mut() {
            if let Err(error) = verifier.finish().await {
                first_error.get_or_insert(error);
            }
        }
        drop(verifiers);
        if let Err(error) = leader.finish().await {
            first_error.get_or_insert(error);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn abort_verifier(mut child: Child, pid: u32, record: &Rc<ResourceRecord>) {
    process::kill_pid(pid);
    let _ = child.wait();
    record.mark_finished();
}

pub async fn replicated(
    base: &BaseCtx,
    options: ReplicatedOptions,
) -> Result<ReplicatedTopology> {
    if options.verifiers == 0 {
        return Err("a replicated topology needs at least one verifier".into());
    }
    let verifier_bin = config::find_verifier_bin()?;
    let identities: Vec<Keypair> =
        (0..options.verifiers).map(|_| Keypair::new()).collect();
    let leader = leader_er(
        base,
        ErOptions {
            label: options.label.clone(),
            env: options.leader_env.clone(),
            request_timeout: options.request_timeout,
        },
        identities
            .iter()
            .map(|identity| identity.pubkey())
            .collect(),
    )
    .await?;

    let dir = state::stack_dir();
    let mut verifier_env = config::mirrored_verifier_env(&options.leader_env);
    verifier_env.extend(options.verifier_env.iter().cloned());
    let (bin_version, bin_fingerprint) =
        report::binary_provenance(&verifier_bin);

    let mut verifiers = Vec::with_capacity(options.verifiers);
    for (index, identity) in identities.into_iter().enumerate() {
        let label = format!("{}-verifier{index}", options.label);
        let storage_dir = dir.join(format!("er-{label}"));
        if let Err(error) = fs::remove_dir_all(&storage_dir) {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error.into());
            }
        }
        let mut ports = process::PortLease::default();
        let metrics_port = ports.single()?;
        let plan = VerifierPlan {
            bin: verifier_bin.clone(),
            identity,
            upstream_port: leader.replication_port(),
            upstream_authority: leader.identity(),
            metrics_port,
            storage_dir,
            env: verifier_env.clone(),
        };
        let config_path = plan.write_config()?;
        let log = dir.join(format!("er-{label}.log"));
        eprintln!(
            "[redsuite] booting verifier `{label}` (metrics 127.0.0.1:\
             {metrics_port}) following {} …",
            plan.upstream_address()
        );
        ports.release();
        let child = process::spawn_child(plan.command(), &log)?;
        let pid = child.id();
        let record = base.resources().register_launch(LaunchRecord {
            label: label.clone(),
            role: "verifier".to_owned(),
            bin: plan.bin.display().to_string(),
            bin_version: bin_version.clone(),
            bin_fingerprint: bin_fingerprint.clone(),
            identity: plan.identity.pubkey().to_string(),
            launched_at: report::utc_stamp(),
            rpc_port: None,
            metrics_port,
            replication_port: None,
            upstream: Some(plan.upstream_address()),
            storage_dir: plan.storage_dir.display().to_string(),
            log: log.display().to_string(),
            config: Some(config_path.display().to_string()),
            pid,
            relaunches: 0,
        });
        let metrics_url = format!("http://127.0.0.1:{metrics_port}");
        let verifier = Verifier {
            index,
            label,
            pid,
            plan,
            log,
            child: Some(child),
            record,
            metrics_url,
        };
        if let Err(error) = verifier.wait_ready(VERIFIER_READY_TIMEOUT).await {
            let mut verifier = verifier;
            if let Some(child) = verifier.child.take() {
                abort_verifier(child, verifier.pid, &verifier.record);
            }
            return Err(error);
        }
        verifiers.push(verifier);
    }

    Ok(ReplicatedTopology { verifiers, leader })
}
