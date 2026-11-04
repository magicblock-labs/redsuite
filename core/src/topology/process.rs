use std::{
    env, fs,
    fs::{File, OpenOptions, TryLockError},
    future::Future,
    net::{TcpListener, UdpSocket},
    ops::Range,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    rc::Rc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure, Context, Error};
use rand::Rng;
use tokio::{net::TcpStream, time};
use tracing::warn;
use url::Url;

use super::config;
use crate::{api::Api, host::proc_running, resources::ResourceRecord, Result};

pub(super) const KILL_GRACE: Duration = Duration::from_secs(5);
pub(super) const POLL: Duration = Duration::from_millis(250);
// Fine cadence for restart timing, where the interval is the measurement floor.
pub(super) const RESTART_POLL: Duration = Duration::from_millis(5);
const LIVENESS_INTERVAL: Duration = Duration::from_millis(250);

// Fresh process group so test-runner group signals don't reap the validator;
// the prior boot's log is rotated to .log.prev.
pub(super) fn spawn_child(mut cmd: Command, log: &Path) -> Result<Child> {
    use std::os::unix::process::CommandExt;
    if log.exists() {
        let _ = fs::rename(log, log.with_extension("log.prev"));
    }
    let logfile = File::create(log)?;
    cmd.stdout(logfile.try_clone()?)
        .stderr(logfile)
        .stdin(Stdio::null())
        .process_group(0);
    cmd.spawn()
        .with_context(|| format!("failed to spawn {}", cmd.get_program().to_string_lossy()))
}

pub(super) fn spawn_detached(cmd: Command, log: &Path) -> Result<u32> {
    Ok(spawn_child(cmd, log)?.id())
}

fn send_signal(pid: u32, signal: &str) {
    let _ = Command::new("kill").args([signal, &pid.to_string()]).status();
}

// The one termination primitive: signal, grace window, SIGKILL escalation,
// reaped exit status. hard_kill=true sends SIGKILL immediately (the crash
// path the ledger-restore scenarios use so nothing flushes on the way down).
pub(super) async fn terminate(
    child: &mut Child,
    pid: u32,
    hard_kill: bool,
) -> Result<(ExitStatus, bool)> {
    send_signal(pid, if hard_kill { "-KILL" } else { "-TERM" });
    let grace_deadline = Instant::now() + KILL_GRACE;
    let hard_deadline = if hard_kill { grace_deadline } else { grace_deadline + KILL_GRACE };
    let mut escalated = hard_kill;
    let mut needed_sigkill = false;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok((status, needed_sigkill));
        }
        if !escalated && Instant::now() >= grace_deadline {
            warn!(pid, "graceful shutdown timed out; sending SIGKILL");
            send_signal(pid, "-KILL");
            escalated = true;
            needed_sigkill = true;
        }
        ensure!(
            Instant::now() < hard_deadline,
            "process {pid} did not exit within {KILL_GRACE:?} of SIGKILL"
        );
        time::sleep(RESTART_POLL).await;
    }
}

pub(super) fn describe_exit(status: &ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt;
    match (status.code(), status.signal()) {
        (Some(code), _) => format!("exit code {code}"),
        (None, Some(signal)) => format!("signal {signal}"),
        (None, None) => "unknown status".to_owned(),
    }
}

pub(crate) fn kill_pid(pid: u32) {
    if pid == 0 || !proc_running(pid) {
        return;
    }
    let _ = Command::new("kill").arg(pid.to_string()).status();
    let deadline = Instant::now() + KILL_GRACE;
    while Instant::now() < deadline && proc_running(pid) {
        thread::sleep(Duration::from_millis(100));
    }
    if proc_running(pid) {
        warn!(pid, "process still alive after SIGTERM; sending SIGKILL");
        let _ = Command::new("kill").args(["-KILL", &pid.to_string()]).status();
    }
}

// TERM every still-matching process, give the group one grace window, then
// KILL the stragglers.
pub(super) fn kill_matching(procs: &[(u32, &str)]) {
    for (pid, bin) in procs {
        if *pid != 0 && proc_matches(*pid, bin) {
            send_signal(*pid, "-TERM");
        }
    }
    let deadline = Instant::now() + KILL_GRACE;
    while Instant::now() < deadline
        && procs.iter().any(|(pid, bin)| *pid != 0 && proc_matches(*pid, bin))
    {
        thread::sleep(Duration::from_millis(100));
    }
    for (pid, bin) in procs {
        if *pid != 0 && proc_matches(*pid, bin) {
            send_signal(*pid, "-KILL");
        }
    }
}

// Every process the topology layer spawns names the stack directory on its
// command line. Only the topology binaries qualify, so a shell or editor
// that merely mentions the directory is never touched.
pub(super) const TOPOLOGY_BINS: [&str; 3] =
    ["solana-test-validator", "magicblock-validator", "magicblock-verifier"];

#[cfg(target_os = "linux")]
pub(super) fn orphaned_topology_processes(stack_dir: &Path, exclude: &[u32]) -> Vec<(u32, String)> {
    let marker = format!("{}/", stack_dir.display());
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for entry in entries.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        if pid == std::process::id() || exclude.contains(&pid) {
            continue;
        }
        let Ok(raw) = fs::read(format!("/proc/{pid}/cmdline")) else {
            continue;
        };
        let argv: Vec<String> = raw
            .split(|byte| *byte == 0)
            .filter(|arg| !arg.is_empty())
            .map(|arg| String::from_utf8_lossy(arg).into_owned())
            .collect();
        let runs_topology_bin = argv.iter().any(|arg| {
            Path::new(arg)
                .file_name()
                .is_some_and(|name| TOPOLOGY_BINS.contains(&name.to_string_lossy().as_ref()))
        });
        if !runs_topology_bin {
            continue;
        }
        let cmdline = argv.join(" ");
        if cmdline.contains(&marker) && proc_running(pid) {
            found.push((pid, cmdline));
        }
    }
    found.sort_unstable();
    found
}

#[cfg(target_os = "macos")]
pub(super) fn orphaned_topology_processes(stack_dir: &Path, exclude: &[u32]) -> Vec<(u32, String)> {
    use libproc::{
        proc_pid::pidpath,
        processes::{pids_by_type, ProcFilter},
    };
    let marker = format!("{}/", stack_dir.display());
    let Ok(pids) = pids_by_type(ProcFilter::All) else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for pid in pids {
        if pid == 0 || pid == std::process::id() || exclude.contains(&pid) {
            continue;
        }
        let Ok(exe) = pidpath(pid as i32) else {
            continue;
        };
        if !runs_topology_bin(&exe) {
            continue;
        }
        let Some(cmdline) = cmdline(pid) else {
            continue;
        };
        if cmdline.contains(&marker) && proc_running(pid) {
            found.push((pid, cmdline));
        }
    }
    found.sort_unstable();
    found
}

#[cfg(target_os = "macos")]
fn runs_topology_bin(cmdline: &str) -> bool {
    cmdline.split_whitespace().any(|arg| {
        Path::new(arg)
            .file_name()
            .is_some_and(|name| TOPOLOGY_BINS.contains(&name.to_string_lossy().as_ref()))
    })
}

#[cfg(target_os = "linux")]
pub(super) fn proc_matches(pid: u32, bin: &str) -> bool {
    cmdline(pid).is_some_and(|cmdline| cmdline.contains(bin))
}

#[cfg(target_os = "macos")]
pub(super) fn proc_matches(pid: u32, bin: &str) -> bool {
    if pid == 0 {
        return false;
    }
    libproc::proc_pid::pidpath(pid as i32).is_ok_and(|exe| exe.contains(bin))
        || cmdline(pid).is_some_and(|cmdline| cmdline.contains(bin))
}

#[cfg(target_os = "linux")]
fn cmdline(pid: u32) -> Option<String> {
    fs::read(format!("/proc/{pid}/cmdline"))
        .ok()
        .map(|raw| String::from_utf8_lossy(&raw).into_owned())
}

#[cfg(target_os = "macos")]
fn cmdline(pid: u32) -> Option<String> {
    if pid == 0 {
        return None;
    }
    let output = Command::new("ps")
        .args(["-ww", "-o", "command=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!text.is_empty()).then_some(text)
}

pub(super) fn rpc_listening(port: u16) -> bool {
    use std::net::{SocketAddr, TcpStream};
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()
}

// Both bases are usable at the same point: RPC answers, everything their
// consumers dial is accepting, and the chain has ticked past genesis.
pub(super) async fn await_base_serving(
    rpc_url: &Url,
    ws_url: &Url,
    log: &Path,
    pid: u32,
    listeners: &[(&str, u16)],
) -> Result<()> {
    let api = Api::new(rpc_url.clone(), ws_url.clone());
    wait_until(
        config::BASE_READY_TIMEOUT,
        "base RPC healthy",
        log,
        pid,
        || async { matches!(api.get_health().await.as_deref(), Ok("ok")) },
    )
    .await?;
    for (what, port) in listeners {
        wait_until(Duration::from_secs(20), what, log, pid, || async {
            TcpStream::connect(("127.0.0.1", *port)).await.is_ok()
        })
        .await?;
    }
    // getHealth answers "ok" mid-genesis; dlp is only invocable once slots tick
    wait_until(
        Duration::from_secs(30),
        "base past genesis (confirmed slot >= 2)",
        log,
        pid,
        || async { matches!(api.get_slot().await, Ok(slot) if slot >= 2) },
    )
    .await
}

pub(super) async fn wait_until<F, Fut>(
    timeout: Duration,
    what: &str,
    log: &Path,
    pid: u32,
    condition: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    wait_until_every(POLL, timeout, what, log, pid, condition).await
}

pub(super) async fn wait_until_every<F, Fut>(
    interval: Duration,
    timeout: Duration,
    what: &str,
    log: &Path,
    pid: u32,
    mut condition: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = time::Instant::now() + timeout;
    let mut liveness_checked = time::Instant::now();
    loop {
        if condition().await {
            return Ok(());
        }
        let now = time::Instant::now();
        if interval >= LIVENESS_INTERVAL
            || now.duration_since(liveness_checked) >= LIVENESS_INTERVAL
        {
            liveness_checked = now;
            ensure!(
                proc_running(pid),
                "process exited while waiting for {what}; log tail:\n{}",
                tail(log, 30)
            );
        }
        ensure!(
            time::Instant::now() < deadline,
            "timed out after {timeout:?} waiting for {what}; log tail:\n{}",
            tail(log, 30)
        );
        time::sleep(interval).await;
    }
}

fn tail(path: &Path, lines: usize) -> String {
    let Ok(content) = fs::read_to_string(path) else {
        return format!("<no log at {}>", path.display());
    };
    let all: Vec<&str> = content.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

pub(super) struct Owned {
    kind: &'static str,
    label: String,
    pid: u32,
    child: Option<Child>,
    record: Rc<ResourceRecord>,
}

impl Owned {
    pub(super) fn new(
        kind: &'static str,
        label: String,
        child: Child,
        record: Rc<ResourceRecord>,
    ) -> Self {
        Self {
            kind,
            pid: child.id(),
            label,
            child: Some(child),
            record,
        }
    }

    pub(super) fn pid(&self) -> u32 {
        self.pid
    }

    pub(super) fn label(&self) -> &str {
        &self.label
    }

    pub(super) fn has_child(&self) -> bool {
        self.child.is_some()
    }

    pub(super) fn is_running(&self) -> bool {
        self.has_child() && proc_running(self.pid)
    }

    pub(super) async fn terminate(
        &mut self,
        hard_kill: bool,
    ) -> Result<Option<(ExitStatus, bool)>> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        let outcome = terminate(child, self.pid, hard_kill).await?;
        self.child = None;
        self.record.mark_finished();
        Ok(Some(outcome))
    }

    pub(super) fn relaunch(&mut self, child: Child) {
        self.pid = child.id();
        self.child = Some(child);
        self.record.relaunched(self.pid);
    }
}

impl Drop for Owned {
    fn drop(&mut self) {
        let (kind, label, pid) = (self.kind, &self.label, self.pid);
        let Some(mut child) = self.child.take() else {
            return;
        };
        match child.try_wait().ok().flatten() {
            Some(status) => {
                let exit = describe_exit(&status);
                warn!(kind, label, %exit, "process exited before cleanup");
                self.record.record_exit(format!("died before cleanup: {exit}"));
            }
            None => {
                kill_pid(pid);
                let exit = child.wait().ok().map(|status| describe_exit(&status));
                self.record.record_exit(match exit {
                    Some(exit) => format!("terminated by cleanup: {exit}"),
                    None => "terminated by cleanup, status unknown".to_owned(),
                });
            }
        }
        self.record.mark_finished();
    }
}

#[derive(Default)]
pub(super) struct PortLease {
    holders: Vec<(TcpListener, UdpSocket)>,
    claims: Vec<(u16, File)>,
}

const PORT_BAND: Range<u16> = 20_000..30_000;
const BAND_ATTEMPTS: usize = 512;

fn registry_dir() -> PathBuf {
    if let Some(dir) = env::var_os("REDSUITE_PORT_REGISTRY") {
        return PathBuf::from(dir);
    }
    let who = env::var("HOME")
        .or_else(|_| env::var("USER"))
        .unwrap_or_else(|_| "shared".to_owned());
    let tag = who.bytes().fold(0xcbf2_9ce4_8422_2325u64, |acc, byte| {
        (acc ^ byte as u64).wrapping_mul(0x100_0000_01b3)
    });
    env::temp_dir().join(format!("redsuite-ports-{tag:x}"))
}

impl PortLease {
    fn claim(&mut self, want: u16) -> Result<u16> {
        let dir = registry_dir();
        fs::create_dir_all(&dir)?;
        let start = rand::thread_rng().gen_range(PORT_BAND);
        for first in (start..PORT_BAND.end).chain(PORT_BAND.start..start).take(BAND_ATTEMPTS) {
            if first + want > PORT_BAND.end {
                continue;
            }
            let mut claims = Vec::new();
            let mut holders = Vec::new();
            for port in first..first + want {
                let lock = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(dir.join(format!("{port}.lock")))?;
                match lock.try_lock() {
                    Ok(()) => {}
                    Err(TryLockError::WouldBlock) => break,
                    Err(error) => return Err(error.into()),
                }
                // A loopback bind can coexist with Agave's wildcard listener on macOS.
                let Ok(tcp) = TcpListener::bind(("0.0.0.0", port)) else {
                    break;
                };
                let Ok(udp) = UdpSocket::bind(("0.0.0.0", port)) else {
                    break;
                };
                claims.push((port, lock));
                holders.push((tcp, udp));
            }
            if holders.len() != usize::from(want) {
                continue;
            }
            self.holders.extend(holders);
            self.claims.extend(claims);
            return Ok(first);
        }
        Err(anyhow!(
            "no free block of {want} in {}-{} after {BAND_ATTEMPTS} attempts",
            PORT_BAND.start,
            PORT_BAND.end - 1
        ))
    }

    pub(super) fn single(&mut self) -> Result<u16> {
        self.claim(1)
    }

    pub(super) fn pair(&mut self) -> Result<(u16, u16)> {
        let first = self.claim(2)?;
        Ok((first, first + 1))
    }

    pub(super) fn release(&mut self) {
        self.holders.clear();
    }

    pub(super) fn failure(&self, error: Error) -> Error {
        let ports: Vec<_> = self.claims.iter().map(|(port, _)| *port).collect();
        error.context(format!("reserved ports: {ports:?}"))
    }
}
