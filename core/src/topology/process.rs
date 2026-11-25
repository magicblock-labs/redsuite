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
use crate::{
    api::Api, host::proc_running, logging::read_log_tail, resources::ResourceRecord, Result,
};

// Engine gives each of its four shutdown tiers up to eight seconds.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(40);
const KILL_GRACE: Duration = Duration::from_secs(5);
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

// Intentional crash tests skip SIGTERM so the validator cannot flush on the way down.
async fn terminate(child: &mut Child, hard_kill: bool) -> Result<(ExitStatus, bool)> {
    if let Some(status) = child.try_wait()? {
        return Ok((status, false));
    }
    let pid = child.id();
    if !hard_kill {
        send_signal(pid, "-TERM");
        if let Some(status) = wait_for_exit(child, SHUTDOWN_GRACE).await? {
            return Ok((status, false));
        }
        warn!(pid, grace = ?SHUTDOWN_GRACE, "graceful shutdown timed out; sending SIGKILL");
    }

    send_signal(pid, "-KILL");
    let status = wait_for_exit(child, KILL_GRACE)
        .await?
        .with_context(|| format!("process {pid} did not exit within {KILL_GRACE:?} of SIGKILL"))?;
    Ok((status, !hard_kill))
}

async fn wait_for_exit(child: &mut Child, grace: Duration) -> Result<Option<ExitStatus>> {
    let deadline = Instant::now() + grace;
    loop {
        let status = child.try_wait()?;
        if status.is_some() || Instant::now() >= deadline {
            return Ok(status);
        }
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
                "process {pid} exited while waiting for {what} (log: {}):\n{}",
                log.display(),
                startup_excerpt(log)
            );
        }
        ensure!(
            time::Instant::now() < deadline,
            "timed out after {timeout:?} waiting for {what} on process {pid} (log: {}):\n{}",
            log.display(),
            startup_excerpt(log)
        );
        time::sleep(interval).await;
    }
}

fn startup_excerpt(path: &Path) -> String {
    let content = match read_log_tail(path) {
        Ok(content) if content.is_empty() => return "<empty log>".into(),
        Ok(content) => content,
        Err(error) => return format!("<cannot read log: {error}>"),
    };
    let content = content.trim_end();
    let last_lines = content.rmatch_indices('\n').nth(7).map_or(0, |(index, _)| index + 1);
    let start = last_lines.max(content.ceil_char_boundary(content.len().saturating_sub(4096)));
    content[start..].to_owned()
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

    pub(super) fn exit_status(&mut self) -> Result<Option<ExitStatus>> {
        self.child
            .as_mut()
            .map(Child::try_wait)
            .transpose()
            .map(Option::flatten)
            .map_err(Into::into)
    }

    pub(super) async fn terminate(
        &mut self,
        hard_kill: bool,
    ) -> Result<Option<(ExitStatus, bool)>> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        let outcome = terminate(child, hard_kill).await?;
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
