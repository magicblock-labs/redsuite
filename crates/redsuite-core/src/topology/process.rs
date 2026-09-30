use std::{
    fs,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    time::Duration,
};

use rand::Rng;

use crate::{host::proc_running, Result};

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
    let logfile = fs::File::create(log)?;
    cmd.stdout(logfile.try_clone()?)
        .stderr(logfile)
        .stdin(Stdio::null())
        .process_group(0);
    cmd.spawn().map_err(|e| {
        format!(
            "failed to spawn {}: {e}",
            cmd.get_program().to_string_lossy()
        )
        .into()
    })
}

pub(super) fn spawn_detached(cmd: Command, log: &Path) -> Result<u32> {
    Ok(spawn_child(cmd, log)?.id())
}

fn send_signal(pid: u32, signal: &str) {
    let _ = Command::new("kill")
        .args([signal, &pid.to_string()])
        .status();
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
    let grace_deadline = std::time::Instant::now() + KILL_GRACE;
    let hard_deadline = if hard_kill {
        grace_deadline
    } else {
        grace_deadline + KILL_GRACE
    };
    let mut escalated = hard_kill;
    let mut needed_sigkill = false;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok((status, needed_sigkill));
        }
        if !escalated && std::time::Instant::now() >= grace_deadline {
            send_signal(pid, "-KILL");
            escalated = true;
            needed_sigkill = true;
        }
        if std::time::Instant::now() >= hard_deadline {
            return Err(format!(
                "process {pid} did not exit within {KILL_GRACE:?} of SIGKILL"
            )
            .into());
        }
        tokio::time::sleep(RESTART_POLL).await;
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
    let deadline = std::time::Instant::now() + KILL_GRACE;
    while std::time::Instant::now() < deadline && proc_running(pid) {
        std::thread::sleep(Duration::from_millis(100));
    }
    if proc_running(pid) {
        let _ = Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status();
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
    let deadline = std::time::Instant::now() + KILL_GRACE;
    while std::time::Instant::now() < deadline
        && procs
            .iter()
            .any(|(pid, bin)| *pid != 0 && proc_matches(*pid, bin))
    {
        std::thread::sleep(Duration::from_millis(100));
    }
    for (pid, bin) in procs {
        if *pid != 0 && proc_matches(*pid, bin) {
            send_signal(*pid, "-KILL");
        }
    }
}

// Every process the topology layer spawns names the stack directory: the
// base and verifiers on their command line, ERs in their MBV_ storage env.
// Only the topology binaries qualify, so a shell or editor that merely
// mentions the directory is never touched.
pub(super) const TOPOLOGY_BINS: [&str; 3] = [
    "solana-test-validator",
    "magicblock-validator",
    "magicblock-verifier",
];

#[cfg(target_os = "linux")]
pub(super) fn orphaned_topology_processes(
    stack_dir: &Path,
    exclude: &[u32],
) -> Vec<(u32, String)> {
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
            Path::new(arg).file_name().is_some_and(|name| {
                TOPOLOGY_BINS.contains(&name.to_string_lossy().as_ref())
            })
        });
        if !runs_topology_bin {
            continue;
        }
        let cmdline = argv.join(" ");
        let environ = fs::read(format!("/proc/{pid}/environ"))
            .map(|raw| String::from_utf8_lossy(&raw).into_owned())
            .unwrap_or_default();
        let owned = cmdline.contains(&marker)
            || environ.split('\0').any(|pair| {
                pair.starts_with("MBV_ENGINE__LEDGER__DIRECTORY=")
                    && pair.contains(&marker)
            });
        if owned && proc_running(pid) {
            found.push((pid, cmdline));
        }
    }
    found.sort_unstable();
    found
}

#[cfg(target_os = "macos")]
pub(super) fn orphaned_topology_processes(
    stack_dir: &Path,
    exclude: &[u32],
) -> Vec<(u32, String)> {
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
        if owned_by_stack(pid, &cmdline, &marker) && proc_running(pid) {
            found.push((pid, cmdline));
        }
    }
    found.sort_unstable();
    found
}

#[cfg(target_os = "macos")]
fn owned_by_stack(pid: u32, cmdline: &str, marker: &str) -> bool {
    cmdline.contains(marker)
        || environment(pid).is_some_and(|environ| {
            environ.contains(&format!("MBV_ENGINE__LEDGER__DIRECTORY={marker}"))
        })
}

#[cfg(target_os = "macos")]
fn runs_topology_bin(cmdline: &str) -> bool {
    cmdline.split_whitespace().any(|arg| {
        Path::new(arg).file_name().is_some_and(|name| {
            TOPOLOGY_BINS.contains(&name.to_string_lossy().as_ref())
        })
    })
}

#[cfg(target_os = "macos")]
fn environment(pid: u32) -> Option<String> {
    let output = Command::new("ps")
        .args(["-ww", "-E", "-o", "command=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sweep_never_matches_this_test_process() {
        let stack_dir = std::env::temp_dir().join("redsuite-stack");
        let orphans = orphaned_topology_processes(&stack_dir, &[]);
        assert!(orphans.iter().all(|(pid, _)| *pid != std::process::id()));
        for (_, cmdline) in &orphans {
            assert!(
                cmdline.split(' ').any(|arg| Path::new(arg)
                    .file_name()
                    .is_some_and(|name| TOPOLOGY_BINS
                        .contains(&name.to_string_lossy().as_ref()))),
                "{cmdline}"
            );
        }
    }
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
    rpc_url: &str,
    log: &Path,
    pid: u32,
    listeners: &[(&str, u16)],
) -> Result<()> {
    let api = crate::api::Api::new(rpc_url.to_owned());
    wait_until(
        super::config::BASE_READY_TIMEOUT,
        "base RPC healthy",
        log,
        pid,
        || async { matches!(api.get_health().await.as_deref(), Ok("ok")) },
    )
    .await?;
    for (what, port) in listeners {
        wait_until(Duration::from_secs(20), what, log, pid, || async {
            tokio::net::TcpStream::connect(("127.0.0.1", *port))
                .await
                .is_ok()
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
    Fut: std::future::Future<Output = bool>,
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
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    let mut liveness_checked = tokio::time::Instant::now();
    loop {
        if condition().await {
            return Ok(());
        }
        let now = tokio::time::Instant::now();
        if interval >= LIVENESS_INTERVAL
            || now.duration_since(liveness_checked) >= LIVENESS_INTERVAL
        {
            liveness_checked = now;
            if !proc_running(pid) {
                return Err(format!(
                    "process exited while waiting for {what}; log tail:\n{}",
                    tail(log, 30)
                )
                .into());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out after {timeout:?} waiting for {what}; log tail:\n{}",
                tail(log, 30)
            )
            .into());
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(target_os = "linux")]
fn socket_address(raw: &str) -> Option<std::net::SocketAddr> {
    use std::net::IpAddr;
    let (ip, port) = raw.rsplit_once(':')?;
    let mut bytes = Vec::new();
    for word in ip.as_bytes().chunks_exact(8) {
        bytes.extend(
            u32::from_str_radix(std::str::from_utf8(word).ok()?, 16)
                .ok()?
                .to_ne_bytes(),
        );
    }
    let ip = match ip.len() {
        8 => IpAddr::from(<[u8; 4]>::try_from(bytes).ok()?),
        32 => IpAddr::from(<[u8; 16]>::try_from(bytes).ok()?),
        _ => return None,
    };
    Some((ip, u16::from_str_radix(port, 16).ok()?).into())
}

#[cfg(target_os = "linux")]
fn port_holders(port: u16) -> Vec<String> {
    let mut holders = Vec::new();
    for protocol in ["tcp", "tcp6", "udp", "udp6"] {
        let Ok(content) = fs::read_to_string(format!("/proc/net/{protocol}"))
        else {
            continue;
        };
        for line in content.lines().skip(1) {
            let cols: Vec<&str> = line.split_whitespace().collect();
            if cols.len() < 10 {
                continue;
            }
            let Some(local) = socket_address(cols[1]) else {
                continue;
            };
            if local.port() != port {
                continue;
            }
            let state = match cols[3] {
                "01" => "ESTABLISHED",
                "02" => "SYN_SENT",
                "03" => "SYN_RECV",
                "04" => "FIN_WAIT1",
                "05" => "FIN_WAIT2",
                "06" => "TIME_WAIT",
                "07" if protocol.starts_with("udp") => "UNCONN",
                "07" => "CLOSE",
                "08" => "CLOSE_WAIT",
                "09" => "LAST_ACK",
                "0A" => "LISTEN",
                "0B" => "CLOSING",
                "0C" => "NEW_SYN_RECV",
                _ => "UNKNOWN",
            };
            let owner = if cols[9] == "0" {
                "no live owner".to_owned()
            } else {
                find_socket_owner(&format!("socket:[{}]", cols[9]))
                    .map(|(pid, cmd)| format!("pid={pid} cmd={cmd}"))
                    .unwrap_or_else(|| "owner=unavailable".to_owned())
            };
            let peer = socket_address(cols[2])
                .map(|address| address.to_string())
                .unwrap_or_default();
            holders.push(format!(
                "{protocol} {local} -> {peer} state={state} {owner}"
            ));
        }
    }
    holders
}

#[cfg(target_os = "linux")]
fn find_socket_owner(target: &str) -> Option<(u32, String)> {
    let procs = fs::read_dir("/proc").ok()?;
    for entry in procs.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(fds) = fs::read_dir(entry.path().join("fd")) else {
            continue;
        };
        for fd in fds.flatten() {
            if fs::read_link(fd.path())
                .map(|link| link.to_string_lossy() == target)
                .unwrap_or(false)
            {
                let cmd = cmdline(pid).unwrap_or_default().replace('\0', " ");
                return Some((pid, cmd.trim_end().chars().take(160).collect()));
            }
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn port_holders(port: u16) -> Vec<String> {
    let output = match Command::new("lsof")
        .args([
            "-nP",
            &format!("-iTCP:{port}"),
            &format!("-iUDP:{port}"),
            "-FpfPnT",
        ])
        .output()
    {
        Ok(output) => output,
        Err(error) => return vec![format!("lsof lookup failed: {error}")],
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let mut holders: Vec<String> = Vec::new();
    let (mut pid, mut protocol, mut current) = (0, "", None);
    for line in text.lines().filter(|line| !line.is_empty()) {
        let (field, value) = line.split_at(1);
        match field {
            "p" => pid = value.parse::<u32>().unwrap_or_default(),
            "f" => current = None,
            "P" => protocol = value,
            "n" if value
                .split("->")
                .next()
                .is_some_and(|local| local.ends_with(&format!(":{port}"))) =>
            {
                let cmd =
                    cmdline(pid).unwrap_or_else(|| "unavailable".to_owned());
                current = Some(holders.len());
                holders.push(format!("{protocol} {value} pid={pid} cmd={cmd}"));
            }
            "T" if value.starts_with("ST=") => {
                if let Some(index) = current {
                    holders[index].push_str(&format!(" state={}", &value[3..]));
                }
            }
            _ => {}
        }
    }
    if !output.stderr.is_empty() {
        holders.push(format!(
            "lsof: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    holders
}

fn tail(path: &Path, lines: usize) -> String {
    let Ok(content) = fs::read_to_string(path) else {
        return format!("<no log at {}>", path.display());
    };
    let all: Vec<&str> = content.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

#[derive(Default)]
pub(super) struct PortLease {
    holders: Vec<(std::net::TcpListener, std::net::UdpSocket)>,
    claims: Vec<(u16, fs::File)>,
}

const PORT_BAND: std::ops::Range<u16> = 20_000..30_000;
const BAND_ATTEMPTS: usize = 512;

fn registry_dir() -> std::path::PathBuf {
    if let Some(dir) = std::env::var_os("REDSUITE_PORT_REGISTRY") {
        return std::path::PathBuf::from(dir);
    }
    let who = std::env::var("HOME")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "shared".to_owned());
    let tag = who.bytes().fold(0xcbf2_9ce4_8422_2325u64, |acc, byte| {
        (acc ^ byte as u64).wrapping_mul(0x100_0000_01b3)
    });
    std::env::temp_dir().join(format!("redsuite-ports-{tag:x}"))
}

impl PortLease {
    fn claim(&mut self, want: u16) -> Result<u16> {
        let dir = registry_dir();
        fs::create_dir_all(&dir)?;
        let start = rand::thread_rng().gen_range(PORT_BAND);
        for first in (start..PORT_BAND.end)
            .chain(PORT_BAND.start..start)
            .take(BAND_ATTEMPTS)
        {
            if first + want > PORT_BAND.end {
                continue;
            }
            let mut claims = Vec::new();
            let mut holders = Vec::new();
            for port in first..first + want {
                let lock = fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(dir.join(format!("{port}.lock")))?;
                match lock.try_lock() {
                    Ok(()) => {}
                    Err(fs::TryLockError::WouldBlock) => break,
                    Err(error) => return Err(error.into()),
                }
                let Ok(tcp) = std::net::TcpListener::bind(("127.0.0.1", port))
                else {
                    break;
                };
                let Ok(udp) = std::net::UdpSocket::bind(("127.0.0.1", port))
                else {
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
        Err(format!(
            "no free block of {want} in {}-{} after {BAND_ATTEMPTS} attempts",
            PORT_BAND.start,
            PORT_BAND.end - 1
        )
        .into())
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

    pub(super) fn failure(
        &self,
        error: impl std::fmt::Display,
    ) -> crate::DynError {
        let ports: Vec<_> = self.claims.iter().map(|(port, _)| *port).collect();
        let mut message = format!("{error}\nreserved ports: {ports:?}");
        for port in ports {
            for holder in port_holders(port) {
                message.push_str(&format!("\n  {holder}"));
            }
        }
        message.into()
    }
}
