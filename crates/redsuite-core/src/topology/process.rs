use std::{
    fs,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    time::Duration,
};

use crate::{host::proc_running, Result};

pub(super) const KILL_GRACE: Duration = Duration::from_secs(5);
pub(super) const POLL: Duration = Duration::from_millis(250);
// Fine cadence for restart timing, where the interval is the measurement floor.
pub(super) const RESTART_POLL: Duration = Duration::from_millis(5);

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

pub(super) fn kill_pid(pid: u32) {
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

pub(super) fn proc_matches(pid: u32, bin: &str) -> bool {
    fs::read(format!("/proc/{pid}/cmdline"))
        .map(|cmdline| String::from_utf8_lossy(&cmdline).contains(bin))
        .unwrap_or(false)
}

pub(super) fn rpc_listening(port: u16) -> bool {
    use std::net::{SocketAddr, TcpStream};
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()
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
    loop {
        if condition().await {
            return Ok(());
        }
        if !proc_running(pid) {
            return Err(format!(
                "process exited while waiting for {what}; log tail:\n{}",
                tail(log, 30)
            )
            .into());
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

fn tail(path: &Path, lines: usize) -> String {
    let Ok(content) = fs::read_to_string(path) else {
        return format!("<no log at {}>", path.display());
    };
    let all: Vec<&str> = content.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

#[derive(Default)]
pub(super) struct PortLease {
    holders: Vec<std::net::TcpListener>,
}

impl PortLease {
    pub(super) fn single(&mut self) -> Result<u16> {
        let holder = std::net::TcpListener::bind(("127.0.0.1", 0))?;
        let port = holder.local_addr()?.port();
        self.holders.push(holder);
        Ok(port)
    }

    pub(super) fn pair(&mut self) -> Result<(u16, u16)> {
        let mut rejected = Vec::new();
        for _ in 0..64 {
            let first = std::net::TcpListener::bind(("127.0.0.1", 0))?;
            let port = first.local_addr()?.port();
            if port == u16::MAX {
                rejected.push(first);
                continue;
            }
            match std::net::TcpListener::bind(("127.0.0.1", port + 1)) {
                Ok(second) => {
                    self.holders.push(first);
                    self.holders.push(second);
                    return Ok((port, port + 1));
                }
                Err(_) => rejected.push(first),
            }
        }
        Err("could not find two adjacent free ports".into())
    }

    pub(super) fn release(&mut self) {
        self.holders.clear();
    }
}
