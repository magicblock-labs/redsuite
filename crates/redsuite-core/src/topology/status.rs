use std::fs;

use super::{process, state};
use crate::Result;

pub fn status() -> Result<()> {
    let Some(state) = state::read_state() else {
        println!(
            "no shared stack ({} absent) — the first scenario boots it",
            state::state_path().display()
        );
        describe_orphans(&[]);
        return Ok(());
    };
    let describe = |name: &str, pid: u32, bin: &str, port: u16| {
        let proc_state = if process::proc_matches(pid, bin) {
            "running"
        } else {
            "DEAD"
        };
        let rpc_state = if process::rpc_listening(port) {
            "rpc up"
        } else {
            "rpc DOWN"
        };
        println!("{name:5} pid {pid:<8} {proc_state:8} 127.0.0.1:{port:<6} {rpc_state}   ({bin})");
    };
    describe("base", state.base_pid, &state.base_bin, state.base_rpc_port);
    if state.er_pid == 0 {
        println!("er    not booted — the first shared-stack scenario boots it");
    } else {
        describe("er", state.er_pid, &state.er_bin, state.er_rpc_port);
        println!(
            "er metrics    http://127.0.0.1:{}/metrics",
            state.er_metrics_port
        );
    }
    println!("er identity   {}", state.er_identity);
    println!("logs          {}", state::stack_dir().display());
    describe_orphans(&[state.base_pid, state.er_pid]);
    Ok(())
}

fn describe_orphans(exclude: &[u32]) {
    let orphans =
        process::orphaned_topology_processes(&state::stack_dir(), exclude);
    if orphans.is_empty() {
        return;
    }
    println!(
        "{} scenario-owned process(es) still running outside the shared \
         stack (private ERs, leaders or verifiers):",
        orphans.len()
    );
    for (pid, cmdline) in orphans {
        println!("  pid {pid:<8} {cmdline}");
    }
}

pub fn down() -> Result<()> {
    let mut known = Vec::new();
    match state::read_state() {
        None => println!("no shared stack to stop"),
        Some(state) => {
            for (name, pid, bin) in [
                ("er", state.er_pid, state.er_bin.as_str()),
                ("base", state.base_pid, state.base_bin.as_str()),
            ] {
                if process::proc_matches(pid, bin) {
                    println!("stopping {name} (pid {pid})");
                    process::kill_pid(pid);
                }
                known.push(pid);
            }
            if let Err(error) = fs::remove_file(state::state_path()) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    return Err(error.into());
                }
            }
        }
    }
    for (pid, cmdline) in
        process::orphaned_topology_processes(&state::stack_dir(), &known)
    {
        println!("stopping orphaned topology process (pid {pid}): {cmdline}");
        process::kill_pid(pid);
    }
    println!("stack down");
    Ok(())
}
