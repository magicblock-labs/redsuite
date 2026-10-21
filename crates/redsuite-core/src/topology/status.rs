use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use super::{process, state};
use crate::Result;

const STACK_STORAGE: &[&str] = &["base-ledger", "genesis-accounts"];

pub fn status() -> Result<()> {
    let Some(state) = state::read_state() else {
        println!(
            "no shared stack ({} absent) — the first scenario boots it",
            state::state_path().display()
        );
        describe_orphans(&[]);
        return Ok(());
    };
    let proc_state = if process::proc_matches(state.base_pid, &state.base_bin) {
        "running"
    } else {
        "DEAD"
    };
    let rpc_state = if process::rpc_listening(state.base_rpc_port) { "rpc up" } else { "rpc DOWN" };
    println!(
        "base  pid {:<8} {proc_state:8} 127.0.0.1:{:<6} {rpc_state}   ({})",
        state.base_pid, state.base_rpc_port, state.base_bin
    );
    println!("ers   booted by each run for its ER groups, stopped when it ends");
    println!("logs          {}", state::stack_dir().display());
    describe_orphans(&[state.base_pid]);
    Ok(())
}

fn describe_orphans(exclude: &[u32]) {
    let orphans = process::orphaned_topology_processes(&state::stack_dir(), exclude);
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
            if process::proc_matches(state.base_pid, &state.base_bin) {
                println!("stopping base (pid {})", state.base_pid);
                process::kill_pid(state.base_pid);
            }
            known.push(state.base_pid);
            if let Err(error) = fs::remove_file(state::state_path()) {
                if error.kind() != ErrorKind::NotFound {
                    return Err(error.into());
                }
            }
        }
    }
    for (pid, cmdline) in process::orphaned_topology_processes(&state::stack_dir(), &known) {
        println!("stopping orphaned topology process (pid {pid}): {cmdline}");
        process::kill_pid(pid);
    }
    let wiped = wipe_storage()?;
    if wiped.is_empty() {
        println!("stack down");
    } else {
        let stack_dir = state::stack_dir();
        let names: Vec<String> = wiped
            .iter()
            .map(|path| match path.strip_prefix(&stack_dir) {
                Ok(name) => name.display().to_string(),
                Err(_) => path.display().to_string(),
            })
            .collect();
        println!("stack down, removed {}", names.join(", "));
    }
    Ok(())
}

pub fn wipe_storage() -> Result<Vec<PathBuf>> {
    let mut removed = sweep(&state::stack_dir())?;
    if let Some(root) = state::accountsdb_root() {
        removed.extend(sweep(&root)?);
    }
    Ok(removed)
}

fn sweep(dir: &Path) -> Result<Vec<PathBuf>> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(Vec::new());
    };
    let mut removed = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let outcome = if path.is_dir() {
            if !(STACK_STORAGE.contains(&name.as_str()) || name.starts_with("er-")) {
                continue;
            }
            fs::remove_dir_all(&path)
        } else {
            if !(name.starts_with("er-") && name.ends_with(".toml")) {
                continue;
            }
            fs::remove_file(&path)
        };
        outcome.map_err(|error| format!("removing stack storage {}: {error}", path.display()))?;
        removed.push(path);
    }
    Ok(removed)
}
