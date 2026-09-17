use std::{
    fs,
    path::{Path, PathBuf},
};

use json::{Deserialize, Serialize};

use crate::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackState {
    pub base_rpc_port: u16,
    pub base_ws_port: u16,
    pub base_faucet_port: u16,
    pub base_gossip_port: u16,
    pub base_pid: u32,
    pub base_bin: String,
    pub er_rpc_port: u16,
    pub er_ws_port: u16,
    pub er_metrics_port: u16,
    pub er_pid: u32,
    pub er_bin: String,
    pub er_identity: String,
    pub er_identity_keypair: Vec<u8>,
    pub er_identity_pool: Vec<Vec<u8>>,
    pub clone_url: String,
    pub base_programs: Vec<String>,
}

pub const ROOT_ENV: &str = "REDSUITE_ROOT";
pub const ACCOUNTSDB_ROOT_ENV: &str = "REDSUITE_ACCOUNTSDB_DIR";

pub fn workspace_root() -> PathBuf {
    // `REDSUITE_ROOT` covers test binaries relocated after compilation
    // (e.g. `cargo nextest archive`).
    if let Some(root) = std::env::var_os(ROOT_ENV) {
        return PathBuf::from(root);
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives two levels under the workspace root")
        .to_path_buf()
}

pub fn stack_dir() -> PathBuf {
    workspace_root().join("target/redsuite-stack")
}

pub fn accountsdb_root() -> Option<PathBuf> {
    std::env::var_os(ACCOUNTSDB_ROOT_ENV).map(PathBuf::from)
}

pub(super) fn accountsdb_dir(storage_dir: &Path) -> PathBuf {
    match (accountsdb_root(), storage_dir.file_name()) {
        (Some(root), Some(name)) => root.join(name).join("accountsdb"),
        _ => storage_dir.join("accountsdb"),
    }
}

pub(super) fn split_accountsdb_dir(storage_dir: &Path) -> Option<PathBuf> {
    let root = accountsdb_root()?;
    Some(root.join(storage_dir.file_name()?))
}

pub(super) fn remove_storage(storage_dir: &Path) -> std::io::Result<()> {
    remove_dir_if_present(storage_dir)?;
    if let Some(split) = split_accountsdb_dir(storage_dir) {
        remove_dir_if_present(&split)?;
    }
    Ok(())
}

pub(crate) fn remove_dir_if_present(dir: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) fn state_path() -> PathBuf {
    stack_dir().join("state.json")
}

pub(super) fn read_state() -> Option<StackState> {
    json::from_str(&fs::read_to_string(state_path()).ok()?).ok()
}

pub fn current_state() -> Option<StackState> {
    read_state()
}

pub(super) fn write_state(state: &StackState) -> Result<()> {
    let tmp = state_path().with_extension("json.tmp");
    fs::write(&tmp, json::to_string(state)?)?;
    fs::rename(&tmp, state_path())?;
    Ok(())
}

pub(super) fn remove_state() {
    let _ = fs::remove_file(state_path());
}

pub(super) struct LockGuard(#[allow(dead_code)] std::fs::File);

pub(super) async fn acquire_lock(path: PathBuf) -> Result<LockGuard> {
    let file = tokio::task::spawn_blocking(
        move || -> std::io::Result<std::fs::File> {
            let file = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)?;
            file.lock()?;
            Ok(file)
        },
    )
    .await
    .map_err(|e| format!("lock task panicked: {e}"))??;
    Ok(LockGuard(file))
}
