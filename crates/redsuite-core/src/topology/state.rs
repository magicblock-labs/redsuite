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

pub fn workspace_root() -> PathBuf {
    // `REDSUITE_ROOT` covers test binaries relocated after compilation
    // (e.g. `cargo nextest archive`).
    if let Some(root) = std::env::var_os("REDSUITE_ROOT") {
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
