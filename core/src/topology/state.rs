use std::{
    env, fs,
    fs::{File, OpenOptions},
    io,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::Context;
use json::{Deserialize, Serialize};
use magicblock_config::config::EngineConfig;
use tokio::task;
use url::Url;

use crate::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackState {
    pub base_rpc_port: u16,
    pub base_ws_port: u16,
    pub base_faucet_port: u16,
    pub base_gossip_port: u16,
    #[serde(default)]
    pub base_grpc_port: u16,
    #[serde(default)]
    pub base_plugin: String,
    pub base_pid: u32,
    pub base_bin: String,
    pub er_identity_pool: Vec<Vec<u8>>,
    pub clone_url: Url,
    pub base_programs: Vec<String>,
}

pub const ROOT_ENV: &str = "REDSUITE_ROOT";
pub const STACK_DIR_ENV: &str = "REDSUITE_STACK_DIR";
pub const ACCOUNTSDB_ROOT_ENV: &str = "REDSUITE_ACCOUNTSDB_DIR";

pub fn workspace_root() -> PathBuf {
    // `REDSUITE_ROOT` covers the binary running outside the checkout.
    if let Some(root) = env::var_os(ROOT_ENV) {
        return PathBuf::from(root);
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("core lives under the workspace root")
        .to_path_buf()
}

pub fn stack_dir() -> PathBuf {
    match env::var_os(STACK_DIR_ENV) {
        Some(dir) => PathBuf::from(dir),
        None => workspace_root().join("target/redsuite-stack"),
    }
}

pub fn accountsdb_root() -> Option<PathBuf> {
    env::var_os(ACCOUNTSDB_ROOT_ENV).map(PathBuf::from)
}

/// The directories managed together even when the ledger and AccountsDB are on separate volumes.
pub(super) struct Storage {
    pub(super) ledger: PathBuf,
    pub(super) accountsdb: PathBuf,
}

impl Storage {
    pub(super) fn new(ledger: PathBuf) -> Self {
        let accounts_root =
            accountsdb_root().zip(ledger.file_name()).map(|(root, name)| root.join(name));
        let accountsdb = accounts_root.as_ref().unwrap_or(&ledger).join("accountsdb");
        Self { ledger, accountsdb }
    }

    pub(super) fn configure<R>(&self, engine: &mut EngineConfig<R>) {
        engine.ledger.directory.clone_from(&self.ledger);
        engine.accountsdb.directory.clone_from(&self.accountsdb);
    }

    pub(super) fn matches<R>(&self, engine: &EngineConfig<R>) -> bool {
        engine.ledger.directory == self.ledger && engine.accountsdb.directory == self.accountsdb
    }

    pub(super) fn clear(&self) -> Result<()> {
        let remove = |dir: &Path| {
            remove_dir_if_present(dir).with_context(|| format!("removing {}", dir.display()))
        };
        let ledger = remove(&self.ledger);
        let split = self
            .accountsdb
            .parent()
            .filter(|root| *root != self.ledger)
            .map_or(Ok(()), remove);
        ledger.and(split)
    }
}

fn remove_dir_if_present(dir: &Path) -> io::Result<()> {
    match fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) fn state_path() -> PathBuf {
    stack_dir().join("state.json")
}

pub(super) fn read_state() -> Option<StackState> {
    json::from_str(&fs::read_to_string(state_path()).ok()?).ok()
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

pub(super) struct LockGuard(#[allow(dead_code)] File);

pub(super) async fn acquire_lock() -> Result<LockGuard> {
    let file = task::spawn_blocking(|| -> io::Result<File> {
        let dir = stack_dir();
        fs::create_dir_all(&dir)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("lock"))?;
        file.lock()?;
        Ok(file)
    })
    .await
    .context("lock task panicked")??;
    Ok(LockGuard(file))
}
