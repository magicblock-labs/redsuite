use std::{
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use keypair::Keypair;
use pubkey::Pubkey;

use crate::{catalog::Fixture, manifest, Result};

pub const DLP_ID: &str = "DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh";
pub const MDP_ID: &str = "DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1";
pub const COMMITTOR_ID: &str = "ComtrB2KEaWgXsW1dhr1xYL4Ht4Bjj3gXnnL6KMdABq";
// The validator's committor (>= 0.13.7)
const NOOP_ID: &str = "noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV";
const MEMO_V1_ID: &str = "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo";
const MEMO_V2_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ATA_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const EATA_PROGRAM_ID: &str = "SPLxh1LVZzEkX99H6rqYizhytLWPZVV296zyYDPagv2";

const CLONED_UPGRADEABLE_PROGRAMS: &[&str] =
    &[DLP_ID, MDP_ID, NOOP_ID, TOKEN_PROGRAM_ID, EATA_PROGRAM_ID];
const CLONED_LEGACY_PROGRAMS: &[&str] =
    &[MEMO_V1_ID, MEMO_V2_ID, ATA_PROGRAM_ID];

// Extra genesis copies of redline_program.so under derived addresses, so
// scenarios can spread identical load across distinct program ids.
const REDLINE_ALIAS_COUNT: usize = 8;

pub fn redline_alias_ids(count: usize) -> Vec<Pubkey> {
    assert!(
        count <= REDLINE_ALIAS_COUNT,
        "only {REDLINE_ALIAS_COUNT} redline aliases are loaded at base boot"
    );
    (0..count)
        .map(|index| {
            Pubkey::find_program_address(
                &[b"redsuite-alias", &[index as u8]],
                &redline_interface::ID,
            )
            .0
        })
        .collect()
}

pub fn redshift_loader_v3_target() -> (Pubkey, Pubkey) {
    let id = Pubkey::find_program_address(
        &[b"redsuite-loader-v3"],
        &redshift_interface::ID,
    )
    .0;
    let authority = Pubkey::find_program_address(
        &[b"redsuite-loader-v3-auth"],
        &redshift_interface::ID,
    )
    .0;
    (id, authority)
}

pub fn redline_loader_v3_pair() -> [(Pubkey, Pubkey); 2] {
    [0u8, 1u8].map(|index| {
        let id = Pubkey::find_program_address(
            &[b"redsuite-redline-v3", &[index]],
            &redline_interface::ID,
        )
        .0;
        let authority = Pubkey::find_program_address(
            &[b"redsuite-redline-v3-auth", &[index]],
            &redline_interface::ID,
        )
        .0;
        (id, authority)
    })
}

pub const CLONE_URL_ENV: &str = "REDSUITE_CLONE_URL";
const DEFAULT_CLONE_URL: &str = "https://api.mainnet-beta.solana.com";

const PROTOCOL_FEES_VAULT_ID: &str =
    "7JrkjmZPprHwtuvtuGTXp9hwfGYFAQLnLeFM52kqAgXg";
pub(super) const CLONED_ACCOUNTS: &[&str] = &[PROTOCOL_FEES_VAULT_ID];

pub(super) fn clone_url() -> String {
    std::env::var(CLONE_URL_ENV)
        .unwrap_or_else(|_| DEFAULT_CLONE_URL.to_owned())
}

const BASE_BIN: &str = "solana-test-validator";
const ER_BIN: &str = "magicblock-validator";
pub const ER_BIN_ENV: &str = "MAGICBLOCK_VALIDATOR_BIN";
const VERIFIER_BIN: &str = "magicblock-verifier";
pub const VERIFIER_BIN_ENV: &str = "MAGICBLOCK_VERIFIER_BIN";

const BASE_LEDGER_SHREDS: &str = "200000";
pub(super) const BASE_READY_TIMEOUT: Duration = Duration::from_secs(60);
pub(super) const ER_READY_TIMEOUT: Duration = Duration::from_secs(120);

pub(super) fn find_base_bin() -> Result<PathBuf> {
    which(BASE_BIN).ok_or_else(|| {
        format!("{BASE_BIN} not found — put the Solana CLI on PATH").into()
    })
}

pub(super) fn find_er_bin() -> Result<PathBuf> {
    if let Some(explicit) = std::env::var_os(ER_BIN_ENV) {
        let explicit = PathBuf::from(explicit);
        return if explicit.exists() {
            Ok(explicit)
        } else {
            Err(
                format!("{ER_BIN_ENV}={} does not exist", explicit.display())
                    .into(),
            )
        };
    }
    which(ER_BIN).ok_or_else(|| {
        format!("{ER_BIN} not found — set {ER_BIN_ENV} to the built binary or put it on PATH")
            .into()
    })
}

pub fn er_bin_path() -> Result<PathBuf> {
    find_er_bin()
}

// The verifier ships beside the ER binary in the validator build tree; an
// explicit env override wins, PATH is the last resort.
pub(super) fn find_verifier_bin() -> Result<PathBuf> {
    if let Some(explicit) = std::env::var_os(VERIFIER_BIN_ENV) {
        let explicit = PathBuf::from(explicit);
        return if explicit.exists() {
            Ok(explicit)
        } else {
            Err(format!(
                "{VERIFIER_BIN_ENV}={} does not exist",
                explicit.display()
            )
            .into())
        };
    }
    if let Some(sibling) = find_er_bin()
        .ok()
        .and_then(|er| er.parent().map(|dir| dir.join(VERIFIER_BIN)))
        .filter(|candidate| candidate.is_file())
    {
        return Ok(sibling);
    }
    which(VERIFIER_BIN).ok_or_else(|| {
        format!(
            "{VERIFIER_BIN} not found — build it beside the ER binary \
             (`cargo build --release -p magicblock-verifier`), set \
             {VERIFIER_BIN_ENV}, or put it on PATH"
        )
        .into()
    })
}

pub fn verifier_bin_path() -> Result<PathBuf> {
    find_verifier_bin()
}

fn which(bin: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(bin))
        .find(|candidate| candidate.is_file())
}

pub(super) fn bin_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

struct StagedPrograms {
    bpf: Vec<(String, PathBuf)>,
    loaded_fixtures: Vec<String>,
}

fn base_programs(er_bin: &Path) -> Result<StagedPrograms> {
    let mut bpf = Vec::new();
    let mut loaded_fixtures = Vec::new();

    match committor_so(er_bin) {
        Some(so) => bpf.push((COMMITTOR_ID.to_owned(), so)),
        None => eprintln!(
            "[redsuite] warning: magicblock_committor_program.so not found in the ER binary's \
             build tree (cargo build-sbf it in the validator repo) — commit scenarios will fail"
        ),
    }

    for fixture in Fixture::ALL {
        if !fixture.loaded_at_base_boot() {
            continue;
        }
        match manifest::resolve(fixture) {
            Ok(so) => {
                bpf.push((fixture.program_id().to_string(), so));
                loaded_fixtures.push(fixture.so_name().to_owned());
            }
            Err(error) => eprintln!(
                "[redsuite] warning: {error} — its family's scenarios will fail"
            ),
        }
    }
    Ok(StagedPrograms {
        bpf,
        loaded_fixtures,
    })
}

fn redline_aliases_of(so: &Path) -> Vec<Pubkey> {
    if so
        .file_name()
        .is_some_and(|name| name == Fixture::RedlineProgram.so_name())
    {
        redline_alias_ids(REDLINE_ALIAS_COUNT)
    } else {
        Vec::new()
    }
}

fn committor_so(er_bin: &Path) -> Option<PathBuf> {
    let deploy = er_bin
        .parent()?
        .parent()?
        .join("deploy/magicblock_committor_program.so");
    deploy.exists().then_some(deploy)
}

// A validated base launch: every input resolved up front, so command() is a
// pure projection that can be inspected without spawning anything.
pub(super) struct BasePlan {
    pub(super) bin: PathBuf,
    pub(super) rpc_port: u16,
    pub(super) faucet_port: u16,
    pub(super) gossip_port: u16,
    pub(super) ledger: PathBuf,
    pub(super) genesis_accounts: PathBuf,
    pub(super) clone_url: String,
    // (program id, .so, upgrade authority) preloaded as loader-v3 programs
    pub(super) upgradeable_v3: Vec<(Pubkey, PathBuf, Pubkey)>,
    // (program id, .so) loaded via --bpf-program; redline gets its aliases
    pub(super) bpf_programs: Vec<(String, PathBuf)>,
    pub(super) loaded_fixtures: Vec<String>,
}

impl BasePlan {
    pub(super) fn gather(
        bin: PathBuf,
        er_bin: &Path,
        stack_dir: &Path,
        rpc_port: u16,
        faucet_port: u16,
        gossip_port: u16,
        clone_url: String,
    ) -> Result<Self> {
        let mut upgradeable_v3 = Vec::new();
        if let Ok(redshift_so) = manifest::resolve(Fixture::RedshiftProgram) {
            let (v3_id, v3_authority) = redshift_loader_v3_target();
            upgradeable_v3.push((v3_id, redshift_so, v3_authority));
        }
        if let Ok(redline_so) = manifest::resolve(Fixture::RedlineProgram) {
            for (id, authority) in redline_loader_v3_pair() {
                upgradeable_v3.push((id, redline_so.clone(), authority));
            }
        }
        let staged = base_programs(er_bin)?;
        Ok(Self {
            bin,
            rpc_port,
            faucet_port,
            gossip_port,
            ledger: stack_dir.join("base-ledger"),
            genesis_accounts: stack_dir.join("genesis-accounts"),
            clone_url,
            upgradeable_v3,
            bpf_programs: staged.bpf,
            loaded_fixtures: staged.loaded_fixtures,
        })
    }

    pub(super) fn command(&self) -> Command {
        let mut cmd = Command::new(&self.bin);
        cmd.args(["--reset", "--log", "--bind-address", "127.0.0.1"])
            .args(["--limit-ledger-size", BASE_LEDGER_SHREDS])
            .arg("--ledger")
            .arg(&self.ledger)
            .args(["--rpc-port", &self.rpc_port.to_string()])
            .args(["--faucet-port", &self.faucet_port.to_string()])
            .args(["--gossip-port", &self.gossip_port.to_string()])
            .args(["--url", &self.clone_url]);
        cmd.arg("--account-dir").arg(&self.genesis_accounts);
        for id in CLONED_ACCOUNTS {
            cmd.arg("--clone").arg(id);
        }
        for id in CLONED_UPGRADEABLE_PROGRAMS {
            cmd.arg("--clone-upgradeable-program").arg(id);
        }
        for id in CLONED_LEGACY_PROGRAMS {
            cmd.arg("--clone").arg(id);
        }
        for (id, so, authority) in &self.upgradeable_v3 {
            cmd.arg("--upgradeable-program")
                .arg(id.to_string())
                .arg(so)
                .arg(authority.to_string());
        }
        for (id, so) in &self.bpf_programs {
            for alias in redline_aliases_of(so) {
                cmd.arg("--bpf-program").arg(alias.to_string()).arg(so);
            }
            cmd.arg("--bpf-program").arg(id).arg(so);
        }
        cmd
    }
}

// A validated ER launch, shared by the attached ER and every private ER;
// command() stays pure so restarts rebuild the exact same invocation.
pub(super) struct ErPlan {
    pub(super) bin: PathBuf,
    pub(super) identity: Keypair,
    pub(super) base_rpc_url: String,
    pub(super) base_ws_url: String,
    pub(super) listen_port: u16,
    pub(super) metrics_port: u16,
    // every validator binds a follower listener (default 127.0.0.1:10000);
    pub(super) replication_port: u16,
    pub(super) storage_dir: PathBuf,
    pub(super) env: Vec<(String, String)>,
    // reset=true wipes only the ledger (rocksdb) and skips replay; it
    // preserves the accountsdb. A restart-in-place relaunch passes false so
    // the ER reopens the on-disk ledger + accountsdb it already has.
    pub(super) reset: bool,
    // follower identities the replication listener admits; empty denies all
    pub(super) allowed_followers: Vec<Pubkey>,
}

// The engine validator keeps only --remotes / --lifecycle / -l on the command
// line. Identity, storage and reset moved into the MBV_ config tree: figment
// strips the prefix, splits on `__` and turns the remaining `_` into `-`, so
// MBV_ENGINE__LEDGER__DIRECTORY sets engine.ledger.directory.
impl ErPlan {
    pub(super) fn command(&self) -> Command {
        let mut cmd = Command::new(&self.bin);
        cmd.arg("--remotes")
            .arg(&self.base_rpc_url)
            .arg("--remotes")
            .arg(&self.base_ws_url);
        cmd.args(["--lifecycle", "ephemeral"])
            .arg("-l")
            .arg(format!("127.0.0.1:{}", self.listen_port));
        cmd.env(
            "MBV_ENGINE__AUTHORITY__LOCAL",
            self.identity.to_base58_string(), // throwaway test identity
        );
        cmd.env("MBV_ENGINE__LEDGER__DIRECTORY", &self.storage_dir);
        // engine.accountsdb.directory defaults to a compile-time constant
        // path, not to the configured engine.ledger.directory — overriding
        // only the ledger directory leaves the accountsdb at the global
        // default. Set both, mirroring the engine's default
        // <ledger dir>/accountsdb layout under this ER's storage dir.
        cmd.env(
            "MBV_ENGINE__ACCOUNTSDB__DIRECTORY",
            self.storage_dir.join("accountsdb"),
        );
        cmd.env(
            "MBV_ENGINE__REPLICATION__BIND_ADDRESS",
            format!("127.0.0.1:{}", self.replication_port),
        );
        cmd.env("MBV_LEDGER__RESET", self.reset.to_string());
        cmd.env(
            "MBV_METRICS__ADDRESS",
            format!("127.0.0.1:{}", self.metrics_port),
        );
        if !self.allowed_followers.is_empty() {
            cmd.env(
                "MBV_ENGINE__REPLICATION__ALLOWED_FOLLOWERS",
                toml_string_array(&self.allowed_followers),
            );
        }
        for (key, value) in &self.env {
            cmd.env(key, value);
        }
        if std::env::var_os("RUST_LOG").is_none() {
            cmd.env("RUST_LOG", "info");
        }
        cmd
    }
}

fn toml_string_array(values: &[Pubkey]) -> String {
    let quoted: Vec<String> =
        values.iter().map(|value| format!("\"{value}\"")).collect();
    format!("[{}]", quoted.join(", "))
}

pub(super) const VERIFIER_CONFIG_FILE: &str = "verifier.toml";
const VERIFIER_ENV_PREFIX: &str = "MBV_VERIFIER_";
const MIRRORED_LEADER_ENV_PREFIX: &str = "MBV_ENGINE__BLOCKSTORE__";

// A validated verifier launch: the bare replicated engine reads one TOML
// (identity, storage, upstream) plus MBV_VERIFIER_ overrides; command()
// stays pure so a relaunch reuses the exact invocation.
pub(super) struct VerifierPlan {
    pub(super) bin: PathBuf,
    pub(super) identity: Keypair,
    pub(super) upstream_port: u16,
    pub(super) upstream_authority: Pubkey,
    pub(super) metrics_port: u16,
    pub(super) storage_dir: PathBuf,
    pub(super) env: Vec<(String, String)>,
    // a `taskset -c` list; the engine sizes its executor pool from the cpus
    // it can see, so this is how a verifier gets a different executor count
    pub(super) cpu_set: Option<String>,
}

impl VerifierPlan {
    pub(super) fn config_path(&self) -> PathBuf {
        self.storage_dir.join(VERIFIER_CONFIG_FILE)
    }

    pub(super) fn upstream_address(&self) -> String {
        format!("127.0.0.1:{}", self.upstream_port)
    }

    pub(super) fn config_toml(&self) -> String {
        format!(
            "[metrics]\naddress = \"127.0.0.1:{metrics}\"\n\n\
             [engine.authority]\nlocal = \"{identity}\"\n\n\
             [engine.accountsdb]\ndirectory = \"{accountsdb}\"\n\n\
             [engine.ledger]\ndirectory = \"{ledger}\"\n\n\
             [engine.replication]\nupstream-address = \"{upstream}\"\n\
             upstream-authority = \"{authority}\"\n",
            metrics = self.metrics_port,
            identity = self.identity.to_base58_string(),
            accountsdb = self.storage_dir.join("accountsdb").display(),
            ledger = self.storage_dir.display(),
            upstream = self.upstream_address(),
            authority = self.upstream_authority,
        )
    }

    pub(super) fn write_config(&self) -> Result<PathBuf> {
        let path = self.config_path();
        std::fs::create_dir_all(&self.storage_dir)?;
        std::fs::write(&path, self.config_toml())?;
        Ok(path)
    }

    pub(super) fn command(&self) -> Command {
        let mut cmd = match &self.cpu_set {
            Some(cpu_set) => {
                let mut cmd = Command::new("taskset");
                cmd.arg("-c").arg(cpu_set).arg(&self.bin);
                cmd
            }
            None => Command::new(&self.bin),
        };
        cmd.arg(self.config_path());
        for (key, value) in &self.env {
            cmd.env(key, value);
        }
        if std::env::var_os("RUST_LOG").is_none() {
            cmd.env("RUST_LOG", "info");
        }
        cmd
    }
}

// Block timing must match between a leader and its followers, so the
// leader's blockstore overrides are mirrored into the verifier's env tree.
pub(super) fn mirrored_verifier_env(
    leader_env: &[(String, String)],
) -> Vec<(String, String)> {
    leader_env
        .iter()
        .filter(|(key, _)| key.starts_with(MIRRORED_LEADER_ENV_PREFIX))
        .map(|(key, value)| {
            (
                format!(
                    "{VERIFIER_ENV_PREFIX}{}",
                    key.trim_start_matches("MBV_")
                ),
                value.clone(),
            )
        })
        .collect()
}

#[derive(Default)]
pub struct ErOptions {
    pub label: String,
    // e.g. ("MBV_ENGINE__ACCOUNTSDB__LRU_CAPACITY", "100")
    pub env: Vec<(String, String)>,
    pub request_timeout: Option<Duration>,
}

pub struct RestartConfig {
    // SIGKILL instead of SIGTERM — no graceful drain (crash-recovery path).
    pub hard_kill: bool,
    // reset=false relaunches in place; true wipes the ledger and skips replay.
    pub reset: bool,
    pub ready_timeout: Duration,
}

impl Default for RestartConfig {
    fn default() -> Self {
        Self {
            hard_kill: false,
            reset: false,
            ready_timeout: ER_READY_TIMEOUT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_of(cmd: &Command) -> Vec<String> {
        cmd.get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    fn env_of(cmd: &Command, key: &str) -> Option<String> {
        cmd.get_envs().find_map(|(name, value)| {
            (name == key).then(|| {
                value.unwrap_or_default().to_string_lossy().into_owned()
            })
        })
    }

    fn plan(reset: bool) -> ErPlan {
        ErPlan {
            bin: PathBuf::from("magicblock-validator"),
            identity: Keypair::new(),
            base_rpc_url: "http://127.0.0.1:8899".to_owned(),
            base_ws_url: "ws://127.0.0.1:8900".to_owned(),
            listen_port: 7799,
            metrics_port: 7801,
            replication_port: 7802,
            storage_dir: PathBuf::from("/tmp/er-storage"),
            env: vec![("MBV_TEST".to_owned(), "1".to_owned())],
            reset,
            allowed_followers: Vec::new(),
        }
    }

    #[test]
    fn leader_plans_carry_the_follower_allowlist() {
        let cmd = plan(true).command();
        assert!(env_of(&cmd, "MBV_ENGINE__REPLICATION__ALLOWED_FOLLOWERS")
            .is_none());
        let mut leader = plan(true);
        let followers = [Pubkey::new_unique(), Pubkey::new_unique()];
        leader.allowed_followers = followers.to_vec();
        let cmd = leader.command();
        assert_eq!(
            env_of(&cmd, "MBV_ENGINE__REPLICATION__ALLOWED_FOLLOWERS")
                .as_deref(),
            Some(
                format!("[\"{}\", \"{}\"]", followers[0], followers[1])
                    .as_str()
            )
        );
    }

    #[test]
    fn verifier_plans_write_one_toml_and_pass_only_its_path() {
        let identity = Keypair::new();
        let upstream_authority = Pubkey::new_unique();
        let plan = VerifierPlan {
            bin: PathBuf::from("magicblock-verifier"),
            identity: Keypair::try_from(&identity.to_bytes()[..]).unwrap(),
            upstream_port: 7802,
            upstream_authority,
            metrics_port: 9101,
            storage_dir: PathBuf::from("/tmp/er-leader-verifier0"),
            env: vec![(
                "MBV_VERIFIER_ENGINE__BLOCKSTORE__BLOCKTIME".to_owned(),
                "20ms".to_owned(),
            )],
            cpu_set: None,
        };
        let toml = plan.config_toml();
        assert!(toml.contains("address = \"127.0.0.1:9101\""));
        assert!(toml
            .contains(&format!("local = \"{}\"", identity.to_base58_string())));
        assert!(toml
            .contains("directory = \"/tmp/er-leader-verifier0/accountsdb\""));
        assert!(toml.contains("directory = \"/tmp/er-leader-verifier0\""));
        assert!(toml.contains("upstream-address = \"127.0.0.1:7802\""));
        assert!(toml.contains(&format!(
            "upstream-authority = \"{upstream_authority}\""
        )));
        let cmd = plan.command();
        assert_eq!(
            args_of(&cmd),
            vec!["/tmp/er-leader-verifier0/verifier.toml".to_owned()]
        );
        assert_eq!(
            env_of(&cmd, "MBV_VERIFIER_ENGINE__BLOCKSTORE__BLOCKTIME")
                .as_deref(),
            Some("20ms")
        );
    }

    #[test]
    fn a_cpu_set_pins_the_verifier_through_taskset() {
        let plan = VerifierPlan {
            bin: PathBuf::from("/x/magicblock-verifier"),
            identity: Keypair::new(),
            upstream_port: 7802,
            upstream_authority: Pubkey::new_unique(),
            metrics_port: 9101,
            storage_dir: PathBuf::from("/tmp/er-leader-verifier1"),
            env: Vec::new(),
            cpu_set: Some("0-3".to_owned()),
        };
        let cmd = plan.command();
        assert_eq!(cmd.get_program().to_string_lossy(), "taskset");
        assert_eq!(
            args_of(&cmd),
            vec![
                "-c".to_owned(),
                "0-3".to_owned(),
                "/x/magicblock-verifier".to_owned(),
                "/tmp/er-leader-verifier1/verifier.toml".to_owned(),
            ]
        );
    }

    #[test]
    fn only_blockstore_overrides_mirror_from_leader_to_verifiers() {
        let mirrored = mirrored_verifier_env(&[
            (
                "MBV_ENGINE__BLOCKSTORE__BLOCKTIME".to_owned(),
                "20ms".to_owned(),
            ),
            (
                "MBV_ENGINE__BLOCKSTORE__SUPERBLOCK".to_owned(),
                "64".to_owned(),
            ),
            (
                "MBV_ENGINE__ACCOUNTSDB__LRU_CAPACITY".to_owned(),
                "100".to_owned(),
            ),
        ]);
        assert_eq!(
            mirrored,
            vec![
                (
                    "MBV_VERIFIER_ENGINE__BLOCKSTORE__BLOCKTIME".to_owned(),
                    "20ms".to_owned()
                ),
                (
                    "MBV_VERIFIER_ENGINE__BLOCKSTORE__SUPERBLOCK".to_owned(),
                    "64".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn er_plans_are_inspectable_without_spawning() {
        let cmd = plan(true).command();
        let args = args_of(&cmd);
        assert_eq!(args.iter().filter(|arg| *arg == "--remotes").count(), 2);
        assert!(args.contains(&"127.0.0.1:7799".to_owned()));
        // identity, storage and reset travel in the MBV_ env tree
        assert_eq!(env_of(&cmd, "MBV_LEDGER__RESET").as_deref(), Some("true"));
        assert_eq!(
            env_of(&cmd, "MBV_ENGINE__LEDGER__DIRECTORY").as_deref(),
            Some("/tmp/er-storage")
        );
        assert_eq!(
            env_of(&cmd, "MBV_ENGINE__ACCOUNTSDB__DIRECTORY").as_deref(),
            Some("/tmp/er-storage/accountsdb")
        );
        assert_eq!(
            env_of(&cmd, "MBV_ENGINE__REPLICATION__BIND_ADDRESS").as_deref(),
            Some("127.0.0.1:7802")
        );
        assert!(env_of(&cmd, "MBV_ENGINE__AUTHORITY__LOCAL").is_some());
        // the flags the engine CLI no longer accepts must be gone
        for dead in ["-k", "--storage", "--reset", "--no-tui"] {
            assert!(!args.contains(&dead.to_owned()), "{dead} must not appear");
        }
    }

    #[test]
    fn restart_relaunch_plans_disable_reset() {
        let cmd = plan(false).command();
        let args = args_of(&cmd);
        assert_eq!(args.iter().filter(|arg| *arg == "--remotes").count(), 2);
        assert_eq!(env_of(&cmd, "MBV_LEDGER__RESET").as_deref(), Some("false"));
    }

    #[test]
    fn base_plans_list_the_genesis_programs() {
        let plan = BasePlan {
            bin: PathBuf::from("solana-test-validator"),
            rpc_port: 8899,
            faucet_port: 8901,
            gossip_port: 8902,
            ledger: PathBuf::from("/tmp/base-ledger"),
            genesis_accounts: PathBuf::from("/tmp/genesis"),
            clone_url: DEFAULT_CLONE_URL.to_owned(),
            upgradeable_v3: vec![(
                Pubkey::new_unique(),
                PathBuf::from("/tmp/redshift_program.so"),
                Pubkey::new_unique(),
            )],
            bpf_programs: vec![(
                Fixture::RedlineProgram.program_id().to_string(),
                PathBuf::from("/tmp/redline_program.so"),
            )],
            loaded_fixtures: vec![Fixture::RedlineProgram.so_name().to_owned()],
        };
        let args = args_of(&plan.command());
        assert!(args.contains(&"--upgradeable-program".to_owned()));
        // the redline .so loads under its id plus every alias
        let loads = args.iter().filter(|arg| *arg == "--bpf-program").count();
        assert_eq!(loads, 1 + REDLINE_ALIAS_COUNT);
        assert!(
            args.contains(&Fixture::RedlineProgram.program_id().to_string())
        );
    }
}
