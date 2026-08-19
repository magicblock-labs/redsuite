use std::{
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use keypair::Keypair;
use pubkey::Pubkey;

use super::state;
use crate::Result;

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

const FAMILY_PROGRAMS: &[(&str, &str)] = &[
    (
        "3JnJ727jWEmPVU8qfXwtH63sCNDX7nMgsLbg8qy8aaPX",
        "redline_program.so",
    ),
    (
        "AijneHkXJVVWyimuwfSJdrJktARZu2WiMaZBqHsq7CS5",
        "redshift_program.so",
    ),
    (
        "BTczL2chGpVHw25pbmMtkFAD1t7rxoa8pVbaUjsybjiq",
        "redhat_program.so",
    ),
];

// Extra genesis copies of redline_program.so under derived addresses, so
// scenarios can spread identical load across distinct program ids.
const REDLINE_ALIAS_COUNT: usize = 8;

pub fn redline_alias_ids(count: usize) -> Vec<Pubkey> {
    assert!(
        count <= REDLINE_ALIAS_COUNT,
        "only {REDLINE_ALIAS_COUNT} redline aliases are loaded at base boot"
    );
    let redline_id: Pubkey = FAMILY_PROGRAMS[0]
        .0
        .parse()
        .expect("redline program id parses");
    (0..count)
        .map(|index| {
            Pubkey::find_program_address(
                &[b"redsuite-alias", &[index as u8]],
                &redline_id,
            )
            .0
        })
        .collect()
}

pub fn redshift_loader_v3_target() -> (Pubkey, Pubkey) {
    let redshift_id: Pubkey = FAMILY_PROGRAMS[1]
        .0
        .parse()
        .expect("redshift program id parses");
    let id =
        Pubkey::find_program_address(&[b"redsuite-loader-v3"], &redshift_id).0;
    let authority = Pubkey::find_program_address(
        &[b"redsuite-loader-v3-auth"],
        &redshift_id,
    )
    .0;
    (id, authority)
}

pub fn redline_loader_v3_pair() -> [(Pubkey, Pubkey); 2] {
    let redline_id: Pubkey = FAMILY_PROGRAMS[0]
        .0
        .parse()
        .expect("redline program id parses");
    [0u8, 1u8].map(|index| {
        let id = Pubkey::find_program_address(
            &[b"redsuite-redline-v3", &[index]],
            &redline_id,
        )
        .0;
        let authority = Pubkey::find_program_address(
            &[b"redsuite-redline-v3-auth", &[index]],
            &redline_id,
        )
        .0;
        (id, authority)
    })
}

pub(super) const CLONE_URL_ENV: &str = "REDSUITE_CLONE_URL";
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
const ER_BIN_ENV: &str = "MAGICBLOCK_VALIDATOR_BIN";

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

fn base_programs(er_bin: &Path) -> Result<Vec<(String, PathBuf)>> {
    let root = state::workspace_root();
    let mut programs = Vec::new();

    match committor_so(er_bin) {
        Some(so) => programs.push((COMMITTOR_ID.to_owned(), so)),
        None => eprintln!(
            "[redsuite] warning: magicblock_committor_program.so not found in the ER binary's \
             build tree (cargo build-sbf it in the validator repo) — commit scenarios will fail"
        ),
    }

    for (id, name) in FAMILY_PROGRAMS {
        let so = root.join("target/deploy").join(name);
        if so.exists() {
            programs.push(((*id).to_owned(), so));
        } else {
            eprintln!(
                "[redsuite] warning: {name} not built (run `cargo xtask programs`) — \
                 its family's scenarios will fail"
            );
        }
    }
    Ok(programs)
}

fn redline_aliases_of(so: &Path) -> Vec<Pubkey> {
    if so
        .file_name()
        .is_some_and(|name| name == "redline_program.so")
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
        let deploy = state::workspace_root().join("target/deploy");
        let mut upgradeable_v3 = Vec::new();
        let redshift_so = deploy.join("redshift_program.so");
        if redshift_so.exists() {
            let (v3_id, v3_authority) = redshift_loader_v3_target();
            upgradeable_v3.push((v3_id, redshift_so, v3_authority));
        }
        let redline_so = deploy.join("redline_program.so");
        if redline_so.exists() {
            for (id, authority) in redline_loader_v3_pair() {
                upgradeable_v3.push((id, redline_so.clone(), authority));
            }
        }
        Ok(Self {
            bin,
            rpc_port,
            faucet_port,
            gossip_port,
            ledger: stack_dir.join("base-ledger"),
            genesis_accounts: stack_dir.join("genesis-accounts"),
            clone_url,
            upgradeable_v3,
            bpf_programs: base_programs(er_bin)?,
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
    pub(super) storage_dir: PathBuf,
    pub(super) env: Vec<(String, String)>,
    // --reset wipes only the ledger (rocksdb) and skips replay; it preserves
    // the accountsdb. A restart-in-place relaunch omits it so the ER reopens
    // the on-disk ledger + accountsdb it already has.
    pub(super) reset: bool,
    // An offline validator serves its restored ledger with no base chain; it
    // gets no --remotes.
    pub(super) lifecycle: String,
}

impl ErPlan {
    pub(super) fn command(&self) -> Command {
        let mut cmd = Command::new(&self.bin);
        if self.lifecycle != "offline" {
            cmd.arg("--remotes")
                .arg(&self.base_rpc_url)
                .arg("--remotes")
                .arg(&self.base_ws_url);
        }
        cmd.args(["--lifecycle", &self.lifecycle])
            .arg("-l")
            .arg(format!("127.0.0.1:{}", self.listen_port))
            .arg("-k")
            .arg(self.identity.to_base58_string()) // throwaway test identity
            .arg("--storage")
            .arg(&self.storage_dir);
        if self.reset {
            cmd.arg("--reset");
        }
        cmd.arg("--no-tui");
        // Not `-m`: the CLI overlay feeds a bare string where a MetricsConfig
        // struct is expected and the validator exits — the env path works.
        cmd.env(
            "MBV_METRICS__ADDRESS",
            format!("127.0.0.1:{}", self.metrics_port),
        );
        for (key, value) in &self.env {
            cmd.env(key, value);
        }
        if std::env::var_os("RUST_LOG").is_none() {
            cmd.env("RUST_LOG", "info");
        }
        cmd
    }
}

pub struct ErOptions {
    pub label: String,
    // e.g. ("MBV_CHAINLINK__MAX_MONITORED_ACCOUNTS", "100")
    pub env: Vec<(String, String)>,
    pub request_timeout: Option<Duration>,
    // "ephemeral" (default) or "offline" (no base chain — ledger-restore reads)
    pub lifecycle: String,
    // Reuse the existing er-<label> storage dir instead of wiping it.
    pub keep_storage: bool,
    // Pass --reset (wipes the ledger, skips replay). A restore boot sets false.
    pub reset: bool,
}

impl Default for ErOptions {
    fn default() -> Self {
        Self {
            label: String::new(),
            env: Vec::new(),
            request_timeout: None,
            lifecycle: "ephemeral".to_owned(),
            keep_storage: false,
            reset: true,
        }
    }
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

    fn plan(lifecycle: &str, reset: bool) -> ErPlan {
        ErPlan {
            bin: PathBuf::from("magicblock-validator"),
            identity: Keypair::new(),
            base_rpc_url: "http://127.0.0.1:8899".to_owned(),
            base_ws_url: "ws://127.0.0.1:8900".to_owned(),
            listen_port: 7799,
            metrics_port: 7801,
            storage_dir: PathBuf::from("/tmp/er-storage"),
            env: vec![("MBV_TEST".to_owned(), "1".to_owned())],
            reset,
            lifecycle: lifecycle.to_owned(),
        }
    }

    #[test]
    fn er_plans_are_inspectable_without_spawning() {
        let args = args_of(&plan("ephemeral", true).command());
        assert_eq!(args.iter().filter(|arg| *arg == "--remotes").count(), 2);
        assert!(args.contains(&"--reset".to_owned()));
        assert!(args.contains(&"127.0.0.1:7799".to_owned()));
    }

    #[test]
    fn offline_er_plans_carry_no_remotes() {
        let args = args_of(&plan("offline", false).command());
        assert!(!args.contains(&"--remotes".to_owned()));
        assert!(!args.contains(&"--reset".to_owned()));
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
                FAMILY_PROGRAMS[0].0.to_owned(),
                PathBuf::from("/tmp/redline_program.so"),
            )],
        };
        let args = args_of(&plan.command());
        assert!(args.contains(&"--upgradeable-program".to_owned()));
        // the redline .so loads under its id plus every alias
        let loads = args.iter().filter(|arg| *arg == "--bpf-program").count();
        assert_eq!(loads, 1 + REDLINE_ALIAS_COUNT);
        assert!(args.contains(&FAMILY_PROGRAMS[0].0.to_owned()));
    }
}
