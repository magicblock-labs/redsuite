use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Child,
    process::Command,
    time::Duration,
};

use keypair::Keypair;
use magicblock_config::{
    config::EngineConfig,
    types::{BindAddress, Remote, SerdePubkey},
    LeaderParams, VerifierParams,
};
use pubkey::Pubkey;
use serde::Serialize;
use signer::Signer;

use super::{process, state};
use crate::{catalog::Fixture, console, manifest, Result};

const LEDGER_SIZE_LIMIT_UNREACHABLE: u64 = 1 << 60;

pub const DLP_ID: &str = "DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh";
pub const MDP_ID: &str = "DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1";
pub const COMMITTOR_ID: &str = "ComtrB2KEaWgXsW1dhr1xYL4Ht4Bjj3gXnnL6KMdABq";
pub const HYDRA_EPHEMERAL_ID: &str =
    "eHyd5BU8QffvHi4GnXwxrK4WpS7pM2x9UGKHBWii7mf";
// The validator's committor (>= 0.13.7)
const NOOP_ID: &str = "noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV";
const MEMO_V1_ID: &str = "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo";
const MEMO_V2_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ATA_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const EATA_PROGRAM_ID: &str = "SPLxh1LVZzEkX99H6rqYizhytLWPZVV296zyYDPagv2";

const CLONED_UPGRADEABLE_PROGRAMS: &[&str] = &[
    DLP_ID,
    MDP_ID,
    NOOP_ID,
    TOKEN_PROGRAM_ID,
    EATA_PROGRAM_ID,
    HYDRA_EPHEMERAL_ID,
];
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
pub const ER_READY_TIMEOUT: Duration = Duration::from_secs(120);

pub(super) fn find_base_bin() -> Result<PathBuf> {
    which(BASE_BIN).ok_or_else(|| {
        format!("{BASE_BIN} not found — put the Solana CLI on PATH").into()
    })
}

fn explicit_bin(name: &str) -> Result<Option<PathBuf>> {
    let path = std::env::var_os(name).map(PathBuf::from);
    if let Some(path) = &path {
        if !path.exists() {
            return Err(
                format!("{name}={} does not exist", path.display()).into()
            );
        }
    }
    Ok(path)
}

pub fn er_bin_path() -> Result<PathBuf> {
    if let Some(explicit) = explicit_bin(ER_BIN_ENV)? {
        return Ok(explicit);
    }
    which(ER_BIN).ok_or_else(|| {
        format!("{ER_BIN} not found — set {ER_BIN_ENV} to the built binary or put it on PATH")
            .into()
    })
}

// The verifier ships beside the ER binary in the validator build tree; an
// explicit env override wins, PATH is the last resort.
pub(super) fn find_verifier_bin() -> Result<PathBuf> {
    if let Some(explicit) = explicit_bin(VERIFIER_BIN_ENV)? {
        return Ok(explicit);
    }
    if let Some(sibling) = er_bin_path()
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
    pub(super) geyser_plugin_config: Option<PathBuf>,
    // the Yellowstone release serving this base's gRPC feed, when it has one
    pub(super) plugin: String,
}

impl BasePlan {
    // `grpc_port` asks for a Yellowstone gRPC feed on that port: the plugin is
    // resolved and configured here, with the rest of the base's launch.
    pub(super) async fn gather(
        bin: PathBuf,
        er_bin: &Path,
        stack_dir: &Path,
        rpc_port: u16,
        faucet_port: u16,
        gossip_port: u16,
        grpc_port: Option<u16>,
    ) -> Result<Self> {
        let mut geyser_plugin_config = None;
        let mut plugin = String::new();
        if let Some(port) = grpc_port {
            let feed = async {
                let yellowstone = super::yellowstone::ensure(&bin).await?;
                let path = stack_dir.join(format!("geyser-{port}.json"));
                yellowstone
                    .write_config(&path, &format!("127.0.0.1:{port}"))?;
                Result::Ok((path, yellowstone.tag))
            }
            .await;
            match feed {
                Ok((path, tag)) => {
                    geyser_plugin_config = Some(path);
                    plugin = tag;
                }
                Err(e) => console::line(format_args!(
                    "no Yellowstone gRPC feed on this base: {e}"
                )),
            }
        }
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
            clone_url: clone_url(),
            upgradeable_v3,
            bpf_programs: staged.bpf,
            loaded_fixtures: staged.loaded_fixtures,
            geyser_plugin_config,
            plugin,
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
        if let Some(plugin) = &self.geyser_plugin_config {
            cmd.arg("--geyser-plugin-config").arg(plugin);
        }
        cmd
    }
}

pub type Settings = Box<dyn FnOnce(&mut LeaderParams)>;

fn local(port: u16) -> BindAddress {
    BindAddress(SocketAddr::from(([127, 0, 0, 1], port)))
}

pub(super) fn config_path(dir: &Path, label: &str) -> PathBuf {
    dir.join(format!("er-{label}.toml"))
}

pub(super) struct ErPorts {
    pub(super) rpc: u16,
    pub(super) metrics: u16,
    pub(super) replication: u16,
}

pub(super) fn leader_params(
    identity: Keypair,
    endpoints: &BaseEndpoints,
    ports: ErPorts,
    storage_dir: PathBuf,
    allowed_followers: Vec<Pubkey>,
    settings: Settings,
) -> Result<LeaderParams> {
    let mut params = LeaderParams {
        remotes: vec![
            Remote::Http(endpoints.rpc_url.parse()?),
            Remote::Websocket(endpoints.ws_url.parse()?, None),
        ],
        ..LeaderParams::default()
    };
    if let Some(grpc_url) = &endpoints.grpc_url {
        params
            .remotes
            .push(Remote::Grpc(with_api_key(grpc_url).parse()?));
    }
    params.aperture.listen = local(ports.rpc);
    params.metrics.address = local(ports.metrics);
    params.engine.authority = identity.into();
    // engine.accountsdb.directory defaults to a compile-time constant
    // path, not to the configured engine.ledger.directory — overriding
    // only the ledger directory leaves the accountsdb at the global
    // default. Set both, mirroring the engine's default
    // <ledger dir>/accountsdb layout under this ER's storage dir.
    params.engine.accountsdb.directory = state::accountsdb_dir(&storage_dir);
    params.engine.ledger.directory = storage_dir;
    params.engine.ledger.size_limit = LEDGER_SIZE_LIMIT_UNREACHABLE;
    params.engine.replication.bind_address = local(ports.replication);
    params.engine.replication.allowed_followers =
        allowed_followers.into_iter().map(SerdePubkey).collect();
    params.ledger.reset = true;
    settings(&mut params);
    Ok(params)
}

// Block timing must match between a leader and its followers.
pub(super) fn verifier_params(
    leader: &LeaderParams,
    identity: Keypair,
    metrics_port: u16,
    storage_dir: PathBuf,
) -> VerifierParams {
    let mut params = VerifierParams {
        metrics: Default::default(),
        engine: EngineConfig::default(),
        programs: Vec::new(),
    };
    params.metrics.address = local(metrics_port);
    params.engine.authority = identity.into();
    params.engine.accountsdb.directory = state::accountsdb_dir(&storage_dir);
    params.engine.ledger.directory = storage_dir;
    params.engine.blockstore = leader.engine.blockstore;
    params.engine.replication.upstream_address =
        *leader.engine.replication.bind_address;
    params.engine.replication.upstream_authority =
        SerdePubkey(leader.engine.authority.local.pubkey());
    params
}

// A validated launch, shared by the attached ER and every private ER;
// command() stays pure so restarts rebuild the exact same invocation.
pub(super) struct Plan<P> {
    pub(super) bin: PathBuf,
    pub(super) config_path: PathBuf,
    pub(super) params: P,
}

pub(super) type ErPlan = Plan<LeaderParams>;
pub(super) type VerifierPlan = Plan<VerifierParams>;

impl<P: Serialize> Plan<P> {
    pub(super) fn command(&self) -> Command {
        let mut cmd = Command::new(&self.bin);
        cmd.arg(&self.config_path);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("MBV_") {
                cmd.env_remove(&key);
            }
        }
        if std::env::var_os("RUST_LOG").is_none() {
            cmd.env("RUST_LOG", "info");
        }
        cmd
    }

    pub(super) fn spawn(&self, log: &Path) -> Result<Child> {
        if let Some(dir) = self.config_path.parent() {
            fs::create_dir_all(dir)?;
        }
        fs::write(&self.config_path, toml::to_string(&self.params)?)?;
        process::spawn_child(self.command(), log)
    }
}

impl ErPlan {
    pub(super) fn listen(&self) -> BindAddress {
        self.params.aperture.listen
    }

    pub(super) fn metrics_port(&self) -> u16 {
        self.params.metrics.address.port()
    }

    pub(super) fn replication_port(&self) -> u16 {
        self.params.engine.replication.bind_address.port()
    }

    pub(super) fn storage_dir(&self) -> &Path {
        &self.params.engine.ledger.directory
    }

    pub(super) fn allowed_followers(&self) -> Vec<Pubkey> {
        self.params
            .engine
            .replication
            .allowed_followers
            .iter()
            .map(|follower| follower.0)
            .collect()
    }
}

impl VerifierPlan {
    pub(super) fn identity(&self) -> Pubkey {
        self.params.engine.authority.local.pubkey()
    }

    pub(super) fn metrics_port(&self) -> u16 {
        self.params.metrics.address.port()
    }

    pub(super) fn storage_dir(&self) -> &Path {
        &self.params.engine.ledger.directory
    }

    pub(super) fn upstream_address(&self) -> SocketAddr {
        self.params.engine.replication.upstream_address
    }
}

const GRPC_API_KEY: &str = "redsuite";

fn with_api_key(grpc_url: &str) -> String {
    if grpc_url.contains('?') {
        grpc_url.to_owned()
    } else {
        format!("{grpc_url}?api-key={GRPC_API_KEY}")
    }
}

pub struct ErOptions {
    pub label: String,
    pub settings: Settings,
    pub request_timeout: Option<Duration>,
    pub base_endpoints: Option<BaseEndpoints>,
}

impl Default for ErOptions {
    fn default() -> Self {
        Self {
            label: String::new(),
            settings: Box::new(|_| {}),
            request_timeout: None,
            base_endpoints: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BaseEndpoints {
    pub rpc_url: String,
    pub ws_url: String,
    pub grpc_url: Option<String>,
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
