use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use magicblock_config::{
    config::EngineConfig,
    types::{network::ResolvedUrl, BindAddress, Remote, SerdePubkey},
    LeaderParams, VerifierParams,
};
use pubkey::Pubkey;
use serde::Serialize;
use signer::Signer;
use tracing::warn;
use url::Url;

use super::{process, state};
use crate::{catalog::Fixture, BaseCtx, ChainCtx, Result};

const LEDGER_SIZE_LIMIT_UNREACHABLE: u64 = 1 << 60;

pub const DLP_ID: Pubkey = Pubkey::from_str_const("DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh");
pub const DLP_SO_ENV: &str = "REDSUITE_DLP_SO";
const MDP_ID: Pubkey = Pubkey::from_str_const("DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1");
pub const COMMITTOR_ID: Pubkey =
    Pubkey::from_str_const("ComtrB2KEaWgXsW1dhr1xYL4Ht4Bjj3gXnnL6KMdABq");
pub const OUTBOX_INTENT_ID: Pubkey =
    Pubkey::from_str_const("outboxintent1111111111111111111111111111111");
pub const SERVICE_OWNERS: &[Pubkey] =
    &[fixture::magic::MAGIC_PROGRAM_ID, HYDRA_EPHEMERAL_ID, OUTBOX_INTENT_ID];

pub fn local_account_owners() -> Vec<Pubkey> {
    let mut owners = vec![
        crate::system::system_id(),
        crate::programs::REDSHIFT.id(),
        crate::programs::REDHAT.id(),
        crate::programs::REDLINE.id(),
        fixture::magic::MAGIC_PROGRAM_ID,
        DLP_ID,
        COMMITTOR_ID,
        HYDRA_EPHEMERAL_ID,
        OUTBOX_INTENT_ID,
        TOKEN_PROGRAM_ID,
        EATA_PROGRAM_ID,
        sdk_ids::bpf_loader::ID,
        sdk_ids::bpf_loader_deprecated::ID,
        sdk_ids::bpf_loader_upgradeable::ID,
        sdk_ids::loader_v4::ID,
        sdk_ids::native_loader::ID,
        redshift_loader_v3_target().0,
    ];
    owners.extend(redline_alias_ids(REDLINE_ALIAS_COUNT));
    owners.extend(redline_loader_v3_pair().map(|(key, _)| key));
    owners.sort_unstable();
    owners.dedup();
    owners
}

pub const HYDRA_EPHEMERAL_ID: Pubkey =
    Pubkey::from_str_const("eHyd5BU8QffvHi4GnXwxrK4WpS7pM2x9UGKHBWii7mf");
// The validator's committor (>= 0.13.7)
const NOOP_ID: Pubkey = Pubkey::from_str_const("noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV");
const MEMO_V1_ID: Pubkey = Pubkey::from_str_const("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");
const MEMO_V2_ID: Pubkey = Pubkey::from_str_const("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const TOKEN_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const ATA_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
const EATA_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("SPLxh1LVZzEkX99H6rqYizhytLWPZVV296zyYDPagv2");

const CLONED_UPGRADEABLE_PROGRAMS: &[Pubkey] =
    &[DLP_ID, MDP_ID, NOOP_ID, TOKEN_PROGRAM_ID, EATA_PROGRAM_ID, HYDRA_EPHEMERAL_ID];
const CLONED_LEGACY_PROGRAMS: &[Pubkey] = &[MEMO_V1_ID, MEMO_V2_ID, ATA_PROGRAM_ID];

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
    let id = Pubkey::find_program_address(&[b"redsuite-loader-v3"], &redshift_interface::ID).0;
    let authority =
        Pubkey::find_program_address(&[b"redsuite-loader-v3-auth"], &redshift_interface::ID).0;
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

const PROTOCOL_FEES_VAULT_ID: Pubkey =
    Pubkey::from_str_const("7JrkjmZPprHwtuvtuGTXp9hwfGYFAQLnLeFM52kqAgXg");
pub(super) const CLONED_ACCOUNTS: &[Pubkey] = &[PROTOCOL_FEES_VAULT_ID];

pub(super) fn clone_url() -> Result<Url> {
    let value = env::var(CLONE_URL_ENV).unwrap_or_else(|_| DEFAULT_CLONE_URL.to_owned());
    value.parse().context(CLONE_URL_ENV)
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
    which(BASE_BIN).with_context(|| format!("{BASE_BIN} not found — put the Solana CLI on PATH"))
}

fn explicit_bin(name: &str) -> Result<Option<PathBuf>> {
    let path = env::var_os(name).map(PathBuf::from);
    if let Some(path) = &path {
        ensure!(path.exists(), "{name}={} does not exist", path.display());
    }
    Ok(path)
}

pub fn er_bin_path() -> Result<PathBuf> {
    if let Some(explicit) = explicit_bin(ER_BIN_ENV)? {
        return Ok(explicit);
    }
    which(ER_BIN).with_context(|| {
        format!("{ER_BIN} not found — set {ER_BIN_ENV} to the built binary or put it on PATH")
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
    which(VERIFIER_BIN).with_context(|| format!("{VERIFIER_BIN} not found — build it beside the ER binary (`cargo build --release -p magicblock-verifier`), set {VERIFIER_BIN_ENV}, or put it on PATH"))
}

fn which(bin: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path)
        .map(|dir| dir.join(bin))
        .find(|candidate| candidate.is_file())
}

pub(super) fn bin_name(path: &Path) -> String {
    path.file_name().unwrap_or_default().to_string_lossy().into_owned()
}

struct StagedPrograms {
    bpf: Vec<(Pubkey, PathBuf)>,
    loaded_fixtures: Vec<String>,
}

fn base_programs(er_bin: &Path) -> StagedPrograms {
    let mut bpf = Vec::new();
    let mut loaded_fixtures = Vec::new();

    match committor_so(er_bin) {
        Some(so) => bpf.push((COMMITTOR_ID, so)),
        None => warn!(
            "magicblock_committor_program.so missing from the ER build; \
             build it with cargo build-sbf or commit scenarios will fail"
        ),
    }

    for fixture in Fixture::ALL {
        let Some(program_id) = fixture.program_id() else {
            continue;
        };
        match fixture.resolve() {
            Ok(so) => {
                bpf.push((program_id, so));
                loaded_fixtures.push(fixture.so_name().to_owned());
            }
            Err(error) => {
                warn!(
                    ?fixture,
                    error = %format_args!("{error:#}"),
                    "fixture unavailable; dependent scenarios cannot run",
                )
            }
        }
    }
    StagedPrograms { bpf, loaded_fixtures }
}

fn redline_aliases_of(so: &Path) -> Vec<Pubkey> {
    if so.file_name().is_some_and(|name| name == Fixture::RedlineProgram.so_name()) {
        redline_alias_ids(REDLINE_ALIAS_COUNT)
    } else {
        Vec::new()
    }
}

fn committor_so(er_bin: &Path) -> Option<PathBuf> {
    let deploy = er_bin.parent()?.parent()?.join("deploy/magicblock_committor_program.so");
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
    pub(super) clone_url: Url,
    // (program id, .so, upgrade authority) preloaded as loader-v3 programs
    pub(super) upgradeable_v3: Vec<(Pubkey, PathBuf, Pubkey)>,
    // (program id, .so) loaded via --bpf-program; redline gets its aliases
    pub(super) bpf_programs: Vec<(Pubkey, PathBuf)>,
    pub(super) loaded_fixtures: Vec<String>,
    pub(super) geyser_plugin_config: PathBuf,
    // The Yellowstone release serving this base's gRPC feed.
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
        grpc_port: u16,
    ) -> Result<Self> {
        let clone_url = clone_url()?;
        let yellowstone = super::yellowstone::ensure(&bin).await?;
        let geyser_plugin_config = stack_dir.join("geyser.json");
        yellowstone.write_config(&geyser_plugin_config, ([127, 0, 0, 1], grpc_port).into())?;
        let mut upgradeable_v3 = Vec::new();
        if let Some(path) = env::var_os(DLP_SO_ENV).map(PathBuf::from) {
            ensure!(
                path.is_file(),
                "{DLP_SO_ENV}={} is not a file",
                path.display()
            );
            upgradeable_v3.push((DLP_ID, fs::canonicalize(path)?, Pubkey::default()));
        }
        if let Ok(redshift_so) = Fixture::RedshiftProgram.resolve() {
            let (v3_id, v3_authority) = redshift_loader_v3_target();
            upgradeable_v3.push((v3_id, redshift_so, v3_authority));
        }
        if let Ok(redline_so) = Fixture::RedlineProgram.resolve() {
            for (id, authority) in redline_loader_v3_pair() {
                upgradeable_v3.push((id, redline_so.clone(), authority));
            }
        }
        let staged = base_programs(er_bin);
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
            geyser_plugin_config,
            plugin: yellowstone.tag,
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
            .args(["--url", self.clone_url.as_str()]);
        cmd.arg("--account-dir").arg(&self.genesis_accounts);
        for id in CLONED_ACCOUNTS {
            cmd.arg("--clone").arg(id.to_string());
        }
        for id in CLONED_UPGRADEABLE_PROGRAMS {
            if self.upgradeable_v3.iter().any(|(loaded, _, _)| loaded == id) {
                continue;
            }
            cmd.arg("--clone-upgradeable-program").arg(id.to_string());
        }
        for id in CLONED_LEGACY_PROGRAMS {
            cmd.arg("--clone").arg(id.to_string());
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
            cmd.arg("--bpf-program").arg(id.to_string()).arg(so);
        }
        cmd.arg("--geyser-plugin-config").arg(&self.geyser_plugin_config);
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
            Remote::Http(ResolvedUrl(endpoints.rpc_url.clone())),
            Remote::Websocket(ResolvedUrl(endpoints.ws_url.clone()), None),
            Remote::Grpc(ResolvedUrl(with_api_key(endpoints.grpc_url.clone()))),
        ],
        ..LeaderParams::default()
    };
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
    params.engine.ledger.size_limit = leader.engine.ledger.size_limit;
    params.engine.blockstore = leader.engine.blockstore;
    params.engine.replication.upstream_address = *leader.engine.replication.bind_address;
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
        for (key, _) in env::vars_os() {
            if key.to_string_lossy().starts_with("MBV_") {
                cmd.env_remove(&key);
            }
        }
        if env::var_os("RUST_LOG").is_none() {
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
    pub(super) fn replication_port(&self) -> u16 {
        self.params.engine.replication.bind_address.port()
    }

    pub(super) fn storage_dir(&self) -> &Path {
        &self.params.engine.ledger.directory
    }
}

impl VerifierPlan {
    pub(super) fn identity(&self) -> Pubkey {
        self.params.engine.authority.local.pubkey()
    }

    pub(super) fn storage_dir(&self) -> &Path {
        &self.params.engine.ledger.directory
    }

    pub(super) fn upstream_address(&self) -> SocketAddr {
        self.params.engine.replication.upstream_address
    }
}

const GRPC_API_KEY: &str = "redsuite";

fn with_api_key(mut grpc_url: Url) -> Url {
    if grpc_url.query().is_none() {
        grpc_url.query_pairs_mut().append_pair("api-key", GRPC_API_KEY);
    }
    grpc_url
}

pub struct ErOptions {
    pub label: String,
    pub settings: Settings,
    pub base_endpoints: Option<BaseEndpoints>,
}

impl Default for ErOptions {
    fn default() -> Self {
        Self {
            label: String::new(),
            settings: Box::new(|_| {}),
            base_endpoints: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BaseEndpoints {
    pub rpc_url: Url,
    pub ws_url: Url,
    pub grpc_url: Url,
}

impl BaseEndpoints {
    pub(crate) fn from_base(base: &BaseCtx) -> Result<Self> {
        let (grpc_url, _) = base.grpc().context("base L1 Yellowstone gRPC endpoint missing")?;
        Ok(Self {
            rpc_url: base.api().url().clone(),
            ws_url: base.ws_url().clone(),
            grpc_url: grpc_url.clone(),
        })
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
