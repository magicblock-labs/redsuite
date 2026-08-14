use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    rc::Rc,
    time::Duration,
};

use base64::Engine;
use json::{Deserialize, Serialize};
use keypair::Keypair;
use pubkey::Pubkey;
use signer::Signer;

use crate::{
    api::Api,
    context::{BaseCtx, ChainCtx, ErCtx},
    host::proc_running,
    resources::ResourceRecord,
    Result,
};

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

const CLONE_URL_ENV: &str = "REDSUITE_CLONE_URL";
const DEFAULT_CLONE_URL: &str = "https://api.mainnet-beta.solana.com";

const PROTOCOL_FEES_VAULT_ID: &str =
    "7JrkjmZPprHwtuvtuGTXp9hwfGYFAQLnLeFM52kqAgXg";
const CLONED_ACCOUNTS: &[&str] = &[PROTOCOL_FEES_VAULT_ID];

pub fn er_identity_keypair() -> Result<Keypair> {
    let state = read_state()
        .ok_or("no shared stack state — boot the shared stack first")?;
    Keypair::try_from(&state.er_identity_keypair[..]).map_err(|e| {
        format!("corrupt state.json: bad er_identity_keypair: {e}").into()
    })
}

const VAULT_SPACE: usize = 8;
const VAULT_LAMPORTS: u64 = 10_000_000;

const IDENTITY_POOL_SIZE: usize = 32;
const POOL_MAP_FILE: &str = "identity-pool.json";
const POOL_LOCK_FILE: &str = "identity-pool.lock";

fn vault_dump_json(vault: &Pubkey) -> String {
    let data =
        base64::engine::general_purpose::STANDARD.encode([0u8; VAULT_SPACE]);
    format!(
        r#"{{"pubkey":"{vault}","account":{{"lamports":{VAULT_LAMPORTS},"data":["{data}","base64"],"owner":"{DLP_ID}","executable":false,"rentEpoch":0,"space":{VAULT_SPACE}}}}}"#
    )
}

fn write_vault_dump(dir: &Path, identity: &Pubkey) -> Result<()> {
    let vault = crate::dlp::validator_fees_vault_pda(identity);
    fs::write(dir.join(format!("{vault}.json")), vault_dump_json(&vault))?;
    Ok(())
}

pub fn identity_for_label(label: &str) -> Result<Keypair> {
    let state = read_state()
        .ok_or("no shared stack state — boot the shared stack first")?;
    if state.er_identity_pool.is_empty() {
        return Err("this stack predates per-ER identities — run \
                    `redsuite stack down` and boot it again"
            .into());
    }
    let dir = stack_dir();
    let _guard = PoolLock::acquire(&dir)?;
    let map_path = dir.join(POOL_MAP_FILE);
    let mut assignments: BTreeMap<String, usize> =
        fs::read_to_string(&map_path)
            .ok()
            .and_then(|raw| json::from_str(&raw).ok())
            .unwrap_or_default();

    let slot = match assignments.get(label) {
        Some(slot) => *slot,
        None => {
            let slot = assignments.len();
            if slot >= state.er_identity_pool.len() {
                return Err(format!(
                    "the stack reserved {} private-ER identities and all are \
                     bound; raise IDENTITY_POOL_SIZE and boot a fresh stack",
                    state.er_identity_pool.len()
                )
                .into());
            }
            assignments.insert(label.to_owned(), slot);
            fs::write(&map_path, json::to_string(&assignments)?)?;
            slot
        }
    };
    Keypair::try_from(&state.er_identity_pool[slot][..]).map_err(|e| {
        format!("corrupt state.json: bad pool identity {slot}: {e}").into()
    })
}

struct PoolLock(#[allow(dead_code)] fs::File);

impl PoolLock {
    fn acquire(dir: &Path) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(POOL_LOCK_FILE))?;
        file.lock()?;
        Ok(Self(file))
    }
}

const BASE_BIN: &str = "solana-test-validator";
const ER_BIN: &str = "magicblock-validator";
const ER_BIN_ENV: &str = "MAGICBLOCK_VALIDATOR_BIN";

const IDENTITY_FUNDING_LAMPORTS: u64 = 20 * 1_000_000_000;

const BASE_LEDGER_SHREDS: &str = "200000";
const BASE_READY_TIMEOUT: Duration = Duration::from_secs(60);
const ER_READY_TIMEOUT: Duration = Duration::from_secs(120);
const KILL_GRACE: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(250);
// Fine cadence for restart timing, where the interval is the measurement floor.
const RESTART_POLL: Duration = Duration::from_millis(5);

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

fn state_path() -> PathBuf {
    stack_dir().join("state.json")
}

fn read_state() -> Option<StackState> {
    json::from_str(&fs::read_to_string(state_path()).ok()?).ok()
}

pub fn current_state() -> Option<StackState> {
    read_state()
}

pub fn er_bin_path() -> Result<PathBuf> {
    find_er_bin()
}

fn write_state(state: &StackState) -> Result<()> {
    let tmp = state_path().with_extension("json.tmp");
    fs::write(&tmp, json::to_string(state)?)?;
    fs::rename(&tmp, state_path())?;
    Ok(())
}

pub async fn shared() -> Result<(BaseCtx, ErCtx)> {
    let dir = stack_dir();
    fs::create_dir_all(&dir)?;
    let _lock = acquire_lock(dir.join("lock")).await?;

    let state = ensure_base().await?;
    let state = ensure_er(state).await?;
    contexts(&state)
}

pub async fn base_only() -> Result<BaseCtx> {
    let dir = stack_dir();
    fs::create_dir_all(&dir)?;
    let _lock = acquire_lock(dir.join("lock")).await?;

    let state = ensure_base().await?;
    Ok(base_ctx(&state))
}

async fn ensure_base() -> Result<StackState> {
    match read_state() {
        Some(state)
            if proc_matches(state.base_pid, &state.base_bin)
                && base_healthy(&state).await =>
        {
            eprintln!(
                "[redsuite] reusing base L1 on 127.0.0.1:{}",
                state.base_rpc_port
            );
            Ok(state)
        }
        stale => {
            if let Some(stale) = stale {
                kill_stack(&stale);
            }
            boot_base().await
        }
    }
}

async fn ensure_er(state: StackState) -> Result<StackState> {
    if state.er_pid != 0 {
        if proc_matches(state.er_pid, &state.er_bin) && er_healthy(&state).await
        {
            eprintln!(
                "[redsuite] reusing shared ER on 127.0.0.1:{}",
                state.er_rpc_port
            );
            return Ok(state);
        }
        kill_stack(&state);
        let state = boot_base().await?;
        return attach_er(state).await;
    }
    attach_er(state).await
}

fn base_ctx(state: &StackState) -> BaseCtx {
    BaseCtx::new(
        format!("http://127.0.0.1:{}", state.base_rpc_port),
        format!("ws://127.0.0.1:{}", state.base_ws_port),
    )
}

fn contexts(state: &StackState) -> Result<(BaseCtx, ErCtx)> {
    let base = base_ctx(state);
    let er = ErCtx::new(
        format!("http://127.0.0.1:{}", state.er_rpc_port),
        format!("ws://127.0.0.1:{}", state.er_ws_port),
        format!("http://127.0.0.1:{}", state.er_metrics_port),
        state
            .er_identity
            .parse::<Pubkey>()
            .map_err(|e| format!("corrupt state.json: bad er_identity: {e}"))?,
    );
    Ok((base, er))
}

fn proc_matches(pid: u32, bin: &str) -> bool {
    fs::read(format!("/proc/{pid}/cmdline"))
        .map(|cmdline| String::from_utf8_lossy(&cmdline).contains(bin))
        .unwrap_or(false)
}

async fn base_healthy(state: &StackState) -> bool {
    let base = Api::new(format!("http://127.0.0.1:{}", state.base_rpc_port));
    matches!(base.get_health().await.as_deref(), Ok("ok"))
}

async fn er_healthy(state: &StackState) -> bool {
    let er = Api::new(format!("http://127.0.0.1:{}", state.er_rpc_port));
    er.server_alive().await
}

async fn boot_base() -> Result<StackState> {
    let dir = stack_dir();
    let base_bin = find_base_bin()?;
    let er_bin = find_er_bin()?;

    let mut base_ports = PortLease::default();
    let (base_rpc_port, base_ws_port) = base_ports.pair()?;
    let base_faucet_port = base_ports.single()?;
    let base_gossip_port = base_ports.single()?;

    let identity = Keypair::new();
    let identity_pool: Vec<Keypair> =
        (0..IDENTITY_POOL_SIZE).map(|_| Keypair::new()).collect();
    let clone_url = std::env::var(CLONE_URL_ENV)
        .unwrap_or_else(|_| DEFAULT_CLONE_URL.to_owned());

    let genesis_accounts = dir.join("genesis-accounts");
    let _ = fs::remove_dir_all(&genesis_accounts);
    fs::create_dir_all(&genesis_accounts)?;
    write_vault_dump(&genesis_accounts, &identity.pubkey())?;
    for reserved in &identity_pool {
        write_vault_dump(&genesis_accounts, &reserved.pubkey())?;
    }
    let _ = fs::remove_file(dir.join(POOL_MAP_FILE));
    let _ = fs::remove_file(dir.join(POOL_LOCK_FILE));

    let mut cmd = Command::new(&base_bin);
    cmd.args(["--reset", "--log", "--bind-address", "127.0.0.1"])
        .args(["--limit-ledger-size", BASE_LEDGER_SHREDS])
        .arg("--ledger")
        .arg(dir.join("base-ledger"))
        .args(["--rpc-port", &base_rpc_port.to_string()])
        .args(["--faucet-port", &base_faucet_port.to_string()])
        .args(["--gossip-port", &base_gossip_port.to_string()])
        .args(["--url", &clone_url]);
    cmd.arg("--account-dir").arg(&genesis_accounts);
    for id in CLONED_ACCOUNTS {
        cmd.arg("--clone").arg(id);
    }
    for id in CLONED_UPGRADEABLE_PROGRAMS {
        cmd.arg("--clone-upgradeable-program").arg(id);
    }
    for id in CLONED_LEGACY_PROGRAMS {
        cmd.arg("--clone").arg(id);
    }
    let redshift_so =
        workspace_root().join("target/deploy/redshift_program.so");
    if redshift_so.exists() {
        let (v3_id, v3_authority) = redshift_loader_v3_target();
        cmd.arg("--upgradeable-program")
            .arg(v3_id.to_string())
            .arg(&redshift_so)
            .arg(v3_authority.to_string());
    }
    let redline_so = workspace_root().join("target/deploy/redline_program.so");
    if redline_so.exists() {
        for (id, authority) in redline_loader_v3_pair() {
            cmd.arg("--upgradeable-program")
                .arg(id.to_string())
                .arg(&redline_so)
                .arg(authority.to_string());
        }
    }
    for (id, so) in base_programs(&er_bin)? {
        for alias in redline_aliases_of(&so) {
            cmd.arg("--bpf-program").arg(alias.to_string()).arg(&so);
        }
        cmd.arg("--bpf-program").arg(id).arg(so);
    }
    eprintln!(
        "[redsuite] booting base L1 on 127.0.0.1:{base_rpc_port} \
         (cloning from {clone_url}) …"
    );
    let base_log = dir.join("base.log");
    base_ports.release();
    let base_pid = spawn_detached(cmd, &base_log)?;

    let state = StackState {
        base_rpc_port,
        base_ws_port,
        base_faucet_port,
        base_gossip_port,
        base_pid,
        base_bin: bin_name(&base_bin),
        er_rpc_port: 0,
        er_ws_port: 0,
        er_metrics_port: 0,
        er_pid: 0,
        er_bin: bin_name(&er_bin),
        er_identity: identity.pubkey().to_string(),
        er_identity_keypair: identity.to_bytes().to_vec(),
        er_identity_pool: identity_pool
            .iter()
            .map(|reserved| reserved.to_bytes().to_vec())
            .collect(),
        clone_url,
    };

    match await_base_ready(&state, &base_log).await {
        Ok(()) => {
            write_state(&state)?;
            eprintln!(
                "[redsuite] base up: 127.0.0.1:{} (ws {}), identity {}",
                state.base_rpc_port, state.base_ws_port, state.er_identity,
            );
            Ok(state)
        }
        Err(e) => {
            kill_pid(base_pid);
            Err(e)
        }
    }
}

async fn await_base_ready(state: &StackState, base_log: &Path) -> Result<()> {
    let base_api =
        Api::new(format!("http://127.0.0.1:{}", state.base_rpc_port));
    wait_until(
        BASE_READY_TIMEOUT,
        "base L1 RPC healthy",
        base_log,
        state.base_pid,
        || async { matches!(base_api.get_health().await.as_deref(), Ok("ok")) },
    )
    .await?;
    // The ER dials the base WS first and exits if it cannot connect.
    wait_until(
        Duration::from_secs(15),
        "base L1 WS listening",
        base_log,
        state.base_pid,
        || async {
            tokio::net::TcpStream::connect(("127.0.0.1", state.base_ws_port))
                .await
                .is_ok()
        },
    )
    .await?;

    // getHealth answers "ok" mid-genesis; dlp is only invocable once slots tick
    wait_until(
        Duration::from_secs(30),
        "base L1 past genesis (confirmed slot >= 2)",
        base_log,
        state.base_pid,
        || async { matches!(base_api.get_slot().await, Ok(slot) if slot >= 2) },
    )
    .await?;

    let base = base_ctx(state);
    let mut required = vec![crate::dlp::validator_fees_vault_pda(
        &state.er_identity.parse()?,
    )];
    for id in CLONED_ACCOUNTS {
        required.push(id.parse()?);
    }
    for reserved in &state.er_identity_pool {
        let reserved = Keypair::try_from(&reserved[..])
            .map_err(|e| format!("corrupt pool identity: {e}"))?;
        required.push(crate::dlp::validator_fees_vault_pda(&reserved.pubkey()));
    }
    for account in required {
        if base.account(&account).await?.is_none() {
            return Err(format!(
                "account {account} missing on the freshly booted base — \
                 genesis injection or the clone from {} failed; check \
                 {CLONE_URL_ENV}",
                state.clone_url
            )
            .into());
        }
    }
    Ok(())
}

async fn attach_er(state: StackState) -> Result<StackState> {
    let dir = stack_dir();
    let er_bin = find_er_bin()?;
    let identity =
        Keypair::try_from(&state.er_identity_keypair[..]).map_err(|e| {
            format!("corrupt state.json: bad er_identity_keypair: {e}")
        })?;
    let base = base_ctx(&state);
    ensure_identity_funded(&base, &identity.pubkey()).await?;

    let mut er_ports = PortLease::default();
    let (er_rpc_port, er_ws_port) = er_ports.pair()?;
    let er_metrics_port = er_ports.single()?;

    // a fresh base is a new chain — prior-generation ER state is invalid
    let _ = fs::remove_dir_all(dir.join("er-storage"));

    let cmd = er_command(
        &er_bin,
        &identity,
        &format!("http://127.0.0.1:{}", state.base_rpc_port),
        &format!("ws://127.0.0.1:{}", state.base_ws_port),
        er_rpc_port,
        er_metrics_port,
        &dir.join("er-storage"),
        &[],
        true,
    );
    eprintln!("[redsuite] booting ER on 127.0.0.1:{er_rpc_port} …");
    let er_log = dir.join("er.log");
    er_ports.release();
    let er_pid = spawn_detached(cmd, &er_log)?;

    let er_api = Api::new(format!("http://127.0.0.1:{er_rpc_port}"));
    let ready = wait_until(
        ER_READY_TIMEOUT,
        "ER RPC answering",
        &er_log,
        er_pid,
        || async { er_api.server_alive().await },
    )
    .await;
    if let Err(e) = ready {
        kill_pid(er_pid);
        return Err(e);
    }

    let state = StackState {
        er_rpc_port,
        er_ws_port,
        er_metrics_port,
        er_pid,
        er_bin: bin_name(&er_bin),
        ..state
    };
    write_state(&state)?;
    eprintln!(
        "[redsuite] stack up: base 127.0.0.1:{} (ws {}), er 127.0.0.1:{} (ws {}, metrics {}), identity {}",
        state.base_rpc_port,
        state.base_ws_port,
        state.er_rpc_port,
        state.er_ws_port,
        state.er_metrics_port,
        state.er_identity,
    );
    Ok(state)
}

#[allow(clippy::too_many_arguments)]
fn er_command(
    er_bin: &Path,
    identity: &Keypair,
    base_rpc_url: &str,
    base_ws_url: &str,
    listen_port: u16,
    metrics_port: u16,
    storage_dir: &Path,
    extra_env: &[(String, String)],
    reset: bool,
) -> Command {
    er_command_with_lifecycle(
        er_bin,
        identity,
        base_rpc_url,
        base_ws_url,
        listen_port,
        metrics_port,
        storage_dir,
        extra_env,
        reset,
        "ephemeral",
    )
}

// An offline validator serves its restored ledger with no base chain; it
// gets no --remotes.
#[allow(clippy::too_many_arguments)]
fn er_command_with_lifecycle(
    er_bin: &Path,
    identity: &Keypair,
    base_rpc_url: &str,
    base_ws_url: &str,
    listen_port: u16,
    metrics_port: u16,
    storage_dir: &Path,
    extra_env: &[(String, String)],
    reset: bool,
    lifecycle: &str,
) -> Command {
    let mut cmd = Command::new(er_bin);
    if lifecycle != "offline" {
        cmd.arg("--remotes")
            .arg(base_rpc_url)
            .arg("--remotes")
            .arg(base_ws_url);
    }
    cmd.args(["--lifecycle", lifecycle])
        .arg("-l")
        .arg(format!("127.0.0.1:{listen_port}"))
        .arg("-k")
        .arg(identity.to_base58_string()) // throwaway test identity
        .arg("--storage")
        .arg(storage_dir);
    // --reset wipes only the ledger (rocksdb) and skips replay; it preserves
    // the accountsdb. A restart-in-place relaunch omits it so the ER reopens
    // the on-disk ledger + accountsdb it already has.
    if reset {
        cmd.arg("--reset");
    }
    cmd.arg("--no-tui");
    // Not `-m`: the CLI overlay feeds a bare string where a MetricsConfig
    // struct is expected and the validator exits — the env path works.
    cmd.env("MBV_METRICS__ADDRESS", format!("127.0.0.1:{metrics_port}"));
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    if std::env::var_os("RUST_LOG").is_none() {
        cmd.env("RUST_LOG", "info");
    }
    cmd
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

#[derive(Debug, Clone)]
pub struct RestartTiming {
    // signal sent → old process exited (the graceful drain under load)
    pub shutdown: Duration,
    // relaunch → /health/primary 200 (reopen + replay the DB)
    pub startup: Duration,
    // signal sent → serving again
    pub total: Duration,
    // a graceful SIGTERM stop had to escalate to SIGKILL (it hung)
    pub needed_sigkill: bool,
    // clean SIGTERM shutdown is Some(0)
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub slot_before: Option<u64>,
    pub slot_after: Option<u64>,
}

pub struct PrivateEr {
    pid: u32,
    label: String,
    identity: Keypair,
    er_bin: PathBuf,
    base_rpc_url: String,
    base_ws_url: String,
    rpc_port: u16,
    metrics_port: u16,
    env: Vec<(String, String)>,
    lifecycle: String,
    storage_dir: PathBuf,
    log: PathBuf,
    child: Option<Child>,
    ctx: ErCtx,
    record: Rc<ResourceRecord>,
}

impl PrivateEr {
    pub fn ctx(&self) -> &ErCtx {
        &self.ctx
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn storage_dir(&self) -> &Path {
        &self.storage_dir
    }

    // Current boot's log. After a restart the prior boot's log (with the
    // shutdown timing lines) is at log().with_extension("log.prev").
    pub fn log(&self) -> &Path {
        &self.log
    }

    fn rpc_api(&self) -> Api {
        Api::new(format!("http://127.0.0.1:{}", self.rpc_port))
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let api = self.rpc_api();
        wait_until(
            timeout,
            "private ER reaching /health/primary",
            &self.log,
            self.pid,
            || api.primary_ready(),
        )
        .await
    }

    // Stop the ER without a relaunch. hard_kill=true sends SIGKILL — the
    // crash path the ledger-restore scenarios use so nothing flushes on the
    // way down.
    pub async fn stop(&mut self, hard_kill: bool) -> Result<()> {
        let mut child = self
            .child
            .take()
            .ok_or("private ER has no running process to stop")?;
        send_signal(self.pid, if hard_kill { "-KILL" } else { "-TERM" });
        let grace_deadline = std::time::Instant::now() + KILL_GRACE;
        let mut escalated = hard_kill;
        loop {
            if child.try_wait()?.is_some() {
                self.record.mark_finished();
                return Ok(());
            }
            if !escalated && std::time::Instant::now() >= grace_deadline {
                send_signal(self.pid, "-KILL");
                escalated = true;
            }
            tokio::time::sleep(RESTART_POLL).await;
        }
    }

    // The explicit teardown path: a graceful stop whose failure lands in the
    // run's teardown audit, not only in the caller's return value.
    pub async fn finish(mut self) -> Result<()> {
        let outcome = self.stop(false).await;
        if let Err(error) = &outcome {
            self.record.record_finish_error(error.to_string());
        }
        outcome
    }

    // Stop the ER (SIGTERM, or SIGKILL if hard_kill), then relaunch it on the
    // same storage dir, identity and ports, timing each phase. Ports are
    // reused, so ctx() stays valid across the restart.
    pub async fn restart(
        &mut self,
        config: RestartConfig,
    ) -> Result<RestartTiming> {
        let api = self.rpc_api();
        let slot_before = api.get_slot().await.ok();
        let mut child = self
            .child
            .take()
            .ok_or("private ER has no running process to restart")?;

        let restart_started = std::time::Instant::now();
        send_signal(self.pid, if config.hard_kill { "-KILL" } else { "-TERM" });
        let grace_deadline = std::time::Instant::now() + KILL_GRACE;
        let mut needed_sigkill = false;
        let exit_status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if !config.hard_kill
                && !needed_sigkill
                && std::time::Instant::now() >= grace_deadline
            {
                send_signal(self.pid, "-KILL");
                needed_sigkill = true;
            }
            tokio::time::sleep(RESTART_POLL).await;
        };
        let shutdown = restart_started.elapsed();
        let exit_code = exit_status.code();
        let exit_signal =
            std::os::unix::process::ExitStatusExt::signal(&exit_status);

        let cmd = er_command_with_lifecycle(
            &self.er_bin,
            &self.identity,
            &self.base_rpc_url,
            &self.base_ws_url,
            self.rpc_port,
            self.metrics_port,
            &self.storage_dir,
            &self.env,
            config.reset,
            &self.lifecycle,
        );
        let launch_started = std::time::Instant::now();
        let new_child = spawn_child(cmd, &self.log)?;
        self.pid = new_child.id();
        self.child = Some(new_child);
        self.record.relaunched(self.pid);
        wait_until_every(
            RESTART_POLL,
            config.ready_timeout,
            "restarted ER reaching /health/primary",
            &self.log,
            self.pid,
            || api.primary_ready(),
        )
        .await?;
        let startup = launch_started.elapsed();
        let total = restart_started.elapsed();
        let slot_after = api.get_slot().await.ok();

        Ok(RestartTiming {
            shutdown,
            startup,
            total,
            needed_sigkill,
            exit_code,
            exit_signal,
            slot_before,
            slot_after,
        })
    }
}

impl Drop for PrivateEr {
    fn drop(&mut self) {
        if self.child.is_none() && !proc_running(self.pid) {
            return;
        }
        eprintln!(
            "[redsuite] stopping private ER `{}` (pid {})",
            self.label, self.pid
        );
        kill_pid(self.pid);
        if let Some(mut child) = self.child.take() {
            let _ = child.wait();
            self.record.mark_finished();
        }
    }
}

// The identity SPENDS concurrently (vault init, mdp sync), so the confirm
// polls the ABSOLUTE funding target — an exact-increment poll can never
// land while the balance moves under it.
async fn ensure_identity_funded(
    base: &BaseCtx,
    identity: &pubkey::Pubkey,
) -> Result<()> {
    let funded = || async {
        base.api().get_balance(identity).await.unwrap_or(0)
            >= IDENTITY_FUNDING_LAMPORTS
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut ticks = 0u32;
    loop {
        if funded().await {
            return Ok(());
        }
        // Re-request on a cadence — a busy freshly-booted base can drop the
        // faucet transaction, so one request is not enough.
        if ticks.is_multiple_of(20) {
            let _ = base
                .api()
                .request_airdrop(identity, IDENTITY_FUNDING_LAMPORTS)
                .await;
        }
        ticks += 1;
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "the identity {identity} did not reach the funding target"
            )
            .into());
        }
        tokio::time::sleep(POLL).await;
    }
}

const MAGIC_FEE_VAULT_TIMEOUT: Duration = Duration::from_secs(30);

// On a fresh stack generation the shared ER creates the magic fee vault in a
// startup-background task. A private ER that boots before the vault is on
// base races that init, loses with "Invalid account owner", and exits.
async fn await_magic_fee_vault(
    base: &BaseCtx,
    identity: &Pubkey,
) -> Result<()> {
    let vault = crate::dlp::magic_fee_vault_pda(identity);
    let deadline = tokio::time::Instant::now() + MAGIC_FEE_VAULT_TIMEOUT;
    loop {
        if let Ok(Some(account)) = base.account(&vault).await {
            if account.owner == crate::dlp::dlp_id() {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "the magic fee vault {vault} for identity {identity} is not \
                 on base — the er did not create it in time"
            )
            .into());
        }
        tokio::time::sleep(POLL).await;
    }
}

pub async fn private_er(
    base: &BaseCtx,
    options: ErOptions,
) -> Result<PrivateEr> {
    let dir = stack_dir();
    fs::create_dir_all(&dir)?;
    let er_bin = find_er_bin()?;
    let identity = identity_for_label(&options.label)?;
    if options.lifecycle == "ephemeral" {
        ensure_identity_funded(base, &identity.pubkey()).await?;
    }

    let mut ports = PortLease::default();
    let (rpc_port, ws_port) = ports.pair()?;
    let metrics_port = ports.single()?;
    let storage_dir = dir.join(format!("er-{}", options.label));
    if !options.keep_storage {
        let _ = fs::remove_dir_all(&storage_dir);
    }
    let log = dir.join(format!("er-{}.log", options.label));
    let base_rpc_url = base.api().url().to_owned();
    let base_ws_url = base.ws_url().to_owned();
    let cmd = er_command_with_lifecycle(
        &er_bin,
        &identity,
        &base_rpc_url,
        &base_ws_url,
        rpc_port,
        metrics_port,
        &storage_dir,
        &options.env,
        options.reset,
        &options.lifecycle,
    );
    eprintln!(
        "[redsuite] booting private ER `{}` on 127.0.0.1:{rpc_port} …",
        options.label
    );
    ports.release();
    let child = spawn_child(cmd, &log)?;
    let pid = child.id();
    let record = base.resources().register(&options.label, pid);

    let er_api = Api::new(format!("http://127.0.0.1:{rpc_port}"));
    let ready = wait_until(
        ER_READY_TIMEOUT,
        "private ER RPC answering",
        &log,
        pid,
        || async { er_api.server_alive().await },
    )
    .await;
    if let Err(e) = ready {
        kill_pid(pid);
        return Err(e);
    }

    if options.lifecycle == "ephemeral" {
        if let Err(e) = await_magic_fee_vault(base, &identity.pubkey()).await {
            kill_pid(pid);
            return Err(e);
        }
    }

    let ctx = ErCtx::new_with_timeout(
        format!("http://127.0.0.1:{rpc_port}"),
        format!("ws://127.0.0.1:{ws_port}"),
        format!("http://127.0.0.1:{metrics_port}"),
        identity.pubkey(),
        options.request_timeout,
    );
    Ok(PrivateEr {
        pid,
        label: options.label,
        identity,
        er_bin,
        base_rpc_url,
        base_ws_url,
        rpc_port,
        metrics_port,
        env: options.env,
        lifecycle: options.lifecycle,
        storage_dir,
        log,
        child: Some(child),
        ctx,
        record,
    })
}

async fn wait_until<F, Fut>(
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

async fn wait_until_every<F, Fut>(
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

fn base_programs(er_bin: &Path) -> Result<Vec<(String, PathBuf)>> {
    let root = workspace_root();
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

fn find_base_bin() -> Result<PathBuf> {
    which(BASE_BIN).ok_or_else(|| {
        format!("{BASE_BIN} not found — put the Solana CLI on PATH").into()
    })
}

fn find_er_bin() -> Result<PathBuf> {
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

fn which(bin: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(bin))
        .find(|candidate| candidate.is_file())
}

fn bin_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

// Fresh process group so test-runner group signals don't reap the validator;
// the prior boot's log is rotated to .log.prev.
fn spawn_child(mut cmd: Command, log: &Path) -> Result<Child> {
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

fn spawn_detached(cmd: Command, log: &Path) -> Result<u32> {
    Ok(spawn_child(cmd, log)?.id())
}

fn send_signal(pid: u32, signal: &str) {
    let _ = Command::new("kill")
        .args([signal, &pid.to_string()])
        .status();
}

fn kill_pid(pid: u32) {
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

fn kill_stack(state: &StackState) {
    let procs = [
        (state.er_pid, state.er_bin.as_str()),
        (state.base_pid, state.base_bin.as_str()),
    ];
    for (pid, bin) in procs {
        if pid != 0 && proc_matches(pid, bin) {
            let _ = Command::new("kill").arg(pid.to_string()).status();
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
        if pid != 0 && proc_matches(pid, bin) {
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
    }
    let _ = fs::remove_file(state_path());
}

fn tail(path: &Path, lines: usize) -> String {
    let Ok(content) = fs::read_to_string(path) else {
        return format!("<no log at {}>", path.display());
    };
    let all: Vec<&str> = content.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

#[derive(Default)]
struct PortLease {
    holders: Vec<std::net::TcpListener>,
}

impl PortLease {
    fn single(&mut self) -> Result<u16> {
        let holder = std::net::TcpListener::bind(("127.0.0.1", 0))?;
        let port = holder.local_addr()?.port();
        self.holders.push(holder);
        Ok(port)
    }

    fn pair(&mut self) -> Result<(u16, u16)> {
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

    fn release(&mut self) {
        self.holders.clear();
    }
}

struct LockGuard(#[allow(dead_code)] std::fs::File);

async fn acquire_lock(path: PathBuf) -> Result<LockGuard> {
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

fn rpc_listening(port: u16) -> bool {
    use std::net::{SocketAddr, TcpStream};
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()
}

pub fn status() -> Result<()> {
    let Some(state) = read_state() else {
        println!(
            "no shared stack ({} absent) — the first scenario boots it",
            state_path().display()
        );
        return Ok(());
    };
    let describe = |name: &str, pid: u32, bin: &str, port: u16| {
        let proc_state = if proc_matches(pid, bin) {
            "running"
        } else {
            "DEAD"
        };
        let rpc_state = if rpc_listening(port) {
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
    println!("logs          {}", stack_dir().display());
    Ok(())
}

pub fn down() -> Result<()> {
    let Some(state) = read_state() else {
        println!("no shared stack to stop");
        return Ok(());
    };
    for (name, pid, bin) in [
        ("er", state.er_pid, state.er_bin.as_str()),
        ("base", state.base_pid, state.base_bin.as_str()),
    ] {
        if proc_matches(pid, bin) {
            println!("stopping {name} (pid {pid})");
            kill_pid(pid);
        }
    }
    fs::remove_file(state_path())?;
    println!("stack down");
    Ok(())
}
