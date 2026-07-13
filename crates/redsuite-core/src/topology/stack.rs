//! The shared stack: one base L1 + one ER, booted by the first scenario that
//! needs it, reused by every scenario in every run until `cargo xtask stack
//! down` (an unhealthy stack is killed and rebooted transparently).
//!
//! nextest runs each test in its own process, so reuse is coordinated across
//! processes: `state.json` records ports + PIDs, an exclusive flock
//! serializes `shared()`, and validators are spawned into their own process
//! group to outlive the tests. Isolation comes from fresh keypairs, not
//! fresh chains; future restart scenarios must use private topologies.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use json::{Deserialize, Serialize};
use keypair::Keypair;
use pubkey::Pubkey;
use signer::Signer;

use crate::{
    api::Api,
    context::{BaseCtx, ChainCtx, ErCtx},
    Result,
};

pub const DLP_ID: &str = "DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh";
pub const MDP_ID: &str = "DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1";
pub const COMMITTOR_ID: &str = "ComtrB2KEaWgXsW1dhr1xYL4Ht4Bjj3gXnnL6KMdABq";

/// Keep in sync with `declare_id!` in `programs/*/src/lib.rs`.
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

/// `mAGicPQYBMvcYveUZA5F5UNNwyHvfYh5xkLS2Fr1mev` — not random: the
/// `base-accounts/` vault fixtures are dlp PDAs of this identity, and dlp
/// refuses `InitMagicFeeVault` unless the validator-fees-vault pre-exists.
const TEST_IDENTITY_KEYPAIR: [u8; 64] = [
    7, 83, 184, 55, 200, 223, 238, 137, 166, 244, 107, 126, 189, 16, 194, 36,
    228, 68, 43, 143, 13, 91, 3, 81, 53, 253, 26, 36, 50, 198, 40, 159, 11, 80,
    9, 208, 183, 189, 108, 200, 89, 77, 168, 76, 233, 197, 132, 22, 21, 186,
    202, 240, 105, 168, 157, 64, 233, 249, 100, 104, 210, 41, 83, 87,
];

const BASE_BIN: &str = "solana-test-validator";
const ER_BIN: &str = "magicblock-validator";
const ER_BIN_ENV: &str = "MAGICBLOCK_VALIDATOR_BIN";

/// ER startup gate: identity must hold ≥ 5 SOL on the base. Fund with headroom.
const IDENTITY_FUNDING_LAMPORTS: u64 = 20 * 1_000_000_000;

const BASE_LEDGER_SHREDS: &str = "200000";
const BASE_READY_TIMEOUT: Duration = Duration::from_secs(60);
const ER_READY_TIMEOUT: Duration = Duration::from_secs(120);
const KILL_GRACE: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackState {
    pub base_rpc_port: u16,
    pub base_ws_port: u16,
    pub base_faucet_port: u16,
    pub base_gossip_port: u16,
    pub base_pid: u32,
    /// Binary file name — guards recorded PIDs against PID reuse.
    pub base_bin: String,
    pub er_rpc_port: u16,
    pub er_ws_port: u16,
    pub er_metrics_port: u16,
    pub er_pid: u32,
    pub er_bin: String,
    pub er_identity: String,
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

    let state = match read_state() {
        Some(state) if alive(&state) && healthy(&state).await => {
            eprintln!(
                "[redsuite] reusing shared stack: base 127.0.0.1:{}, er 127.0.0.1:{}",
                state.base_rpc_port, state.er_rpc_port
            );
            state
        }
        stale => {
            if let Some(stale) = stale {
                kill_stack(&stale);
            }
            boot().await?
        }
    };
    contexts(&state)
}

fn contexts(state: &StackState) -> Result<(BaseCtx, ErCtx)> {
    let base = BaseCtx::new(
        format!("http://127.0.0.1:{}", state.base_rpc_port),
        format!("ws://127.0.0.1:{}", state.base_ws_port),
    );
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

/// Reuse-path liveness: PID alive *and* still the recorded binary (guards
/// against PID recycling; only sound after the exec is known to be done).
fn proc_matches(pid: u32, bin: &str) -> bool {
    fs::read(format!("/proc/{pid}/cmdline"))
        .map(|cmdline| String::from_utf8_lossy(&cmdline).contains(bin))
        .unwrap_or(false)
}

/// Boot-path liveness: existence + non-zombie. Right after `spawn()` the
/// cmdline may still show the parent image (exec not done), and recycling is
/// impossible while we hold the child un-reaped.
fn proc_running(pid: u32) -> bool {
    let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    // `pid (comm) S …` — comm may contain anything, so find the last ')'.
    let state = stat
        .rfind(')')
        .and_then(|paren_at| stat[paren_at + 1..].trim_start().chars().next());
    !matches!(state, Some('Z') | None)
}

fn alive(state: &StackState) -> bool {
    proc_matches(state.base_pid, &state.base_bin)
        && proc_matches(state.er_pid, &state.er_bin)
}

async fn healthy(state: &StackState) -> bool {
    let base = Api::new(format!("http://127.0.0.1:{}", state.base_rpc_port));
    let er = Api::new(format!("http://127.0.0.1:{}", state.er_rpc_port));
    matches!(base.get_health().await.as_deref(), Ok("ok"))
        && er.server_alive().await
}

async fn boot() -> Result<StackState> {
    let dir = stack_dir();
    let base_bin = find_base_bin()?;
    let er_bin = find_er_bin()?;

    let (base_rpc_port, base_ws_port) = free_port_pair()?;
    let base_faucet_port = free_port()?;
    let base_gossip_port = free_port()?;
    let (er_rpc_port, er_ws_port) = free_port_pair()?;
    let er_metrics_port = free_port()?;

    let identity = Keypair::try_from(&TEST_IDENTITY_KEYPAIR[..])
        .map_err(|e| format!("bad TEST_IDENTITY_KEYPAIR: {e}"))?;

    let mut cmd = Command::new(&base_bin);
    cmd.args(["--reset", "--log", "--bind-address", "127.0.0.1"])
        .args(["--limit-ledger-size", BASE_LEDGER_SHREDS])
        .arg("--ledger")
        .arg(dir.join("base-ledger"))
        .args(["--rpc-port", &base_rpc_port.to_string()])
        .args(["--faucet-port", &base_faucet_port.to_string()])
        .args(["--gossip-port", &base_gossip_port.to_string()]);
    for (id, so) in base_programs(&er_bin)? {
        cmd.arg("--bpf-program").arg(id).arg(so);
    }
    for (pubkey, file) in base_accounts()? {
        cmd.arg("--account").arg(pubkey).arg(file);
    }
    eprintln!("[redsuite] booting base L1 on 127.0.0.1:{base_rpc_port} …");
    let base_log = dir.join("base.log");
    let base_pid = spawn_detached(cmd, &base_log)?;

    let state = StackState {
        base_rpc_port,
        base_ws_port,
        base_faucet_port,
        base_gossip_port,
        base_pid,
        base_bin: bin_name(&base_bin),
        er_rpc_port,
        er_ws_port,
        er_metrics_port,
        er_pid: 0, // patched below once the ER is spawned
        er_bin: bin_name(&er_bin),
        er_identity: identity.pubkey().to_string(),
    };

    match boot_er(&dir, &er_bin, &identity, &state, &base_log).await {
        Ok(er_pid) => {
            let state = StackState { er_pid, ..state };
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
        Err(e) => {
            // No state.json yet; boot_er reaped the ER, the base is ours.
            kill_pid(state.base_pid);
            Err(e)
        }
    }
}

async fn boot_er(
    dir: &Path,
    er_bin: &Path,
    identity: &Keypair,
    state: &StackState,
    base_log: &Path,
) -> Result<u32> {
    let base_rpc_url = format!("http://127.0.0.1:{}", state.base_rpc_port);

    // The ER dials the base WS first and exits if it cannot connect.
    let base_api = Api::new(base_rpc_url.clone());
    wait_until(
        BASE_READY_TIMEOUT,
        "base L1 RPC healthy",
        base_log,
        state.base_pid,
        || async { matches!(base_api.get_health().await.as_deref(), Ok("ok")) },
    )
    .await?;
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

    let base_ctx = BaseCtx::new(
        base_rpc_url,
        format!("ws://127.0.0.1:{}", state.base_ws_port),
    );
    ensure_identity_funded(&base_ctx, &identity.pubkey()).await?;

    // a fresh base is a new chain — prior-generation ER state is invalid
    let _ = fs::remove_dir_all(dir.join("er-storage"));

    let cmd = er_command(
        er_bin,
        identity,
        &format!("http://127.0.0.1:{}", state.base_rpc_port),
        &format!("ws://127.0.0.1:{}", state.base_ws_port),
        state.er_rpc_port,
        state.er_metrics_port,
        &dir.join("er-storage"),
        &[],
    );
    eprintln!("[redsuite] booting ER on 127.0.0.1:{} …", state.er_rpc_port);
    let er_log = dir.join("er.log");
    let er_pid = spawn_detached(cmd, &er_log)?;

    let er_api = Api::new(format!("http://127.0.0.1:{}", state.er_rpc_port));
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

    Ok(er_pid)
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
) -> Command {
    let mut cmd = Command::new(er_bin);
    cmd.arg("--remotes")
        .arg(base_rpc_url)
        .arg("--remotes")
        .arg(base_ws_url)
        .args(["--lifecycle", "ephemeral"])
        .arg("-l")
        .arg(format!("127.0.0.1:{listen_port}"))
        .arg("-k")
        .arg(identity.to_base58_string()) // throwaway test identity
        .arg("--storage")
        .arg(storage_dir)
        .arg("--reset")
        .arg("--no-tui");
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
}

pub struct PrivateEr {
    pid: u32,
    label: String,
    storage_dir: PathBuf,
    ctx: ErCtx,
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
}

impl Drop for PrivateEr {
    fn drop(&mut self) {
        eprintln!(
            "[redsuite] stopping private ER `{}` (pid {})",
            self.label, self.pid
        );
        kill_pid(self.pid);
    }
}

async fn ensure_identity_funded(
    base: &BaseCtx,
    identity: &pubkey::Pubkey,
) -> Result<()> {
    let balance = base.api().get_balance(identity).await.unwrap_or(0);
    if balance >= IDENTITY_FUNDING_LAMPORTS {
        return Ok(());
    }
    base.airdrop(identity, IDENTITY_FUNDING_LAMPORTS).await
}

pub async fn private_er(
    base: &BaseCtx,
    options: ErOptions,
) -> Result<PrivateEr> {
    let dir = stack_dir();
    fs::create_dir_all(&dir)?;
    let er_bin = find_er_bin()?;
    let identity = Keypair::try_from(&TEST_IDENTITY_KEYPAIR[..])
        .map_err(|e| format!("bad TEST_IDENTITY_KEYPAIR: {e}"))?;
    ensure_identity_funded(base, &identity.pubkey()).await?;

    let (rpc_port, ws_port) = free_port_pair()?;
    let metrics_port = free_port()?;
    let storage_dir = dir.join(format!("er-{}", options.label));
    let _ = fs::remove_dir_all(&storage_dir);
    let log = dir.join(format!("er-{}.log", options.label));
    let cmd = er_command(
        &er_bin,
        &identity,
        base.api().url(),
        base.ws_url(),
        rpc_port,
        metrics_port,
        &storage_dir,
        &options.env,
    );
    eprintln!(
        "[redsuite] booting private ER `{}` on 127.0.0.1:{rpc_port} …",
        options.label
    );
    let pid = spawn_detached(cmd, &log)?;

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
        storage_dir,
        ctx,
    })
}

async fn wait_until<F, Fut>(
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
        tokio::time::sleep(POLL).await;
    }
}

fn base_programs(er_bin: &Path) -> Result<Vec<(String, PathBuf)>> {
    let root = workspace_root();
    let mut programs = Vec::new();

    for (id, name) in [(DLP_ID, "dlp.so"), (MDP_ID, "mdp.so")] {
        let so = root.join("base-programs").join(name);
        if !so.exists() {
            return Err(format!(
                "missing pinned base program {}",
                so.display()
            )
            .into());
        }
        programs.push((id.to_owned(), so));
    }

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

#[derive(Deserialize)]
struct AccountFixture {
    pubkey: String,
}

/// `base-accounts/*.json` — the ER cannot boot without them, see
/// base-accounts/README.md.
fn base_accounts() -> Result<Vec<(String, PathBuf)>> {
    let dir = workspace_root().join("base-accounts");
    let mut entries: Vec<_> = fs::read_dir(&dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    let mut accounts = Vec::new();
    for entry in entries {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            let fixture: AccountFixture =
                json::from_str(&fs::read_to_string(&path)?)
                    .map_err(|e| format!("{}: {e}", path.display()))?;
            accounts.push((fixture.pubkey, path));
        }
    }
    if accounts.is_empty() {
        return Err(
            format!("no account fixtures found in {}", dir.display()).into()
        );
    }
    Ok(accounts)
}

/// Version-coupled to the ER, so taken from the ER binary's own build tree.
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

/// Fresh process group so test-runner group signals don't reap the validator;
/// the log is truncated on each boot.
fn spawn_detached(mut cmd: Command, log: &Path) -> Result<u32> {
    use std::os::unix::process::CommandExt;
    if log.exists() {
        let _ = fs::rename(log, log.with_extension("log.prev"));
    }
    let logfile = fs::File::create(log)?;
    cmd.stdout(logfile.try_clone()?)
        .stderr(logfile)
        .stdin(Stdio::null())
        .process_group(0);
    let child = cmd.spawn().map_err(|e| {
        format!(
            "failed to spawn {}: {e}",
            cmd.get_program().to_string_lossy()
        )
    })?;
    Ok(child.id())
}

/// Boot-error cleanup: the pid is a freshly spawned child of ours, so no
/// cmdline gating (which would race the exec).
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

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind(("127.0.0.1", 0))?
        .local_addr()?
        .port())
}

/// Adjacent pair — both validators hardwire WS = RPC + 1.
fn free_port_pair() -> Result<(u16, u16)> {
    for _ in 0..64 {
        let holder = std::net::TcpListener::bind(("127.0.0.1", 0))?;
        let port = holder.local_addr()?.port();
        if port == u16::MAX {
            continue;
        }
        if std::net::TcpListener::bind(("127.0.0.1", port + 1)).is_ok() {
            return Ok((port, port + 1));
        }
    }
    Err("could not find two adjacent free ports".into())
}

struct LockGuard(#[allow(dead_code)] std::fs::File);

/// Exclusive advisory flock; released when the guard drops.
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
