use std::{fs, path::Path, time::Duration};

use keypair::Keypair;
use pubkey::Pubkey;
use signer::Signer;

use super::{config, identity, process, state, state::StackState};
use crate::{
    api::Api,
    context::{BaseCtx, ChainCtx, ErCtx},
    profile::ExecutionConfig,
    Result,
};

pub async fn shared(config: ExecutionConfig) -> Result<(BaseCtx, ErCtx)> {
    let dir = state::stack_dir();
    fs::create_dir_all(&dir)?;
    let _lock = state::acquire_lock(dir.join("lock")).await?;

    let state = ensure_base(config).await?;
    let state = ensure_er(state, config).await?;
    contexts(&state, config)
}

pub fn running_base_programs() -> Option<Vec<String>> {
    let state = state::read_state()?;
    process::proc_matches(state.base_pid, &state.base_bin)
        .then_some(state.base_programs)
}

pub async fn base_only(config: ExecutionConfig) -> Result<BaseCtx> {
    let dir = state::stack_dir();
    fs::create_dir_all(&dir)?;
    let _lock = state::acquire_lock(dir.join("lock")).await?;

    let state = ensure_base(config).await?;
    Ok(base_ctx(&state, config))
}

async fn ensure_base(config: ExecutionConfig) -> Result<StackState> {
    match state::read_state() {
        Some(state)
            if process::proc_matches(state.base_pid, &state.base_bin)
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
            boot_base(config).await
        }
    }
}

async fn ensure_er(
    state: StackState,
    config: ExecutionConfig,
) -> Result<StackState> {
    if state.er_pid != 0 {
        if process::proc_matches(state.er_pid, &state.er_bin)
            && er_healthy(&state).await
        {
            eprintln!(
                "[redsuite] reusing shared ER on 127.0.0.1:{}",
                state.er_rpc_port
            );
            return Ok(state);
        }
        kill_stack(&state);
        let state = boot_base(config).await?;
        return attach_er(state, config).await;
    }
    attach_er(state, config).await
}

fn base_ctx(state: &StackState, config: ExecutionConfig) -> BaseCtx {
    BaseCtx::new(
        format!("http://127.0.0.1:{}", state.base_rpc_port),
        format!("ws://127.0.0.1:{}", state.base_ws_port),
        config,
    )
}

fn contexts(
    state: &StackState,
    config: ExecutionConfig,
) -> Result<(BaseCtx, ErCtx)> {
    let base = base_ctx(state, config);
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

async fn base_healthy(state: &StackState) -> bool {
    let base = Api::new(format!("http://127.0.0.1:{}", state.base_rpc_port));
    matches!(base.get_health().await.as_deref(), Ok("ok"))
}

async fn er_healthy(state: &StackState) -> bool {
    let er = Api::new(format!("http://127.0.0.1:{}", state.er_rpc_port));
    er.server_alive().await
}

async fn boot_base(config: ExecutionConfig) -> Result<StackState> {
    let dir = state::stack_dir();
    let base_bin = config::find_base_bin()?;
    let er_bin = config::find_er_bin()?;

    let mut base_ports = process::PortLease::default();
    let (base_rpc_port, base_ws_port) = base_ports.pair()?;
    let base_faucet_port = base_ports.single()?;
    let base_gossip_port = base_ports.single()?;

    let identity = Keypair::new();
    let identity_pool: Vec<Keypair> = (0..identity::IDENTITY_POOL_SIZE)
        .map(|_| Keypair::new())
        .collect();
    let clone_url = config::clone_url();

    let plan = config::BasePlan::gather(
        base_bin,
        &er_bin,
        &dir,
        base_rpc_port,
        base_faucet_port,
        base_gossip_port,
        clone_url.clone(),
    )?;
    let _ = fs::remove_dir_all(&plan.genesis_accounts);
    fs::create_dir_all(&plan.genesis_accounts)?;
    identity::write_vault_dump(&plan.genesis_accounts, &identity.pubkey())?;
    for reserved in &identity_pool {
        identity::write_vault_dump(&plan.genesis_accounts, &reserved.pubkey())?;
    }
    identity::reset_pool(&dir);
    eprintln!(
        "[redsuite] booting base L1 on 127.0.0.1:{base_rpc_port} \
         (cloning from {clone_url}) …"
    );
    let base_log = dir.join("base.log");
    base_ports.release();
    let base_pid = process::spawn_detached(plan.command(), &base_log)?;

    let state = StackState {
        base_rpc_port,
        base_ws_port,
        base_faucet_port,
        base_gossip_port,
        base_pid,
        base_bin: config::bin_name(&plan.bin),
        er_rpc_port: 0,
        er_ws_port: 0,
        er_metrics_port: 0,
        er_pid: 0,
        er_bin: config::bin_name(&er_bin),
        er_identity: identity.pubkey().to_string(),
        er_identity_keypair: identity.to_bytes().to_vec(),
        er_identity_pool: identity_pool
            .iter()
            .map(|reserved| reserved.to_bytes().to_vec())
            .collect(),
        clone_url,
        base_programs: plan.loaded_fixtures.clone(),
    };

    match await_base_ready(&state, &base_log, config).await {
        Ok(()) => {
            state::write_state(&state)?;
            eprintln!(
                "[redsuite] base up: 127.0.0.1:{} (ws {}), identity {}",
                state.base_rpc_port, state.base_ws_port, state.er_identity,
            );
            Ok(state)
        }
        Err(e) => {
            process::kill_pid(base_pid);
            Err(e)
        }
    }
}

async fn await_base_ready(
    state: &StackState,
    base_log: &Path,
    config: ExecutionConfig,
) -> Result<()> {
    let base_api =
        Api::new(format!("http://127.0.0.1:{}", state.base_rpc_port));
    process::wait_until(
        config::BASE_READY_TIMEOUT,
        "base L1 RPC healthy",
        base_log,
        state.base_pid,
        || async { matches!(base_api.get_health().await.as_deref(), Ok("ok")) },
    )
    .await?;
    // The ER dials the base WS first and exits if it cannot connect.
    process::wait_until(
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
    process::wait_until(
        Duration::from_secs(30),
        "base L1 past genesis (confirmed slot >= 2)",
        base_log,
        state.base_pid,
        || async { matches!(base_api.get_slot().await, Ok(slot) if slot >= 2) },
    )
    .await?;

    let base = base_ctx(state, config);
    let mut required = vec![crate::dlp::validator_fees_vault_pda(
        &state.er_identity.parse()?,
    )];
    for id in config::CLONED_ACCOUNTS {
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
                 {}",
                state.clone_url,
                config::CLONE_URL_ENV
            )
            .into());
        }
    }
    Ok(())
}

async fn attach_er(
    state: StackState,
    config: ExecutionConfig,
) -> Result<StackState> {
    let dir = state::stack_dir();
    let er_bin = config::find_er_bin()?;
    let er_identity = Keypair::try_from(&state.er_identity_keypair[..])
        .map_err(|e| {
            format!("corrupt state.json: bad er_identity_keypair: {e}")
        })?;
    let base = base_ctx(&state, config);
    identity::ensure_identity_funded(&base, &er_identity.pubkey()).await?;

    let mut er_ports = process::PortLease::default();
    let (er_rpc_port, er_ws_port) = er_ports.pair()?;
    let er_metrics_port = er_ports.single()?;

    // a fresh base is a new chain — prior-generation ER state is invalid
    let _ = fs::remove_dir_all(dir.join("er-storage"));

    let plan = config::ErPlan {
        bin: er_bin,
        identity: er_identity,
        base_rpc_url: format!("http://127.0.0.1:{}", state.base_rpc_port),
        base_ws_url: format!("ws://127.0.0.1:{}", state.base_ws_port),
        listen_port: er_rpc_port,
        metrics_port: er_metrics_port,
        storage_dir: dir.join("er-storage"),
        env: Vec::new(),
        reset: true,
    };
    eprintln!("[redsuite] booting ER on 127.0.0.1:{er_rpc_port} …");
    let er_log = dir.join("er.log");
    er_ports.release();
    let er_pid = process::spawn_detached(plan.command(), &er_log)?;

    let er_api = Api::new(format!("http://127.0.0.1:{er_rpc_port}"));
    let ready = process::wait_until(
        config::ER_READY_TIMEOUT,
        "ER RPC answering",
        &er_log,
        er_pid,
        || async { er_api.server_alive().await },
    )
    .await;
    if let Err(e) = ready {
        process::kill_pid(er_pid);
        return Err(e);
    }

    let state = StackState {
        er_rpc_port,
        er_ws_port,
        er_metrics_port,
        er_pid,
        er_bin: config::bin_name(&plan.bin),
        ..state
    };
    state::write_state(&state)?;
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

fn kill_stack(state: &StackState) {
    process::kill_matching(&[
        (state.er_pid, state.er_bin.as_str()),
        (state.base_pid, state.base_bin.as_str()),
    ]);
    state::remove_state();
}
