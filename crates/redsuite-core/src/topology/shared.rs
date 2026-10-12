use std::{fs, path::Path};

use keypair::Keypair;
use signer::Signer;

use super::{config, identity, process, state, state::StackState};
use crate::{
    api::Api,
    console,
    context::{BaseCtx, ChainCtx},
    profile::ExecutionConfig,
    Result,
};

pub fn running_base_programs() -> Option<Vec<String>> {
    let state = state::read_state()?;
    process::proc_matches(state.base_pid, &state.base_bin)
        .then_some(state.base_programs)
}

pub async fn base_only(config: ExecutionConfig) -> Result<BaseCtx> {
    let _lock = state::acquire_lock().await?;

    let state = ensure_base(config).await?;
    Ok(base_ctx(&state, config))
}

async fn ensure_base(config: ExecutionConfig) -> Result<StackState> {
    match state::read_state() {
        Some(state)
            if process::proc_matches(state.base_pid, &state.base_bin)
                && base_healthy(&state).await =>
        {
            console::debug(format_args!(
                "reusing base L1 on 127.0.0.1:{}",
                state.base_rpc_port
            ));
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

fn base_ctx(state: &StackState, config: ExecutionConfig) -> BaseCtx {
    BaseCtx::new(
        format!("http://127.0.0.1:{}", state.base_rpc_port),
        format!("ws://127.0.0.1:{}", state.base_ws_port),
        (state.base_grpc_port != 0).then(|| {
            (
                format!("grpc://127.0.0.1:{}", state.base_grpc_port),
                state.base_plugin.clone(),
            )
        }),
        config,
    )
}

async fn base_healthy(state: &StackState) -> bool {
    let base = Api::new(format!("http://127.0.0.1:{}", state.base_rpc_port));
    matches!(base.get_health().await.as_deref(), Ok("ok"))
}

async fn boot_base(config: ExecutionConfig) -> Result<StackState> {
    let dir = state::stack_dir();
    let base_bin = config::find_base_bin()?;
    let er_bin = config::er_bin_path()?;

    let mut base_ports = process::PortLease::default();
    let (base_rpc_port, base_ws_port) = base_ports.pair()?;
    let base_faucet_port = base_ports.single()?;
    let base_gossip_port = base_ports.single()?;
    let feed_port = base_ports.single()?;

    let identity_pool: Vec<Keypair> = (0..identity::IDENTITY_POOL_SIZE)
        .map(|_| Keypair::new())
        .collect();
    let clone_url = config::clone_url();
    let base_log = dir.join("base.log");
    let mut feed = Some(feed_port);

    loop {
        let plan = config::BasePlan::gather(
            base_bin.clone(),
            &er_bin,
            &dir,
            base_rpc_port,
            base_faucet_port,
            base_gossip_port,
            feed,
        )
        .await?;
        let _ = fs::remove_dir_all(&plan.genesis_accounts);
        fs::create_dir_all(&plan.genesis_accounts)?;
        for reserved in &identity_pool {
            identity::write_vault_dump(
                &plan.genesis_accounts,
                &reserved.pubkey(),
            )?;
        }
        identity::reset_pool(&dir);
        console::debug(format_args!(
            "booting base L1 on 127.0.0.1:{base_rpc_port} (cloning from \
             {clone_url}) …"
        ));
        base_ports.release();
        let base_pid = process::spawn_detached(plan.command(), &base_log)?;

        let state = StackState {
            base_rpc_port,
            base_ws_port,
            base_faucet_port,
            base_gossip_port,
            base_grpc_port: if plan.plugin.is_empty() { 0 } else { feed_port },
            base_plugin: plan.plugin.clone(),
            base_pid,
            base_bin: config::bin_name(&plan.bin),
            er_identity_pool: identity_pool
                .iter()
                .map(|reserved| reserved.to_bytes().to_vec())
                .collect(),
            clone_url: clone_url.clone(),
            base_programs: plan.loaded_fixtures.clone(),
        };

        match await_base_ready(&state, &base_log, config).await {
            Ok(()) => {
                state::write_state(&state)?;
                console::debug(format_args!(
                    "base up: 127.0.0.1:{} (ws {}, grpc {})",
                    state.base_rpc_port,
                    state.base_ws_port,
                    state.base_grpc_port,
                ));
                return Ok(state);
            }
            Err(e) => {
                let e = base_ports.failure(e);
                process::kill_pid(base_pid);
                if state.base_grpc_port == 0 {
                    return Err(e);
                }
                console::line(format_args!(
                    "base L1 refused to serve with the Yellowstone plugin \
                     ({}): {e}; retrying without the gRPC feed",
                    state.base_plugin
                ));
                feed = None;
            }
        }
    }
}

async fn await_base_ready(
    state: &StackState,
    base_log: &Path,
    config: ExecutionConfig,
) -> Result<()> {
    let mut listeners = vec![("base L1 WS listening", state.base_ws_port)];
    if state.base_grpc_port != 0 {
        listeners.push(("base L1 Yellowstone feed", state.base_grpc_port));
    }
    process::await_base_serving(
        &format!("http://127.0.0.1:{}", state.base_rpc_port),
        base_log,
        state.base_pid,
        // The ER dials the base WS first and exits if it cannot connect.
        &listeners,
    )
    .await?;

    let base = base_ctx(state, config);
    let mut required = Vec::new();
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

fn kill_stack(state: &StackState) {
    process::kill_matching(&[(state.base_pid, state.base_bin.as_str())]);
    state::remove_state();
}
