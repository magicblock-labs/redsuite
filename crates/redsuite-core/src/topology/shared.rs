use std::{fs, path::Path};

use anyhow::{ensure, Context};
use keypair::Keypair;
use signer::Signer;
use tracing::debug;
use url::Url;

use super::{
    config, config::BasePlan, identity, process, process::PortLease, state, state::StackState,
};
use crate::{
    api::Api,
    context::{BaseCtx, ChainCtx},
    profile::ExecutionConfig,
    Result,
};

pub fn running_base_programs() -> Option<Vec<String>> {
    let state = state::read_state()?;
    process::proc_matches(state.base_pid, &state.base_bin).then_some(state.base_programs)
}

pub async fn base_only(config: ExecutionConfig) -> Result<BaseCtx> {
    let _lock = state::acquire_lock().await?;

    let state = ensure_base(config).await?;
    base_ctx(&state, config)
}

async fn ensure_base(config: ExecutionConfig) -> Result<StackState> {
    match state::read_state() {
        Some(state)
            if process::proc_matches(state.base_pid, &state.base_bin)
                && state.base_grpc_port != 0
                && base_healthy(&state).await =>
        {
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

fn base_ctx(state: &StackState, config: ExecutionConfig) -> Result<BaseCtx> {
    Ok(BaseCtx::new(
        format!("http://127.0.0.1:{}", state.base_rpc_port).parse()?,
        format!("ws://127.0.0.1:{}", state.base_ws_port).parse()?,
        Some((
            format!("grpc://127.0.0.1:{}", state.base_grpc_port).parse()?,
            state.base_plugin.clone(),
        )),
        config,
    ))
}

async fn base_healthy(state: &StackState) -> bool {
    let Ok(url) = Url::parse(&format!("http://127.0.0.1:{}", state.base_rpc_port)) else {
        return false;
    };
    let base = Api::new(
        url,
        format!("ws://127.0.0.1:{}", state.base_ws_port).parse().unwrap(),
    );
    matches!(base.get_health().await.as_deref(), Ok("ok"))
}

async fn boot_base(config: ExecutionConfig) -> Result<StackState> {
    let dir = state::stack_dir();
    let base_bin = config::find_base_bin()?;
    let er_bin = config::er_bin_path()?;

    let mut base_ports = PortLease::default();
    let (base_rpc_port, base_ws_port) = base_ports.pair()?;
    let base_faucet_port = base_ports.single()?;
    let base_gossip_port = base_ports.single()?;
    let feed_port = base_ports.single()?;

    let identity_pool: Vec<Keypair> =
        (0..identity::IDENTITY_POOL_SIZE).map(|_| Keypair::new()).collect();
    let base_log = dir.join("base.log");
    let plan = BasePlan::gather(
        base_bin,
        &er_bin,
        &dir,
        base_rpc_port,
        base_faucet_port,
        base_gossip_port,
        feed_port,
    )
    .await?;
    let _ = fs::remove_dir_all(&plan.genesis_accounts);
    fs::create_dir_all(&plan.genesis_accounts)?;
    for reserved in &identity_pool {
        identity::write_vault_dump(&plan.genesis_accounts, &reserved.pubkey())?;
    }
    identity::reset_pool(&dir);
    debug!("starting base L1");
    base_ports.release();
    let base_pid = process::spawn_detached(plan.command(), &base_log)?;
    let state = StackState {
        base_rpc_port,
        base_ws_port,
        base_faucet_port,
        base_gossip_port,
        base_grpc_port: feed_port,
        base_plugin: plan.plugin,
        base_pid,
        base_bin: config::bin_name(&plan.bin),
        er_identity_pool: identity_pool
            .iter()
            .map(|reserved| reserved.to_bytes().to_vec())
            .collect(),
        clone_url: plan.clone_url.clone(),
        base_programs: plan.loaded_fixtures,
    };
    if let Err(error) = await_base_ready(&state, &base_log, config).await {
        process::kill_pid(base_pid);
        return Err(base_ports.failure(error));
    }
    if let Err(error) = state::write_state(&state) {
        process::kill_pid(base_pid);
        return Err(error);
    }
    Ok(state)
}

async fn await_base_ready(
    state: &StackState,
    base_log: &Path,
    config: ExecutionConfig,
) -> Result<()> {
    process::await_base_serving(
        &format!("http://127.0.0.1:{}", state.base_rpc_port).parse()?,
        &format!("ws://127.0.0.1:{}", state.base_ws_port).parse()?,
        base_log,
        state.base_pid,
        // The ER dials the base WS first and exits if it cannot connect.
        &[
            ("base L1 WS listening", state.base_ws_port),
            ("base L1 Yellowstone feed", state.base_grpc_port),
        ],
    )
    .await?;

    let base = base_ctx(state, config)?;
    let mut required = config::CLONED_ACCOUNTS.to_vec();
    for reserved in &state.er_identity_pool {
        let reserved = Keypair::try_from(&reserved[..]).context("corrupt pool identity")?;
        required.push(crate::dlp::validator_fees_vault_pda(&reserved.pubkey()));
    }
    for account in required {
        ensure!(
            base.account(&account).await?.is_some(),
            "required account missing on the freshly booted base — \
                 genesis injection or the clone from {} failed; check \
                 {}",
            state.clone_url,
            config::CLONE_URL_ENV
        );
    }
    Ok(())
}

fn kill_stack(state: &StackState) {
    process::kill_matching(&[(state.base_pid, state.base_bin.as_str())]);
    state::remove_state();
}
