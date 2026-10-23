use std::{
    fs,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    rc::Rc,
    time::Duration,
};

use anyhow::{ensure, Context};
use magicblock_config::LeaderParams;
use pubkey::Pubkey;
use signer::Signer;
use tokio::sync::Mutex;
use tracing::debug;

use super::{
    config,
    config::{BaseEndpoints, ErOptions, ErPlan, ErPorts, RestartConfig},
    identity, process,
    process::PortLease,
    state,
};
use crate::{
    api::Api,
    context::{BaseCtx, ChainCtx, ErCtx},
    netfault::BaseProxies,
    report,
    resources::LaunchRecord,
    Result,
};

#[derive(Debug, Clone)]
pub struct RestartOutcome {
    // a graceful SIGTERM stop had to escalate to SIGKILL (it hung)
    pub needed_sigkill: bool,
    // clean SIGTERM shutdown is Some(0)
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub slot_before: Option<u64>,
    pub slot_after: Option<u64>,
}

pub struct PrivateEr {
    proc: process::Owned,
    plan: ErPlan,
    log: PathBuf,
    ctx: ErCtx,
    ports: PortLease,
}

impl PrivateEr {
    pub fn ctx(&self) -> &ErCtx {
        &self.ctx
    }

    pub fn with_proxies(mut self, proxies: Rc<BaseProxies>) -> Self {
        self.ctx.proxies = Some(proxies);
        self
    }

    pub fn shared(mut self) -> Rc<Mutex<Self>> {
        Rc::new_cyclic(|owner| {
            self.ctx.owner = Some(owner.clone());
            Mutex::new(self)
        })
    }

    pub fn pid(&self) -> u32 {
        self.proc.pid()
    }

    pub fn identity(&self) -> Pubkey {
        self.ctx.identity()
    }

    pub fn replication_port(&self) -> u16 {
        self.plan.replication_port()
    }

    pub(super) fn params(&self) -> &LeaderParams {
        &self.plan.params
    }

    pub fn storage_dir(&self) -> &Path {
        self.plan.storage_dir()
    }

    // Current boot's log. After a restart the prior boot's log (with the
    // shutdown timing lines) is at log().with_extension("log.prev").
    pub fn log(&self) -> &Path {
        &self.log
    }

    fn rpc_api(&self) -> Api {
        Api::new(self.ctx.api().url().clone(), self.ctx.ws_url().clone())
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let api = self.rpc_api();
        process::wait_until(
            timeout,
            "private ER reaching /health/primary",
            &self.log,
            self.proc.pid(),
            || api.primary_ready(),
        )
        .await
        .map_err(|error| self.ports.failure(error))
    }

    // Stop the ER without a relaunch. hard_kill=true is the crash path the
    // ledger-restore scenarios use so nothing flushes on the way down.
    pub async fn stop(&mut self, hard_kill: bool) -> Result<()> {
        self.proc
            .terminate(hard_kill)
            .await?
            .context("private ER has no running process to stop")?;
        Ok(())
    }

    // Explicit teardown reports errors; Drop remains the process-killing fallback.
    pub async fn finish(mut self) -> Result<()> {
        self.proc.terminate(false).await?;
        self.remove_storage()
    }

    fn remove_storage(&self) -> Result<()> {
        state::remove_storage(self.plan.storage_dir())
            .with_context(|| format!("removing the storage of private ER `{}`", self.proc.label()))
    }

    // Stop the ER (SIGTERM, or SIGKILL if hard_kill), then relaunch it on the
    // same storage dir, identity and ports, so ctx() stays valid across the restart.
    pub async fn restart(&mut self, config: RestartConfig) -> Result<RestartOutcome> {
        let api = self.rpc_api();
        let slot_before = api.get_slot().await.ok();
        let (exit_status, needed_sigkill) = self
            .proc
            .terminate(config.hard_kill)
            .await?
            .context("private ER has no running process to restart")?;
        let exit_code = exit_status.code();
        let exit_signal = ExitStatusExt::signal(&exit_status);

        self.plan.params.ledger.reset = config.reset;
        self.proc.relaunch(self.plan.spawn(&self.log)?);
        process::wait_until_every(
            process::RESTART_POLL,
            config.ready_timeout,
            "restarted ER reaching /health/primary",
            &self.log,
            self.proc.pid(),
            || api.primary_ready(),
        )
        .await
        .map_err(|error| self.ports.failure(error))?;
        self.ctx.pid = self.proc.pid();
        self.ctx.reset_blockhash();
        let slot_after = api.get_slot().await.ok();

        Ok(RestartOutcome {
            needed_sigkill,
            exit_code,
            exit_signal,
            slot_before,
            slot_after,
        })
    }
}

// On a fresh stack generation the shared ER creates the magic fee vault in a
// startup-background task. A private ER that boots before the vault is on
// base races that init, loses with "Invalid account owner", and exits.
async fn await_magic_fee_vault(base: &BaseCtx, identity: &Pubkey) -> Result<()> {
    let vault = crate::dlp::magic_fee_vault_pda(identity);
    base.await_account(&vault, |account| {
        ensure!(account.owner == crate::dlp::dlp_id());
        Ok(())
    })
    .await?;
    Ok(())
}

pub async fn private_er(base: &BaseCtx, options: ErOptions) -> Result<PrivateEr> {
    launch(base, options, Vec::new()).await
}

// A leader is a private ER whose replication listener admits followers.
pub(super) async fn leader_er(
    base: &BaseCtx,
    options: ErOptions,
    allowed_followers: Vec<Pubkey>,
) -> Result<PrivateEr> {
    launch(base, options, allowed_followers).await
}

async fn launch(
    base: &BaseCtx,
    options: ErOptions,
    allowed_followers: Vec<Pubkey>,
) -> Result<PrivateEr> {
    let dir = state::stack_dir();
    fs::create_dir_all(&dir)?;
    let er_bin = config::er_bin_path()?;
    let er_identity = identity::identity_for_label(&options.label)?;
    identity::ensure_identity_funded(base, &er_identity.pubkey()).await?;

    let mut ports = PortLease::default();
    let (rpc_port, ws_port) = ports.pair()?;
    let metrics_port = ports.single()?;
    let replication_port = ports.single()?;
    let storage_dir = dir.join(format!("er-{}", options.label));
    state::remove_storage(&storage_dir)?;
    let log = dir.join(format!("er-{}.log", options.label));
    let identity_pubkey = er_identity.pubkey();
    let role = if allowed_followers.is_empty() { "private-er" } else { "leader" };
    let endpoints = match options.base_endpoints {
        Some(endpoints) => endpoints,
        None => BaseEndpoints::from_base(base)?,
    };
    let plan = ErPlan {
        bin: er_bin,
        config_path: config::config_path(&dir, &options.label),
        params: config::leader_params(
            er_identity,
            &endpoints,
            ErPorts {
                rpc: rpc_port,
                metrics: metrics_port,
                replication: replication_port,
            },
            storage_dir,
            allowed_followers,
            options.settings,
        )?,
    };
    debug!(%options.label, "starting rollup");
    ports.release();
    let child = plan.spawn(&log)?;
    let pid = child.id();
    let (bin_version, bin_fingerprint) = report::binary_provenance(&plan.bin);
    let record = base.resources().register_launch(LaunchRecord {
        label: options.label.clone(),
        role: role.to_owned(),
        bin: plan.bin.display().to_string(),
        bin_version,
        bin_fingerprint,
        identity: identity_pubkey.to_string(),
        rpc_port: Some(rpc_port),
        ws_port: Some(ws_port),
        metrics_port,
        replication_port: Some(replication_port),
        storage_dir: plan.storage_dir().display().to_string(),
        accountsdb_dir: state::split_accountsdb_dir(plan.storage_dir())
            .map(|dir| dir.display().to_string()),
        log: log.display().to_string(),
        pid,
        ..LaunchRecord::default()
    });

    let proc = process::Owned::new("private ER", options.label, child, record);
    let er_api = Api::new(
        format!("http://127.0.0.1:{rpc_port}").parse()?,
        format!("ws://127.0.0.1:{ws_port}").parse()?,
    );
    let ready = process::wait_until(
        config::ER_READY_TIMEOUT,
        "private ER RPC answering",
        &log,
        pid,
        || async { er_api.server_alive().await },
    )
    .await
    .map_err(|error| ports.failure(error.context(format!("private ER `{}`", proc.label()))));
    ready?;
    await_magic_fee_vault(base, &identity_pubkey).await?;

    let ctx = ErCtx::new(
        er_api.url().clone(),
        format!("ws://127.0.0.1:{ws_port}").parse()?,
        format!("http://127.0.0.1:{metrics_port}").parse()?,
        identity_pubkey,
        pid,
    );
    Ok(PrivateEr { proc, plan, log, ctx, ports })
}
