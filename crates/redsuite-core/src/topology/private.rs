use std::{
    fs,
    path::{Path, PathBuf},
    process::Child,
    rc::Rc,
    time::Duration,
};

use pubkey::Pubkey;
use signer::Signer;

use super::{
    config,
    config::{ErOptions, RestartConfig},
    identity, process, state,
};
use crate::{
    api::Api,
    context::{BaseCtx, ChainCtx, ErCtx},
    host::proc_running,
    resources::ResourceRecord,
    Result,
};

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
    plan: config::ErPlan,
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
        &self.plan.storage_dir
    }

    // Current boot's log. After a restart the prior boot's log (with the
    // shutdown timing lines) is at log().with_extension("log.prev").
    pub fn log(&self) -> &Path {
        &self.log
    }

    fn rpc_api(&self) -> Api {
        Api::new(format!("http://127.0.0.1:{}", self.plan.listen_port))
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let api = self.rpc_api();
        process::wait_until(
            timeout,
            "private ER reaching /health/primary",
            &self.log,
            self.pid,
            || api.primary_ready(),
        )
        .await
    }

    // Stop the ER without a relaunch. hard_kill=true is the crash path the
    // ledger-restore scenarios use so nothing flushes on the way down.
    pub async fn stop(&mut self, hard_kill: bool) -> Result<()> {
        let child = self
            .child
            .as_mut()
            .ok_or("private ER has no running process to stop")?;
        process::terminate(child, self.pid, hard_kill).await?;
        self.child = None;
        self.record.mark_finished();
        Ok(())
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
        let child = self
            .child
            .as_mut()
            .ok_or("private ER has no running process to restart")?;

        let restart_started = std::time::Instant::now();
        let (exit_status, needed_sigkill) =
            process::terminate(child, self.pid, config.hard_kill).await?;
        self.child = None;
        let shutdown = restart_started.elapsed();
        let exit_code = exit_status.code();
        let exit_signal =
            std::os::unix::process::ExitStatusExt::signal(&exit_status);

        self.plan.reset = config.reset;
        let launch_started = std::time::Instant::now();
        let new_child = process::spawn_child(self.plan.command(), &self.log)?;
        self.pid = new_child.id();
        self.child = Some(new_child);
        self.record.relaunched(self.pid);
        process::wait_until_every(
            process::RESTART_POLL,
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
        process::kill_pid(self.pid);
        if let Some(mut child) = self.child.take() {
            let _ = child.wait();
            self.record.mark_finished();
        }
    }
}

const MAGIC_FEE_VAULT_TIMEOUT: Duration = Duration::from_secs(30);
const VAULT_POLL: Duration = Duration::from_millis(250);

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
        tokio::time::sleep(VAULT_POLL).await;
    }
}

fn abort_boot(mut child: Child, pid: u32, record: &Rc<ResourceRecord>) {
    process::kill_pid(pid);
    let _ = child.wait();
    record.mark_finished();
}

pub async fn private_er(
    base: &BaseCtx,
    options: ErOptions,
) -> Result<PrivateEr> {
    let dir = state::stack_dir();
    fs::create_dir_all(&dir)?;
    let er_bin = config::find_er_bin()?;
    let er_identity = identity::identity_for_label(&options.label)?;
    identity::ensure_identity_funded(base, &er_identity.pubkey()).await?;

    let mut ports = process::PortLease::default();
    let (rpc_port, ws_port) = ports.pair()?;
    let metrics_port = ports.single()?;
    let storage_dir = dir.join(format!("er-{}", options.label));
    let _ = fs::remove_dir_all(&storage_dir);
    let log = dir.join(format!("er-{}.log", options.label));
    let identity_pubkey = er_identity.pubkey();
    let plan = config::ErPlan {
        bin: er_bin,
        identity: er_identity,
        base_rpc_url: base.api().url().to_owned(),
        base_ws_url: base.ws_url().to_owned(),
        listen_port: rpc_port,
        metrics_port,
        storage_dir,
        env: options.env,
        reset: true,
    };
    eprintln!(
        "[redsuite] booting private ER `{}` on 127.0.0.1:{rpc_port} …",
        options.label
    );
    ports.release();
    let child = process::spawn_child(plan.command(), &log)?;
    let pid = child.id();
    let record = base.resources().register(&options.label, pid);

    let er_api = Api::new(format!("http://127.0.0.1:{rpc_port}"));
    let ready = process::wait_until(
        config::ER_READY_TIMEOUT,
        "private ER RPC answering",
        &log,
        pid,
        || async { er_api.server_alive().await },
    )
    .await;
    if let Err(e) = ready {
        abort_boot(child, pid, &record);
        return Err(e);
    }

    if let Err(e) = await_magic_fee_vault(base, &identity_pubkey).await {
        abort_boot(child, pid, &record);
        return Err(e);
    }

    let ctx = ErCtx::new_with_timeout(
        format!("http://127.0.0.1:{rpc_port}"),
        format!("ws://127.0.0.1:{ws_port}"),
        format!("http://127.0.0.1:{metrics_port}"),
        identity_pubkey,
        options.request_timeout,
    );
    Ok(PrivateEr {
        pid,
        label: options.label,
        plan,
        log,
        child: Some(child),
        ctx,
        record,
    })
}
