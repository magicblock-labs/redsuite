use anyhow::{anyhow, ensure, Context};
use redsuite_core::api::{transaction_logs, transaction_meta};
use redsuite_core::config::ExecutionConfig;
use redsuite_core::{wait, ChainCtx, ErCtx, Result};
use signature::Signature;
use std::time::{Duration, Instant};
use tokio::sync::watch;

pub mod scenarios;
pub use redline_interface as program;

pub const ACCOUNT_SPACE: u32 = 256;

pub fn consumed_cus(logs: &[String]) -> Option<f64> {
    logs.iter().find_map(|line| {
        let (_, rest) = line.split_once(" consumed ")?;
        let (cus, tail) = rest.split_once(" of ")?;
        if !tail.contains("compute units") {
            return None;
        }
        cus.parse().ok()
    })
}

pub async fn probe_cus(er: &ErCtx, signature: &Signature, label: &str, _iters: u32) -> Result<f64> {
    let probe = er.api().await_transaction(signature).await?;
    let meta = transaction_meta(&probe)?;
    let logs = transaction_logs(meta);
    ensure!(meta.err == None);
    consumed_cus(logs).ok_or_else(|| anyhow!("{label}: missing compute-unit consumption: {logs:?}"))
}

pub mod metrics {
    pub const ENGINE_TRANSACTIONS: &str = "engine_ledger_transactions";
    pub const RPC_HANDLED_TRANSACTIONS: &str = "mbv_transaction_processing_time_count";
    pub const RPC_ACCEPTED_TRANSACTIONS: &str = "mbv_transaction_skip_preflight_count";
    pub const FAILED_TRANSACTIONS: &str = "engine_processor_failed_transactions";
}

pub async fn await_executed(er: &ErCtx, target: f64, timeout: Duration) -> Result<Duration> {
    let started = Instant::now();
    wait::retry("validator transaction count", timeout, async || {
        let metrics = er.scrape_metrics().await?;
        let count = metrics
            .get(metrics::ENGINE_TRANSACTIONS)
            .context("transaction count metric missing")?;
        ensure!(count >= target);
        Ok(())
    })
    .await?;
    Ok(started.elapsed())
}

pub(crate) struct RunControl {
    pub duration: Duration,
    pub stop: watch::Receiver<bool>,
}
impl RunControl {
    pub(crate) fn from_config(config: &ExecutionConfig) -> Result<Self> {
        Ok(Self {
            duration: config.duration.context("Redline requires --duration")?,
            stop: config.stop.clone().context("missing benchmark stop signal")?,
        })
    }
    pub(crate) fn deadline(&self) -> Result<Instant> {
        Instant::now()
            .checked_add(self.duration)
            .context("duration exceeds monotonic clock range")
    }
    pub(crate) fn stopped(&self, deadline: Instant) -> bool {
        Instant::now() >= deadline || *self.stop.borrow()
    }

    pub(crate) fn completion(&self, result: &Result<()>) -> &'static str {
        if *self.stop.borrow() {
            "interrupted"
        } else if result.is_ok() {
            "completed"
        } else {
            "failed"
        }
    }
}
