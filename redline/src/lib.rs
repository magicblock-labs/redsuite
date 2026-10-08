use std::time::{Duration, Instant};

use redsuite_core::{check, ChainCtx, CheckError, ErCtx, Result};
use signature::Signature;

pub mod scenarios;
pub use redline_interface as program;
pub use redsuite_core::redline::written_id as account_update_id;

pub const ACCOUNT_SPACE: u32 = 256;
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

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

pub async fn probe_cus(
    er: &ErCtx,
    signature: &Signature,
    label: &str,
    iters: u32,
) -> Result<f64> {
    let probe = er.api().await_transaction(signature, PROBE_TIMEOUT).await?;
    check!(
        probe.err.is_none(),
        "{label}: probe tx failed on-chain (sha256 iters {iters} over the \
         compute budget?): {:?}\nlogs: {:#?}",
        probe.err,
        probe.logs
    )?;
    consumed_cus(&probe.logs).ok_or_else(|| {
        CheckError::new(format!(
            "{label}: probe logs carry no `consumed .. compute units` line"
        ))
        .actual(format!("{:#?}", probe.logs))
        .into()
    })
}

pub async fn await_executed(
    er: &ErCtx,
    target: f64,
    timeout: Duration,
) -> Result<Duration> {
    let started = Instant::now();
    check::poll(
        &format!("the validator transaction count reaches {target:.0}"),
        timeout,
        || async {
            matches!(
                er.scrape_metrics().await.ok().and_then(|metrics| metrics.get(metrics::ENGINE_TRANSACTIONS)),
                Some(count) if count >= target
            )
        },
    )
    .await?;
    Ok(started.elapsed())
}

pub mod metrics {
    pub const ENGINE_TRANSACTIONS: &str = "engine_ledger_transactions";
    pub const RPC_HANDLED_TRANSACTIONS: &str =
        "mbv_transaction_processing_time_count";
    pub const RPC_ACCEPTED_TRANSACTIONS: &str =
        "mbv_transaction_skip_preflight_count";
    pub const FAILED_TRANSACTIONS: &str =
        "engine_processor_failed_transactions";
}
