pub mod api;
pub mod catalog;
pub mod cells;
pub mod config;
pub mod context;
pub mod dlp;
pub mod frontend;
pub mod host;
pub mod logging;
pub mod netfault;
pub mod prep;
pub mod programs;
pub mod random;
pub mod receipt;
pub mod redline;
pub mod report;
mod resources;
pub mod runner;
pub mod sampler;
pub mod scenario;
pub mod signed;
pub mod stats;
pub mod system;
pub mod topology;
pub mod transport;
pub mod wait;

pub use anyhow::Error as DynError;
pub use api::{Api, Metrics, MetricsDelta, SendBody};
pub use context::{BaseCtx, ChainCtx, ErClient, ErCtx, TxSender};
pub use report::ScenarioReport;
pub use resources::LaunchRecord;
pub use scenario::{
    run_private_er_scenario, run_shared_scenario, PrivateErScenario, RunRecord, Scenario,
};
pub use solana_account_decoder_client_types as account_types;
pub use solana_commitment_config as commitment;
pub use solana_rpc_client_types as rpc_types;
pub use solana_transaction_status_client_types as transaction_types;

#[doc(hidden)]
pub use anyhow::ensure as __ensure;
#[doc(hidden)]
pub use const_str as __const_str;

pub type Result<T> = anyhow::Result<T>;

/// Requires a failed operation and returns its original error for inspection.
pub fn expect_error<T>(result: Result<T>, check: &str) -> Result<DynError> {
    match result {
        Err(error) => Ok(error),
        Ok(_) => anyhow::bail!("{check}: unexpectedly succeeded"),
    }
}
