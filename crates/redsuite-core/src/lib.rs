pub mod api;
pub mod catalog;
pub mod check;
pub mod context;
pub mod dlp;
pub mod host;
pub mod loader_v4;
pub mod manifest;
pub mod mdp;
pub mod monitor;
pub mod prep;
pub mod profile;
pub mod receipt;
pub mod report;
mod resources;
pub mod runner;
pub mod scenario;
pub mod stats;
pub mod system;
pub mod topology;
pub mod transport;

pub use api::{Api, Metrics, MetricsDelta};
pub use check::CheckError;
pub use context::{BaseCtx, ChainCtx, ErClient, ErCtx, TxSender};
pub use report::ScenarioReport;
pub use scenario::{
    run_private_er_scenario, run_shared_scenario, Phase, PhaseOutcome,
    PrivateErScenario, RunError, RunRecord, Scenario, ScenarioOutcome,
};

pub type DynError = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, DynError>;
