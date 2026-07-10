pub mod api;
pub mod assert;
pub mod context;
pub mod host;
pub mod monitor;
pub mod prep;
pub mod receipt;
pub mod report;
pub mod runner;
pub mod scenario;
pub mod stats;
pub mod topology;
pub mod transport;

pub use api::{Api, Metrics, MetricsDelta};
pub use context::{BaseCtx, ChainCtx, ErClient, ErCtx, TxSender};
pub use report::ScenarioReport;
pub use scenario::{run_scenario, Scenario};

pub type DynError = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, DynError>;
