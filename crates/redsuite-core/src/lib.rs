pub mod api;
pub mod assert;
pub mod context;
pub mod prep;
pub mod report;
pub mod scenario;
pub mod stats;
pub mod topology;
pub mod transport;

pub use api::{Api, Metrics};
pub use context::{BaseCtx, ChainCtx, ErCtx};
pub use report::ScenarioReport;
pub use scenario::{run_scenario, Scenario};

pub type DynError = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, DynError>;
