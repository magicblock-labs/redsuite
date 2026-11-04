use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(30);
const BODY_LIMIT: usize = 1024 * 1024;
const QUERY_LIMIT: usize = 100;

mod admission;
mod amplification;
mod authority;
mod fixture;
mod protocol;
mod signals;
mod simulation;
mod structure;

pub use admission::TransactionAdmission;
pub use amplification::UnpaidWork;
pub use authority::TransactionAuthority;
pub use protocol::ProtocolInputs;
pub use signals::{
    DivergenceSignals, LedgerIndexSignals, OutcomeSignals, PrunedReadSignals, SlowSubscriberSignals,
};
pub use simulation::SimulationIsolation;
pub use structure::TransactionStructure;
