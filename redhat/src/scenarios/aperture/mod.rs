use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(30);

mod admission;
mod authority;
mod fixture;
mod protocol;
mod simulation;
mod structure;

pub use admission::TransactionAdmission;
pub use authority::TransactionAuthority;
pub use protocol::ProtocolInputs;
pub use simulation::SimulationIsolation;
pub use structure::TransactionStructure;
