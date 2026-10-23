use std::time::Duration;

/// Shared deadline for network requests and event-driven readiness waits.
pub const NETWORK_TIMEOUT: Duration = Duration::from_secs(60);

pub mod clock;
pub mod events;
pub mod http;
pub mod rate;
pub(crate) mod rpc;
mod socket;
mod subscription;
pub mod ws;
pub mod wsraw;
