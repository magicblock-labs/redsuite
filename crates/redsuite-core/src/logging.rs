use std::io::{self, IsTerminal};

use tracing::Level;
use tracing_subscriber::{fmt, EnvFilter};

use crate::Result;

/// Installs the host binary's stderr subscriber; libraries leave this to callers.
/// Uses `RUST_LOG`, defaulting to info when no valid directive is supplied.
pub fn init() -> Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(Level::INFO.into())
        .from_env_lossy();
    fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .with_target(false)
        .with_ansi(io::stderr().is_terminal())
        .try_init()
}
