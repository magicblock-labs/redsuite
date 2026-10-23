use std::io::{self, IsTerminal};

use tracing::Level;
use tracing_error::ErrorLayer;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::Result;

/// Installs the host binary's stderr subscriber; libraries leave this to callers.
/// Uses `RUST_LOG`, defaulting to info when no valid directive is supplied.
pub fn init() -> Result<()> {
    let filter = EnvFilter::builder().with_default_directive(Level::INFO.into()).from_env_lossy();
    tracing_subscriber::registry()
        .with(filter)
        .with(ErrorLayer::default())
        .with(
            fmt::layer()
                .compact()
                .with_writer(io::stderr)
                .with_target(false)
                .with_ansi(io::stderr().is_terminal()),
        )
        .try_init()
        .map_err(Into::into)
}
