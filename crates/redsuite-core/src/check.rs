use std::future::Future;

use anyhow::{bail, Context};
use hash::Hash;
use tracing::{debug, instrument};

use crate::{DynError, Result};

pub fn content_hash(bytes: &[u8]) -> Hash {
    use sha2::{Digest, Sha256};
    Hash::new_from_array(Sha256::digest(bytes).into())
}

/// Requires a failed operation and returns its original error for inspection.
pub fn expect_error<T>(result: Result<T>, check: &str) -> Result<DynError> {
    match result {
        Err(error) => Ok(error),
        Ok(_) => bail!("{check}: unexpectedly succeeded"),
    }
}

/// Attach phase context to both tracing and returned errors.
#[instrument(skip_all, level = "error", fields(phase = name))]
pub async fn phase<T>(name: &str, work: impl Future<Output = Result<T>>) -> Result<T> {
    debug!("started");
    work.await.with_context(|| name.to_owned())
}
