use anyhow::bail;
use hash::Hash;

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
