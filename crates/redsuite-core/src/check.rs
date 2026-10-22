use std::{
    fmt::{Debug, Display},
    future::Future,
    result,
    time::Duration,
};

use anyhow::{anyhow, bail, Context};
use hash::Hash;
use tokio::{time, time::Instant};
use tracing::{debug, instrument};

use crate::{DynError, Result};

const POLL_INTERVAL: Duration = Duration::from_millis(200);

pub fn content_hash(bytes: &[u8]) -> Hash {
    use sha2::{Digest, Sha256};
    Hash::new_from_array(Sha256::digest(bytes).into())
}

/// Requires a failed operation and returns its original error for inspection.
pub fn expect_error<T: Debug>(result: Result<T>, check: &str) -> Result<DynError> {
    match result {
        Err(error) => Ok(error),
        Ok(value) => bail!("{check}: unexpectedly succeeded: {value:?}"),
    }
}

pub async fn poll<Fut>(
    check: &str,
    timeout: Duration,
    mut condition: impl FnMut() -> Fut,
) -> Result<()>
where
    Fut: Future<Output = bool>,
{
    poll_until(timeout, POLL_INTERVAL, async || {
        Result::Ok(condition().await.then_some(()))
    })
    .await?
    .with_context(|| format!("{check} after {timeout:?}"))
}

pub async fn poll_for<T, Observed, Fut>(
    check: &str,
    timeout: Duration,
    mut observe: impl FnMut() -> Fut,
) -> Result<T>
where
    Observed: Display,
    Fut: Future<Output = result::Result<T, Observed>>,
{
    let mut last = None;
    poll_until(timeout, POLL_INTERVAL, async || {
        Result::Ok(match observe().await {
            Ok(value) => Some(value),
            Err(observed) => {
                last = Some(observed);
                None
            }
        })
    })
    .await?
    .ok_or_else(|| {
        anyhow!(
            "{check} after {timeout:?}: {:#}",
            last.expect("polled at least once")
        )
    })
}

// Observe once even at a zero deadline. A pending observation may be retried;
// errors return immediately, and an in-flight observation is never cancelled.
pub async fn poll_until<T, E>(
    timeout: Duration,
    interval: Duration,
    mut observe: impl AsyncFnMut() -> result::Result<Option<T>, E>,
) -> result::Result<Option<T>, E> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = observe().await? {
            return Ok(Some(value));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        time::sleep(interval).await;
    }
}

/// Attach phase context to both tracing and returned errors.
#[instrument(skip_all, level = "error", fields(phase = name))]
pub async fn phase<T>(name: &str, work: impl Future<Output = Result<T>>) -> Result<T> {
    debug!("started");
    work.await.with_context(|| name.to_owned())
}
