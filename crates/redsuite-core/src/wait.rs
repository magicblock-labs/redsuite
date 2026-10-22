use std::time::Duration;

use anyhow::Context;
use tokio::{
    sync::Notify,
    time::{self, Instant},
};

use crate::Result;

const RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Retry failed attempts without cancelling an in-flight operation.
pub async fn retry<T>(
    what: &str,
    timeout: Duration,
    operation: impl AsyncFnMut() -> Result<T>,
) -> Result<T> {
    retry_every(what, timeout, RETRY_INTERVAL, operation).await
}

pub async fn retry_every<T>(
    what: &str,
    timeout: Duration,
    interval: Duration,
    mut operation: impl AsyncFnMut() -> Result<T>,
) -> Result<T> {
    let deadline = Instant::now() + timeout;
    loop {
        let error = match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if Instant::now() >= deadline {
            return Err(error).with_context(|| format!("{what} after {timeout:?}"));
        }
        time::sleep_until((Instant::now() + interval).min(deadline)).await;
    }
}

pub async fn on_change<T>(
    changed: &Notify,
    timeout: Duration,
    what: &str,
    mut observe: impl FnMut() -> Result<Option<T>>,
) -> Result<T> {
    time::timeout(timeout, async {
        loop {
            let notified = changed.notified();
            tokio::pin!(notified);
            // Register before observing: another thread may update state concurrently.
            notified.as_mut().enable();
            if let Some(value) = observe()? {
                return Ok(value);
            }
            notified.await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {what} ({timeout:?})"))?
}
