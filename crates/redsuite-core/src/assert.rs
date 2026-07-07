use std::{future::Future, time::Duration};

const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Polls `condition` until it returns true; panics once `timeout` elapses.
pub async fn poll_until<F, Fut>(timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("poll_until: condition not met within {timeout:?}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
