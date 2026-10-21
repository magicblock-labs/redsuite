use std::{fmt, future::Future, time::Duration};

use thiserror::Error;
use tracing::{debug, error_span, Instrument};

use crate::DynError;

const POLL_INTERVAL: Duration = Duration::from_millis(200);

pub fn content_hash(bytes: &[u8]) -> hash::Hash {
    use sha2::{Digest, Sha256};
    hash::Hash::new_from_array(Sha256::digest(bytes).into())
}

#[derive(Debug, Error)]
#[error("{self:?}")]
pub struct CheckError {
    pub check: String,
    pub expected: Option<String>,
    pub actual: Option<String>,
    pub context: Vec<(String, String)>,
    pub source: Option<DynError>,
}

impl CheckError {
    pub fn new(check: impl Into<String>) -> Self {
        Self {
            check: check.into(),
            expected: None,
            actual: None,
            context: Vec::new(),
            source: None,
        }
    }

    pub fn expected(mut self, value: impl Into<String>) -> Self {
        self.expected = Some(value.into());
        self
    }

    pub fn actual(mut self, value: impl Into<String>) -> Self {
        self.actual = Some(value.into());
        self
    }

    pub fn context(mut self, label: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.push((label.into(), value.into()));
        self
    }

    pub fn caused_by(mut self, source: impl Into<DynError>) -> Self {
        self.source = Some(source.into());
        self
    }
}

#[macro_export]
macro_rules! check {
    ($condition:expr, $($message:tt)+) => {
        if $condition {
            Ok(())
        } else {
            Err($crate::check::CheckError::new(format!($($message)+))
                .context("condition", stringify!($condition)))
        }
    };
}

#[macro_export]
macro_rules! check_eq {
    ($actual:expr, $expected:expr, $($message:tt)+) => {
        match (&$actual, &$expected) {
            (actual, expected) => {
                if *actual == *expected {
                    Ok(())
                } else {
                    Err($crate::check::CheckError::new(format!($($message)+))
                        .expected(format!("{expected:?}"))
                        .actual(format!("{actual:?}"))
                        .context("compared", stringify!($actual)))
                }
            }
        }
    };
}

#[macro_export]
macro_rules! check_ne {
    ($actual:expr, $unwanted:expr, $($message:tt)+) => {
        match (&$actual, &$unwanted) {
            (actual, unwanted) => {
                if *actual != *unwanted {
                    Ok(())
                } else {
                    Err($crate::check::CheckError::new(format!($($message)+))
                        .expected(format!("not {unwanted:?}"))
                        .actual(format!("{actual:?}"))
                        .context("compared", stringify!($actual)))
                }
            }
        }
    };
}

pub async fn poll<Fut>(
    check: &str,
    timeout: Duration,
    mut condition: impl FnMut() -> Fut,
) -> Result<(), CheckError>
where
    Fut: Future<Output = bool>,
{
    poll_until(timeout, POLL_INTERVAL, async || {
        Ok::<_, CheckError>(condition().await.then_some(()))
    })
    .await?
    .ok_or_else(|| CheckError::new(check).context("waited", format!("{timeout:?}")))
}

pub async fn poll_for<T, Observed, Fut>(
    check: &str,
    timeout: Duration,
    mut observe: impl FnMut() -> Fut,
) -> Result<T, CheckError>
where
    Observed: fmt::Debug,
    Fut: Future<Output = std::result::Result<T, Observed>>,
{
    let mut last = None;
    poll_until(timeout, POLL_INTERVAL, async || {
        Ok::<_, CheckError>(match observe().await {
            Ok(value) => Some(value),
            Err(observed) => {
                last = Some(observed);
                None
            }
        })
    })
    .await?
    .ok_or_else(|| {
        CheckError::new(check)
            .actual(format!("{:?}", last.expect("polled at least once")))
            .context("waited", format!("{timeout:?}"))
    })
}

// Observe once even at a zero deadline. A pending observation may be retried;
// errors return immediately, and an in-flight observation is never cancelled.
pub async fn poll_until<T, E>(
    timeout: Duration,
    interval: Duration,
    mut observe: impl AsyncFnMut() -> Result<Option<T>, E>,
) -> Result<Option<T>, E> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = observe().await? {
            return Ok(Some(value));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(interval).await;
    }
}

/// Attach phase context to both tracing and returned errors.
/// Check errors retain their type so reports preserve assertion details.
pub async fn phase<T>(
    name: &str,
    work: impl Future<Output = crate::Result<T>>,
) -> crate::Result<T> {
    debug!(phase = name);
    work.instrument(error_span!("phase", phase = name)).await.map_err(|error| {
        match error.downcast::<CheckError>() {
            Ok(error) => error.context("phase", name).into(),
            Err(error) => format!("{name}: {error}").into(),
        }
    })
}
