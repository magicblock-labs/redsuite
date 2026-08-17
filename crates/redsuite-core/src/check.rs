use std::{fmt, future::Future, time::Duration};

use crate::DynError;

const POLL_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Debug)]
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

    pub fn context(
        mut self,
        label: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.context.push((label.into(), value.into()));
        self
    }

    pub fn caused_by(mut self, source: impl Into<DynError>) -> Self {
        self.source = Some(source.into());
        self
    }
}

impl fmt::Display for CheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.check)?;
        match (&self.expected, &self.actual) {
            (Some(expected), Some(actual)) => {
                write!(formatter, " — expected {expected}, observed {actual}")?
            }
            (Some(expected), None) => {
                write!(formatter, " — expected {expected}")?
            }
            (None, Some(actual)) => write!(formatter, " — observed {actual}")?,
            (None, None) => {}
        }
        for (label, value) in &self.context {
            write!(formatter, "; {label}: {value}")?;
        }
        if let Some(source) = &self.source {
            write!(formatter, "; caused by: {source}")?;
        }
        Ok(())
    }
}

impl std::error::Error for CheckError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
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
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition().await {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(CheckError::new(check)
                .context("waited", format!("{timeout:?}")));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
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
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let observed = match observe().await {
            Ok(value) => return Ok(value),
            Err(observed) => observed,
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(CheckError::new(check)
                .actual(format!("{observed:?}"))
                .context("waited", format!("{timeout:?}")));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(result: Result<(), CheckError>) -> String {
        result.unwrap_err().to_string()
    }

    #[test]
    fn check_passes_and_fails_with_the_condition_as_context() {
        let ok: Result<(), CheckError> = check!(1 + 1 == 2, "arithmetic holds");
        assert!(ok.is_ok());
        let message = render(check!(1 > 2, "one outranks {}", "two"));
        assert_eq!(message, "one outranks two; condition: 1 > 2");
    }

    #[test]
    fn check_eq_captures_both_sides() {
        let ok: Result<(), CheckError> = check_eq!(7, 7, "sevens agree");
        assert!(ok.is_ok());
        let error = check_eq!(6, 7, "sevens agree").unwrap_err();
        assert_eq!(error.expected.as_deref(), Some("7"));
        assert_eq!(error.actual.as_deref(), Some("6"));
        assert_eq!(
            error.to_string(),
            "sevens agree — expected 7, observed 6; compared: 6"
        );
    }

    #[test]
    fn check_ne_rejects_the_unwanted_value() {
        let ok: Result<(), CheckError> = check_ne!(6, 7, "differs from seven");
        assert!(ok.is_ok());
        let error = check_ne!(7, 7, "differs from seven").unwrap_err();
        assert_eq!(error.expected.as_deref(), Some("not 7"));
        assert_eq!(error.actual.as_deref(), Some("7"));
    }

    #[test]
    fn caused_by_joins_the_source_chain() {
        let source: DynError = "connection reset".into();
        let error = CheckError::new("the clone lands").caused_by(source);
        assert_eq!(
            error.to_string(),
            "the clone lands; caused by: connection reset"
        );
        assert!(std::error::Error::source(&error).is_some());
    }

    #[tokio::test]
    async fn poll_timeout_names_the_condition() {
        let error =
            poll("the account appears", Duration::ZERO, || async { false })
                .await
                .unwrap_err();
        assert_eq!(error.check, "the account appears");
        assert_eq!(error.context[0].0, "waited");
    }

    #[tokio::test]
    async fn poll_for_returns_the_value_and_keeps_the_last_observation() {
        let mut calls = 0;
        let value = poll_for("the counter reaches 2", Duration::ZERO, || {
            calls += 1;
            let seen = calls;
            async move {
                if seen >= 1 {
                    Ok(seen)
                } else {
                    Err(seen)
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(value, 1);

        let error =
            poll_for("the counter reaches 2", Duration::ZERO, || async {
                Err::<u32, _>("still empty")
            })
            .await
            .unwrap_err();
        assert_eq!(error.actual.as_deref(), Some("\"still empty\""));
    }
}
