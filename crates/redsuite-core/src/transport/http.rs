//! Interim HTTP shim over `reqwest` (plain HTTP, no TLS features) until
//! redline's pooled transport is ported in its place.

use std::time::Duration;

use crate::Result;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Kept per-`Api`, never global: a reqwest client's connections are tied to
/// the tokio runtime that drove them, and every `#[tokio::test]` has its own.
pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .expect("plain-HTTP reqwest client is infallible to build")
}

pub async fn post_json(
    client: &reqwest::Client,
    url: &str,
    body: String,
) -> Result<String> {
    let response = client
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    ok_text(url, response).await
}

pub async fn get_once(url: &str) -> Result<String> {
    let response = client()
        .get(url)
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    ok_text(url, response).await
}

async fn ok_text(url: &str, response: reqwest::Response) -> Result<String> {
    let status = response.status();
    let text = response.text().await.map_err(|e| format!("{url}: {e}"))?;
    if !status.is_success() {
        return Err(format!("{url}: HTTP {status}: {text}").into());
    }
    Ok(text)
}
