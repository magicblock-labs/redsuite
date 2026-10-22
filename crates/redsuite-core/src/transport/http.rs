use std::{error, time::Duration};

use reqwest::{Body, Client, RequestBuilder, StatusCode};
use thiserror::Error;
use url::Url;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CAUSE_CHAIN_LIMIT: usize = 8;
const CAUSE_LINK_LIMIT: usize = 200;

#[derive(Debug, Error)]
#[error("{self:?}")]
pub struct TransportError {
    pub url: Url,
    pub method: Option<String>,
    pub status: Option<StatusCode>,
    pub detail: String,
    pub kind: Option<&'static str>,
    pub cause: Option<String>,
}

impl TransportError {
    fn request(url: &Url, error: reqwest::Error) -> Self {
        Self {
            url: url.clone(),
            method: None,
            status: None,
            detail: format!("{error:#}"),
            kind: classify(&error),
            cause: cause_chain(&error),
        }
    }

    fn http_status(url: &Url, status: StatusCode, detail: String) -> Self {
        Self {
            url: url.clone(),
            method: None,
            status: Some(status),
            detail,
            kind: None,
            cause: None,
        }
    }
}

fn classify(error: &reqwest::Error) -> Option<&'static str> {
    if error.is_timeout() {
        Some("timeout")
    } else if error.is_connect() {
        Some("connect")
    } else if error.is_body() {
        Some("body")
    } else if error.is_decode() {
        Some("decode")
    } else if error.is_request() {
        Some("request")
    } else {
        None
    }
}

fn cause_chain(error: &dyn error::Error) -> Option<String> {
    let mut links: Vec<String> = Vec::new();
    let mut source = error.source();
    while let Some(current) = source {
        links.push(current.to_string().chars().take(CAUSE_LINK_LIMIT).collect());
        if links.len() == CAUSE_CHAIN_LIMIT {
            break;
        }
        source = current.source();
    }
    (!links.is_empty()).then(|| links.join(": "))
}

pub fn client() -> Client {
    client_with_timeout(REQUEST_TIMEOUT)
}

pub fn client_with_timeout(timeout: Duration) -> Client {
    Client::builder()
        .timeout(timeout)
        .build()
        .expect("plain-HTTP reqwest client is infallible to build")
}

pub async fn post_json(
    client: &Client,
    url: &Url,
    body: impl Into<Body>,
) -> Result<String, TransportError> {
    let request = client.post(url.clone()).header("content-type", "application/json").body(body);
    send(url, request).await
}

pub async fn get(client: &Client, url: &Url) -> Result<String, TransportError> {
    send(url, client.get(url.clone())).await
}

async fn send(url: &Url, request: RequestBuilder) -> Result<String, TransportError> {
    let response = request.send().await.map_err(|error| TransportError::request(url, error))?;
    let status = response.status();
    let text = response.text().await.map_err(|error| TransportError::request(url, error))?;
    if !status.is_success() {
        return Err(TransportError::http_status(url, status, text));
    }
    Ok(text)
}
