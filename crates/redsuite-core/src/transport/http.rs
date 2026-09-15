use std::time::Duration;

use crate::{DynError, Result};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CAUSE_CHAIN_LIMIT: usize = 8;
const CAUSE_LINK_LIMIT: usize = 200;

#[derive(Debug)]
pub struct TransportError {
    pub url: String,
    pub method: Option<String>,
    pub status: Option<u16>,
    pub detail: String,
    pub kind: Option<&'static str>,
    pub cause: Option<String>,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(method) = &self.method {
            write!(f, "{method} ")?;
        }
        match self.status {
            Some(status) => {
                write!(f, "{}: HTTP {status}: {}", self.url, self.detail)?
            }
            None => write!(f, "{}: {}", self.url, self.detail)?,
        }
        if let Some(kind) = self.kind {
            write!(f, " [{kind}]")?;
        }
        if let Some(cause) = &self.cause {
            write!(f, ": {cause}")?;
        }
        Ok(())
    }
}

impl std::error::Error for TransportError {}

impl TransportError {
    fn request(url: &str, error: reqwest::Error) -> DynError {
        Box::new(Self {
            url: url.to_owned(),
            method: None,
            status: None,
            detail: error.to_string(),
            kind: classify(&error),
            cause: cause_chain(&error),
        })
    }

    fn http_status(url: &str, status: u16, detail: String) -> DynError {
        Box::new(Self {
            url: url.to_owned(),
            method: None,
            status: Some(status),
            detail,
            kind: None,
            cause: None,
        })
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

fn cause_chain(error: &dyn std::error::Error) -> Option<String> {
    let mut links: Vec<String> = Vec::new();
    let mut source = error.source();
    while let Some(current) = source {
        links
            .push(current.to_string().chars().take(CAUSE_LINK_LIMIT).collect());
        if links.len() == CAUSE_CHAIN_LIMIT {
            break;
        }
        source = current.source();
    }
    (!links.is_empty()).then(|| links.join(": "))
}

pub fn client() -> reqwest::Client {
    client_with_timeout(REQUEST_TIMEOUT)
}

pub fn client_with_timeout(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(timeout)
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
        .map_err(|error| TransportError::request(url, error))?;
    ok_text(url, response).await
}

pub async fn get_once(url: &str) -> Result<String> {
    let response = client()
        .get(url)
        .send()
        .await
        .map_err(|error| TransportError::request(url, error))?;
    ok_text(url, response).await
}

async fn ok_text(url: &str, response: reqwest::Response) -> Result<String> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| TransportError::request(url, error))?;
    if !status.is_success() {
        return Err(TransportError::http_status(url, status.as_u16(), text));
    }
    Ok(text)
}
