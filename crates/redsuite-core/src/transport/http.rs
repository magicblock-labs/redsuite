use std::time::Duration;

use crate::Result;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct TransportError {
    pub url: String,
    pub status: Option<u16>,
    pub detail: String,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.status {
            Some(status) => {
                write!(f, "{}: HTTP {status}: {}", self.url, self.detail)
            }
            None => write!(f, "{}: {}", self.url, self.detail),
        }
    }
}

impl std::error::Error for TransportError {}

impl TransportError {
    fn request(url: &str, error: reqwest::Error) -> Box<Self> {
        Box::new(Self {
            url: url.to_owned(),
            status: None,
            detail: error.to_string(),
        })
    }
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
        return Err(Box::new(TransportError {
            url: url.to_owned(),
            status: Some(status.as_u16()),
            detail: text,
        }));
    }
    Ok(text)
}
