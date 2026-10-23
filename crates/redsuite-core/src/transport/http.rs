use std::result;

use anyhow::Context;
use jsonrpc_core::Output;
use reqwest::{Body, Client, RequestBuilder, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use solana_rpc_client_types::request::RpcRequest;
use thiserror::Error;
use url::Url;

use super::{
    rpc::{self, RpcError},
    NETWORK_TIMEOUT,
};
use crate::Result;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error("HTTP {status}: {body}")]
    Status { status: StatusCode, body: String },
}

pub fn client() -> Client {
    Client::builder()
        .timeout(NETWORK_TIMEOUT)
        .build()
        .expect("plain-HTTP reqwest client is infallible to build")
}

pub async fn post_json(
    client: &Client,
    url: &Url,
    body: impl Into<Body>,
) -> result::Result<String, TransportError> {
    let request = client.post(url.clone()).header("content-type", "application/json").body(body);
    send(request).await
}

pub async fn get(client: &Client, url: &Url) -> result::Result<String, TransportError> {
    send(client.get(url.clone())).await
}

async fn send(request: RequestBuilder) -> result::Result<String, TransportError> {
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(TransportError::Status { status, body: text });
    }
    Ok(text)
}

#[derive(Clone)]
pub(crate) struct RpcClient {
    url: Url,
    client: Client,
}

impl RpcClient {
    pub fn new(url: Url) -> Self {
        Self { url, client: client() }
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub async fn get_status(&self, url: Url) -> Result<StatusCode> {
        Ok(self.client.get(url).send().await?.status())
    }

    pub async fn post<T: DeserializeOwned>(
        &self,
        method: RpcRequest,
        body: impl Into<Body>,
    ) -> Result<T> {
        let text = post_json(&self.client, &self.url, body)
            .await
            .with_context(|| method.to_string())?;
        Ok(json::from_str(&text)?)
    }

    pub async fn try_call<T: DeserializeOwned>(
        &self,
        method: RpcRequest,
        params: &impl Serialize,
    ) -> Result<result::Result<T, RpcError>> {
        let body = rpc::encode_request(1, method, params)?;
        match self.post(method, body).await? {
            Output::Success(reply) => {
                let result = serde_json::from_value(reply.result)?;
                Ok(Ok(result))
            }
            Output::Failure(reply) => Ok(Err(RpcError { error: reply.error, method })),
        }
    }
}
