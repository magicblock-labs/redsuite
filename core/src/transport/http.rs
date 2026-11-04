use std::result;

use anyhow::Context;
use jsonrpc_core::Output;
use reqwest::{header::HeaderMap, Body, Client, RequestBuilder, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use solana_rpc_client_types::request::RpcRequest;
use thiserror::Error;
use url::Url;

use super::{
    rpc::{self, RpcError},
    NETWORK_TIMEOUT,
};
use crate::Result;

const REMOTE_ACCOUNT_CLAIMS: &str = "x-mb-remote-account-claims";

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
    Ok(post_headed(client, url, body).await?.1)
}

async fn post_headed(
    client: &Client,
    url: &Url,
    body: impl Into<Body>,
) -> result::Result<(HeaderMap, String), TransportError> {
    let request = client.post(url.clone()).header("content-type", "application/json").body(body);
    send(request).await
}

pub async fn get(client: &Client, url: &Url) -> result::Result<String, TransportError> {
    Ok(send(client.get(url.clone())).await?.1)
}

async fn send(request: RequestBuilder) -> result::Result<(HeaderMap, String), TransportError> {
    let response = request.send().await?;
    let status = response.status();
    let headers = response.headers().clone();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(TransportError::Status { status, body: text });
    }
    Ok((headers, text))
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
        Ok(self.post_headed(method, body).await?.1)
    }

    async fn post_headed<T: DeserializeOwned>(
        &self,
        method: RpcRequest,
        body: impl Into<Body>,
    ) -> Result<(HeaderMap, T)> {
        let (headers, text) = post_headed(&self.client, &self.url, body)
            .await
            .with_context(|| method.to_string())?;
        Ok((headers, json::from_str(&text)?))
    }

    pub async fn try_call<T: DeserializeOwned>(
        &self,
        method: RpcRequest,
        params: &impl Serialize,
    ) -> Result<result::Result<T, RpcError>> {
        Ok(self.try_call_headed(method, params).await?.1)
    }

    pub async fn try_call_claimed<T: DeserializeOwned>(
        &self,
        method: RpcRequest,
        params: &impl Serialize,
    ) -> Result<(u64, result::Result<T, RpcError>)> {
        let (headers, result) = self.try_call_headed(method, params).await?;
        let claims = headers
            .get(REMOTE_ACCOUNT_CLAIMS)
            .context("remote account claims header missing")?
            .to_str()?
            .parse()?;
        Ok((claims, result))
    }

    async fn try_call_headed<T: DeserializeOwned>(
        &self,
        method: RpcRequest,
        params: &impl Serialize,
    ) -> Result<(HeaderMap, result::Result<T, RpcError>)> {
        let body = rpc::encode_request(1, method, params)?;
        let (headers, output) = self.post_headed(method, body).await?;
        let result = match output {
            Output::Success(reply) => Ok(serde_json::from_value(reply.result)?),
            Output::Failure(reply) => Err(RpcError { error: reply.error, method }),
        };
        Ok((headers, result))
    }
}
