use serde::Serialize;
use serde_json::Value;
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client_types::{config::RpcAccountInfoConfig, request::RpcRequest};
use thiserror::Error;

use crate::Result;

pub const NO_PARAMS: [u8; 0] = [];

pub fn account_config() -> RpcAccountInfoConfig {
    RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::confirmed()),
        ..RpcAccountInfoConfig::default()
    }
}

#[derive(Debug, Error)]
#[error("{method}: {error:?}")]
pub struct RpcError {
    #[source]
    pub error: jsonrpc_core::Error,
    pub method: RpcRequest,
}

pub(crate) fn request(id: u64, method: RpcRequest, params: &impl Serialize) -> Result<Value> {
    Ok(method.build_request_json(id, serde_json::to_value(params)?))
}

pub(super) fn encode_request(
    id: u64,
    method: RpcRequest,
    params: &impl Serialize,
) -> Result<String> {
    Ok(json::to_string(&request(id, method, params)?)?)
}
