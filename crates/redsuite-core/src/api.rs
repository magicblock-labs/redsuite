use std::{collections::HashMap, rc::Rc, result};

use account::Account;
use anyhow::Context;
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use hash::Hash;
use json::{Deserialize, Serialize};
use jsonrpc_core::{Output, Response as RpcResponse};
use pubkey::Pubkey;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use signature::Signature;
use solana_account_decoder_client_types::UiAccount;
use solana_commitment_config::CommitmentConfig;
use solana_instruction_error::InstructionError;
use solana_rpc_client_types::{
    config::{
        RpcBlockConfig, RpcProgramAccountsConfig, RpcSendTransactionConfig,
        RpcSignaturesForAddressConfig, RpcSimulateTransactionConfig, RpcTransactionConfig,
    },
    request::RpcRequest,
    response::{
        Response, RpcBlockhash, RpcConfirmedTransactionStatusWithSignature, RpcKeyedAccount,
        RpcSimulateTransactionResult,
    },
};
use solana_transaction_error::TransactionError;
use solana_transaction_status_client_types::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
    TransactionDetails, TransactionStatus, UiConfirmedBlock, UiTransactionEncoding,
    UiTransactionStatusMeta,
};
use thiserror::Error;
use tokio::sync::Mutex;
use transaction::{versioned::VersionedTransaction, Transaction};
use url::Url;

mod wait;

pub use crate::transport::rpc::{account_config, RpcError};
use crate::{
    transport::{
        events::EventSubscriptions,
        http,
        rpc::{self, NO_PARAMS},
        NETWORK_TIMEOUT,
    },
    Result,
};

#[derive(Debug, Error)]
#[error("transaction failed on-chain: {err:?}")]
pub struct TxError {
    pub err: TransactionError,
}

#[derive(Debug, Error)]
#[error("transaction unconfirmed after {NETWORK_TIMEOUT:?} (outcome unknown)")]
pub struct ConfirmTimeout;

pub fn decode_account(raw: UiAccount) -> Result<Account> {
    raw.to_account().context("invalid RPC account encoding or owner")
}

pub fn transaction_meta(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
) -> Result<&UiTransactionStatusMeta> {
    tx.transaction.meta.as_ref().context("transaction has no execution metadata")
}

pub fn transaction_logs(meta: &UiTransactionStatusMeta) -> &[String] {
    match &meta.log_messages {
        OptionSerializer::Some(logs) => logs,
        OptionSerializer::None | OptionSerializer::Skip => &[],
    }
}

pub struct SendBody {
    body: Bytes,
    count: usize,
}

/// HTTP RPC and lazily shared WebSocket subscriptions on the current `LocalSet`.
#[derive(Clone)]
pub struct Api {
    rpc: http::RpcClient,
    ws_url: Url,
    events: Rc<Mutex<Option<Rc<EventSubscriptions>>>>,
}

impl Api {
    pub fn new(url: Url, ws_url: Url) -> Self {
        Self {
            rpc: http::RpcClient::new(url),
            ws_url,
            events: Rc::default(),
        }
    }

    pub fn ws_url(&self) -> &Url {
        &self.ws_url
    }

    pub fn url(&self) -> &Url {
        self.rpc.url()
    }

    pub async fn call<T: DeserializeOwned>(
        &self,
        method: RpcRequest,
        params: &impl Serialize,
    ) -> Result<T> {
        self.try_call(method, params).await?.map_err(Into::into)
    }

    pub async fn call_value<T: DeserializeOwned>(
        &self,
        method: RpcRequest,
        params: &impl Serialize,
    ) -> Result<T> {
        let response: Response<T> = self.call(method, params).await?;
        Ok(response.value)
    }

    /// Keeps RPC rejection typed; transport and decoding failures use the outer error.
    pub async fn try_call<T: DeserializeOwned>(
        &self,
        method: RpcRequest,
        params: &impl Serialize,
    ) -> Result<result::Result<T, RpcError>> {
        self.rpc.try_call(method, params).await
    }

    pub async fn get_health(&self) -> Result<String> {
        self.call(RpcRequest::GetHealth, &NO_PARAMS).await
    }

    pub async fn get_slot(&self) -> Result<u64> {
        self.call(RpcRequest::GetSlot, &(CommitmentConfig::confirmed(),)).await
    }

    pub async fn server_alive(&self) -> bool {
        self.try_call::<String>(RpcRequest::GetHealth, &NO_PARAMS).await.is_ok()
    }

    pub async fn primary_ready(&self) -> bool {
        let mut url = self.url().clone();
        url.set_path(&format!(
            "{}/health/primary",
            url.path().trim_end_matches('/')
        ));
        matches!(self.rpc.get_status(url).await, Ok(StatusCode::OK))
    }

    pub async fn get_balance(&self, pk: &Pubkey) -> Result<u64> {
        self.call_value(
            RpcRequest::GetBalance,
            &(pk.to_string(), CommitmentConfig::confirmed()),
        )
        .await
    }

    pub async fn request_airdrop(&self, pk: &Pubkey, lamports: u64) -> Result<String> {
        self.call(RpcRequest::RequestAirdrop, &(pk.to_string(), lamports)).await
    }

    pub async fn get_account(&self, pk: &Pubkey) -> Result<Option<Account>> {
        let account: Option<UiAccount> = self
            .call_value(
                RpcRequest::GetAccountInfo,
                &(pk.to_string(), account_config()),
            )
            .await?;
        account.map(decode_account).transpose()
    }

    pub async fn get_multiple_accounts(&self, pks: &[Pubkey]) -> Result<Vec<Option<Account>>> {
        let keys: Vec<_> = pks.iter().map(ToString::to_string).collect();
        let accounts: Vec<Option<UiAccount>> = self
            .call_value(RpcRequest::GetMultipleAccounts, &(keys, account_config()))
            .await?;
        accounts
            .into_iter()
            .map(|account| account.map(decode_account).transpose())
            .collect()
    }

    pub async fn get_program_accounts(&self, program: &Pubkey) -> Result<Vec<(Pubkey, Account)>> {
        let config = RpcProgramAccountsConfig {
            account_config: account_config(),
            ..RpcProgramAccountsConfig::default()
        };
        let accounts: Vec<RpcKeyedAccount> = self
            .call(
                RpcRequest::GetProgramAccounts,
                &(program.to_string(), config),
            )
            .await?;
        accounts
            .into_iter()
            .map(|keyed| Ok((keyed.pubkey.parse()?, decode_account(keyed.account)?)))
            .collect()
    }

    pub async fn get_latest_blockhash(&self) -> Result<Hash> {
        let blockhash: RpcBlockhash = self
            .call_value(
                RpcRequest::GetLatestBlockhash,
                &(CommitmentConfig::confirmed(),),
            )
            .await?;
        Ok(blockhash.blockhash.parse()?)
    }

    pub async fn is_blockhash_valid(&self, hash: &Hash) -> Result<bool> {
        self.call_value(
            RpcRequest::IsBlockhashValid,
            &(hash.to_string(), CommitmentConfig::confirmed()),
        )
        .await
    }

    pub async fn simulate_transaction(
        &self,
        tx: &Transaction,
    ) -> Result<RpcSimulateTransactionResult> {
        let config = RpcSimulateTransactionConfig {
            sig_verify: true,
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            ..RpcSimulateTransactionConfig::default()
        };
        let encoded = encode(tx)?;
        self.call_value(RpcRequest::SimulateTransaction, &(encoded, config)).await
    }

    fn send_params(tx: &Transaction) -> Result<(String, RpcSendTransactionConfig)> {
        let config = RpcSendTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
            ..RpcSendTransactionConfig::default()
        };
        Ok((encode(tx)?, config))
    }

    pub fn send_body(transactions: &[Transaction]) -> Result<SendBody> {
        let requests: Vec<_> = transactions
            .iter()
            .enumerate()
            .map(|(index, tx)| {
                let params = Self::send_params(tx)?;
                rpc::request(index as u64 + 1, RpcRequest::SendTransaction, &params)
            })
            .collect::<Result<_>>()?;
        let body = match requests.as_slice() {
            [one] => json::to_vec(one)?,
            _ => json::to_vec(&requests)?,
        };
        Ok(SendBody {
            body: body.into(),
            count: transactions.len(),
        })
    }

    pub async fn send_prepared(&self, body: &SendBody) -> Result<usize> {
        if body.count == 0 {
            return Ok(0);
        }
        let response: RpcResponse =
            self.rpc.post(RpcRequest::SendTransaction, body.body.clone()).await?;
        let rejected = match response {
            RpcResponse::Single(reply) => usize::from(matches!(reply, Output::Failure(_))),
            RpcResponse::Batch(replies) => {
                replies.iter().filter(|reply| matches!(reply, Output::Failure(_))).count()
            }
        };
        Ok(rejected)
    }

    pub async fn send_transaction(&self, tx: &Transaction) -> Result<Signature> {
        self.try_send_transaction(tx).await?.map_err(Into::into)
    }

    pub async fn try_send_transaction(
        &self,
        tx: &Transaction,
    ) -> Result<result::Result<Signature, RpcError>> {
        let params = Self::send_params(tx)?;
        match self.try_call::<String>(RpcRequest::SendTransaction, &params).await? {
            Ok(sig) => Ok(Ok(sig.parse()?)),
            Err(error) => Ok(Err(error)),
        }
    }

    pub async fn get_signature_status(&self, sig: &Signature) -> Result<Option<TransactionStatus>> {
        Ok(self.get_signature_statuses(&[*sig]).await?.pop().flatten())
    }

    pub async fn get_signature_statuses(
        &self,
        signatures: &[Signature],
    ) -> Result<Vec<Option<TransactionStatus>>> {
        let signatures: Vec<_> = signatures.iter().map(ToString::to_string).collect();
        self.call_value(RpcRequest::GetSignatureStatuses, &(signatures,)).await
    }

    /// Reads receipts without JSON's per-instruction base58 encoding, which is
    /// expensive for materialization transactions carrying whole program images.
    pub async fn get_transaction(
        &self,
        sig: &Signature,
    ) -> Result<Option<EncodedConfirmedTransactionWithStatusMeta>> {
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(1),
        };
        self.call(RpcRequest::GetTransaction, &(sig.to_string(), config)).await
    }

    pub async fn get_block_time(&self, slot: u64) -> Result<Option<i64>> {
        self.call(RpcRequest::GetBlockTime, &(slot,)).await
    }

    pub async fn get_block(&self, slot: u64) -> Result<Option<UiConfirmedBlock>> {
        let config = RpcBlockConfig {
            transaction_details: Some(TransactionDetails::None),
            rewards: Some(false),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(1),
            ..RpcBlockConfig::default()
        };
        self.call(RpcRequest::GetBlock, &(slot, config)).await
    }

    pub async fn get_signatures_for_address(
        &self,
        pk: &Pubkey,
        limit: usize,
    ) -> Result<Vec<Signature>> {
        let config = RpcSignaturesForAddressConfig {
            limit: Some(limit),
            commitment: Some(CommitmentConfig::confirmed()),
            ..RpcSignaturesForAddressConfig::default()
        };
        let infos: Vec<RpcConfirmedTransactionStatusWithSignature> = self
            .call(
                RpcRequest::GetSignaturesForAddress,
                &(pk.to_string(), config),
            )
            .await?;
        infos.into_iter().map(|info| Ok(info.signature.parse()?)).collect()
    }
}
pub fn encode<T: wincode::Serialize<Src = T>>(value: &T) -> Result<String> {
    Ok(STANDARD.encode(wincode::serialize(value)?))
}

pub(crate) fn decode_transaction(text: &str) -> Result<VersionedTransaction> {
    let bytes = STANDARD.decode(text)?;
    let transaction: VersionedTransaction = wincode::deserialize(&bytes)?;
    transaction.sanitize()?;
    Ok(transaction)
}

pub fn custom_error_code(err: impl Into<TransactionError>) -> Option<u32> {
    match err.into() {
        TransactionError::InstructionError(_, InstructionError::Custom(code)) => Some(code),
        _ => None,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metrics(pub HashMap<String, f64>);

impl Metrics {
    pub fn get(&self, name: &str) -> Option<f64> {
        self.0.get(name).copied()
    }

    pub fn value_sum(&self, name: &str) -> Option<f64> {
        self.sum_series(name, |_, value| value)
    }

    fn sum_series(&self, name: &str, value: impl Fn(&str, f64) -> f64) -> Option<f64> {
        let label_prefix = format!("{name}{{");
        let mut sum = 0.0;
        let mut matched = false;
        for (key, sample) in &self.0 {
            if key.starts_with(&label_prefix) {
                matched = true;
                sum += value(key, *sample);
            }
        }
        if matched {
            Some(sum)
        } else {
            self.get(name).map(|sample| value(name, sample))
        }
    }

    pub fn parse(text: &str) -> Self {
        let mut map = HashMap::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // Labels may contain spaces — cut after the label block.
            let key_end = match line.find('{') {
                Some(_) => match line.rfind('}') {
                    Some(i) => i + 1,
                    None => continue,
                },
                None => match line.find(' ') {
                    Some(i) => i,
                    None => continue,
                },
            };
            let (key, rest) = line.split_at(key_end);
            let value = rest.split_whitespace().next().and_then(|value| value.parse::<f64>().ok());
            let Some(value) = value else { continue };
            if let Some((bare, _)) = key.split_once('{') {
                map.insert(bare.to_owned(), value);
            }
            map.insert(key.to_owned(), value);
        }
        Self(map)
    }
}

#[derive(Clone)]
pub struct MetricsCollector {
    client: Client,
    url: Url,
}

impl MetricsCollector {
    pub fn new(mut url: Url) -> Self {
        url.set_path(&format!("{}/metrics", url.path().trim_end_matches('/')));
        Self { client: http::client(), url }
    }

    pub async fn scrape(&self) -> Result<Metrics> {
        Ok(Metrics::parse(&http::get(&self.client, &self.url).await?))
    }
}

#[derive(Debug)]
pub struct MetricsDelta {
    before: Metrics,
    after: Metrics,
}

impl MetricsDelta {
    pub fn new(before: Metrics, after: Metrics) -> Self {
        Self { before, after }
    }

    pub fn counter(&self, name: &str) -> Option<f64> {
        let after = self.after.get(name)?;
        Some(after - self.before.get(name).unwrap_or(0.0))
    }

    pub fn gauge(&self, name: &str) -> Option<f64> {
        self.after.get(name)
    }

    pub fn histogram_avg(&self, name: &str) -> Option<f64> {
        histogram_average(
            self.counter(&suffixed(name, "_count"))?,
            self.counter(&suffixed(name, "_sum"))?,
        )
    }

    pub fn counter_all(&self, name: &str) -> Option<f64> {
        self.after.sum_series(name, |key, after| {
            after - self.before.get(key).unwrap_or(0.0)
        })
    }

    // Window average over ALL series of a (possibly labeled) histogram.
    pub fn histogram_avg_all(&self, name: &str) -> Option<f64> {
        histogram_average(
            self.counter_all(&format!("{name}_count"))?,
            self.counter_all(&format!("{name}_sum"))?,
        )
    }

    pub fn before(&self) -> &Metrics {
        &self.before
    }
}

fn histogram_average(count: f64, sum: f64) -> Option<f64> {
    if count <= 0.0 {
        None
    } else {
        Some(sum / count)
    }
}

// mbv_x{kind="y"} + _sum → mbv_x_sum{kind="y"}
fn suffixed(name: &str, suffix: &str) -> String {
    match name.find('{') {
        Some(i) => format!("{}{}{}", &name[..i], suffix, &name[i..]),
        None => format!("{name}{suffix}"),
    }
}
