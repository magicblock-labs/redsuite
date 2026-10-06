use std::{collections::HashMap, str::FromStr, time::Duration};

use account::Account;
use base64::Engine;
use hash::Hash;
use json::{Deserialize, Serialize};
use pubkey::Pubkey;
use serde::de::DeserializeOwned;
use signature::Signature;
use transaction::Transaction;

use crate::{transport::http, Result};

#[derive(Debug)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<json::Value>,
    pub method: String,
    pub url: String,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rpc error {}: {} ({} at {})",
            self.code, self.message, self.method, self.url
        )?;
        if let Some(data) = &self.data {
            write!(f, " data: {data:?}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RpcError {}

#[derive(Debug)]
pub struct TxError {
    pub signature: Signature,
    pub err: json::Value,
}

impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "transaction {} failed on-chain: {:?}",
            self.signature, self.err
        )
    }
}

impl std::error::Error for TxError {}

#[derive(Debug)]
pub struct ConfirmTimeout {
    pub signature: Signature,
    pub deadline: Duration,
}

impl std::fmt::Display for ConfirmTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "transaction {} not confirmed within {:?} — execution outcome \
             unknown, do not resubmit",
            self.signature, self.deadline
        )
    }
}

impl std::error::Error for ConfirmTimeout {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Commitment {
    Confirmed,
    Finalized,
}

impl Commitment {
    pub fn as_str(self) -> &'static str {
        match self {
            Commitment::Confirmed => "confirmed",
            Commitment::Finalized => "finalized",
        }
    }
}

// Same 20s budget as test-integration's 40x500ms convention, but polled at
// the ER's block cadence so confirm latency reflects the chain, not the poll.
pub const CONFIRM_DEADLINE: Duration = Duration::from_secs(20);
pub const CONFIRM_POLL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy)]
pub struct ConfirmOptions {
    pub commitment: Commitment,
    pub deadline: Duration,
    pub poll: Duration,
}

impl Default for ConfirmOptions {
    fn default() -> Self {
        Self {
            commitment: Commitment::Confirmed,
            deadline: CONFIRM_DEADLINE,
            poll: CONFIRM_POLL,
        }
    }
}

#[derive(Deserialize)]
struct Envelope<T> {
    #[serde(default)]
    id: Option<u64>,
    result: Option<T>,
    error: Option<EnvelopeError>,
}

#[derive(Deserialize)]
struct EnvelopeError {
    code: i64,
    message: String,
    #[serde(default)]
    data: Option<json::Value>,
}

#[derive(Deserialize)]
struct WithContext<T> {
    value: T,
}

#[derive(Deserialize)]
struct RpcBlockhash {
    blockhash: String,
}

#[derive(Deserialize)]
struct RpcSignatureStatus {
    #[serde(rename = "confirmationStatus")]
    confirmation_status: Option<String>,
    err: Option<json::Value>,
}

#[derive(Deserialize)]
struct RpcAccount {
    lamports: u64,
    owner: String,
    data: (String, String),
    executable: bool,
    #[serde(rename = "rentEpoch")]
    rent_epoch: u64,
}

#[derive(Deserialize)]
struct RpcKeyedAccount {
    pubkey: String,
    account: RpcAccount,
}

fn decode_rpc_account(raw: RpcAccount) -> Result<Account> {
    let data = base64::engine::general_purpose::STANDARD
        .decode(&raw.data.0)
        .map_err(|e| format!("bad base64 account data: {e}"))?;
    Ok(Account {
        lamports: raw.lamports,
        data,
        owner: Pubkey::from_str(&raw.owner)?,
        executable: raw.executable,
        rent_epoch: raw.rent_epoch,
    })
}

#[derive(Deserialize)]
struct RpcTransaction {
    slot: u64,
    #[serde(rename = "blockTime")]
    block_time: Option<i64>,
    meta: Option<RpcTransactionMeta>,
    transaction: Option<RpcTransactionBody>,
}

#[derive(Deserialize)]
struct RpcTransactionBody {
    message: RpcTransactionMessage,
}

#[derive(Deserialize)]
struct RpcTransactionMessage {
    #[serde(rename = "addressTableLookups")]
    address_table_lookups: Option<Vec<json::Value>>,
}

#[derive(Default, Deserialize)]
struct RpcTransactionMeta {
    err: Option<json::Value>,
    fee: u64,
    #[serde(rename = "preBalances")]
    pre_balances: Vec<u64>,
    #[serde(rename = "postBalances")]
    post_balances: Vec<u64>,
    #[serde(rename = "logMessages")]
    log_messages: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct BlockInfo {
    #[serde(rename = "blockTime")]
    pub block_time: Option<i64>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct TransactionInfo {
    pub slot: u64,
    pub block_time: Option<i64>,
    // on-chain execution error; None = the transaction succeeded
    pub err: Option<json::Value>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub logs: Vec<String>,
    // lookup tables the transaction message loads addresses through;
    // 0 for legacy transactions
    pub lookup_tables: usize,
}

#[derive(Debug)]
pub struct SignatureStatus {
    pub confirmed: bool,
    pub finalized: bool,
    pub err: Option<json::Value>,
}

impl SignatureStatus {
    pub fn meets(&self, commitment: Commitment) -> bool {
        match commitment {
            Commitment::Confirmed => self.confirmed,
            Commitment::Finalized => self.finalized,
        }
    }
}

#[derive(Deserialize)]
struct RpcSignatureInfo {
    signature: String,
}

const TX_POLL: Duration = Duration::from_millis(200);

const NO_PARAMS: [&str; 0] = [];

#[derive(Serialize)]
struct CommitmentConfig {
    commitment: &'static str,
}

impl CommitmentConfig {
    fn confirmed() -> Self {
        Self {
            commitment: Commitment::Confirmed.as_str(),
        }
    }
}

#[derive(Serialize)]
struct AccountConfig {
    encoding: &'static str,
    commitment: &'static str,
}

impl AccountConfig {
    fn base64_confirmed() -> Self {
        Self {
            encoding: "base64",
            commitment: Commitment::Confirmed.as_str(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SendTransactionConfig {
    encoding: &'static str,
    skip_preflight: bool,
    preflight_commitment: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TransactionConfig {
    encoding: &'static str,
    commitment: &'static str,
    max_supported_transaction_version: u8,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockConfig {
    transaction_details: &'static str,
    rewards: bool,
    commitment: &'static str,
    max_supported_transaction_version: u8,
}

#[derive(Serialize)]
struct SignaturesConfig {
    limit: usize,
    commitment: &'static str,
}

pub struct SendBody {
    body: bytes::Bytes,
    expect: Vec<Signature>,
}

#[derive(Clone)]
pub struct Api {
    url: String,
    client: reqwest::Client,
}

impl Api {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: http::client(),
        }
    }

    pub fn with_timeout(url: impl Into<String>, timeout: Duration) -> Self {
        Self {
            url: url.into(),
            client: http::client_with_timeout(timeout),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &impl Serialize,
    ) -> Result<T> {
        self.call_nullable(method, params).await?.ok_or_else(|| {
            format!("{method}: response carried neither result nor error")
                .into()
        })
    }

    // For methods where a null result is a legitimate answer (getTransaction
    // on an unknown signature) rather than a protocol violation.
    pub async fn call_nullable<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &impl Serialize,
    ) -> Result<Option<T>> {
        let params = json::to_string(params)?;
        let body = crate::transport::conn::request_text(1, method, &params);
        let response = http::post_json(&self.client, &self.url, body)
            .await
            .map_err(|mut error| {
                if let Some(transport) =
                    error.downcast_mut::<http::TransportError>()
                {
                    transport.method = Some(method.to_owned());
                }
                error
            })?;
        let envelope: Envelope<T> = json::from_str(&response).map_err(|e| {
            format!("{method}: unexpected response shape: {e} ({response})")
        })?;
        if let Some(err) = envelope.error {
            return Err(Box::new(RpcError {
                code: err.code,
                message: err.message,
                data: err.data,
                method: method.to_owned(),
                url: self.url.clone(),
            }));
        }
        Ok(envelope.result)
    }

    pub async fn get_health(&self) -> Result<String> {
        self.call("getHealth", &NO_PARAMS).await
    }

    pub async fn get_slot(&self) -> Result<u64> {
        self.call("getSlot", &(CommitmentConfig::confirmed(),))
            .await
    }

    pub async fn server_alive(&self) -> bool {
        match self.get_health().await {
            Ok(_) => true,
            Err(e) => e.is::<RpcError>(),
        }
    }

    pub async fn primary_ready(&self) -> bool {
        let url = format!("{}/health/primary", self.url.trim_end_matches('/'));
        matches!(
            self.client.get(&url).send().await,
            Ok(response) if response.status().as_u16() == 200
        )
    }

    pub async fn get_balance(&self, pk: &Pubkey) -> Result<u64> {
        let params = (pk.to_string(), CommitmentConfig::confirmed());
        let resp: WithContext<u64> = self.call("getBalance", &params).await?;
        Ok(resp.value)
    }

    pub async fn request_airdrop(
        &self,
        pk: &Pubkey,
        lamports: u64,
    ) -> Result<String> {
        self.call("requestAirdrop", &(pk.to_string(), lamports))
            .await
    }

    pub async fn get_account(&self, pk: &Pubkey) -> Result<Option<Account>> {
        let params = (pk.to_string(), AccountConfig::base64_confirmed());
        let resp: WithContext<Option<RpcAccount>> =
            self.call("getAccountInfo", &params).await?;
        resp.value.map(decode_rpc_account).transpose()
    }

    pub async fn get_multiple_accounts(
        &self,
        pks: &[Pubkey],
    ) -> Result<Vec<Option<Account>>> {
        let keys: Vec<String> = pks.iter().map(Pubkey::to_string).collect();
        let params = (keys, AccountConfig::base64_confirmed());
        let resp: WithContext<Vec<Option<RpcAccount>>> =
            self.call("getMultipleAccounts", &params).await?;
        if resp.value.len() != pks.len() {
            return Err(format!(
                "getMultipleAccounts: asked for {} accounts, got {}",
                pks.len(),
                resp.value.len()
            )
            .into());
        }
        resp.value
            .into_iter()
            .map(|raw| raw.map(decode_rpc_account).transpose())
            .collect()
    }

    pub async fn get_program_accounts(
        &self,
        program: &Pubkey,
    ) -> Result<Vec<(Pubkey, Account)>> {
        let params = (program.to_string(), AccountConfig::base64_confirmed());
        let resp: Vec<RpcKeyedAccount> =
            self.call("getProgramAccounts", &params).await?;
        resp.into_iter()
            .map(|keyed| {
                Ok((
                    Pubkey::from_str(&keyed.pubkey)?,
                    decode_rpc_account(keyed.account)?,
                ))
            })
            .collect()
    }

    pub async fn get_latest_blockhash(&self) -> Result<Hash> {
        let resp: WithContext<RpcBlockhash> = self
            .call("getLatestBlockhash", &(CommitmentConfig::confirmed(),))
            .await?;
        Ok(Hash::from_str(&resp.value.blockhash)?)
    }

    fn send_params(
        tx: &Transaction,
    ) -> Result<(String, SendTransactionConfig)> {
        Ok((
            base64::engine::general_purpose::STANDARD
                .encode(bincode::serialize(tx)?),
            SendTransactionConfig {
                encoding: "base64",
                skip_preflight: true,
                preflight_commitment: Commitment::Confirmed.as_str(),
            },
        ))
    }

    pub fn send_body(transactions: &[Transaction]) -> Result<SendBody> {
        let batched = transactions.len() != 1;
        let mut body = String::new();
        let mut expect = Vec::with_capacity(transactions.len());
        if batched {
            body.push('[');
        }
        for (index, tx) in transactions.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(&crate::transport::conn::request_text(
                index as u64 + 1,
                "sendTransaction",
                &json::to_string(&Self::send_params(tx)?)?,
            ));
            expect.push(
                *tx.signatures
                    .first()
                    .ok_or("a transaction to send carries no signature")?,
            );
        }
        if batched {
            body.push(']');
        }
        Ok(SendBody {
            body: body.into(),
            expect,
        })
    }

    pub async fn send_prepared(&self, body: &SendBody) -> Result<usize> {
        let count = body.expect.len();
        if count == 0 {
            return Ok(0);
        }
        let response =
            http::post_json(&self.client, &self.url, body.body.clone()).await?;
        let envelopes: Vec<Envelope<String>> = match count {
            1 => json::from_str(&response).map(|one| vec![one]),
            _ => json::from_str(&response),
        }
        .map_err(|error| {
            format!("sendTransaction: unexpected response shape: {error} ({response})")
        })?;
        if envelopes.len() != count {
            return Err(format!(
                "sendTransaction: sent {count} requests, got {} responses",
                envelopes.len()
            )
            .into());
        }
        let mut answered = vec![false; count];
        let mut rejected = 0;
        for envelope in &envelopes {
            let id = envelope
                .id
                .and_then(|id| usize::try_from(id).ok())
                .filter(|id| (1..=count).contains(id))
                .ok_or_else(|| {
                    format!(
                        "sendTransaction: response id {:?} outside 1..={count}",
                        envelope.id
                    )
                })?;
            if std::mem::replace(&mut answered[id - 1], true) {
                return Err(format!(
                    "sendTransaction: duplicate response id {id}"
                )
                .into());
            }
            if envelope.error.is_some() {
                rejected += 1;
                continue;
            }
            let signature = envelope.result.as_ref().ok_or_else(|| {
                format!(
                    "sendTransaction: id {id} carries neither result nor error"
                )
            })?;
            let acknowledged = Signature::from_str(signature)?;
            let expected = body.expect[id - 1];
            if acknowledged != expected {
                return Err(format!(
                    "sendTransaction: id {id} acknowledged \
                     {acknowledged}, expected {expected}"
                )
                .into());
            }
        }
        Ok(rejected)
    }

    pub async fn send_transaction(
        &self,
        tx: &Transaction,
    ) -> Result<Signature> {
        let sig: String = self
            .call("sendTransaction", &Self::send_params(tx)?)
            .await?;
        Ok(Signature::from_str(&sig)?)
    }

    pub async fn confirm(
        &self,
        sig: &Signature,
        options: ConfirmOptions,
    ) -> Result<()> {
        crate::check::poll_until(options.deadline, options.poll, async || {
            if let Some(status) = self.get_signature_status(sig).await? {
                if let Some(err) = status.err {
                    return Err(Box::new(TxError {
                        signature: *sig,
                        err,
                    }) as crate::DynError);
                }
                if status.meets(options.commitment) {
                    return Ok(Some(()));
                }
            }
            Ok(None)
        })
        .await?
        .ok_or_else(|| {
            Box::new(ConfirmTimeout {
                signature: *sig,
                deadline: options.deadline,
            }) as crate::DynError
        })
    }

    pub async fn get_signature_status(
        &self,
        sig: &Signature,
    ) -> Result<Option<SignatureStatus>> {
        let params = ([sig.to_string()],);
        let resp: WithContext<Vec<Option<RpcSignatureStatus>>> =
            self.call("getSignatureStatuses", &params).await?;
        let Some(Some(status)) = resp.value.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some(SignatureStatus {
            confirmed: matches!(
                status.confirmation_status.as_deref(),
                Some("confirmed" | "finalized")
            ),
            finalized: matches!(
                status.confirmation_status.as_deref(),
                Some("finalized")
            ),
            err: status.err,
        }))
    }

    pub async fn get_transaction(
        &self,
        sig: &Signature,
    ) -> Result<Option<TransactionInfo>> {
        let params = (
            sig.to_string(),
            TransactionConfig {
                encoding: "json",
                commitment: Commitment::Confirmed.as_str(),
                max_supported_transaction_version: 0,
            },
        );
        let raw: Option<RpcTransaction> =
            self.call_nullable("getTransaction", &params).await?;
        Ok(raw.map(|tx| {
            let meta = tx.meta.unwrap_or_default();
            let lookup_tables = tx
                .transaction
                .and_then(|body| body.message.address_table_lookups)
                .map(|lookups| lookups.len())
                .unwrap_or(0);
            TransactionInfo {
                slot: tx.slot,
                block_time: tx.block_time,
                err: meta.err,
                fee: meta.fee,
                pre_balances: meta.pre_balances,
                post_balances: meta.post_balances,
                logs: meta.log_messages.unwrap_or_default(),
                lookup_tables,
            }
        }))
    }

    pub async fn get_block_time(&self, slot: u64) -> Result<Option<i64>> {
        self.call_nullable("getBlockTime", &(slot,)).await
    }

    pub async fn get_block(&self, slot: u64) -> Result<Option<BlockInfo>> {
        let params = (
            slot,
            BlockConfig {
                transaction_details: "none",
                rewards: false,
                commitment: Commitment::Confirmed.as_str(),
                max_supported_transaction_version: 0,
            },
        );
        self.call_nullable("getBlock", &params).await
    }

    pub async fn get_signatures_for_address(
        &self,
        pk: &Pubkey,
        limit: usize,
    ) -> Result<Vec<String>> {
        let params = (
            pk.to_string(),
            SignaturesConfig {
                limit,
                commitment: Commitment::Confirmed.as_str(),
            },
        );
        let infos: Vec<RpcSignatureInfo> =
            self.call("getSignaturesForAddress", &params).await?;
        Ok(infos.into_iter().map(|info| info.signature).collect())
    }

    // A delivered transaction lands in the ledger a moment later — poll for it.
    pub async fn await_transaction(
        &self,
        sig: &Signature,
        timeout: Duration,
    ) -> Result<TransactionInfo> {
        crate::check::poll_until(timeout, TX_POLL, async || {
            self.get_transaction(sig).await
        })
        .await?
        .ok_or_else(|| {
            format!("transaction {sig} not found within {timeout:?}").into()
        })
    }
}

pub fn custom_error_code(err: &json::Value) -> Option<u32> {
    use json::JsonValueTrait;
    err.get("InstructionError")?
        .get(1)?
        .get("Custom")?
        .as_u64()
        .and_then(|code| u32::try_from(code).ok())
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

    fn sum_series(
        &self,
        name: &str,
        value: impl Fn(&str, f64) -> f64,
    ) -> Option<f64> {
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
            let value = rest
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<f64>().ok());
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
    client: reqwest::Client,
    url: String,
}

impl MetricsCollector {
    pub fn new(metrics_url: &str) -> Self {
        Self {
            client: http::client(),
            url: format!("{}/metrics", metrics_url.trim_end_matches('/')),
        }
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
