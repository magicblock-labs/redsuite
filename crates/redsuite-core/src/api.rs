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

impl TxError {
    pub fn custom_code(&self) -> Option<u32> {
        custom_error_code(&self.err)
    }
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

#[derive(Deserialize)]
struct RpcTransactionMeta {
    err: Option<json::Value>,
    #[serde(rename = "logMessages")]
    log_messages: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct RpcBlock {
    #[serde(rename = "blockTime")]
    block_time: Option<i64>,
}

#[derive(Debug)]
pub struct BlockInfo {
    pub block_time: Option<i64>,
}

#[derive(Debug)]
pub struct TransactionInfo {
    pub slot: u64,
    pub block_time: Option<i64>,
    // on-chain execution error; None = the transaction succeeded
    pub err: Option<json::Value>,
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

pub struct BatchBody {
    body: String,
    len: usize,
}

impl BatchBody {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
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
        let response = http::post_json(&self.client, &self.url, body).await?;
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

    pub async fn get_latest_blockhash(&self) -> Result<Hash> {
        let resp: WithContext<RpcBlockhash> = self
            .call("getLatestBlockhash", &(CommitmentConfig::confirmed(),))
            .await?;
        Ok(Hash::from_str(&resp.value.blockhash)?)
    }

    pub fn batch_send_body(transactions: &[Transaction]) -> Result<BatchBody> {
        const CONFIG: &str = r#"{"encoding":"base64","skipPreflight":true,"preflightCommitment":"confirmed"}"#;
        let mut body = String::from("[");
        for (index, tx) in transactions.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            let encoded = base64::engine::general_purpose::STANDARD
                .encode(bincode::serialize(tx)?);
            let params = format!(r#"["{encoded}",{CONFIG}]"#);
            body.push_str(&crate::transport::conn::request_text(
                index as u64 + 1,
                "sendTransaction",
                &params,
            ));
        }
        body.push(']');
        Ok(BatchBody {
            body,
            len: transactions.len(),
        })
    }

    pub async fn send_batch(&self, batch: &BatchBody) -> Result<usize> {
        if batch.is_empty() {
            return Ok(0);
        }
        let response =
            http::post_json(&self.client, &self.url, batch.body.clone())
                .await?;
        let envelopes: Vec<Envelope<String>> = json::from_str(&response)
            .map_err(|error| {
                format!(
                    "sendTransaction batch: unexpected response shape: \
                     {error} ({response})"
                )
            })?;
        if envelopes.len() != batch.len {
            return Err(format!(
                "sendTransaction batch: sent {} requests, got {} responses",
                batch.len,
                envelopes.len()
            )
            .into());
        }
        let mut answered = vec![false; batch.len];
        let mut rejected = 0;
        for envelope in &envelopes {
            let id = envelope.id.ok_or_else(|| {
                "sendTransaction batch: response entry carries no id"
                    .to_string()
            })?;
            let index = usize::try_from(id)
                .ok()
                .filter(|id| (1..=batch.len).contains(id))
                .ok_or_else(|| {
                    format!(
                        "sendTransaction batch: response id {id} outside 1..={}",
                        batch.len
                    )
                })?
                - 1;
            if std::mem::replace(&mut answered[index], true) {
                return Err(format!(
                    "sendTransaction batch: duplicate response id {id}"
                )
                .into());
            }
            if envelope.error.is_some() {
                rejected += 1;
            } else if envelope.result.is_none() {
                return Err(format!(
                    "sendTransaction batch: response id {id} carries neither \
                     result nor error"
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
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(bincode::serialize(tx)?);
        let params = (
            encoded,
            SendTransactionConfig {
                encoding: "base64",
                skip_preflight: true,
                preflight_commitment: Commitment::Confirmed.as_str(),
            },
        );
        let sig: String = self.call("sendTransaction", &params).await?;
        Ok(Signature::from_str(&sig)?)
    }

    pub async fn confirm(
        &self,
        sig: &Signature,
        options: ConfirmOptions,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + options.deadline;
        loop {
            if let Some(status) = self.get_signature_status(sig).await? {
                if let Some(err) = status.err {
                    return Err(Box::new(TxError {
                        signature: *sig,
                        err,
                    }));
                }
                if status.meets(options.commitment) {
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Box::new(ConfirmTimeout {
                    signature: *sig,
                    deadline: options.deadline,
                }));
            }
            tokio::time::sleep(options.poll).await;
        }
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
            let (err, logs) = match tx.meta {
                Some(meta) => (meta.err, meta.log_messages.unwrap_or_default()),
                None => (None, Vec::new()),
            };
            let lookup_tables = tx
                .transaction
                .and_then(|body| body.message.address_table_lookups)
                .map(|lookups| lookups.len())
                .unwrap_or(0);
            TransactionInfo {
                slot: tx.slot,
                block_time: tx.block_time,
                err,
                logs,
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
        let raw: Option<RpcBlock> =
            self.call_nullable("getBlock", &params).await?;
        Ok(raw.map(|block| BlockInfo {
            block_time: block.block_time,
        }))
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
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(tx) = self.get_transaction(sig).await? {
                return Ok(tx);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "transaction {sig} not found within {timeout:?}"
                )
                .into());
            }
            tokio::time::sleep(TX_POLL).await;
        }
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
        let label_prefix = format!("{name}{{");
        let mut sum = 0.0;
        let mut matched = false;
        for (key, value) in &self.0 {
            if key.starts_with(&label_prefix) {
                matched = true;
                sum += value;
            }
        }
        if matched {
            Some(sum)
        } else {
            self.get(name)
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

pub async fn scrape_metrics(metrics_url: &str) -> Result<Metrics> {
    let url = format!("{}/metrics", metrics_url.trim_end_matches('/'));
    let text = http::get_once(&url).await?;
    Ok(Metrics::parse(&text))
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
        let count = self.counter(&suffixed(name, "_count"))?;
        if count <= 0.0 {
            return None;
        }
        let sum = self.counter(&suffixed(name, "_sum"))?;
        Some(sum / count)
    }

    pub fn counter_all(&self, name: &str) -> Option<f64> {
        let label_prefix = format!("{name}{{");
        let mut sum = 0.0;
        let mut matched = false;
        for (key, after_value) in &self.after.0 {
            if !key.starts_with(&label_prefix) {
                continue;
            }
            matched = true;
            sum += after_value - self.before.get(key).unwrap_or(0.0);
        }
        if matched {
            Some(sum)
        } else {
            self.counter(name)
        }
    }

    // Window average over ALL series of a (possibly labeled) histogram.
    pub fn histogram_avg_all(&self, name: &str) -> Option<f64> {
        let count = self.counter_all(&format!("{name}_count"))?;
        if count <= 0.0 {
            return None;
        }
        let sum = self.counter_all(&format!("{name}_sum"))?;
        Some(sum / count)
    }

    pub fn before(&self) -> &Metrics {
        &self.before
    }

    pub fn after(&self) -> &Metrics {
        &self.after
    }
}

// mbv_x{kind="y"} + _sum → mbv_x_sum{kind="y"}
fn suffixed(name: &str, suffix: &str) -> String {
    match name.find('{') {
        Some(i) => format!("{}{}{}", &name[..i], suffix, &name[i..]),
        None => format!("{name}{suffix}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_delta_semantics() {
        let before = Metrics::parse(
            "# HELP mbv_clones total clones\n\
             mbv_clones 10\n\
             mbv_monitored_accounts 5\n",
        );
        let after = Metrics::parse(
            "mbv_clones 17\n\
             mbv_monitored_accounts 41\n\
             mbv_evictions{reason=\"lru\"} 3\n",
        );
        let delta = MetricsDelta::new(before, after);
        assert_eq!(delta.counter("mbv_clones"), Some(7.0));
        // appeared inside the window: counts from zero
        assert_eq!(delta.counter("mbv_evictions"), Some(3.0));
        assert_eq!(delta.counter(r#"mbv_evictions{reason="lru"}"#), Some(3.0));
        // absent from the after scrape: not exposed by this build
        assert_eq!(delta.counter("mbv_nonexistent"), None);
        assert_eq!(delta.gauge("mbv_monitored_accounts"), Some(41.0));
        assert_eq!(delta.before().get("mbv_monitored_accounts"), Some(5.0));
    }

    #[test]
    fn histogram_window_average() {
        let before = Metrics::parse(
            "mbv_proc_time_sum 10.0\n\
             mbv_proc_time_count 100\n\
             mbv_ensure_time_sum{kind=\"transaction\"} 2.0\n\
             mbv_ensure_time_count{kind=\"transaction\"} 40\n\
             mbv_idle_time_sum 5.0\n\
             mbv_idle_time_count 50\n",
        );
        let after = Metrics::parse(
            "mbv_proc_time_sum 14.0\n\
             mbv_proc_time_count 300\n\
             mbv_ensure_time_sum{kind=\"transaction\"} 3.0\n\
             mbv_ensure_time_count{kind=\"transaction\"} 140\n\
             mbv_idle_time_sum 5.0\n\
             mbv_idle_time_count 50\n",
        );
        let delta = MetricsDelta::new(before, after);
        assert_eq!(delta.histogram_avg("mbv_proc_time"), Some(0.02));
        assert_eq!(
            delta.histogram_avg(r#"mbv_ensure_time{kind="transaction"}"#),
            Some(0.01)
        );
        // recorded nothing inside the window
        assert_eq!(delta.histogram_avg("mbv_idle_time"), None);
        assert_eq!(delta.histogram_avg("mbv_absent_time"), None);
    }

    #[test]
    fn labeled_family_sums_series_not_the_bare_duplicate() {
        let before = Metrics::parse(
            "mbv_failed{intent_kind=\"commit\",error_kind=\"fit\"} 2\n",
        );
        let after = Metrics::parse(
            "mbv_failed{intent_kind=\"commit\",error_kind=\"fit\"} 5\n\
             mbv_failed{intent_kind=\"commit\",error_kind=\"alt\"} 4\n",
        );
        let delta = MetricsDelta::new(before, after);
        // 3 from the fit series + 4 appeared-in-window, bare key ignored
        assert_eq!(delta.counter_all("mbv_failed"), Some(7.0));
        // unlabeled metrics fall through to the plain counter
        let plain = MetricsDelta::new(
            Metrics::parse("mbv_intents 10\n"),
            Metrics::parse("mbv_intents 16\n"),
        );
        assert_eq!(plain.counter_all("mbv_intents"), Some(6.0));
        assert_eq!(plain.counter_all("mbv_never_seen"), None);
    }

    #[test]
    fn labeled_histogram_window_average_across_series() {
        let before = Metrics::parse(
            "mbv_exec_sum{outcome=\"ok\"} 1.0\n\
             mbv_exec_count{outcome=\"ok\"} 10\n",
        );
        let after = Metrics::parse(
            "mbv_exec_sum{outcome=\"ok\"} 3.0\n\
             mbv_exec_count{outcome=\"ok\"} 30\n\
             mbv_exec_sum{outcome=\"err\"} 1.0\n\
             mbv_exec_count{outcome=\"err\"} 20\n",
        );
        let delta = MetricsDelta::new(before, after);
        // (2.0 + 1.0) / (20 + 20)
        assert_eq!(delta.histogram_avg_all("mbv_exec"), Some(0.075));
    }

    #[test]
    fn typed_params_serialize_to_the_rpc_wire_shapes() {
        assert_eq!(json::to_string(&NO_PARAMS).unwrap(), "[]");
        assert_eq!(
            json::to_string(&(CommitmentConfig::confirmed(),)).unwrap(),
            r#"[{"commitment":"confirmed"}]"#
        );
        assert_eq!(
            json::to_string(&(
                "abc".to_owned(),
                AccountConfig::base64_confirmed()
            ))
            .unwrap(),
            r#"["abc",{"encoding":"base64","commitment":"confirmed"}]"#
        );
        let send = (
            "dGVzdA==".to_owned(),
            SendTransactionConfig {
                encoding: "base64",
                skip_preflight: true,
                preflight_commitment: Commitment::Confirmed.as_str(),
            },
        );
        assert_eq!(
            json::to_string(&send).unwrap(),
            r#"["dGVzdA==",{"encoding":"base64","skipPreflight":true,"preflightCommitment":"confirmed"}]"#
        );
        assert_eq!(
            json::to_string(&(["sig1".to_owned()],)).unwrap(),
            r#"[["sig1"]]"#
        );
        assert_eq!(json::to_string(&(42u64,)).unwrap(), "[42]");
    }

    #[test]
    fn rpc_envelope_errors_keep_code_message_and_data() {
        let text = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32003,"message":"tx verification error","data":{"logs":["a"]}}}"#;
        let envelope: Envelope<String> = json::from_str(text).unwrap();
        assert!(envelope.result.is_none());
        let error = envelope.error.unwrap();
        assert_eq!(error.code, -32003);
        assert_eq!(error.message, "tx verification error");
        assert!(error.data.is_some());

        let bare = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}"#;
        let envelope: Envelope<String> = json::from_str(bare).unwrap();
        assert!(envelope.error.unwrap().data.is_none());
    }

    #[test]
    fn signature_status_meets_commitment_levels() {
        let confirmed = SignatureStatus {
            confirmed: true,
            finalized: false,
            err: None,
        };
        assert!(confirmed.meets(Commitment::Confirmed));
        assert!(!confirmed.meets(Commitment::Finalized));
        let finalized = SignatureStatus {
            confirmed: true,
            finalized: true,
            err: None,
        };
        assert!(finalized.meets(Commitment::Confirmed));
        assert!(finalized.meets(Commitment::Finalized));
    }

    #[test]
    fn confirm_options_default_matches_the_confirm_budget() {
        let options = ConfirmOptions::default();
        assert_eq!(options.commitment, Commitment::Confirmed);
        assert_eq!(options.deadline, Duration::from_secs(20));
        assert_eq!(options.poll, Duration::from_millis(50));
    }

    #[test]
    fn custom_error_code_extraction() {
        let err: json::Value =
            json::from_str(r#"{"InstructionError":[0,{"Custom":2684354560}]}"#)
                .unwrap();
        assert_eq!(custom_error_code(&err), Some(0xA000_0000));
        let not_custom: json::Value =
            json::from_str(r#"{"InstructionError":[1,"InvalidArgument"]}"#)
                .unwrap();
        assert_eq!(custom_error_code(&not_custom), None);
        let other: json::Value =
            json::from_str(r#""BlockhashNotFound""#).unwrap();
        assert_eq!(custom_error_code(&other), None);
    }
}
