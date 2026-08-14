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
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rpc error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

#[derive(Deserialize)]
struct Envelope<T> {
    result: Option<T>,
    error: Option<EnvelopeError>,
}

#[derive(Deserialize)]
struct EnvelopeError {
    code: i64,
    message: String,
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
    pub err: Option<json::Value>,
}

#[derive(Deserialize)]
struct RpcSignatureInfo {
    signature: String,
}

const TX_POLL: Duration = Duration::from_millis(200);

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
        params: &str,
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
        params: &str,
    ) -> Result<Option<T>> {
        let body = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{params}}}"#
        );
        let response = http::post_json(&self.client, &self.url, body).await?;
        let envelope: Envelope<T> = json::from_str(&response).map_err(|e| {
            format!("{method}: unexpected response shape: {e} ({response})")
        })?;
        if let Some(err) = envelope.error {
            return Err(Box::new(RpcError {
                code: err.code,
                message: err.message,
            }));
        }
        Ok(envelope.result)
    }

    pub async fn get_health(&self) -> Result<String> {
        self.call("getHealth", "[]").await
    }

    pub async fn get_slot(&self) -> Result<u64> {
        self.call("getSlot", r#"[{"commitment":"confirmed"}]"#)
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
        let params = format!(r#"["{pk}", {{"commitment":"confirmed"}}]"#);
        let resp: WithContext<u64> = self.call("getBalance", &params).await?;
        Ok(resp.value)
    }

    pub async fn request_airdrop(
        &self,
        pk: &Pubkey,
        lamports: u64,
    ) -> Result<String> {
        let params = format!(r#"["{pk}", {lamports}]"#);
        self.call("requestAirdrop", &params).await
    }

    pub async fn get_account(&self, pk: &Pubkey) -> Result<Option<Account>> {
        let params = format!(
            r#"["{pk}", {{"encoding":"base64","commitment":"confirmed"}}]"#
        );
        let resp: WithContext<Option<RpcAccount>> =
            self.call("getAccountInfo", &params).await?;
        resp.value.map(decode_rpc_account).transpose()
    }

    pub async fn get_multiple_accounts(
        &self,
        pks: &[Pubkey],
    ) -> Result<Vec<Option<Account>>> {
        let keys = pks
            .iter()
            .map(|pk| format!(r#""{pk}""#))
            .collect::<Vec<_>>()
            .join(",");
        let params = format!(
            r#"[[{keys}], {{"encoding":"base64","commitment":"confirmed"}}]"#
        );
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
            .call("getLatestBlockhash", r#"[{"commitment":"confirmed"}]"#)
            .await?;
        Ok(Hash::from_str(&resp.value.blockhash)?)
    }

    pub async fn send_transaction(
        &self,
        tx: &Transaction,
    ) -> Result<Signature> {
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(bincode::serialize(tx)?);
        let params = format!(
            r#"["{encoded}", {{"encoding":"base64","skipPreflight":true,"preflightCommitment":"confirmed"}}]"#
        );
        let sig: String = self.call("sendTransaction", &params).await?;
        Ok(Signature::from_str(&sig)?)
    }

    pub async fn signature_confirmed(&self, sig: &Signature) -> Result<bool> {
        let Some(status) = self.get_signature_status(sig).await? else {
            return Ok(false);
        };
        if let Some(err) = &status.err {
            return Err(
                format!("transaction {sig} failed on-chain: {err:?}").into()
            );
        }
        Ok(status.confirmed)
    }

    pub async fn get_signature_status(
        &self,
        sig: &Signature,
    ) -> Result<Option<SignatureStatus>> {
        let params = format!(r#"[["{sig}"]]"#);
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
            err: status.err,
        }))
    }

    pub async fn get_transaction(
        &self,
        sig: &Signature,
    ) -> Result<Option<TransactionInfo>> {
        let params = format!(
            r#"["{sig}", {{"encoding":"json","commitment":"confirmed","maxSupportedTransactionVersion":0}}]"#
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
        self.call_nullable("getBlockTime", &format!("[{slot}]"))
            .await
    }

    pub async fn get_block(&self, slot: u64) -> Result<Option<BlockInfo>> {
        let params = format!(
            r#"[{slot}, {{"transactionDetails":"none","rewards":false,"commitment":"confirmed","maxSupportedTransactionVersion":0}}]"#
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
        let params = format!(
            r#"["{pk}", {{"limit":{limit},"commitment":"confirmed"}}]"#
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
