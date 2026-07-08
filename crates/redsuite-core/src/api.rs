//! Public-API client. Interim JSON-RPC subset until the redline engine port
//! (pooled transports, WebSocket subscriptions).

use std::{collections::HashMap, str::FromStr};

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

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Raw JSON-RPC call; `params` is the literal JSON params array, e.g.
    /// `["4Nd1…", {"encoding":"base64"}]`.
    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &str,
    ) -> Result<T> {
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
        envelope.result.ok_or_else(|| {
            format!("{method}: response carried neither result nor error")
                .into()
        })
    }

    pub async fn get_health(&self) -> Result<String> {
        self.call("getHealth", "[]").await
    }

    pub async fn server_alive(&self) -> bool {
        match self.get_health().await {
            Ok(_) => true,
            Err(e) => e.is::<RpcError>(),
        }
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
        let Some(raw) = resp.value else {
            return Ok(None);
        };
        let data = base64::engine::general_purpose::STANDARD
            .decode(&raw.data.0)
            .map_err(|e| {
                format!("getAccountInfo: bad base64 account data: {e}")
            })?;
        Ok(Some(Account {
            lamports: raw.lamports,
            data,
            owner: Pubkey::from_str(&raw.owner)?,
            executable: raw.executable,
            rent_epoch: raw.rent_epoch,
        }))
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
        let params = format!(r#"[["{sig}"]]"#);
        let resp: WithContext<Vec<Option<RpcSignatureStatus>>> =
            self.call("getSignatureStatuses", &params).await?;
        let Some(Some(status)) = resp.value.first() else {
            return Ok(false);
        };
        if let Some(err) = &status.err {
            return Err(
                format!("transaction {sig} failed on-chain: {err:?}").into()
            );
        }
        Ok(matches!(
            status.confirmation_status.as_deref(),
            Some("confirmed" | "finalized")
        ))
    }
}

/// Snapshot of the ER's Prometheus `/metrics` endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct Metrics(pub HashMap<String, f64>);

impl Metrics {
    pub fn get(&self, name: &str) -> Option<f64> {
        self.0.get(name).copied()
    }

    /// Keys are stored both with their label set (`mbv_x{a="b"}`) and bare
    /// (`mbv_x`, last sample wins).
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
}
