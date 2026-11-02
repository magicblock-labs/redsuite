use std::{io, rc::Rc, result, time::Duration};

use anyhow::{ensure, Context};
use base64::{engine::general_purpose::STANDARD, Engine};
use json::JsonValueTrait;
use message::VersionedMessage;
use serde_json::{json, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};
use tracing::debug;
use url::Url;

use redsuite_core::{
    api::RpcError,
    random::{self, Rng},
    rpc_types::request::RpcRequest,
    transport::wsraw::RawWs,
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};

use super::{fixture::Fixture, TIMEOUT};

const FLOOD: usize = 64;
const NESTING: usize = 256;
const BODY_LIMIT: usize = 1024 * 1024;
const QUERY_LIMIT: usize = 100;
const FRAME_ID: u64 = 9_001;
const FRAME_WAIT: Duration = Duration::from_secs(3);

/// Checks HTTP/JSON/WebSocket input bounds and isolation between client connections.
///
/// Malformed requests are interleaved with confirmed writes on a healthy connection,
/// so refusal alone cannot mask lost RPC or subscription service.
pub struct ProtocolInputs {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

/// Sends deliberately malformed HTTP framing that a normal client would repair.
/// A reset or incomplete-body timeout may be a valid refusal, represented by `None`.
async fn raw_request(url: &Url, body: &str, declared: Option<usize>) -> Result<Option<String>> {
    let host = url.host_str().context("rpc host missing")?;
    let port = url.port_or_known_default().context("rpc port missing")?;
    let mut socket = TcpStream::connect((host, port)).await?;
    let length = declared.unwrap_or(body.len());
    let head = format!(
        "POST {} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n",
        url.path()
    );
    let written = async {
        socket.write_all(head.as_bytes()).await?;
        socket.write_all(body.as_bytes()).await?;
        socket.flush().await
    }
    .await;
    if let Err(error) = written {
        ensure!(
            matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
            ),
            "{error}"
        );
        return Ok(None);
    }
    let mut reply = Vec::new();
    match time::timeout(FRAME_WAIT, socket.read_to_end(&mut reply)).await {
        Ok(Ok(_)) => Ok(Some(String::from_utf8_lossy(&reply).into_owned())),
        Ok(Err(error)) => {
            ensure!(error.kind() == io::ErrorKind::ConnectionReset, "{error}");
            Ok(None)
        }
        Err(_) => {
            ensure!(
                declared.is_some(),
                "complete HTTP request received no response"
            );
            Ok(None)
        }
    }
}

/// Distinguishes a successfully served slot query from a transport-level refusal.
fn slot_reply(reply: Option<&str>) -> Result<bool> {
    let Some(reply) = reply.filter(|reply| reply.starts_with("HTTP/1.1 200")) else {
        return Ok(false);
    };
    let (_, body) = reply.split_once("\r\n\r\n").context("HTTP response body missing")?;
    let value: Value = serde_json::from_str(body).context("invalid HTTP JSON response")?;
    let slot =
        |response: &Value| response["result"].as_u64().is_some() && response["error"].is_null();
    Ok(match value.as_array() {
        Some(responses) => responses.len() == 2 && responses.iter().all(slot),
        None => slot(&value),
    })
}

fn slot_request(id: u64) -> String {
    format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"getSlot","params":[]}}"#)
}

fn notified_data(payload: &json::Value) -> Result<Vec<u8>> {
    let encoded = payload["value"]["data"][0].as_str().context("notification data missing")?;
    Ok(STANDARD.decode(encoded)?)
}

impl ProtocolInputs {
    async fn healthy(&self) -> Result<()> {
        wait::retry(
            "rpc keeps serving after hostile input",
            TIMEOUT,
            async || {
                self.er.api().get_slot().await?;
                Ok(())
            },
        )
        .await
    }

    /// Applies the case-specific acceptance policy, then requires RPC service to recover.
    async fn probe(
        &self,
        label: &str,
        body: &str,
        declared: Option<usize>,
        expected: Option<bool>,
    ) -> Result<()> {
        let reply = raw_request(self.er.api().url(), body, declared)
            .await
            .with_context(|| label.to_owned())?;
        let accepted = slot_reply(reply.as_deref()).with_context(|| label.to_owned())?;
        debug!(
            case = label,
            replied = reply.is_some(),
            accepted,
            bytes = body.len(),
            "hostile protocol input"
        );
        if let Some(expected) = expected {
            ensure!(
                accepted == expected,
                "{label}: accepted {accepted}, expected {expected}; reply {reply:?}"
            );
        }
        self.healthy().await?;
        Ok(())
    }

    /// Exercises JSON nesting and framing ambiguity separately from body-size bounds.
    async fn http_cases(&self) -> Result<()> {
        let slot = slot_request(1);
        let nested = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{}{}]}}"#,
            "[".repeat(NESTING),
            "]".repeat(NESTING)
        );
        let cases = [
            (
                "batch",
                format!("[{},{}]", slot_request(1), slot_request(2)),
                None,
                Some(true),
            ),
            ("nesting", nested, None, None),
            (
                "short body for declared length",
                slot.clone(),
                Some(slot.len() + 64),
                Some(false),
            ),
            (
                "trailing bytes after body",
                format!("{slot}{{"),
                None,
                Some(false),
            ),
            ("empty body", String::new(), None, Some(false)),
            ("not json", "not json at all".to_owned(), None, Some(false)),
        ];
        for (label, body, declared, expected) in cases {
            self.probe(label, &body, declared, expected).await?;
        }
        Ok(())
    }

    /// Checks both sides of the HTTP body limit with an otherwise valid query.
    async fn size_cases(&self) -> Result<()> {
        for (size, expected) in [(BODY_LIMIT, true), (BODY_LIMIT + 1, false)] {
            let mut body = slot_request(1);
            body.extend(std::iter::repeat_n(' ', size - body.len()));
            self.probe("HTTP body bound", &body, None, Some(expected)).await?;
        }
        Ok(())
    }

    /// Requires intact account data at the query limit and rejection above it.
    async fn query_cases(&self, fixture: &Fixture) -> Result<()> {
        for count in [QUERY_LIMIT, QUERY_LIMIT + 1] {
            let keys = vec![fixture.keys[1].to_string(); count];
            let result: result::Result<Value, RpcError> = self
                .er
                .api()
                .try_call(
                    RpcRequest::GetMultipleAccounts,
                    &(keys, json!({"encoding": "base64"})),
                )
                .await?;
            if count > QUERY_LIMIT {
                let error = result.expect_err("over-limit account query accepted");
                ensure!(error.error.code.code() == -32602, "{error}");
                continue;
            }
            let response = result?;
            let accounts =
                response["value"].as_array().context("account query returned no array")?;
            ensure!(accounts.len() == count);
            let expected = STANDARD
                .encode(&fixture.expected[1].as_ref().context("query control missing")?.data);
            ensure!(
                accounts.iter().all(|account| account["data"][0] == expected),
                "bounded query lost account data"
            );
        }
        Ok(())
    }

    /// Checks unknown-field tolerance separately from invalid methods and parameter shapes.
    async fn request_cases(&self, rng: &mut impl Rng) -> Result<()> {
        let absent = random::pubkey(rng);
        let lenient: Value = self
            .er
            .api()
            .call(
                RpcRequest::GetAccountInfo,
                &(
                    absent.to_string(),
                    json!({"encoding": "base64", "redsuiteUnknownField": true}),
                ),
            )
            .await?;
        ensure!(lenient["value"].is_null(), "{lenient}");

        for (label, method, params) in [
            (
                "unknown method",
                RpcRequest::Custom { method: "redsuiteNoSuchMethod" },
                json!([]),
            ),
            (
                "wrong parameter type",
                RpcRequest::GetAccountInfo,
                json!([42]),
            ),
            ("missing parameter", RpcRequest::GetAccountInfo, json!([])),
        ] {
            let result: result::Result<Value, RpcError> =
                self.er.api().try_call(method, &params).await?;
            let error = result.err().context("hostile request acknowledged")?;
            debug!(
                case = label,
                code = error.error.code.code(),
                "hostile protocol request"
            );
            self.healthy().await?;
        }
        Ok(())
    }

    /// Separates per-socket refusal policy from continued service on fresh connections.
    async fn frame_cases(&self, fixture: &Fixture) -> Result<()> {
        let cell = fixture.keys[1];
        let frames = [
            ("not json", "not a frame".to_owned(), None),
            ("empty frame", String::new(), None),
            ("array frame", "[1,2,3]".to_owned(), None),
            (
                "unknown subscription method",
                format!(
                    r#"{{"jsonrpc":"2.0","id":{FRAME_ID},"method":"redsuiteNoSuchSub","params":[]}}"#
                ),
                Some(true),
            ),
            (
                "nested params",
                format!(
                    r#"{{"jsonrpc":"2.0","id":{FRAME_ID},"method":"accountSubscribe","params":[{}{}]}}"#,
                    "[".repeat(NESTING),
                    "]".repeat(NESTING)
                ),
                None,
            ),
            (
                "oversized frame",
                format!(
                    r#"{{"jsonrpc":"2.0","id":{FRAME_ID},"method":"accountSubscribe","params":["{}"]}}"#,
                    "A".repeat(BODY_LIMIT + 1)
                ),
                Some(false),
            ),
        ];
        for (label, frame, expected) in frames {
            let mut socket = RawWs::connect(self.er.ws_url()).await?;
            let bytes = frame.len();
            socket.send_text(frame).await?;
            let survived = socket.responsive(&cell).await;
            debug!(case = label, bytes, survived, "hostile socket frame");
            if let Some(expected) = expected {
                ensure!(
                    survived == expected,
                    "{label}: connection survived {survived}, expected {expected}"
                );
            }
            let _ = socket.close().await;
            self.healthy().await?;
        }
        Ok(())
    }

    /// Floods one socket and attempts to cancel its subscriptions from another connection.
    async fn socket_cases(&self, fixture: &mut Fixture) -> Result<()> {
        let cell = fixture.keys[1];
        let mut victim = RawWs::connect(self.er.ws_url()).await?;
        let mut ids = Vec::with_capacity(FLOOD);
        for _ in 0..FLOOD {
            ids.push(victim.account_subscribe(&cell).await?);
        }
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        ensure!(
            sorted.len() == ids.len(),
            "subscription ids reused under flood"
        );

        let mut intruder = RawWs::connect(self.er.ws_url()).await?;
        for id in ids.iter().take(4) {
            match intruder.try_account_unsubscribe(*id).await? {
                Ok(removed) => ensure!(!removed, "cross-connection unsubscribe accepted for {id}"),
                Err(error) => debug!(
                    id,
                    code = error.error.code.code(),
                    "cross-connection unsubscribe refused"
                ),
            }
        }

        let expected = self.write(fixture, 7_100, 3).await?;
        let mut pending = ids[..4].to_vec();
        time::timeout(TIMEOUT, async {
            while !pending.is_empty() {
                let (method, id, payload) = victim
                    .next_notification(TIMEOUT)
                    .await?
                    .context("victim subscription lost after a cross-connection unsubscribe")?;
                ensure!(method == "accountNotification" && ids.contains(&id));
                if notified_data(&payload)? == expected {
                    pending.retain(|target| *target != id);
                }
            }
            Result::Ok(())
        })
        .await
        .with_context(|| format!("attacked subscriptions lost delivery: {pending:?}"))??;
        ensure!(victim.malformed_frames() == 0 && intruder.malformed_frames() == 0);
        debug!(
            subscriptions = ids.len(),
            "flood isolated and victim delivery intact"
        );
        intruder.close().await?;
        victim.close().await?;
        self.healthy().await
    }

    async fn write(&self, fixture: &mut Fixture, nonce: u64, amount: u64) -> Result<Vec<u8>> {
        let message = fixture.message(nonce, amount as i64, &[]).await?;
        fixture
            .check_control(VersionedMessage::Legacy(message), amount, 1, false)
            .await?;
        Ok(fixture.expected[1].as_ref().context("cell expectation missing")?.data.clone())
    }

    /// Proves that an established subscription still observes a newly confirmed write.
    async fn confirmed_write(
        &self,
        fixture: &mut Fixture,
        control: &mut RawWs,
        subscription: u64,
        nonce: u64,
    ) -> Result<()> {
        time::timeout(TIMEOUT, async {
            let expected = self.write(fixture, nonce, 5).await?;
            loop {
                let (method, id, payload) = control
                    .next_notification(TIMEOUT)
                    .await?
                    .context("control notification lost during hostile protocol inputs")?;
                ensure!(method == "accountNotification" && id == subscription);
                if notified_data(&payload)? == expected {
                    return Ok(());
                }
            }
        })
        .await
        .context("control write exceeded the confirmation deadline")?
    }
}

impl Scenario for ProtocolInputs {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    /// Keeps a control subscription alive across hostile HTTP and WebSocket stages.
    async fn run(self) -> Result<()> {
        let mut rng = random::rng(self.base.config().seed, &["redhat", "protocol_inputs"], 0);
        let mut fixture = Fixture::new(self.base.clone(), self.er.clone(), &mut rng).await?;
        let mut control = RawWs::connect(self.er.ws_url()).await?;
        let subscription = control.account_subscribe(&fixture.keys[1]).await?;

        self.http_cases().await?;
        self.confirmed_write(&mut fixture, &mut control, subscription, 7_001).await?;

        self.size_cases().await?;
        self.confirmed_write(&mut fixture, &mut control, subscription, 7_002).await?;

        self.request_cases(&mut rng).await?;
        self.confirmed_write(&mut fixture, &mut control, subscription, 7_003).await?;

        self.frame_cases(&fixture).await?;
        self.socket_cases(&mut fixture).await?;
        self.confirmed_write(&mut fixture, &mut control, subscription, 7_004).await?;

        self.query_cases(&fixture)
            .await
            .context("validator#1845: account-query bound")?;
        control.close().await?;
        Ok(())
    }
}
