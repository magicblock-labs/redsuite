use super::{
    hostile::{seeded_key, Fixture},
    *,
};
use message::VersionedMessage;
use redsuite_core::{
    random::{self, Rng},
    rpc_types::config::RpcSimulateTransactionConfig,
    transport::wsraw::RawWs,
    wait,
};
use serde_json::{json, Value};
use std::time::Instant;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};
use tracing::info;
use url::Url;

const FLOOD: usize = 64;
const NESTING: usize = 256;
const OVERSIZE: usize = 2 * 1024 * 1024;
const FRAME_WAIT: Duration = Duration::from_secs(3);

pub struct ProtocolInputs {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

struct Outcome {
    label: &'static str,
    replied: bool,
    accepted: bool,
}

async fn raw_request(
    url: &Url,
    body: &str,
    declared: Option<usize>,
    wait: Duration,
) -> Result<Option<String>> {
    let host = url.host_str().context("rpc host missing")?;
    let port = url.port_or_known_default().context("rpc port missing")?;
    let mut socket = TcpStream::connect((host, port)).await?;
    let length = declared.unwrap_or(body.len());
    let head = format!(
        "POST {} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n",
        url.path()
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    socket.flush().await?;
    let mut reply = Vec::new();
    match time::timeout(wait, socket.read_to_end(&mut reply)).await {
        Ok(read) => {
            read?;
            Ok(Some(String::from_utf8_lossy(&reply).into_owned()))
        }
        Err(_) => Ok(None),
    }
}

fn classify(label: &'static str, reply: Option<&str>) -> Outcome {
    let accepted = reply.is_some_and(|reply| {
        reply.starts_with("HTTP/1.1 200")
            && reply.contains("\"result\"")
            && !reply.contains("\"error\"")
    });
    Outcome {
        label,
        replied: reply.is_some(),
        accepted,
    }
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

    async fn http_cases(&self, fixture: &Fixture, rng: &mut impl Rng) -> Result<()> {
        let url = self.er.api().url().clone();
        let before = fixture.images(&[]).await?;
        let absent = seeded_key(rng);
        let slot =
            |id: u64| format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"getSlot","params":[]}}"#);

        let batch = format!("[{},{}]", slot(1), slot(2));
        let nested = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{}{}]}}"#,
            "[".repeat(NESTING),
            "]".repeat(NESTING)
        );
        let oversized = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{}"]}}"#,
            "A".repeat(OVERSIZE)
        );
        let truncated = slot(1);

        let cases: Vec<(&'static str, String, Option<usize>)> = vec![
            ("batch", batch, None),
            ("nesting", nested, None),
            ("oversized body", oversized, None),
            (
                "short body for declared length",
                truncated.clone(),
                Some(truncated.len() + 64),
            ),
            ("trailing bytes after body", format!("{truncated}{{"), None),
            ("empty body", String::new(), None),
            ("not json", "not json at all".to_owned(), None),
        ];

        for (label, body, declared) in cases {
            let reply = raw_request(&url, &body, declared, FRAME_WAIT).await?;
            let outcome = classify(label, reply.as_deref());
            info!(
                case = outcome.label,
                replied = outcome.replied,
                accepted = outcome.accepted,
                bytes = body.len(),
                "hostile protocol input"
            );
            self.healthy().await?;
            fixture.unchanged(&before).await?;
        }

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

        for (label, params) in [
            ("unknown method", json!([])),
            ("wrong parameter type", json!([42])),
            ("missing parameter", json!([])),
        ] {
            let method = if label == "unknown method" {
                RpcRequest::Custom { method: "redsuiteNoSuchMethod" }
            } else {
                RpcRequest::GetAccountInfo
            };
            let result: result::Result<Value, RpcError> =
                self.er.api().try_call(method, &params).await?;
            let error = result.err().context("hostile request acknowledged")?;
            info!(
                case = label,
                code = error.error.code.code(),
                "hostile protocol request"
            );
            self.healthy().await?;
        }
        fixture.unchanged(&before).await
    }

    async fn simulation_cases(&self, fixture: &mut Fixture, rng: &mut impl Rng) -> Result<()> {
        let before = fixture.images(&[]).await?;
        for sig_verify in [false, true] {
            let nonce = rng.gen_range(1..u64::MAX);
            let message = fixture.message(nonce, 1, &[]).await?;
            let tx = Signed::from_versioned(
                &self.er,
                &fixture.payer,
                VersionedMessage::Legacy(message),
                None,
            )
            .await?;
            let config = RpcSimulateTransactionConfig {
                sig_verify,
                replace_recent_blockhash: !sig_verify,
                encoding: Some(UiTransactionEncoding::Base64),
                ..RpcSimulateTransactionConfig::default()
            };
            let simulated: Value = self
                .er
                .api()
                .call_value(RpcRequest::SimulateTransaction, &(&tx.encoded, config))
                .await?;
            ensure!(simulated["err"].is_null(), "{simulated}");
            fixture.statusless(&tx, self.er.api().get_slot().await?, None).await?;
            info!(sig_verify, "simulation leaves no admission");
        }
        fixture.unchanged(&before).await
    }

    async fn socket_cases(&self, fixture: &Fixture) -> Result<()> {
        let before = fixture.images(&[]).await?;
        let cell = fixture.keys[1];
        let mut flood = RawWs::connect(self.er.ws_url()).await?;
        let mut ids = Vec::with_capacity(FLOOD);
        for _ in 0..FLOOD {
            ids.push(flood.account_subscribe(&cell).await?);
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
            ensure!(
                !intruder.account_unsubscribe(*id).await?,
                "cross-connection unsubscribe accepted for {id}"
            );
        }
        ensure!(flood.account_unsubscribe(ids[0]).await?);
        ensure!(flood.malformed_frames() == 0 && intruder.malformed_frames() == 0);
        info!(
            subscriptions = ids.len(),
            "subscription flood isolated from other connections"
        );
        intruder.close().await?;
        flood.close().await?;
        self.healthy().await?;
        fixture.unchanged(&before).await
    }
}

impl Scenario for ProtocolInputs {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        let mut rng = random::rng(self.base.config().seed, &["redhat", "protocol_inputs"], 0);
        let mut fixture = Fixture::new(self.base.clone(), self.er.clone(), &mut rng).await?;
        let mut control = RawWs::connect(self.er.ws_url()).await?;
        let subscription = control.account_subscribe(&fixture.keys[1]).await?;

        self.http_cases(&fixture, &mut rng).await?;
        self.simulation_cases(&mut fixture, &mut rng).await?;
        self.socket_cases(&fixture).await?;

        let started = Instant::now();
        let message = fixture.message(9_000, 5, &[]).await?;
        fixture.repaired(VersionedMessage::Legacy(message), 5, 1, false).await?;
        let (method, id, _) = control
            .next_notification(TIMEOUT)
            .await?
            .context("control notification lost after hostile protocol inputs")?;
        ensure!(method == "accountNotification" && id == subscription);
        ensure!(
            started.elapsed() < TIMEOUT,
            "recovery write exceeded the confirmation deadline"
        );
        control.close().await?;
        Ok(())
    }
}
