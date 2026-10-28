use super::{
    hostile::{seeded_key, Fixture, Images},
    *,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use message::VersionedMessage;
use redsuite_core::{
    dlp,
    programs::{Action, CellId, Commit, IntentBundle, ResultCallback, SettlementMode, Task, MAGIC},
    random::{self, Rng},
    rpc_types::config::RpcSimulateTransactionConfig,
    topology,
    transport::wsraw::RawWs,
    wait,
};
use serde_json::{json, Value};
use std::{io, time::Instant};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};
use tracing::info;
use url::Url;

const FLOOD: usize = 64;
const NESTING: usize = 256;
const SIZES: [usize; 4] = [16 * 1024, 256 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024];
const QUERY_WITHIN: usize = 5;
const QUERY_BEYOND: usize = 128;
const FRAME_ID: u64 = 9_001;
const FRAME_WAIT: Duration = Duration::from_secs(3);

pub struct ProtocolInputs {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

#[derive(Clone, Copy, Debug)]
enum Permit {
    Refused,
    Either,
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
    match time::timeout(wait, socket.read_to_end(&mut reply)).await {
        Ok(Ok(_)) => Ok(Some(String::from_utf8_lossy(&reply).into_owned())),
        Ok(Err(error)) => {
            ensure!(error.kind() == io::ErrorKind::ConnectionReset, "{error}");
            Ok(None)
        }
        Err(_) => Ok(None),
    }
}

fn accepted(reply: Option<&str>) -> bool {
    reply.is_some_and(|reply| {
        reply.starts_with("HTTP/1.1 200")
            && reply.contains("\"result\"")
            && !reply.contains("\"error\"")
    })
}

fn slot_request(id: u64) -> String {
    format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"getSlot","params":[]}}"#)
}

fn notified_data(payload: &json::Value) -> Result<Vec<u8>> {
    let payload: Value = serde_json::from_str(&payload.to_string())?;
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

    async fn probe(
        &self,
        fixture: &Fixture,
        before: &Images,
        label: &str,
        body: &str,
        declared: Option<usize>,
        permit: Permit,
    ) -> Result<bool> {
        let url = self.er.api().url().clone();
        let reply = raw_request(&url, body, declared, FRAME_WAIT).await?;
        let accepted = accepted(reply.as_deref());
        info!(
            case = label,
            replied = reply.is_some(),
            accepted,
            bytes = body.len(),
            ?permit,
            "hostile protocol input"
        );
        if matches!(permit, Permit::Refused) {
            ensure!(!accepted, "{label} was accepted");
        }
        self.healthy().await?;
        fixture.unchanged(before).await?;
        Ok(accepted)
    }

    async fn http_cases(&self, fixture: &Fixture) -> Result<()> {
        let before = fixture.images(&[]).await?;
        let slot = slot_request(1);
        let nested = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{}{}]}}"#,
            "[".repeat(NESTING),
            "]".repeat(NESTING)
        );
        let cases: [(&str, String, Option<usize>, Permit); 6] = [
            (
                "batch",
                format!("[{},{}]", slot_request(1), slot_request(2)),
                None,
                Permit::Either,
            ),
            ("nesting", nested, None, Permit::Either),
            (
                "short body for declared length",
                slot.clone(),
                Some(slot.len() + 64),
                Permit::Refused,
            ),
            (
                "trailing bytes after body",
                format!("{slot}{{"),
                None,
                Permit::Refused,
            ),
            ("empty body", String::new(), None, Permit::Refused),
            (
                "not json",
                "not json at all".to_owned(),
                None,
                Permit::Refused,
            ),
        ];
        for (label, body, declared, permit) in cases {
            self.probe(fixture, &before, label, &body, declared, permit).await?;
        }
        Ok(())
    }

    async fn size_cases(&self, fixture: &Fixture) -> Result<()> {
        let before = fixture.images(&[]).await?;
        let mut refused_at = None;
        for size in SIZES {
            let body = format!(
                r#"{{"jsonrpc":"2.0",{}"id":1,"method":"getSlot","params":[]}}"#,
                " ".repeat(size)
            );
            let permit = if refused_at.is_some() { Permit::Refused } else { Permit::Either };
            let taken =
                self.probe(fixture, &before, "body size ladder", &body, None, permit).await?;
            if !taken && refused_at.is_none() {
                refused_at = Some(size);
            }
        }
        info!(?refused_at, "body size boundary located");
        ensure!(
            refused_at.is_some(),
            "no body size in the ladder was refused"
        );
        Ok(())
    }

    async fn query_cases(&self, fixture: &Fixture, rng: &mut impl Rng) -> Result<()> {
        let before = fixture.images(&[]).await?;
        for (label, count, permit) in [
            ("within query bound", QUERY_WITHIN, Permit::Either),
            ("beyond query bound", QUERY_BEYOND, Permit::Refused),
        ] {
            let keys: Vec<String> = (0..count).map(|_| seeded_key(rng).to_string()).collect();
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[{}]}}"#,
                json!(keys)
            );
            self.probe(fixture, &before, label, &body, None, permit).await?;
        }
        Ok(())
    }

    async fn request_cases(&self, fixture: &Fixture, rng: &mut impl Rng) -> Result<()> {
        let before = fixture.images(&[]).await?;
        let absent = seeded_key(rng);
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
        for sig_verify in [false, true] {
            for skip_preflight in [false, true] {
                let mut before = fixture.images(&[]).await?;
                let nonce = rng.gen_range(1..u64::MAX);
                let message = fixture.message(nonce, 1, &fixture.keys[3..4]).await?;
                let tx = Signed::from_versioned(
                    &self.er,
                    &fixture.payer,
                    VersionedMessage::Legacy(message.clone()),
                    None,
                )
                .await?;
                let config = RpcSimulateTransactionConfig {
                    sig_verify,
                    replace_recent_blockhash: !sig_verify,
                    encoding: Some(UiTransactionEncoding::Base64),
                    ..RpcSimulateTransactionConfig::default()
                };
                let start = self.er.api().get_slot().await?;
                let simulated: Value = self
                    .er
                    .api()
                    .call_value(
                        RpcRequest::SimulateTransaction,
                        &(&tx.encoded, config.clone()),
                    )
                    .await?;
                ensure!(simulated["err"].is_null(), "{simulated}");
                ensure!(simulated["unitsConsumed"].as_u64().is_some_and(|units| units > 0));
                fixture.statusless(&tx, start, None).await?;
                let cold = self.er.local_images(&fixture.keys[3..4]).await?.pop().flatten();
                ensure!(cold.is_none() || cold == fixture.base_images[3]);
                before.local[3] = cold;
                fixture.unchanged(&before).await?;
                let mut forged: transaction::versioned::VersionedTransaction =
                    wincode::deserialize_exact(&STANDARD.decode(&tx.encoded)?)?;
                forged.signatures[0] = Signature::default();
                let encoded = STANDARD.encode(wincode::serialize(&forged)?);
                let result: result::Result<
                    redsuite_core::rpc_types::response::Response<Value>,
                    RpcError,
                > = self
                    .er
                    .api()
                    .try_call(RpcRequest::SimulateTransaction, &(&encoded, config))
                    .await?;
                if sig_verify {
                    let error =
                        result.expect_err("forged signature simulated with verification enabled");
                    ensure!(
                        error.error.message.to_lowercase().contains("signature"),
                        "{error}"
                    );
                } else {
                    ensure!(result?.value["err"].is_null());
                }
                let result: result::Result<String, RpcError> = self
                    .er
                    .api()
                    .try_call(
                        RpcRequest::SendTransaction,
                        &(
                            &encoded,
                            RpcSendTransactionConfig {
                                skip_preflight,
                                encoding: Some(UiTransactionEncoding::Base64),
                                max_retries: Some(0),
                                ..Default::default()
                            },
                        ),
                    )
                    .await?;
                let error = result.expect_err("forged signature submitted");
                ensure!(
                    error.error.message.to_lowercase().contains("signature"),
                    "{error}"
                );
                fixture.unchanged(&before).await?;
                let signature = fixture
                    .repaired(VersionedMessage::Legacy(message), 1, 1, skip_preflight)
                    .await?;
                ensure!(signature == tx.signature);
                info!(sig_verify, "simulation leaves no admission");
            }
        }
        self.service_simulations(fixture).await
    }

    async fn service_simulations(&self, fixture: &mut Fixture) -> Result<()> {
        let payer = fixture.payer.pubkey();
        let cell = fixture.keys[1];
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        self.er.account(&vault).await?.context("simulation fee vault missing")?;
        let protected = [magic_api::MAGIC_CONTEXT_PUBKEY, magic_api::EPHEMERAL_VAULT_PUBKEY, vault];
        self.er.accounts(&protected).await?;
        let id = CellId::new(payer, 777);
        let target = REDSHIFT.pda(id).0;
        let create = Instruction {
            program_id: magic_api::id(),
            accounts: vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(target, true),
                AccountMeta::new(magic_api::EPHEMERAL_VAULT_PUBKEY, false),
            ],
            data: magic_api::instruction::MagicBlockInstruction::CreateEphemeralAccount {
                data_len: 32,
            }
            .try_to_vec()?,
        };
        let mut commit = Commit::new(SettlementMode::Commit, &[cell]);
        commit.after_commit.push(
            Action::new(
                payer,
                REDSHIFT
                    .batch()
                    .transfer_from_escrow(fixture.keys[2], 1)
                    .action(cell, dlp::dlp_id()),
                100_000,
            )
            .then(ResultCallback {
                cell,
                refund: None,
                compute_units: 20_000,
            }),
        );
        let mut bundle = IntentBundle::new(payer);
        bundle.validator = Some(self.er.identity());
        bundle.commits.push(commit);
        let settle = |mode| {
            let mut bundle = IntentBundle::new(payer);
            bundle.validator = Some(self.er.identity());
            bundle.commits.push(Commit::new(mode, &[cell]));
            REDSHIFT.batch().intents(bundle).build()
        };
        let services = [
            ("commit", settle(SettlementMode::Commit)),
            ("undelegate", settle(SettlementMode::Undelegate)),
            ("callback", REDSHIFT.batch().intents(bundle).build()),
            (
                "ephemeral",
                REDSHIFT.batch().invoke_signed(create, &[id]).build(),
            ),
            (
                "task",
                REDSHIFT
                    .batch()
                    .invoke(MAGIC.schedule(
                        payer,
                        payer,
                        Task {
                            id: 254,
                            interval_ms: 100,
                            iterations: 1,
                        },
                        vec![REDSHIFT.batch().add(&[cell], 1).build()],
                    ))
                    .build(),
            ),
        ];
        for (label, instruction) in services {
            let service = self.er.local_program_accounts(topology::SERVICE_OWNERS).await?;
            let mut before = fixture.images(&[target]).await?;
            let local = self.er.local_images(&protected).await?;
            let base = self.base.accounts(&protected).await?;
            let message = transaction::Message::new_with_blockhash(
                &[instruction],
                Some(&payer),
                &self.er.api().get_latest_blockhash().await?,
            );
            let tx = Signed::from_versioned(
                &self.er,
                &fixture.payer,
                VersionedMessage::Legacy(message),
                None,
            )
            .await?;
            let start = self.er.api().get_slot().await?;
            let simulated: Value = self
                .er
                .api()
                .call_value(
                    RpcRequest::SimulateTransaction,
                    &(
                        &tx.encoded,
                        RpcSimulateTransactionConfig {
                            sig_verify: true,
                            encoding: Some(UiTransactionEncoding::Base64),
                            ..Default::default()
                        },
                    ),
                )
                .await?;
            ensure!(
                simulated["err"].is_null()
                    && simulated["unitsConsumed"].as_u64().is_some_and(|units| units > 0),
                "{label}: {simulated}"
            );
            let hydrated = self.er.local_images(&[target]).await?.pop().flatten();
            let index = before.keys.iter().position(|key| *key == target).unwrap();
            if before.local[index].is_none() {
                ensure!(
                    hydrated.as_ref().is_none_or(|account| account.lamports == 0
                        && account.data.is_empty()
                        && account.owner == sdk_ids::system_program::ID
                        && !account.executable),
                    "{label}: simulated creation persisted: {hydrated:?}"
                );
                before.local[index] = hydrated;
            }
            fixture.statusless(&tx, start, None).await?;
            fixture.unchanged(&before).await?;
            ensure!(
                self.er.local_images(&protected).await? == local,
                "{label}: simulation changed protocol state"
            );
            ensure!(self.base.accounts(&protected).await? == base);
            ensure!(
                self.er.local_program_accounts(topology::SERVICE_OWNERS).await? == service,
                "{label}: simulation published task or outbox state"
            );
            info!(label, "simulated service body leaves live state unchanged");
        }
        let signature = self
            .er
            .submit_and_confirm(&fixture.payer, &[settle(SettlementMode::Commit)])
            .await?;
        self.er
            .check_commit(&self.base, &signature, &[cell], false)
            .await?
            .check_success("settlement after simulation")?;
        let mut expected = fixture.base_images[1].clone().context("base cell missing")?;
        expected.data = fixture.expected[1].as_ref().context("local cell missing")?.data.clone();
        ensure!(self.base.account(&cell).await? == Some(expected.clone()));
        fixture.base_images[1] = Some(expected);
        Ok(())
    }

    async fn frame_cases(&self, fixture: &Fixture) -> Result<()> {
        let before = fixture.images(&[]).await?;
        let cell = fixture.keys[1];
        let frames: [(&str, String, bool); 6] = [
            ("not json", "not a frame".to_owned(), false),
            ("empty frame", String::new(), false),
            ("array frame", "[1,2,3]".to_owned(), false),
            (
                "unknown subscription method",
                format!(
                    r#"{{"jsonrpc":"2.0","id":{FRAME_ID},"method":"redsuiteNoSuchSub","params":[]}}"#
                ),
                true,
            ),
            (
                "nested params",
                format!(
                    r#"{{"jsonrpc":"2.0","id":{FRAME_ID},"method":"accountSubscribe","params":[{}{}]}}"#,
                    "[".repeat(NESTING),
                    "]".repeat(NESTING)
                ),
                false,
            ),
            (
                "oversized frame",
                format!(
                    r#"{{"jsonrpc":"2.0","id":{FRAME_ID},"method":"accountSubscribe","params":["{}"]}}"#,
                    "A".repeat(SIZES[1])
                ),
                false,
            ),
        ];
        for (label, frame, must_survive) in frames {
            let mut socket = RawWs::connect(self.er.ws_url()).await?;
            let bytes = frame.len();
            socket.send_text(frame).await?;
            let survived = socket.responsive(&cell).await;
            info!(case = label, bytes, survived, "hostile socket frame");
            if must_survive {
                ensure!(survived, "{label} cost a connection that must stay usable");
            }
            let _ = socket.close().await;
            self.healthy().await?;
        }
        fixture.unchanged(&before).await
    }

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
                Err(error) => info!(
                    id,
                    code = error.error.code.code(),
                    "cross-connection unsubscribe refused"
                ),
            }
        }

        let expected = self.write(fixture, 7_100, 3).await?;
        let (method, id, payload) = victim
            .next_notification(TIMEOUT)
            .await?
            .context("victim subscription lost after a cross-connection unsubscribe")?;
        ensure!(method == "accountNotification" && ids.contains(&id));
        ensure!(
            notified_data(&payload)? == expected,
            "victim notification did not carry the fresh write"
        );
        ensure!(victim.malformed_frames() == 0 && intruder.malformed_frames() == 0);
        info!(
            subscriptions = ids.len(),
            "flood isolated and victim delivery intact"
        );
        intruder.close().await?;
        victim.close().await?;
        self.healthy().await
    }

    async fn write(&self, fixture: &mut Fixture, nonce: u64, amount: u64) -> Result<Vec<u8>> {
        let message = fixture.message(nonce, amount as i64, &[]).await?;
        fixture.repaired(VersionedMessage::Legacy(message), amount, 1, false).await?;
        Ok(fixture.expected[1].as_ref().context("cell expectation missing")?.data.clone())
    }

    async fn confirmed_write(
        &self,
        fixture: &mut Fixture,
        control: &mut RawWs,
        subscription: u64,
        nonce: u64,
    ) -> Result<()> {
        let started = Instant::now();
        time::timeout(TIMEOUT, async {
            let expected = self.write(fixture, nonce, 5).await?;
            loop {
                let left = TIMEOUT.saturating_sub(started.elapsed());
                let (method, id, payload) = control
                    .next_notification(left)
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

    async fn run(self) -> Result<()> {
        let mut rng = random::rng(self.base.config().seed, &["redhat", "protocol_inputs"], 0);
        let mut fixture = Fixture::new(self.base.clone(), self.er.clone(), &mut rng).await?;
        let mut control = RawWs::connect(self.er.ws_url()).await?;
        let subscription = control.account_subscribe(&fixture.keys[1]).await?;

        self.http_cases(&fixture).await?;
        self.confirmed_write(&mut fixture, &mut control, subscription, 7_001).await?;

        self.size_cases(&fixture).await?;
        self.confirmed_write(&mut fixture, &mut control, subscription, 7_002).await?;

        self.request_cases(&fixture, &mut rng).await?;
        self.simulation_cases(&mut fixture, &mut rng).await?;
        self.confirmed_write(&mut fixture, &mut control, subscription, 7_003).await?;

        self.frame_cases(&fixture).await?;
        self.socket_cases(&mut fixture).await?;
        self.confirmed_write(&mut fixture, &mut control, subscription, 7_004).await?;

        self.query_cases(&fixture, &mut rng)
            .await
            .context("validator#1845: account-query bound")?;
        control.close().await?;
        Ok(())
    }
}
