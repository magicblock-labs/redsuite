use super::{Fixture, OBSERVE, TIMEOUT};
use account::Account;
use anyhow::{bail, ensure, Context};
use instruction::Instruction;
use json::{JsonContainerTrait, JsonValueTrait, Value};
use message::{Message, VersionedMessage};
use pubkey::Pubkey;
use redsuite_core::{
    api::decode_account,
    rpc_types::response::TransactionError,
    signed::{audit, Signed},
    transport::wsraw::RawWs,
    wait, ChainCtx, Result,
};
use signer::Signer;
use std::{collections::BTreeSet, time::Duration};
use tokio::time::Instant;
use tracing::info;

pub(super) struct Probe {
    signature: String,
    subscription: u64,
    error: Option<TransactionError>,
}

impl Probe {
    pub(super) fn new(tx: &Signed, subscription: u64, error: Option<TransactionError>) -> Self {
        Self {
            signature: tx.signature.to_string(),
            subscription,
            error,
        }
    }
}

struct Stream {
    ws: RawWs,
    subscription: u64,
}

pub(super) struct Channels {
    accounts: Stream,
    logs: Stream,
    failures: Stream,
    signatures: RawWs,
}

#[derive(Default)]
struct Evidence {
    recovered: BTreeSet<&'static str>,
    probe: BTreeSet<&'static str>,
}

const CONTROL_CHANNELS: [&str; 3] = ["account", "logs", "signature"];

pub(super) fn account_image(value: &Value) -> Result<Account> {
    decode_account(json::from_value(value)?)
}

fn failure(value: &Value, expected: Option<&TransactionError>) -> Result<()> {
    ensure!(
        !value.is_null(),
        "failure notification omitted its error (#261)"
    );
    let actual: TransactionError = json::from_value(value)?;
    if let Some(expected) = expected {
        ensure!(
            actual == *expected,
            "wrong diagnostic: expected {expected:?}, observed {actual:?}"
        );
    }
    Ok(())
}

impl Evidence {
    fn record(&mut self, channel: &'static str, control: bool) {
        if control {
            self.recovered.insert(channel);
        } else {
            self.probe.insert(channel);
        }
    }

    fn missing(&self) -> Vec<&'static str> {
        CONTROL_CHANNELS
            .into_iter()
            .filter(|channel| !self.recovered.contains(channel))
            .collect()
    }
}

impl Channels {
    pub(super) async fn open(fixture: &Fixture) -> Result<Self> {
        let url = fixture.er.ws_url();
        let (mut accounts, mut logs, mut failures, signatures) = tokio::try_join!(
            RawWs::connect(url),
            RawWs::connect(url),
            RawWs::connect(url),
            RawWs::connect(url),
        )?;
        let (account, log, failed) = tokio::try_join!(
            accounts.account_subscribe(&fixture.keys[1]),
            logs.logs_subscribe_mentions(&fixture.keys[1]),
            failures.transaction_failure_subscribe(),
        )?;
        Ok(Self {
            accounts: Stream {
                ws: accounts,
                subscription: account,
            },
            logs: Stream { ws: logs, subscription: log },
            failures: Stream {
                ws: failures,
                subscription: failed,
            },
            signatures,
        })
    }

    pub(super) async fn watch(&mut self, tx: &Signed) -> Result<u64> {
        self.signatures.signature_subscribe(&tx.signature).await
    }

    pub(super) async fn submit_failure(
        &mut self,
        fixture: &mut Fixture,
        instructions: &[Instruction],
        error: &TransactionError,
    ) -> Result<(Signed, Probe, u64)> {
        let mut before = fixture.snapshot(&[]).await?;
        let hash = fixture.er.api().get_latest_blockhash().await?;
        let message =
            Message::new_with_blockhash(instructions, Some(&fixture.payer.pubkey()), &hash);
        let tx = Signed::from_versioned(
            &fixture.er,
            &fixture.payer,
            VersionedMessage::Legacy(message),
            Some(error.clone().into()),
        )
        .await?;
        let watch = self.watch(&tx).await?;
        let start = fixture.er.api().get_slot().await?;
        ensure!(tx.send(&fixture.er).await?? == tx.signature.to_string());
        let status = wait::retry(
            "failed transaction has a terminal status",
            TIMEOUT,
            async || {
                let status = fixture.er.api().get_signature_status(&tx.signature).await?;
                status.context("terminal status missing")
            },
        )
        .await?;
        ensure!(
            status.err.as_ref() == Some(error),
            "wrong failure status: {:?}",
            status.err
        );
        audit(&fixture.er, start, &[(&tx, 1)]).await?;
        before.local[0].as_mut().context("payer missing")?.lamports -= tx.fee;
        fixture.expected[0].as_mut().context("payer missing")?.lamports -= tx.fee;
        fixture.unchanged(&before).await?;
        let probe = Probe::new(&tx, watch, Some(error.clone()));
        Ok((tx, probe, status.slot))
    }

    pub(super) async fn control(
        &mut self,
        fixture: &mut Fixture,
        nonce: u64,
        inputs: &[Pubkey],
    ) -> Result<()> {
        self.control_with_probe(fixture, nonce, inputs, None).await?;
        Ok(())
    }

    pub(super) async fn pressure_control(
        &mut self,
        fixture: &mut Fixture,
        nonce: u64,
    ) -> Result<()> {
        self.write(fixture, nonce, &[], None, Duration::ZERO).await?;
        Ok(())
    }

    pub(super) async fn control_with_probe(
        &mut self,
        fixture: &mut Fixture,
        nonce: u64,
        inputs: &[Pubkey],
        probe: Option<&Probe>,
    ) -> Result<BTreeSet<&'static str>> {
        self.write(fixture, nonce, inputs, probe, OBSERVE).await
    }

    async fn write(
        &mut self,
        fixture: &mut Fixture,
        nonce: u64,
        inputs: &[Pubkey],
        probe: Option<&Probe>,
        grace: Duration,
    ) -> Result<BTreeSet<&'static str>> {
        let message = fixture.message(nonce, 1, inputs).await?;
        let control = Signed::from_versioned(
            &fixture.er,
            &fixture.payer,
            VersionedMessage::Legacy(message),
            None,
        )
        .await?;
        let watch = self.watch(&control).await?;
        fixture.check_signed_control(&control, 1, 1, false).await?;
        let image = fixture.expected[1].as_ref().context("control image missing")?;
        self.observe(&control, watch, image, probe, grace).await
    }

    async fn observe(
        &mut self,
        control: &Signed,
        watch: u64,
        image: &Account,
        probe: Option<&Probe>,
        grace: Duration,
    ) -> Result<BTreeSet<&'static str>> {
        let control = control.signature.to_string();
        let mut evidence = Evidence::default();
        let mut deadline = Instant::now() + TIMEOUT;
        let mut settling = false;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (channel, notification) = tokio::select! {
                frame = self.accounts.ws.next_notification(remaining) => ("account", frame?),
                frame = self.logs.ws.next_notification(remaining) => ("logs", frame?),
                frame = self.signatures.next_notification(remaining) => ("signature", frame?),
                frame = self.failures.ws.next_notification(remaining) => ("failures", frame?),
            };
            let Some((method, id, payload)) = notification else { break };
            let value = &payload["value"];
            let is_control = match channel {
                "account" => {
                    ensure!(method == "accountNotification" && id == self.accounts.subscription);
                    let actual = account_image(value)?;
                    ensure!(actual == *image, "account channel reported an uncommitted image (#261): expected {image:?}, observed {actual:?}");
                    true
                }
                "logs" => {
                    ensure!(method == "logsNotification" && id == self.logs.subscription);
                    let signature =
                        value["signature"].as_str().context("logs signature missing")?;
                    ensure!(value.get("err").is_some(), "logs omitted terminal result");
                    if signature == control {
                        ensure!(value["err"].is_null(), "control logs reported failure");
                        ensure!(
                            value["logs"].as_array().is_some_and(|logs| !logs.is_empty()),
                            "logs channel omitted control execution logs (#261)"
                        );
                        true
                    } else if let Some(probe) = probe.filter(|probe| probe.signature == signature) {
                        failure(&value["err"], probe.error.as_ref())?;
                        if matches!(probe.error, Some(TransactionError::InstructionError(..))) {
                            ensure!(
                                value["logs"].as_array().is_some_and(|logs| !logs.is_empty()),
                                "logs channel omitted failed execution logs (#261)"
                            );
                        }
                        false
                    } else {
                        bail!("logs channel reported an unrelated/uncommitted outcome (#261): {payload}");
                    }
                }
                "signature" => {
                    ensure!(method == "signatureNotification");
                    ensure!(
                        value.get("err").is_some(),
                        "signature channel omitted terminal result"
                    );
                    if id == watch {
                        ensure!(value["err"].is_null(), "control signature reported failure");
                        true
                    } else if let Some(probe) = probe.filter(|probe| probe.subscription == id) {
                        failure(&value["err"], probe.error.as_ref())?;
                        false
                    } else {
                        bail!("signature channel reported an unexpected outcome (#261): {payload}");
                    }
                }
                "failures" => {
                    ensure!(
                        method == "transactionFailureNotification"
                            && id == self.failures.subscription
                    );
                    let signature =
                        payload["signature"].as_str().context("failure signature missing")?;
                    let probe = probe.filter(|probe| probe.signature == signature).context(
                        "failure stream reported a legitimate or unrelated transaction (#261)",
                    )?;
                    failure(&payload["error"], probe.error.as_ref())?;
                    false
                }
                _ => unreachable!(),
            };
            evidence.record(channel, is_control);
            if !settling && evidence.missing().is_empty() {
                deadline = deadline.min(Instant::now() + grace);
                settling = true;
            }
        }
        let missing = evidence.missing();
        ensure!(
            missing.is_empty(),
            "legitimate write missing channels {missing:?} (#261)"
        );
        ensure!(
            self.accounts.ws.malformed_frames() == 0
                && self.logs.ws.malformed_frames() == 0
                && self.signatures.malformed_frames() == 0
                && self.failures.ws.malformed_frames() == 0,
            "malformed public notification (#261)"
        );
        info!(signature = control, channels = ?evidence.recovered, diagnostics = ?evidence.probe, "live control delivered");
        Ok(evidence.probe)
    }
}
