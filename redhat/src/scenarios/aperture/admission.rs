use super::{
    fixture::{vacant, Fixture},
    TIMEOUT,
};
use anyhow::{ensure, Context};
use base64::{engine::general_purpose::STANDARD, Engine};
use hash::Hash;
use message::{Message, VersionedMessage};
use redsuite_core::{
    netfault::Selector,
    programs::REDSHIFT,
    random,
    rpc_types::{request::RpcRequest, response::TransactionError},
    signed::{audit, subscribe, Signed},
    transaction_types::UiTransactionError,
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use serde_json::{json, Value};
use signer::Signer;
use solana_instruction_error::InstructionError;
use std::{rc::Rc, time::Duration};
use tracing::debug;
use transaction::versioned::VersionedTransaction;

pub struct TransactionAdmission {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

impl Scenario for TransactionAdmission {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        let mut rng = random::rng(
            self.base.config().seed,
            &["redhat", "transaction_admission"],
            0,
        );
        let mut fixture = Fixture::new(self.base, self.er, &mut rng).await?;
        let faults = fixture.er.faults()?;
        for skip_preflight in [false, true] {
            let cold = fixture.base.seeded_payer(&mut rng, 1_000_000).await?.pubkey();
            let source = fixture.base.account(&cold).await?;
            ensure!(fixture.er.local_images(&[cold]).await? == vec![None]);
            let selector = Selector::methods(&["getAccountInfo", "getMultipleAccounts"])
                .http()
                .request()
                .account(&cold);
            let refused = faults.reject(selector, "hostile account read refused");
            let before = fixture.snapshot(&[]).await?;
            let read = fixture.er.account(&cold).await;
            match read {
                Ok(account) => {
                    ensure!(
                        account.as_ref().is_none_or(vacant),
                        "refused account read leaked {account:?}"
                    );
                    debug!(
                        skip_preflight,
                        placeholder = account.is_some(),
                        "failed account preparation stays unresolved without a client error"
                    );
                }
                Err(error) => {
                    let error = format!("{error:#}");
                    ensure!(
                        error.contains("hostile account read refused")
                            || error.contains(&cold.to_string()),
                        "{error}"
                    )
                }
            }
            fixture.unchanged(&before).await?;
            let message = fixture.message(1_000 + u64::from(skip_preflight), 3, &[cold]).await?;
            let tx = Signed::from_versioned(
                &fixture.er,
                &fixture.payer,
                VersionedMessage::Legacy(message.clone()),
                None,
            )
            .await?;
            let start = fixture.er.api().get_slot().await?;
            let error = tx
                .send_config(&fixture.er, skip_preflight)
                .await?
                .expect_err("faulted preparation acknowledged");
            ensure!(
                error.error.code.code() == -32003
                    && (error.error.message.contains("hostile account read refused")
                        || error.error.message.contains(&cold.to_string())),
                "{error}"
            );
            fixture.not_admitted(&tx.signature, start, None).await?;
            fixture.unchanged(&before).await?;
            refused.remove();
            wait::retry("faulted account input recovers", TIMEOUT, async || {
                ensure!(fixture.er.account(&cold).await?.is_some());
                Ok(())
            })
            .await?;
            fixture
                .check_control(VersionedMessage::Legacy(message), 3, 1, skip_preflight)
                .await?;
            let local = fixture.er.local_images(&[cold]).await?;
            ensure!(
                local[0]
                    .as_ref()
                    .is_none_or(|account| vacant(account) || Some(account) == source.as_ref()),
                "faulted input diverged locally: {:?} against {source:?}",
                local[0]
            );
            ensure!(fixture.base.account(&cold).await? == source);
        }

        let expiry_message = fixture.message(2_000, 7, &[]).await?;
        let expired = expiry_message.recent_blockhash;
        wait::retry(
            "hostile probe blockhash expires",
            Duration::from_secs(90),
            async || {
                let valid: bool = fixture
                    .er
                    .api()
                    .call_value(RpcRequest::IsBlockhashValid, &(expired.to_string(),))
                    .await?;
                ensure!(!valid);
                Ok(())
            },
        )
        .await?;
        let hashes = [
            Hash::new_from_array(random::pubkey(&mut rng).to_bytes()),
            fixture.base.api().get_latest_blockhash().await?,
            expired,
        ];
        for (index, hash) in hashes.into_iter().enumerate() {
            for skip_preflight in [false, true] {
                let cold = fixture.base.seeded_payer(&mut rng, 1_000_000).await?.pubkey();
                let source = fixture.base.account(&cold).await?;
                ensure!(fixture.er.local_images(&[cold]).await? == vec![None]);
                let mut message = fixture
                    .message(
                        3_000 + index as u64 * 2 + u64::from(skip_preflight),
                        7,
                        &[cold],
                    )
                    .await?;
                message.recent_blockhash = hash;
                let transaction = VersionedTransaction::try_new(
                    VersionedMessage::Legacy(message.clone()),
                    &[&fixture.payer],
                )?;
                let tx = Signed {
                    encoded: STANDARD.encode(wincode::serialize(&transaction)?),
                    signature: transaction.signatures[0],
                    blockhash: hash,
                    fee: 0,
                    error: None,
                };
                let before = fixture.snapshot(&[]).await?;
                let trap = faults.intercept(
                    Selector::methods(&["getAccountInfo", "getMultipleAccounts"])
                        .http()
                        .request()
                        .account(&cold),
                );
                ensure!(!tx.valid(&fixture.er).await?);
                let start = fixture.er.api().get_slot().await?;
                let (sent, held) =
                    tokio::join!(tx.send_config(&fixture.er, skip_preflight), async {
                        let held = trap.wait(TIMEOUT).await?;
                        ensure!(fixture.er.local_images(&[cold]).await? == vec![None]);
                        fixture.unchanged(&before).await?;
                        held.release();
                        Result::Ok(())
                    });
                held?;
                if skip_preflight {
                    ensure!(sent?? == tx.signature.to_string());
                } else {
                    let error = sent?.expect_err("invalid blockhash admitted with preflight");
                    ensure!(
                        error.error.code.code() == -32003
                            && (error.error.message.contains("BlockhashNotFound")
                                || error.error.message.contains("Blockhash not found")),
                        "{error}"
                    );
                }
                fixture.unchanged(&before).await?;
                let local = fixture.er.local_images(&[cold]).await?;
                ensure!(
                    local[0]
                        .as_ref()
                        .is_none_or(|account| vacant(account) || Some(account) == source.as_ref()),
                    "faulted input diverged locally: {:?} against {source:?}",
                    local[0]
                );
                ensure!(fixture.base.account(&cold).await? == source);
                debug!(
                    index,
                    skip_preflight, "invalid hash preparation witnessed without execution"
                );
                message.recent_blockhash = fixture.er.api().get_latest_blockhash().await?;
                let control = fixture
                    .check_control(VersionedMessage::Legacy(message), 7, 1, skip_preflight)
                    .await?;
                fixture.not_admitted(&tx.signature, start, Some(&control)).await?;
            }
        }
        let message = fixture.message(4_000, 5, &fixture.keys[4..5]).await?;
        fixture.check_control(VersionedMessage::Legacy(message), 5, 1, true).await?;
        for skip_preflight in [false, true] {
            let message = Message::new_with_blockhash(
                &[
                    REDSHIFT
                        .batch()
                        .nonce(5_000 + u64::from(skip_preflight))
                        .add(&fixture.keys[1..2], 11)
                        .build(),
                    REDSHIFT.batch().writable(&fixture.keys[1..2]).fail(7).build(),
                ],
                Some(&fixture.payer.pubkey()),
                &fixture.er.api().get_latest_blockhash().await?,
            );
            let error: UiTransactionError =
                TransactionError::InstructionError(1, InstructionError::Custom(7)).into();
            let tx = Signed::from_versioned(
                &fixture.er,
                &fixture.payer,
                VersionedMessage::Legacy(message),
                Some(error),
            )
            .await?;
            let mut before = fixture.snapshot(&[]).await?;
            let (mut listener, subscription) = subscribe(&fixture.er, &tx).await?;
            let start = fixture.er.api().get_slot().await?;
            let sent = tx.send_config(&fixture.er, skip_preflight).await?;
            if skip_preflight {
                ensure!(sent? == tx.signature.to_string());
            } else {
                let error = sent.expect_err("deliberate execution failure succeeded");
                ensure!(
                    error.error.code.code() == -32003
                        && (error.error.message.contains("Custom(7)")
                            || error.error.message.contains("custom program error: 0x7")),
                    "{error}"
                );
            }
            let slot = tx.outcome(&fixture.er).await?;
            let status = fixture
                .er
                .api()
                .get_signature_status(&tx.signature)
                .await?
                .context("executed failure lost its terminal status")?;
            ensure!(status.slot == slot && status.err.map(Into::into) == tx.error);
            let (method, id, payload) = listener
                .next_notification(TIMEOUT)
                .await?
                .context("live failure notification missing")?;
            ensure!(method == "signatureNotification" && id == subscription);
            ensure!(
                serde_json::from_str::<Value>(&payload.to_string())?["value"]
                    == json!({"err":tx.error})
            );
            listener.close().await?;
            audit(&fixture.er, start, &[(&tx, 1)]).await?;
            before.local[0].as_mut().unwrap().lamports -= tx.fee;
            fixture.expected[0].as_mut().unwrap().lamports -= tx.fee;
            fixture.unchanged(&before).await?;
            let repaired = fixture.message(6_000 + u64::from(skip_preflight), 11, &[]).await?;
            fixture
                .check_control(VersionedMessage::Legacy(repaired), 11, 1, skip_preflight)
                .await?;
        }
        faults.reset().await?;
        Ok(())
    }
}
