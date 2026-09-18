use std::time::Duration;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::future::join_all;
use hash::Hash;
use instruction::AccountMeta;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::flexi::{build, FlexiCounter};
use redsuite_core::{
    api::RpcError,
    check, check_eq,
    netfault::{self, BaseProxies, Selector},
    prep, topology,
    topology::{ErOptions, RestartConfig},
    transport::wsraw::RawWs,
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport,
};
use serde_json::{json, Value};
use signature::Signature;
use signer::Signer;
use transaction::Transaction;

const TIMEOUT: Duration = Duration::from_secs(30);
const EXPIRY_TIMEOUT: Duration = Duration::from_secs(120);
const COPIES: usize = 32;

pub enum TransactionRetries {
    ColdFetch,
    ConcurrentSuccess,
    ConcurrentFailure,
    Subscriptions,
    ExpiryRestart,
}

struct Signed {
    encoded: String,
    signature: Signature,
    blockhash: Hash,
    fee: u64,
    error: Value,
}

impl Signed {
    async fn new(
        er: &ErCtx,
        payer: &Keypair,
        owner: Pubkey,
        count: u8,
        fail: bool,
        cold: Option<Pubkey>,
    ) -> Result<Self> {
        let mut instructions = vec![build::add_unsigned(owner, count)];
        if let Some(cold) = cold {
            instructions[0]
                .accounts
                .push(AccountMeta::new_readonly(cold, false));
        }
        if fail {
            instructions.push(build::add_error(owner, count));
        }
        let blockhash = er.api().get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&payer.pubkey()),
            &[payer],
            blockhash,
        );
        let quoted: Value = er
            .api()
            .call(
                "getFeeForMessage",
                &(STANDARD.encode(bincode::serialize(&tx.message)?),),
            )
            .await?;
        let fee = quoted["value"].as_u64().ok_or("missing fee quote")?;
        Ok(Self {
            encoded: STANDARD.encode(bincode::serialize(&tx)?),
            signature: tx.signatures[0],
            blockhash,
            fee,
            error: if fail {
                json!({"InstructionError": [1, {"Custom": 0}]})
            } else {
                Value::Null
            },
        })
    }

    async fn send(&self, er: &ErCtx) -> Result<String> {
        er.api()
            .call(
                "sendTransaction",
                &(
                    &self.encoded,
                    json!({"encoding": "base64", "skipPreflight": true, "maxRetries": 0}),
                ),
            )
            .await
    }

    async fn valid(&self, er: &ErCtx) -> Result<bool> {
        let result: Value = er
            .api()
            .call("isBlockhashValid", &(self.blockhash.to_string(),))
            .await?;
        result["value"]
            .as_bool()
            .ok_or_else(|| "blockhash validity missing".into())
    }

    async fn copies(&self, er: &ErCtx, count: usize) -> Result<()> {
        for result in join_all((0..count).map(|_| self.send(er))).await {
            match result {
                Ok(signature) => check_eq!(
                    signature,
                    self.signature.to_string(),
                    "RPC signature"
                )?,
                Err(error) => {
                    check!(
                        error
                            .downcast_ref::<RpcError>()
                            .is_some_and(|rpc| rpc.code == -32003),
                        "retry failed outside transaction validation: {error}"
                    )?;
                }
            }
        }
        Ok(())
    }

    async fn outcome(&self, er: &ErCtx) -> Result<u64> {
        let tx = check::poll_for("transaction published", TIMEOUT, || async {
            er.api()
                .call_nullable::<Value>(
                    "getTransaction",
                    &(self.signature.to_string(), json!({"encoding": "json", "maxSupportedTransactionVersion": 0})),
                )
                .await?
                .ok_or_else(|| redsuite_core::DynError::from("transaction missing"))
        })
        .await?;
        check_eq!(tx["meta"]["err"], self.error, "execution result")?;
        check_eq!(tx["meta"]["fee"].as_u64(), Some(self.fee), "execution fee")?;
        tx["slot"]
            .as_u64()
            .ok_or_else(|| "transaction slot missing".into())
    }
}

async fn state(er: &ErCtx, keys: &[Pubkey; 2]) -> Result<(u64, FlexiCounter)> {
    let accounts = er.accounts(keys).await?;
    let payer = accounts[0].as_ref().ok_or("payer missing")?;
    let counter = accounts[1].as_ref().ok_or("counter missing")?;
    Ok((payer.lamports, FlexiCounter::try_decode(&counter.data)?))
}

async fn subscribe(er: &ErCtx, tx: &Signed) -> Result<(RawWs, u64)> {
    let mut ws = RawWs::connect(er.ws_url()).await?;
    let id = ws.signature_subscribe(&tx.signature.to_string()).await?;
    Ok((ws, id))
}

async fn execute(er: &ErCtx, tx: &Signed, copies: usize) -> Result<()> {
    let mut listeners = Vec::new();
    for _ in 0..2 {
        listeners.push(subscribe(er, tx).await?);
    }
    let (sent, racing) = tokio::join!(
        tx.copies(er, copies),
        join_all((0..4).map(|_| subscribe(er, tx))),
    );
    sent?;
    for listener in racing {
        listeners.push(listener?);
    }
    tx.outcome(er).await?;
    listeners.push(subscribe(er, tx).await?);
    for (index, (mut ws, id)) in listeners.into_iter().enumerate() {
        let (method, subscription, payload) =
            ws.next_notification(TIMEOUT).await?.ok_or_else(|| {
                redsuite_core::CheckError::new(format!(
                    "{}: subscriber {index} missed terminal result",
                    tx.signature
                ))
                .expected(tx.error.to_string())
                .actual("no notification")
            })?;
        let payload: Value = serde_json::from_str(&payload.to_string())?;
        check_eq!(method, "signatureNotification", "notification method")?;
        check_eq!(subscription, id, "notification subscription")?;
        check_eq!(
            payload["value"],
            json!({"err": tx.error}),
            "subscriber {index} terminal result"
        )?;
        ws.close().await?;
    }
    Ok(())
}

async fn audit(
    er: &ErCtx,
    start: u64,
    expected: &[(&Signed, usize)],
) -> Result<u64> {
    let end = er.api().get_slot().await? + 2;
    check::poll(
        "accepted work reaches a block boundary",
        TIMEOUT,
        || async {
            matches!(er.api().get_slot().await, Ok(slot) if slot >= end)
        },
    )
    .await?;
    check::poll("block is published", TIMEOUT, || async {
        matches!(er.api().get_block(end).await, Ok(Some(_)))
    })
    .await?;
    let blocks: Vec<u64> = er.api().call("getBlocks", &(start, end)).await?;
    let mut counts = vec![0; expected.len()];
    for slot in blocks {
        let block: Value = er.api().call(
            "getBlock",
            &(slot, json!({"transactionDetails": "signatures", "rewards": false})),
        ).await?;
        let signatures = block["signatures"]
            .as_array()
            .ok_or("block signatures missing")?;
        for (i, (tx, _)) in expected.iter().enumerate() {
            counts[i] += signatures
                .iter()
                .filter(|s| s.as_str() == Some(&tx.signature.to_string()))
                .count();
        }
    }
    for ((tx, expected), count) in expected.iter().zip(counts) {
        check_eq!(count, *expected, "ledger occurrences of {}", tx.signature)?;
    }
    Ok(end + 1)
}

#[async_trait(?Send)]
impl PrivateErScenario for TransactionRetries {
    fn name(&self) -> &str {
        match self {
            Self::ColdFetch => "redshift/transaction_retry_cold_fetch",
            Self::ConcurrentSuccess => "redshift/transaction_retry_success",
            Self::ConcurrentFailure => "redshift/transaction_retry_failure",
            Self::Subscriptions => "redshift/transaction_retry_subscriptions",
            Self::ExpiryRestart => "redshift/transaction_retry_expiry_restart",
        }
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let proxies = BaseProxies::spawn(base).await?;
        let mut private = topology::private_er(
            base,
            ErOptions {
                label: self.name().replace("redshift/", ""),
                base_endpoints: Some(proxies.endpoints()),
                ..Default::default()
            },
        )
        .await?;
        let er = private.ctx();
        let owner = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let (init, counter) = build::init_counter(owner.pubkey(), self.name());
        base.submit_and_confirm(
            &owner,
            &[
                init,
                build::delegate_counter(
                    owner.pubkey(),
                    u32::MAX,
                    Some(er.identity()),
                ),
            ],
        )
        .await?;
        let payer = prep::delegated_payer(
            base,
            &owner,
            er.identity(),
            crate::PAYER_LAMPORTS,
        )
        .await?;
        let owner = owner.pubkey();
        let keys = [payer.pubkey(), counter];
        let mut expected =
            check::poll_for("accounts cloned", TIMEOUT, || state(er, &keys))
                .await?;
        check_eq!(
            (expected.1.count, expected.1.updates),
            (0, 0),
            "fresh counter"
        )?;
        let mut report = ScenarioReport::ok(self.name())
            .setting("payer", keys[0])
            .setting("counter", keys[1])
            .setting("before", format!("{expected:?}"));
        let mut start = er.api().get_slot().await?;

        match self {
            Self::ColdFetch => {
                let cold = prep::funded_payer(base, 1_000_000).await?.pubkey();
                let selector = Selector::methods(&[
                    "getAccountInfo",
                    "getMultipleAccounts",
                ])
                .http()
                .request()
                .account(&cold);
                let trap = proxies.intercept(selector);
                let tx = Signed::new(er, &payer, owner, 7, false, Some(cold))
                    .await?;
                let (failed, fault) = tokio::join!(tx.send(er), async {
                    trap.wait(TIMEOUT)
                        .await?
                        .reject("retry-test cold fetch unavailable");
                    Ok::<_, redsuite_core::DynError>(())
                });
                fault?;
                let error =
                    failed.expect_err("cold fetch must fail before scheduling");
                let rpc = error
                    .downcast_ref::<RpcError>()
                    .ok_or(error.to_string())?;
                check!(
                    rpc.code == -32003
                        && rpc
                            .message
                            .contains("retry-test cold fetch unavailable"),
                    "wrong cold-fetch error: {rpc}"
                )?;
                check!(
                    er.api()
                        .get_signature_status(&tx.signature)
                        .await?
                        .is_none(),
                    "fetch failure poisoned signature status"
                )?;
                check_eq!(
                    state(er, &keys).await?,
                    expected,
                    "pre-scheduling failure has no effect or fee"
                )?;
                check_eq!(
                    tx.send(er).await?,
                    tx.signature.to_string(),
                    "immediate identical retry"
                )?;
                tx.outcome(er).await?;
                audit(er, start, &[(&tx, 1)]).await?;
                expected.0 -= tx.fee;
                expected.1.count += 7;
                expected.1.updates += 1;
                report = report.setting("signature", tx.signature);
            }
            Self::ConcurrentSuccess
            | Self::ConcurrentFailure
            | Self::Subscriptions => {
                for round in 1..=8 {
                    let fail = matches!(self, Self::ConcurrentFailure)
                        || matches!(self, Self::Subscriptions)
                            && round % 2 == 0;
                    let copies = if matches!(self, Self::Subscriptions) {
                        1
                    } else {
                        COPIES
                    };
                    let tx = Signed::new(er, &payer, owner, round, fail, None)
                        .await?;
                    let notifications = execute(er, &tx, copies).await;
                    start = audit(er, start, &[(&tx, 1)]).await?;
                    expected.0 -= tx.fee;
                    if !fail {
                        expected.1.count += u64::from(round);
                        expected.1.updates += 1;
                    }
                    check_eq!(
                        state(er, &keys).await?,
                        expected,
                        "round {round}: exact effect and fee"
                    )?;
                    notifications?;
                    report = report.setting(
                        format!("round_{round}"),
                        format!("{} fee={} {expected:?}", tx.signature, tx.fee),
                    );
                }
            }
            Self::ExpiryRestart => {
                let mut probes = Vec::new();
                for epoch in 0..2 {
                    let er = private.ctx();
                    let start = er.api().get_slot().await? + 1;
                    let first = probes.len();
                    for i in 0..if epoch == 0 { 3 } else { 2 } {
                        let count = epoch * 10 + i * 2 + 3;
                        let tx =
                            Signed::new(er, &payer, owner, count, i == 1, None)
                                .await?;
                        check!(
                            tx.valid(er).await?,
                            "new signed input is valid"
                        )?;
                        let slot = if i < 2 {
                            tx.copies(er, 1).await?;
                            let slot = tx.outcome(er).await?;
                            expected.0 -= tx.fee;
                            if i == 0 {
                                expected.1.count += u64::from(count);
                                expected.1.updates += 1;
                            }
                            Some(slot)
                        } else {
                            None
                        };
                        report = report.setting(
                            format!("input_{epoch}_{i}"),
                            tx.signature,
                        );
                        probes.push((tx, slot));
                    }
                    let appended: Vec<_> = probes[first..]
                        .iter()
                        .map(|(tx, slot)| (tx, usize::from(slot.is_some())))
                        .collect();
                    audit(er, start, &appended).await?;
                    check_eq!(
                        state(er, &keys).await?,
                        expected,
                        "before expiry/restart"
                    )?;
                    if epoch == 0 {
                        check::poll_for(
                            "blockhashes expire",
                            EXPIRY_TIMEOUT,
                            || async {
                                for (tx, _) in &probes {
                                    check!(
                                        !tx.valid(er).await?,
                                        "blockhash still valid"
                                    )?;
                                }
                                Ok::<_, redsuite_core::DynError>(())
                            },
                        )
                        .await?;
                    } else {
                        for (tx, _) in &probes[first..] {
                            check!(
                                tx.valid(er).await?,
                                "recent input is valid before restart"
                            )?;
                        }
                        private.restart(RestartConfig::default()).await?;
                    }
                    let er = private.ctx();
                    check_eq!(
                        state(er, &keys).await?,
                        expected,
                        "restart preserves state"
                    )?;
                    let start = er.api().get_slot().await? + 1;
                    for (tx, _) in &probes {
                        tx.copies(er, COPIES).await?;
                    }
                    let retries: Vec<_> =
                        probes.iter().map(|(tx, _)| (tx, 0)).collect();
                    audit(er, start, &retries).await?;
                    check_eq!(
                        state(er, &keys).await?,
                        expected,
                        "retries have no effect or fee"
                    )?;
                    for (tx, slot) in &probes {
                        if let Some(slot) = slot {
                            check_eq!(
                                tx.outcome(er).await?,
                                *slot,
                                "original execution is immutable"
                            )?;
                        } else {
                            let status = er
                                .api()
                                .get_signature_status(&tx.signature)
                                .await?;
                            check!(
                                status
                                    .is_none_or(|status| status.err.is_some()),
                                "unscheduled input must be rejected"
                            )?;
                            check!(
                                er.api()
                                    .get_transaction(&tx.signature)
                                    .await?
                                    .is_none(),
                                "rejected input never executes"
                            )?;
                        }
                    }
                }
            }
        }
        check_eq!(
            state(private.ctx(), &keys).await?,
            expected,
            "final effect and payer balance"
        )?;
        report = report.setting("after", format!("{expected:?}"));
        private.finish().await?;
        Ok(netfault::report_events(report, &proxies.finish()?))
    }
}
