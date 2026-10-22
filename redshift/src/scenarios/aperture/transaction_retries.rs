use std::{rc::Rc, result, time::Duration};

use anyhow::{anyhow, ensure, Context};
use futures_util::future::join_all;
use hash::Hash;
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    api::{encode, transaction_meta, RpcError},
    netfault::Selector,
    programs::{CellHeader, REDLINE, REDSHIFT},
    rpc_types::{
        config::{RpcBlockConfig, RpcSendTransactionConfig, RpcTransactionConfig},
        request::RpcRequest,
        response::TransactionError,
    },
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    transaction_types::{
        EncodedConfirmedTransactionWithStatusMeta, TransactionDetails, UiConfirmedBlock,
        UiTransactionEncoding, UiTransactionError,
    },
    transport::wsraw::RawWs,
    BaseCtx, ChainCtx, DynError, ErCtx, PrivateErScenario, Result, Scenario,
};
use serde_json::{json, Value};
use signature::Signature;
use signer::Signer;
use solana_instruction_error::InstructionError;
use tracing::instrument;
use transaction::Transaction;

use crate::PAYER_LAMPORTS;

const TIMEOUT: Duration = Duration::from_secs(30);
const COPIES: usize = 32;

pub struct TransactionRetries {
    base: Rc<BaseCtx>,
    er: ErCtx,
    mode: Mode,
}

pub enum Mode {
    ColdFetch,
    Concurrent,
    Subscriptions,
    ExpiryRestart,
}

struct Signed {
    encoded: String,
    signature: Signature,
    blockhash: Hash,
    fee: u64,
    error: Option<UiTransactionError>,
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
        let mut instructions =
            vec![REDSHIFT.batch().add(&[REDSHIFT.primary(owner)], i64::from(count)).build()];
        if let Some(cold) = cold {
            instructions[0].accounts.push(AccountMeta::new_readonly(cold, false));
        }
        if fail {
            instructions
                .push(REDSHIFT.batch().writable(&[REDSHIFT.primary(owner)]).fail(0).build());
        }
        let blockhash = er.api().get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&payer.pubkey()),
            &[payer],
            blockhash,
        );
        let quoted: Option<u64> = er
            .api()
            .call_value(RpcRequest::GetFeeForMessage, &(encode(&tx.message)?,))
            .await?;
        let fee = quoted.context("missing fee quote")?;
        Ok(Self {
            encoded: encode(&tx)?,
            signature: tx.signatures[0],
            blockhash,
            fee,
            error: fail
                .then(|| TransactionError::InstructionError(1, InstructionError::Custom(0)).into()),
        })
    }

    async fn send(&self, er: &ErCtx) -> Result<result::Result<String, RpcError>> {
        let config = RpcSendTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            skip_preflight: true,
            max_retries: Some(0),
            ..RpcSendTransactionConfig::default()
        };
        er.api().try_call(RpcRequest::SendTransaction, &(&self.encoded, config)).await
    }

    async fn valid(&self, er: &ErCtx) -> Result<bool> {
        er.api()
            .call_value(RpcRequest::IsBlockhashValid, &(self.blockhash.to_string(),))
            .await
    }

    async fn copies(&self, er: &ErCtx, count: usize) -> Result<()> {
        for result in join_all((0..count).map(|_| self.send(er))).await {
            match result? {
                Ok(_) => {}
                Err(error) => {
                    ensure!(error.error.code.code() == -32003);
                }
            }
        }
        Ok(())
    }

    async fn outcome(&self, er: &ErCtx) -> Result<u64> {
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Json),
            max_supported_transaction_version: Some(0),
            ..RpcTransactionConfig::default()
        };
        let tx = er
            .api()
            .await_query("transaction published", async || {
                er.api()
                    .call::<Option<EncodedConfirmedTransactionWithStatusMeta>>(
                        RpcRequest::GetTransaction,
                        &(self.signature.to_string(), config),
                    )
                    .await?
                    .ok_or_else(|| DynError::msg("transaction missing"))
            })
            .await?;
        let meta = transaction_meta(&tx)?;
        ensure!(meta.err == self.error);
        ensure!(meta.fee == self.fee);
        Ok(tx.slot)
    }
}

async fn state(er: &ErCtx, keys: &[Pubkey; 2]) -> Result<(u64, CellHeader)> {
    let accounts = er.accounts(keys).await?;
    let payer = accounts[0].as_ref().with_context(|| format!("payer {} missing", keys[0]))?;
    let cell = accounts[1].as_ref().with_context(|| format!("cell {} missing", keys[1]))?;
    Ok((payer.lamports, CellHeader::try_decode(&cell.data)?))
}

async fn subscribe(er: &ErCtx, tx: &Signed) -> Result<(RawWs, u64)> {
    let mut ws = RawWs::connect(er.ws_url()).await?;
    let id = ws.signature_subscribe(&tx.signature).await?;
    Ok((ws, id))
}

async fn queue_readers(
    er: &ErCtx,
    payer: &Keypair,
    cell: Pubkey,
    round: u8,
) -> Result<Vec<Signature>> {
    let blockhash = er.api().get_latest_blockhash().await?;
    let mut data = vec![2];
    data.extend_from_slice(&1_400_000u32.to_le_bytes());
    let budget = Instruction {
        program_id: sdk_ids::compute_budget::ID,
        accounts: Vec::new(),
        data,
    };
    let transactions: Vec<_> = (0..32u64 << ((round - 1) / 2))
        .map(|id| {
            Transaction::new_signed_with_payer(
                &[
                    budget.clone(),
                    REDLINE.inspect(&[cell]),
                    REDLINE.hash(u64::from(round) * 256 + id, cell.to_bytes(), 180, &[]),
                ],
                Some(&payer.pubkey()),
                &[payer],
                blockhash,
            )
        })
        .collect();
    join_all(transactions.iter().map(|tx| er.api().send_transaction(tx)))
        .await
        .into_iter()
        .collect()
}

async fn execute(
    er: &ErCtx,
    tx: &Signed,
    copies: usize,
    queued: Option<(&Keypair, Pubkey, u8)>,
) -> Result<bool> {
    let mut listeners = Vec::new();
    for _ in 0..2 {
        listeners.push(subscribe(er, tx).await?);
    }
    let mut readers = Vec::new();
    if let Some((payer, cell, round)) = queued {
        readers = queue_readers(er, payer, cell, round).await?;
    }
    let (sent, racing) = tokio::join!(
        tx.copies(er, copies),
        join_all((0..4).map(|_| subscribe(er, tx))),
    );
    sent?;
    for listener in racing {
        listeners.push(listener?);
    }
    let pending = if queued.is_some() {
        listeners.push(subscribe(er, tx).await?);
        er.api().get_signature_status(&tx.signature).await?.is_none()
    } else {
        false
    };
    tx.outcome(er).await?;
    listeners.push(subscribe(er, tx).await?);
    for (index, (mut ws, id)) in listeners.into_iter().enumerate() {
        let (method, subscription, payload) =
            ws.next_notification(TIMEOUT).await?.ok_or_else(|| {
                anyhow!(
                    "subscriber {index} missed terminal result: expected {:?}",
                    tx.error
                )
            })?;
        let payload: Value = serde_json::from_str(&payload.to_string())?;
        ensure!(method == "signatureNotification");
        ensure!(subscription == id);
        ensure!(payload["value"] == json!({"err": tx.error}));
        ws.close().await?;
    }
    for reader in readers {
        er.api().await_transaction(&reader).await?;
    }
    Ok(pending)
}

async fn audit(er: &ErCtx, start: u64, expected: &[(&Signed, usize)]) -> Result<u64> {
    let end = er.api().get_slot().await? + 2;
    er.api().await_slot(end).await?;
    er.api()
        .await_query("block is published", async || {
            ensure!(er.api().get_block(end).await?.is_some());
            Ok(())
        })
        .await?;
    let blocks: Vec<u64> = er.api().call(RpcRequest::GetBlocks, &(start, end)).await?;
    let mut counts = vec![0; expected.len()];
    for slot in blocks {
        let config = RpcBlockConfig {
            transaction_details: Some(TransactionDetails::Signatures),
            rewards: Some(false),
            ..RpcBlockConfig::default()
        };
        let block: UiConfirmedBlock = er.api().call(RpcRequest::GetBlock, &(slot, config)).await?;
        let signatures = block.signatures.context("block signatures missing")?;
        for (i, (tx, _)) in expected.iter().enumerate() {
            let expected = tx.signature.to_string();
            counts[i] += signatures.iter().filter(|signature| **signature == expected).count();
        }
    }
    for ((_, expected), count) in expected.iter().zip(counts) {
        ensure!(count == *expected);
    }
    Ok(end + 1)
}

impl Scenario<(), Mode> for TransactionRetries {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, mode: Mode) -> Result<Self> {
        Ok(Self { base, er, mode })
    }

    async fn run(self) -> Result<()> {
        self.mode.run_phases(&self.base, &self.er, None).await
    }
}

pub struct ExpiryRestart {
    base: Rc<BaseCtx>,
    private: PrivateEr,
}

impl PrivateErScenario for ExpiryRestart {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let private = topology::private_er(
            &base,
            ErOptions {
                label: "transaction_retry_expiry_restart".to_owned(),
                ..Default::default()
            },
        )
        .await?;
        Ok(Self { base, private })
    }

    async fn run(mut self) -> Result<()> {
        let er = self.private.ctx().clone();
        Mode::ExpiryRestart.run_phases(&self.base, &er, Some(&mut self.private)).await?;
        self.private.finish().await
    }
}

impl Mode {
    async fn run_phases(
        &self,
        base: &BaseCtx,
        er: &ErCtx,
        mut private: Option<&mut PrivateEr>,
    ) -> Result<()> {
        let phases: &[&str] = if matches!(self, Self::Concurrent) {
            &["success", "failure"]
        } else {
            &["case"]
        };
        for &phase in phases {
            // Each outcome owns fresh payer/cell state and its ledger window.
            self.run_case(base, er, phase, private.as_deref_mut()).await?;
        }
        Ok(())
    }

    #[instrument(skip_all, err, fields(case = phase))]
    async fn run_case(
        &self,
        base: &BaseCtx,
        er: &ErCtx,
        phase: &str,
        private: Option<&mut PrivateEr>,
    ) -> Result<()> {
        let owner = base.funded_payer(PAYER_LAMPORTS).await?;
        let (cell, setup) = base.cells(REDSHIFT, &owner).prepare_delegated(er.identity(), u32::MAX);
        base.submit_and_confirm(&owner, &setup).await?;
        let payer = base.delegated_payer(&owner, er.identity(), PAYER_LAMPORTS).await?;
        let load_payer = if matches!(self, Self::Subscriptions) {
            Some(base.delegated_payer(&owner, er.identity(), PAYER_LAMPORTS).await?)
        } else {
            None
        };
        let owner = owner.pubkey();
        let keys = [payer.pubkey(), cell];
        let mut expected =
            er.api().await_accounts(&keys, "accounts cloned", || state(er, &keys)).await?;
        ensure!((expected.1.value, expected.1.revision) == (0, 0));
        let mut start = er.api().get_slot().await?;

        match self {
            Self::ColdFetch => {
                let cold = base.funded_payer(1_000_000).await?.pubkey();
                let selector = Selector::methods(&["getAccountInfo", "getMultipleAccounts"])
                    .http()
                    .request()
                    .account(&cold);
                let proxies = er.proxies().context(
                    "the cold-fetch case needs the base-chain proxies of its \
                     ER group",
                )?;
                let trap = proxies.intercept(selector);
                let tx = Signed::new(er, &payer, owner, 7, false, Some(cold)).await?;
                let (failed, fault) = tokio::join!(tx.send(er), async {
                    trap.wait(TIMEOUT).await?.reject("retry-test cold fetch unavailable");
                    Result::Ok(())
                });
                fault?;
                let Err(rpc) = failed? else {
                    return Err(anyhow!("cold fetch unexpectedly succeeded"));
                };
                ensure!(
                    rpc.error.code.code() == -32003
                        && rpc.error.message.contains("retry-test cold fetch unavailable")
                );
                ensure!(er.api().get_signature_status(&tx.signature).await?.is_none());
                ensure!(state(er, &keys).await? == expected);
                tx.send(er).await??;
                tx.outcome(er).await?;
                audit(er, start, &[(&tx, 1)]).await?;
                expected.0 -= tx.fee;
                expected.1.value += 7;
                expected.1.revision += 1;
            }
            Self::Concurrent | Self::Subscriptions => {
                let mut pending_results = [false; 2];
                for round in 1..=8 {
                    let fail =
                        phase == "failure" || matches!(self, Self::Subscriptions) && round % 2 == 0;
                    let copies = if matches!(self, Self::Subscriptions) { 1 } else { COPIES };
                    let tx = Signed::new(er, &payer, owner, round, fail, None).await?;
                    let notifications = execute(
                        er,
                        &tx,
                        copies,
                        load_payer
                            .as_ref()
                            .filter(|_| !pending_results[usize::from(fail)])
                            .map(|payer| (payer, cell, round)),
                    )
                    .await;
                    start = audit(er, start, &[(&tx, 1)]).await?;
                    expected.0 -= tx.fee;
                    if !fail {
                        expected.1.value += u64::from(round);
                        expected.1.revision += 1;
                    }
                    ensure!(state(er, &keys).await? == expected);
                    let pending = notifications?;
                    pending_results[usize::from(fail)] |= pending;
                }
                if matches!(self, Self::Subscriptions) {
                    ensure!(pending_results == [true, true]);
                }
            }
            Self::ExpiryRestart => {
                let private =
                    private.context("the expiry-restart case needs an ER of its own to restart")?;
                let mut probes = Vec::new();
                for epoch in 0..2 {
                    let start = er.api().get_slot().await? + 1;
                    let first = probes.len();
                    for i in 0..if epoch == 0 { 3 } else { 2 } {
                        let count = epoch * 10 + i * 2 + 3;
                        let tx = Signed::new(er, &payer, owner, count, i == 1, None).await?;
                        ensure!(tx.valid(er).await?);
                        let slot = if i < 2 {
                            tx.copies(er, 1).await?;
                            let slot = tx.outcome(er).await?;
                            expected.0 -= tx.fee;
                            if i == 0 {
                                expected.1.value += u64::from(count);
                                expected.1.revision += 1;
                            }
                            Some(slot)
                        } else {
                            None
                        };
                        probes.push((tx, slot));
                    }
                    let appended: Vec<_> = probes[first..]
                        .iter()
                        .map(|(tx, slot)| (tx, usize::from(slot.is_some())))
                        .collect();
                    audit(er, start, &appended).await?;
                    ensure!(state(er, &keys).await? == expected);
                    if epoch == 0 {
                        er.api()
                            .await_query("blockhashes expire", async || {
                                for (tx, _) in &probes {
                                    ensure!(!tx.valid(er).await?);
                                }
                                Ok(())
                            })
                            .await?;
                    } else {
                        for (tx, _) in &probes[first..] {
                            ensure!(tx.valid(er).await?);
                        }
                        private.restart(RestartConfig::default()).await?;
                    }
                    ensure!(state(er, &keys).await? == expected);
                    let start = er.api().get_slot().await? + 1;
                    for (tx, _) in &probes {
                        tx.copies(er, COPIES).await?;
                    }
                    let retries: Vec<_> = probes.iter().map(|(tx, _)| (tx, 0)).collect();
                    audit(er, start, &retries).await?;
                    ensure!(state(er, &keys).await? == expected);
                    for (tx, slot) in &probes {
                        if let Some(slot) = slot {
                            ensure!(tx.outcome(er).await? == *slot);
                        } else {
                            let status = er.api().get_signature_status(&tx.signature).await?;
                            ensure!(status.is_none_or(|status| status.err.is_some()));
                            ensure!(er.api().get_transaction(&tx.signature).await?.is_none());
                        }
                    }
                }
            }
        }
        ensure!(state(er, &keys).await? == expected);
        Ok(())
    }
}
