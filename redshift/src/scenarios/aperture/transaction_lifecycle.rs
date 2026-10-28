use std::{rc::Rc, time::Duration};

use anyhow::{anyhow, ensure, Context};
use futures_util::future::join_all;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    netfault::Selector,
    programs::{CellHeader, REDLINE, REDSHIFT},
    signed::{audit, decode_state, subscribe, Signed},
    topology::RestartConfig,
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use serde_json::{json, Value};
use signature::Signature;
use signer::Signer;
use tracing::instrument;
use transaction::Transaction;

use crate::PAYER_LAMPORTS;

const TIMEOUT: Duration = Duration::from_secs(30);
// The 1,200-slot blockhash window already takes 60s at 50ms/slot;
// allow additional time for delayed block production in CI.
const BLOCKHASH_EXPIRY_TIMEOUT: Duration = Duration::from_secs(180);
const COPIES: usize = 32;

pub struct TransactionLifecycle {
    base: Rc<BaseCtx>,
    er: ErCtx,
    coverage: Coverage,
}

pub enum Coverage {
    Admission,
    Replay,
}

enum Mode {
    ColdFetch,
    Concurrent,
    Subscriptions,
    ExpiryRestart,
}

async fn state(er: &ErCtx, keys: &[Pubkey; 2]) -> Result<(u64, CellHeader)> {
    let accounts = er.accounts(keys).await?;
    decode_state(&accounts)
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

impl Scenario<(), Coverage> for TransactionLifecycle {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, coverage: Coverage) -> Result<Self> {
        Ok(Self { base, er, coverage })
    }

    async fn run(self) -> Result<()> {
        let modes: &[Mode] = match self.coverage {
            Coverage::Admission => &[Mode::ColdFetch, Mode::Concurrent, Mode::Subscriptions],
            Coverage::Replay => &[Mode::ExpiryRestart],
        };
        for mode in modes {
            mode.run_phases(&self.base, &self.er).await?;
        }
        Ok(())
    }
}

impl Mode {
    async fn run_phases(&self, base: &BaseCtx, er: &ErCtx) -> Result<()> {
        let phases: &[&str] = if matches!(self, Self::Concurrent) {
            &["success", "failure"]
        } else {
            &["case"]
        };
        for &phase in phases {
            // Each outcome owns fresh payer/cell state and its ledger window.
            self.run_case(base, er, phase).await?;
        }
        Ok(())
    }

    #[instrument(skip_all, err, fields(case = phase))]
    async fn run_case(&self, base: &BaseCtx, er: &ErCtx, phase: &str) -> Result<()> {
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
        let mut expected = er.api().await_accounts(&keys, "accounts cloned", decode_state).await?;
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
                for round in 1..=if matches!(self, Self::Concurrent) { 1 } else { 8 } {
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
                    if matches!(self, Self::Subscriptions) && pending_results == [true, true] {
                        break;
                    }
                }
                if matches!(self, Self::Subscriptions) {
                    ensure!(pending_results == [true, true]);
                }
            }
            Self::ExpiryRestart => {
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
                        wait::retry("blockhashes expire", BLOCKHASH_EXPIRY_TIMEOUT, async || {
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
                        er.restart(RestartConfig::default()).await?;
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
