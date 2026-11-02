use super::{hold_nonces, prove_landed, write_and_commit};
use crate::{ACCOUNT_SPACE, PAYER_LAMPORTS};
use anyhow::{anyhow, ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    dlp,
    netfault::{Faults, RuleHandle, Selector, Transport},
    programs::{Action, CellHeader, CellId, IntentBundle, REDLINE, REDSHIFT},
    receipt::{self, CommitReceipt},
    topology::RestartConfig,
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use std::{
    rc::Rc,
    slice,
    time::{Duration, Instant},
};
use tokio::time;

const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const BLACKOUT_WINDOW: Duration = Duration::from_secs(5);
const BLACKOUT_POLL: Duration = Duration::from_millis(200);
const SUBMISSION_WRITE: u64 = 41;
const CONFIRMATION_WRITE: u64 = 42;
const RECONNECT_WRITE: u64 = 43;

const SETTLE_WINDOW: Duration = Duration::from_secs(3);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
const BLACKOUT_WRITE: u64 = 52;
const RECOVERY_WRITE: u64 = 53;
const RESTART_WRITE: u64 = 54;
const RESTART_RECOVERY_WRITE: u64 = 55;
const ACTION_ESCROW_LAMPORTS: u64 = 500_000_000;
const ACTION_COMPUTE_UNITS: u32 = 50_000;
const ACTION_COUNT: u8 = 1;

/// One landed commit whose confirmation is withheld, optionally across an ER restart.
struct FaultedCommit<'a> {
    phase: &'a str,
    commit_id: u64,
    write: u64,
    restart: bool,
}

/// Blocks both HTTP status and WebSocket notification evidence for the same base signature.
fn withhold_confirmations(faults: &Faults, signature: &Signature) -> [RuleHandle; 2] {
    [
        faults
            .stall(Selector::method("getSignatureStatuses").http().response().signature(signature)),
        faults.stall(
            Selector::method("signatureNotification")
                .ws()
                .notification()
                .signature(signature),
        ),
    ]
}

/// Preserves commit effects across response loss, confirmation loss, reconnect, and restart.
/// Distinct snapshots and base nonces distinguish reconciliation of landed
/// work from duplicate effects.
pub struct CommitRecovery {
    base: Rc<BaseCtx>,
    er: ErCtx,
}
impl Scenario for CommitRecovery {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }
    /// Uses independent fixtures for confirmation convergence
    /// and exactly-once base-effect recovery.
    async fn run(self) -> Result<()> {
        CommitFixture::prepare(self.base.clone(), self.er.clone())
            .await?
            .confirmation_loss()
            .await?;
        CommitFixture::prepare(self.base, self.er).await?.effect_recovery().await
    }
}

/// One delegated cell with isolated snapshots and nonce history for fault-recovery probes.
struct CommitFixture {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
    account: Pubkey,
}

impl CommitFixture {
    async fn prepare(base: Rc<BaseCtx>, er: ErCtx) -> Result<Self> {
        er.faults()?;
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let account = base
            .cells(REDLINE, &payer)
            .create_delegated(ACCOUNT_SPACE, er.identity(), 0)
            .await?;
        er.await_clones(&[account], ACCOUNT_SPACE as usize).await?;

        Ok(Self { base, er, payer, account })
    }

    /// Loses send/confirmation responses while newer ER writes test
    /// snapshot fidelity during recovery.
    async fn confirmation_loss(self) -> Result<()> {
        let faults = self.er.faults()?;
        let submission_trap = faults.intercept(
            Selector::method("sendTransaction").http().response().account(&self.account),
        );
        let (snapshot, commit_signature) =
            write_and_commit(&self.er, &self.payer, 1, SUBMISSION_WRITE, &self.account).await?;
        let held = submission_trap.wait(INTERCEPT_TIMEOUT).await?;
        let base_signature = held.operation.signature()?;
        prove_landed(&self.base, &base_signature, &self.account, &snapshot).await?;
        held.discard();
        self.await_convergence(&snapshot, &commit_signature).await?;

        let submission_probe = faults.intercept(
            Selector::method("sendTransaction").http().response().account(&self.account),
        );
        let (snapshot, commit_signature) =
            write_and_commit(&self.er, &self.payer, 2, CONFIRMATION_WRITE, &self.account).await?;
        let receipt_signature =
            receipt::scheduled_receipt_signature(self.er.api(), &commit_signature).await?;
        let probe = submission_probe.wait(INTERCEPT_TIMEOUT).await?;
        let base_signature = probe.operation.signature()?;
        let status_blackout = faults.stall(
            Selector::method("getSignatureStatuses")
                .http()
                .response()
                .signature(&base_signature),
        );
        let notification_trap = faults.intercept(
            Selector::method("signatureNotification")
                .ws()
                .notification()
                .signature(&base_signature),
        );
        probe.release();
        prove_landed(&self.base, &base_signature, &self.account, &snapshot).await?;
        let held = notification_trap.wait(INTERCEPT_TIMEOUT).await?;
        let blackout_end = Instant::now() + BLACKOUT_WINDOW;
        loop {
            ensure!(self.er.api().get_transaction(&receipt_signature).await?.is_none());
            if Instant::now() >= blackout_end {
                break;
            }
            time::sleep(BLACKOUT_POLL).await;
        }
        held.release();
        status_blackout.remove();
        self.await_convergence(&snapshot, &commit_signature).await?;

        faults.disconnect(Transport::Http);
        faults.disconnect(Transport::Ws);
        let fresh = self
            .base
            .cells(REDLINE, &self.payer)
            .create_delegated(ACCOUNT_SPACE, self.er.identity(), 1)
            .await?;
        self.er.await_clones(&[fresh], ACCOUNT_SPACE as usize).await?;
        let (snapshot, commit_signature) =
            write_and_commit(&self.er, &self.payer, 3, RECONNECT_WRITE, &fresh).await?;
        let plain = receipt::fetch_commit_receipt(self.er.api(), &commit_signature).await?;
        ensure!(plain.succeeded());
        plain.confirm(self.base.api()).await?;
        self.base
            .api()
            .await_accounts(
                &[fresh],
                "the fresh account commits to base through restored traffic",
                |accounts| {
                    let account = accounts[0].as_ref().context("base account missing")?;
                    ensure!(account.data == snapshot);
                    Ok(())
                },
            )
            .await?;

        Ok(())
    }

    /// Checks landed commit/action effects during blackouts, then
    /// requires fresh settlement progress.
    async fn effect_recovery(self) -> Result<()> {
        let faults = self.er.faults()?;
        self.commit_blackout(
            &faults,
            FaultedCommit {
                phase: "blackout",
                commit_id: 2,
                write: BLACKOUT_WRITE,
                restart: false,
            },
        )
        .await?;
        self.follow_up_commit(3, RECOVERY_WRITE, "post-blackout").await?;
        self.action_blackout(&faults).await?;
        self.commit_blackout(
            &faults,
            FaultedCommit {
                phase: "restart",
                commit_id: 4,
                write: RESTART_WRITE,
                restart: true,
            },
        )
        .await?;
        self.follow_up_commit(5, RESTART_RECOVERY_WRITE, "post-restart").await?;

        Ok(())
    }

    /// Accepts the historical duplicate-refusal outcome only when the
    /// intended base snapshot converges.
    async fn await_convergence(&self, snapshot: &[u8], signature: &Signature) -> Result<()> {
        let account = &self.account;

        let receipt = receipt::fetch_commit_receipt(self.er.api(), signature).await?;
        match &receipt.error_message {
            Some(message) if receipt.failure_is_duplicate_rejection() => {
                receipt::warn_duplicate_rejection(message);
            }
            Some(message) => {
                return Err(anyhow!(
                    "the commit intent converges after the fault is lifted: {message}"
                ));
            }
            None => {}
        }
        self.base
            .api()
            .await_accounts(
                slice::from_ref(account),
                "the base copy still matches the er snapshot after convergence",
                |accounts| {
                    let account = accounts[0].as_ref().context("base account missing")?;
                    ensure!(account.data == snapshot);
                    Ok(())
                },
            )
            .await?;
        Ok(())
    }

    async fn base_count(&self, cell: &Pubkey) -> Result<u64> {
        let account = self.base.account(cell).await?.context("the base cell is missing")?;
        Ok(CellHeader::try_decode(&account.data)?.value)
    }

    async fn hold_count(&self, cell: &Pubkey, expected: u64, window: Duration) -> Result<()> {
        let deadline = Instant::now() + window;
        loop {
            let count = self.base_count(cell).await?;
            ensure!(count == expected);
            if Instant::now() >= deadline {
                return Ok(());
            }
            time::sleep(SAMPLE_INTERVAL).await;
        }
    }

    /// Requires successful settlement, using a durable intent ID when
    /// restart invalidates log lookup.
    async fn settled_receipt(
        &self,
        commit_signature: &Signature,
        restart_intent_id: Option<u64>,
        phase: &str,
    ) -> Result<CommitReceipt> {
        let receipt = if let Some(intent_id) = restart_intent_id {
            receipt::fetch_commit_receipt_by_intent(self.er.api(), &self.er.identity(), intent_id)
                .await?
        } else {
            receipt::fetch_commit_receipt(self.er.api(), commit_signature).await?
        };
        ensure!(!receipt.failure_is_duplicate_rejection());
        receipt.check_success(phase)?;
        receipt.confirm(self.base.api()).await?;
        Ok(receipt)
    }

    /// Proves a commit landed before hiding confirmation, then checks its
    /// nonce cannot advance twice.
    async fn commit_blackout(&self, faults: &Faults, faulted: FaultedCommit<'_>) -> Result<()> {
        let FaultedCommit { phase, commit_id, write, restart } = faulted;
        let nonce_before = crate::last_commit_id(self.base.as_ref(), &self.account).await?;
        let submission = faults.intercept(
            Selector::method("sendTransaction").http().response().account(&self.account),
        );
        let (snapshot, commit_signature) =
            write_and_commit(&self.er, &self.payer, commit_id, write, &self.account).await?;
        let held = submission.wait(INTERCEPT_TIMEOUT).await?;
        let landed = held.operation.signature()?;
        prove_landed(self.base.as_ref(), &landed, &self.account, &snapshot).await?;
        let nonce = crate::last_commit_id(self.base.as_ref(), &self.account).await?;
        ensure!(nonce == (nonce_before + 1));
        let restart_intent_id = if restart {
            Some(receipt::scheduled_intent_id(self.er.api(), &commit_signature).await?)
        } else {
            None
        };
        let confirmations = withhold_confirmations(faults, &landed);
        if restart {
            self.er.restart(RestartConfig::default()).await?;
        }
        held.discard();
        // The base transaction already landed and advanced the DLP nonce. Recovery
        // must reconcile that landed transaction instead of producing another base
        // effect for the same intent.
        let settled = nonce;
        hold_nonces(
            self.base.as_ref(),
            &[self.account],
            &[settled],
            BLACKOUT_WINDOW,
        )
        .await?;
        for rule in confirmations {
            rule.remove();
        }
        let receipt = self.settled_receipt(&commit_signature, restart_intent_id, phase).await?;
        ensure!(receipt.base_signatures.contains(&landed));
        hold_nonces(
            self.base.as_ref(),
            &[self.account],
            &[settled],
            SETTLE_WINDOW,
        )
        .await?;
        let on_base = self
            .base
            .account(&self.account)
            .await?
            .context("the delegated pda vanished from base")?;
        ensure!(on_base.data == snapshot);
        Ok(())
    }

    /// Requires the next snapshot and exactly one nonce advance after fault recovery.
    async fn follow_up_commit(&self, commit_id: u64, write: u64, phase: &str) -> Result<()> {
        let account = self.account;

        let nonce_before = crate::last_commit_id(self.base.as_ref(), &account).await?;
        let (snapshot, commit_signature) =
            write_and_commit(&self.er, &self.payer, commit_id, write, &account).await?;
        self.settled_receipt(&commit_signature, None, phase).await?;
        self.base
            .api()
            .await_accounts(
                &[account],
                "the base copy matches the er snapshot after the follow-up commit",
                |accounts| {
                    let account = accounts[0].as_ref().context("base account missing")?;
                    ensure!(account.data == snapshot);
                    Ok(())
                },
            )
            .await?;
        let nonce = crate::last_commit_id(self.base.as_ref(), &account).await?;
        ensure!(nonce == (nonce_before + 1));
        Ok(())
    }

    /// Hides a landed standalone action's confirmation while checking
    /// its increment remains singular.
    async fn action_blackout(&self, faults: &Faults) -> Result<()> {
        let payer = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let (init, cell) = REDSHIFT.create(
            payer.pubkey(),
            CellId::new(payer.pubkey(), 0),
            CellHeader::SIZE as u32,
        );
        self.base
            .submit_and_confirm(
                &payer,
                &[init, dlp::top_up_ephemeral_balance(&payer.pubkey(), ACTION_ESCROW_LAMPORTS, 1)],
            )
            .await?;
        ensure!(self.base_count(&cell).await? == 0);
        let submission =
            faults.intercept(Selector::method("sendTransaction").http().response().account(&cell));
        let action = REDSHIFT
            .batch()
            .add(&[cell], i64::from(ACTION_COUNT))
            .action(cell, REDSHIFT.id());
        let mut bundle = IntentBundle::new(payer.pubkey());
        bundle.actions.push(Action::new(payer.pubkey(), action, ACTION_COMPUTE_UNITS));
        let intent_signature = self
            .er
            .submit_and_confirm(&payer, &[REDSHIFT.batch().intents(bundle).build()])
            .await?;
        let held = submission.wait(INTERCEPT_TIMEOUT).await?;
        let landed = held.operation.signature()?;
        let base_tx = self.base.api().await_transaction(&landed).await?;
        let meta = transaction_meta(&base_tx)?;
        ensure!(meta.err == None);
        self.base
            .api()
            .await_accounts(
                &[cell],
                "the base cell shows the action once before the fault is applied",
                |accounts| {
                    let account = accounts[0].as_ref().context("base cell missing")?;
                    ensure!(CellHeader::try_decode(&account.data)?.value == 1);
                    Ok(())
                },
            )
            .await?;
        let confirmations = withhold_confirmations(faults, &landed);
        held.discard();
        self.hold_count(&cell, 1, BLACKOUT_WINDOW).await?;
        for rule in confirmations {
            rule.remove();
        }
        let receipt = receipt::fetch_commit_receipt(self.er.api(), &intent_signature).await?;
        if receipt.succeeded() {
            ensure!(!receipt.base_signatures.is_empty());
            receipt.confirm(self.base.api()).await?;
        }
        self.hold_count(&cell, 1, SETTLE_WINDOW).await?;
        Ok(())
    }
}
