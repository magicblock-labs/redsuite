use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, netfault::Selector, receipt, BaseCtx, ChainCtx, CheckError, ErCtx, Result,
    Scenario,
};
use signature::Signature;
use tokio::time;

use super::{
    prove_landed, write_and_commit, BASE_CONFIRM_TIMEOUT, BASE_STATE_TIMEOUT, PROXIED_GROUP,
};
use crate::{program::DELEGATION_PROGRAM_ID, ACCOUNT_SPACE, PAYER_LAMPORTS};

const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);
const BLACKOUT_WINDOW: Duration = Duration::from_secs(5);
const BLACKOUT_POLL: Duration = Duration::from_millis(200);
const SUBMISSION_WRITE: u64 = 41;
const CONFIRMATION_WRITE: u64 = 42;
const RECONNECT_WRITE: u64 = 43;

struct Commit {
    account: Pubkey,
    snapshot: Vec<u8>,
    signature: Signature,
}

async fn await_convergence(base: &BaseCtx, er: &ErCtx, commit: &Commit) -> Result<()> {
    let receipt =
        receipt::fetch_commit_receipt(er.api(), &commit.signature, RECEIPT_TIMEOUT).await?;
    match &receipt.error_message {
        Some(message) if receipt.failure_is_duplicate_rejection() => {
            receipt::warn_duplicate_rejection(message);
        }
        Some(message) => {
            return Err(
                CheckError::new("the commit intent converges after the fault is lifted")
                    .actual(message)
                    .into(),
            );
        }
        None => {}
    }
    check::poll(
        "the base copy still matches the er snapshot after convergence",
        BASE_STATE_TIMEOUT,
        || async {
            matches!(base.account(&commit.account).await, Ok(Some(acc)) if acc.data == commit.snapshot)
        },
    )
    .await?;
    Ok(())
}

pub struct CommitBlackout {
    base: Rc<BaseCtx>,
    er: ErCtx,
    identity: Pubkey,
    payer: Keypair,
    account: Pubkey,
}

impl Scenario for CommitBlackout {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        er.proxies().ok_or(PROXIED_GROUP)?;
        let identity = er.identity();
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let account = base.redline(&payer).init_delegated(ACCOUNT_SPACE, identity, 0).await?;
        let on_base = base.account(&account).await?.ok_or("pda not on base")?;
        check_eq!(
            on_base.owner,
            DELEGATION_PROGRAM_ID,
            "a delegated pda must be dlp-owned on base"
        )?;
        er.await_clones(&[account], ACCOUNT_SPACE as usize, CLONE_TIMEOUT).await?;

        Ok(Self {
            base,
            er,
            identity,
            payer,
            account,
        })
    }

    async fn run(self) -> Result<()> {
        let Self {
            base,
            er,
            identity,
            payer,
            account,
        } = self;
        let proxies = er.proxies().ok_or(PROXIED_GROUP)?;
        let submission_trap = proxies
            .intercept(Selector::method("sendTransaction").http().response().account(&account));
        let (snapshot, commit_signature) =
            write_and_commit(&er, &payer, 1, SUBMISSION_WRITE, &account).await?;
        let held = submission_trap.wait(INTERCEPT_TIMEOUT).await?;
        let base_signature = held.operation.signature()?;
        prove_landed(&base, &base_signature, &account, &snapshot).await?;
        held.discard();
        await_convergence(
            &base,
            &er,
            &Commit {
                account,
                snapshot,
                signature: commit_signature,
            },
        )
        .await?;

        let submission_probe = proxies
            .intercept(Selector::method("sendTransaction").http().response().account(&account));
        let (snapshot, commit_signature) =
            write_and_commit(&er, &payer, 2, CONFIRMATION_WRITE, &account).await?;
        let receipt_signature =
            receipt::scheduled_receipt_signature(er.api(), &commit_signature).await?;
        let probe = submission_probe.wait(INTERCEPT_TIMEOUT).await?;
        let base_signature = probe.operation.signature()?;
        let status_blackout = proxies.stall(
            Selector::method("getSignatureStatuses")
                .http()
                .response()
                .signature(&base_signature),
        );
        let notification_trap = proxies.intercept(
            Selector::method("signatureNotification")
                .ws()
                .notification()
                .signature(&base_signature),
        );
        probe.release();
        prove_landed(&base, &base_signature, &account, &snapshot).await?;
        let held = notification_trap.wait(INTERCEPT_TIMEOUT).await?;
        let blackout_end = Instant::now() + BLACKOUT_WINDOW;
        loop {
            check!(
                er.api().get_transaction(&receipt_signature).await?.is_none(),
                "no commit receipt {receipt_signature} may appear while \
                 confirmation is withheld"
            )?;
            if Instant::now() >= blackout_end {
                break;
            }
            time::sleep(BLACKOUT_POLL).await;
        }
        held.release();
        status_blackout.remove();
        await_convergence(
            &base,
            &er,
            &Commit {
                account,
                snapshot,
                signature: commit_signature,
            },
        )
        .await?;

        proxies.close_connections();
        let fresh = base.redline(&payer).init_delegated(ACCOUNT_SPACE, identity, 1).await?;
        er.await_clones(&[fresh], ACCOUNT_SPACE as usize, CLONE_TIMEOUT).await?;
        let (snapshot, commit_signature) =
            write_and_commit(&er, &payer, 3, RECONNECT_WRITE, &fresh).await?;
        let plain =
            receipt::fetch_commit_receipt(er.api(), &commit_signature, RECEIPT_TIMEOUT).await?;
        check!(
            plain.succeeded(),
            "a commit after the proxies are restored must succeed, got {:?}",
            plain.error_message
        )?;
        plain.confirm(base.api(), BASE_CONFIRM_TIMEOUT).await?;
        check::poll(
            "the fresh account commits to base through restored traffic",
            BASE_STATE_TIMEOUT,
            || async {
                matches!(base.account(&fresh).await, Ok(Some(acc)) if acc.data == snapshot)
            },
        )
        .await?;

        Ok(())
    }
}
