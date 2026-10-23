use std::{
    rc::Rc,
    slice,
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    netfault::Selector, programs::REDLINE, receipt, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use tokio::time;

use super::{prove_landed, write_and_commit, PROXIED_GROUP};
use crate::{program::DELEGATION_PROGRAM_ID, ACCOUNT_SPACE, PAYER_LAMPORTS};

const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const BLACKOUT_WINDOW: Duration = Duration::from_secs(5);
const BLACKOUT_POLL: Duration = Duration::from_millis(200);
const SUBMISSION_WRITE: u64 = 41;
const CONFIRMATION_WRITE: u64 = 42;
const RECONNECT_WRITE: u64 = 43;

async fn await_convergence(
    base: &BaseCtx,
    er: &ErCtx,
    account: &Pubkey,
    snapshot: &[u8],
    signature: &Signature,
) -> Result<()> {
    let receipt = receipt::fetch_commit_receipt(er.api(), signature).await?;
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
    base.api()
        .await_accounts(
            slice::from_ref(account),
            "the base copy still matches the er snapshot after convergence",
            async || {
                let account = base.account(account).await?.context("base account missing")?;
                ensure!(account.data == snapshot);
                Ok(())
            },
        )
        .await?;
    Ok(())
}

pub struct CommitBlackout {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
    account: Pubkey,
}

impl Scenario for CommitBlackout {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        er.proxies().context(PROXIED_GROUP)?;
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let account = base
            .cells(REDLINE, &payer)
            .create_delegated(ACCOUNT_SPACE, er.identity(), 0)
            .await?;
        let on_base = base.account(&account).await?.context("pda not on base")?;
        ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
        er.await_clones(&[account], ACCOUNT_SPACE as usize).await?;

        Ok(Self { base, er, payer, account })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er, payer, account } = self;
        let proxies = er.proxies().context(PROXIED_GROUP)?;
        let submission_trap = proxies
            .intercept(Selector::method("sendTransaction").http().response().account(&account));
        let (snapshot, commit_signature) =
            write_and_commit(&er, &payer, 1, SUBMISSION_WRITE, &account).await?;
        let held = submission_trap.wait(INTERCEPT_TIMEOUT).await?;
        let base_signature = held.operation.signature()?;
        prove_landed(&base, &base_signature, &account, &snapshot).await?;
        held.discard();
        await_convergence(&base, &er, &account, &snapshot, &commit_signature).await?;

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
            ensure!(er.api().get_transaction(&receipt_signature).await?.is_none());
            if Instant::now() >= blackout_end {
                break;
            }
            time::sleep(BLACKOUT_POLL).await;
        }
        held.release();
        status_blackout.remove();
        await_convergence(&base, &er, &account, &snapshot, &commit_signature).await?;

        proxies.close_connections();
        let fresh = base
            .cells(REDLINE, &payer)
            .create_delegated(ACCOUNT_SPACE, er.identity(), 1)
            .await?;
        er.await_clones(&[fresh], ACCOUNT_SPACE as usize).await?;
        let (snapshot, commit_signature) =
            write_and_commit(&er, &payer, 3, RECONNECT_WRITE, &fresh).await?;
        let plain = receipt::fetch_commit_receipt(er.api(), &commit_signature).await?;
        ensure!(plain.succeeded());
        plain.confirm(base.api()).await?;
        base.api()
            .await_accounts(
                &[fresh],
                "the fresh account commits to base through restored traffic",
                async || {
                    let account = base.account(&fresh).await?.context("base account missing")?;
                    ensure!(account.data == snapshot);
                    Ok(())
                },
            )
            .await?;

        Ok(())
    }
}
