use std::time::{Duration, Instant};

use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq,
    netfault::{BaseProxies, Selector},
    prep, receipt, topology,
    topology::ErOptions,
    BaseCtx, ChainCtx, CheckError, ErCtx, PrivateErScenario, Result,
};
use signature::Signature;

use crate::program::DELEGATION_PROGRAM_ID;

use super::{
    prove_landed, write_and_commit, BASE_CONFIRM_TIMEOUT, BASE_STATE_TIMEOUT,
};

const LABEL: &str = "commit-blackout";
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);
const BLACKOUT_WINDOW: Duration = Duration::from_secs(5);
const BLACKOUT_POLL: Duration = Duration::from_millis(200);
const SUBMISSION_WRITE: u64 = 41;
const CONFIRMATION_WRITE: u64 = 42;
const RECONNECT_WRITE: u64 = 43;

pub struct CommitBlackout;

struct Commit {
    account: Pubkey,
    snapshot: Vec<u8>,
    signature: Signature,
}

async fn await_convergence(
    scenario: &str,
    base: &BaseCtx,
    er: &ErCtx,
    commit: &Commit,
) -> Result<()> {
    let receipt = receipt::fetch_commit_receipt(
        er.api(),
        &commit.signature,
        RECEIPT_TIMEOUT,
    )
    .await?;
    match &receipt.error_message {
        Some(message) if receipt.failure_is_duplicate_rejection() => {
            receipt::warn_duplicate_rejection(scenario, message);
        }
        Some(message) => {
            return Err(CheckError::new(
                "the commit intent converges after the fault is lifted",
            )
            .actual(message)
            .into());
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

#[async_trait(?Send)]
impl PrivateErScenario for CommitBlackout {
    fn name(&self) -> &str {
        "redshift/commit_blackout"
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let proxies = BaseProxies::spawn(base).await?;
        let private = topology::private_er(
            base,
            ErOptions {
                label: LABEL.to_owned(),
                env: Vec::new(),
                request_timeout: None,
                base_endpoints: Some(proxies.endpoints()),
            },
        )
        .await?;
        let identity = private.identity();
        let er = private.ctx();
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let account =
            crate::init_delegated_account(base, &payer, 0, identity).await?;
        let on_base = base.account(&account).await?.ok_or("pda not on base")?;
        check_eq!(
            on_base.owner,
            DELEGATION_PROGRAM_ID,
            "a delegated pda must be dlp-owned on base"
        )?;
        prep::await_clones(
            er,
            &[account],
            crate::ACCOUNT_SPACE as usize,
            CLONE_TIMEOUT,
        )
        .await?;

        let submission_trap = proxies.intercept(
            Selector::method("sendTransaction")
                .http()
                .response()
                .account(&account),
        );
        let (snapshot, commit_signature) =
            write_and_commit(er, &payer, 1, SUBMISSION_WRITE, &account).await?;
        let held = submission_trap.wait(INTERCEPT_TIMEOUT).await?;
        let base_signature = held.operation.signature()?;
        prove_landed(base, &base_signature, &account, &snapshot).await?;
        held.discard();
        await_convergence(
            self.name(),
            base,
            er,
            &Commit {
                account,
                snapshot,
                signature: commit_signature,
            },
        )
        .await?;

        let submission_probe = proxies.intercept(
            Selector::method("sendTransaction")
                .http()
                .response()
                .account(&account),
        );
        let (snapshot, commit_signature) =
            write_and_commit(er, &payer, 2, CONFIRMATION_WRITE, &account)
                .await?;
        let receipt_signature =
            receipt::scheduled_receipt_signature(er.api(), &commit_signature)
                .await?;
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
        prove_landed(base, &base_signature, &account, &snapshot).await?;
        let held = notification_trap.wait(INTERCEPT_TIMEOUT).await?;
        let blackout_end = Instant::now() + BLACKOUT_WINDOW;
        loop {
            check!(
                er.api()
                    .get_transaction(&receipt_signature)
                    .await?
                    .is_none(),
                "no commit receipt {receipt_signature} may appear while \
                 confirmation is withheld"
            )?;
            if Instant::now() >= blackout_end {
                break;
            }
            tokio::time::sleep(BLACKOUT_POLL).await;
        }
        held.release();
        status_blackout.remove();
        await_convergence(
            self.name(),
            base,
            er,
            &Commit {
                account,
                snapshot,
                signature: commit_signature,
            },
        )
        .await?;

        proxies.close_connections();
        let fresh =
            crate::init_delegated_account(base, &payer, 1, identity).await?;
        prep::await_clones(
            er,
            &[fresh],
            crate::ACCOUNT_SPACE as usize,
            CLONE_TIMEOUT,
        )
        .await?;
        let (snapshot, commit_signature) =
            write_and_commit(er, &payer, 3, RECONNECT_WRITE, &fresh).await?;
        let plain = receipt::fetch_commit_receipt(
            er.api(),
            &commit_signature,
            RECEIPT_TIMEOUT,
        )
        .await?;
        check!(
            plain.succeeded(),
            "a commit after the proxies are restored must succeed, got {:?}",
            plain.error_message
        )?;
        receipt::confirm_base_signatures(
            base.api(),
            &plain,
            BASE_CONFIRM_TIMEOUT,
        )
        .await?;
        check::poll(
            "the fresh account commits to base through restored traffic",
            BASE_STATE_TIMEOUT,
            || async {
                matches!(base.account(&fresh).await, Ok(Some(acc)) if acc.data == snapshot)
            },
        )
        .await?;

        proxies.finish()?;
        private.finish().await?;
        Ok(())
    }
}
