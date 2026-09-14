use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq,
    netfault::{self, BaseProxies, Selector},
    prep, receipt, topology,
    topology::ErOptions,
    BaseCtx, ChainCtx, CheckError, ErCtx, PrivateErScenario, Result,
    ScenarioReport,
};
use signature::Signature;
use signer::Signer;

use crate::program::{instruction::build, DELEGATION_PROGRAM_ID};

const LABEL: &str = "commit-blackout";
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);
const BLACKOUT_WINDOW: Duration = Duration::from_secs(5);
const SUBMISSION_WRITE: u64 = 41;
const CONFIRMATION_WRITE: u64 = 42;
const RECONNECT_WRITE: u64 = 43;

pub struct CommitBlackout;

struct Convergence {
    receipt_base_signatures: usize,
    resubmitted: bool,
    seconds: f64,
}

struct Commit {
    account: Pubkey,
    snapshot: Vec<u8>,
    signature: Signature,
    base_signature: Signature,
    started: Instant,
}

async fn write_and_commit(
    er: &ErCtx,
    payer: &Keypair,
    commit_id: u64,
    write: u64,
    account: &Pubkey,
) -> Result<(Vec<u8>, Signature)> {
    er.submit_and_confirm(payer, &[build::simple_byte_set(write, &[*account])])
        .await?;
    let snapshot = er
        .account(account)
        .await?
        .ok_or("er copy vanished after the write")?
        .data;
    check_eq!(
        crate::written_id(&snapshot),
        Some(write),
        "the er write must land before the commit"
    )?;
    let commit_signature = er
        .submit_and_confirm(
            payer,
            &[build::commit_accounts(
                commit_id,
                payer.pubkey(),
                &[*account],
            )],
        )
        .await?;
    Ok((snapshot, commit_signature))
}

async fn prove_landed(
    base: &BaseCtx,
    base_signature: &Signature,
    account: &Pubkey,
    snapshot: &[u8],
) -> Result<()> {
    let base_tx = base
        .api()
        .await_transaction(base_signature, BASE_CONFIRM_TIMEOUT)
        .await?;
    check!(
        base_tx.err.is_none(),
        "the intercepted base commit {base_signature} must succeed on base, \
         got {:?}",
        base_tx.err
    )?;
    check::poll(
        "the base copy carries the er snapshot before the fault is applied",
        BASE_STATE_TIMEOUT,
        || async {
            matches!(base.account(account).await, Ok(Some(acc)) if acc.data == snapshot)
        },
    )
    .await?;
    Ok(())
}

async fn await_convergence(
    scenario: &str,
    base: &BaseCtx,
    er: &ErCtx,
    commit: &Commit,
) -> Result<Convergence> {
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
    Ok(Convergence {
        receipt_base_signatures: receipt.base_signatures.len(),
        resubmitted: !receipt.base_signatures.contains(&commit.base_signature),
        seconds: commit.started.elapsed().as_secs_f64(),
    })
}

async fn await_clone(er: &ErCtx, account: &Pubkey) -> Result<()> {
    check::poll(
        &format!("the private er clones the delegated account {account}"),
        CLONE_TIMEOUT,
        || async {
            matches!(er.account(account).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
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

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
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
        await_clone(er, &account).await?;

        let submission_trap = proxies.intercept(
            Selector::method("sendTransaction")
                .http()
                .response()
                .account(&account),
        );
        let submission_started = Instant::now();
        let (snapshot, commit_signature) =
            write_and_commit(er, &payer, 1, SUBMISSION_WRITE, &account).await?;
        let held = submission_trap.wait(INTERCEPT_TIMEOUT).await?;
        let base_signature = held.operation.signature()?;
        prove_landed(base, &base_signature, &account, &snapshot).await?;
        held.discard();
        let submission = await_convergence(
            self.name(),
            base,
            er,
            &Commit {
                account,
                snapshot,
                signature: commit_signature,
                base_signature,
                started: submission_started,
            },
        )
        .await?;

        let submission_probe = proxies.intercept(
            Selector::method("sendTransaction")
                .http()
                .response()
                .account(&account),
        );
        let confirmation_started = Instant::now();
        let (snapshot, commit_signature) =
            write_and_commit(er, &payer, 2, CONFIRMATION_WRITE, &account)
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
        let blackout_receipt = receipt::fetch_commit_receipt(
            er.api(),
            &commit_signature,
            BLACKOUT_WINDOW,
        )
        .await;
        check!(
            blackout_receipt.is_err(),
            "no commit receipt may appear while the confirmation is withheld, \
             got {:?}",
            blackout_receipt.map(|receipt| receipt.base_signatures)
        )?;
        held.release();
        status_blackout.remove();
        let confirmation = await_convergence(
            self.name(),
            base,
            er,
            &Commit {
                account,
                snapshot,
                signature: commit_signature,
                base_signature,
                started: confirmation_started,
            },
        )
        .await?;

        proxies.close_connections();
        let reconnect_started = Instant::now();
        let fresh =
            crate::init_delegated_account(base, &payer, 1, identity).await?;
        await_clone(er, &fresh).await?;
        let reconnect_clone_s = reconnect_started.elapsed().as_secs_f64();
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

        let events = proxies.finish()?;
        private.finish().await?;

        let report = ScenarioReport::ok(self.name())
            .setting("er", LABEL)
            .setting("delegated account", account)
            .setting("submission fault", "sendTransaction response discarded")
            .setting(
                "confirmation fault",
                "signatureNotification held, getSignatureStatuses stalled",
            )
            .setting("submission resubmitted", submission.resubmitted)
            .setting(
                "submission receipt base sigs",
                submission.receipt_base_signatures,
            )
            .setting("confirmation resubmitted", confirmation.resubmitted)
            .setting(
                "confirmation receipt base sigs",
                confirmation.receipt_base_signatures,
            )
            .metric(
                "submission blackout convergence s",
                Unit::Seconds,
                submission.seconds,
            )
            .metric(
                "confirmation blackout convergence s",
                Unit::Seconds,
                confirmation.seconds,
            )
            .metric("reconnect clone s", Unit::Seconds, reconnect_clone_s)
            .metric("fault events", Unit::Count, events.len() as f64);
        Ok(netfault::report_events(report, &events))
    }
}
