use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::flexi::{
    build as flexi, FlexiCounter, ACTOR_ESCROW_INDEX,
};
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, dlp,
    netfault::{self, BaseProxies, RuleHandle, Selector},
    prep,
    receipt::{self, CommitReceipt},
    topology::{self, ErOptions, PrivateEr, RestartConfig, RestartTiming},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport,
};
use signature::Signature;
use signer::Signer;

use crate::program::{instruction::build, DELEGATION_PROGRAM_ID};

const LABEL: &str = "commit-exactly-once";
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);
const BLACKOUT_WINDOW: Duration = Duration::from_secs(5);
const SETTLE_WINDOW: Duration = Duration::from_secs(3);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
const WARMUP_WRITE: u64 = 51;
const BLACKOUT_WRITE: u64 = 52;
const RECOVERY_WRITE: u64 = 53;
const RESTART_WRITE: u64 = 54;
const RESTART_RECOVERY_WRITE: u64 = 55;
const ACTION_ESCROW_LAMPORTS: u64 = 500_000_000;
const ACTION_COMPUTE_UNITS: u32 = 50_000;
const ACTION_COUNT: u8 = 1;

pub struct CommitExactlyOnce;

struct FaultedCommit<'a> {
    phase: &'a str,
    commit_id: u64,
    write: u64,
    restart: bool,
}

struct Settled {
    landed: Signature,
    nonce: u64,
    receipt_base_signatures: usize,
    seconds: f64,
    restart: Option<RestartTiming>,
}

struct ActionOutcome {
    landed: Signature,
    receipt_succeeded: bool,
    receipt_error: Option<String>,
    seconds: f64,
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

async fn base_count(base: &BaseCtx, counter: &Pubkey) -> Result<u64> {
    let account = base
        .account(counter)
        .await?
        .ok_or("the base counter is missing")?;
    Ok(FlexiCounter::try_decode(&account.data)?.count)
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

async fn hold_nonce(
    base: &BaseCtx,
    account: &Pubkey,
    expected: u64,
    window: Duration,
    phase: &str,
) -> Result<()> {
    let deadline = Instant::now() + window;
    loop {
        let nonce = crate::last_commit_id(base, account).await?;
        check_eq!(
            nonce,
            expected,
            "{phase}: the base commit nonce must not move once the commit \
             has settled"
        )?;
        if Instant::now() >= deadline {
            return Ok(());
        }
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
}

async fn hold_count(
    base: &BaseCtx,
    counter: &Pubkey,
    expected: u64,
    window: Duration,
    phase: &str,
) -> Result<()> {
    let deadline = Instant::now() + window;
    loop {
        let count = base_count(base, counter).await?;
        check_eq!(
            count,
            expected,
            "{phase}: the base counter must not move once the action has \
             executed"
        )?;
        if Instant::now() >= deadline {
            return Ok(());
        }
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
}

fn withhold_confirmations(
    proxies: &BaseProxies,
    signature: &Signature,
) -> [RuleHandle; 2] {
    [
        proxies.stall(
            Selector::method("getSignatureStatuses")
                .http()
                .response()
                .signature(signature),
        ),
        proxies.stall(
            Selector::method("signatureNotification")
                .ws()
                .notification()
                .signature(signature),
        ),
    ]
}

async fn settled_receipt(
    base: &BaseCtx,
    er: &ErCtx,
    commit_signature: &Signature,
    restarted: bool,
    phase: &str,
) -> Result<CommitReceipt> {
    let receipt = if restarted {
        let intent_id =
            receipt::scheduled_intent_id(er.api(), commit_signature).await?;
        receipt::fetch_commit_receipt_by_intent(
            er.api(),
            &er.identity(),
            intent_id,
            RECEIPT_TIMEOUT,
        )
        .await?
    } else {
        receipt::fetch_commit_receipt(
            er.api(),
            commit_signature,
            RECEIPT_TIMEOUT,
        )
        .await?
    };
    check!(
        !receipt.failure_is_duplicate_rejection(),
        "{phase}: a duplicate-rejection receipt does not pass on base bytes \
         alone, got {:?}",
        receipt.error_message
    )?;
    check!(
        receipt.succeeded(),
        "{phase}: the receipt must report the settled commit as succeeded, \
         got {:?}",
        receipt.error_message
    )?;
    check!(
        !receipt.base_signatures.is_empty(),
        "{phase}: a succeeded receipt must name the base transaction that \
         settled the commit"
    )?;
    receipt::confirm_base_signatures(
        base.api(),
        &receipt,
        BASE_CONFIRM_TIMEOUT,
    )
    .await?;
    Ok(receipt)
}

async fn commit_blackout(
    proxies: &BaseProxies,
    base: &BaseCtx,
    private: &mut PrivateEr,
    payer: &Keypair,
    account: Pubkey,
    faulted: FaultedCommit<'_>,
) -> Result<Settled> {
    let FaultedCommit {
        phase,
        commit_id,
        write,
        restart,
    } = faulted;
    let started = Instant::now();
    let nonce_before = crate::last_commit_id(base, &account).await?;
    let submission = proxies.intercept(
        Selector::method("sendTransaction")
            .http()
            .response()
            .account(&account),
    );
    let (snapshot, commit_signature) =
        write_and_commit(private.ctx(), payer, commit_id, write, &account)
            .await?;
    let held = submission.wait(INTERCEPT_TIMEOUT).await?;
    let landed = held.operation.signature()?;
    prove_landed(base, &landed, &account, &snapshot).await?;
    let nonce = crate::last_commit_id(base, &account).await?;
    check_eq!(
        nonce,
        nonce_before + 1,
        "{phase}: the intercepted commit advances the base nonce exactly once"
    )?;
    let confirmations = withhold_confirmations(proxies, &landed);
    let restart = if restart {
        Some(private.restart(RestartConfig::default()).await?)
    } else {
        None
    };
    held.discard();
    let settled = if restart.is_some() {
        let replayed = nonce + 1;
        check::poll(
            "the recovered intent is replayed exactly once after the restart",
            BASE_STATE_TIMEOUT,
            || async {
                matches!(crate::last_commit_id(base, &account).await, Ok(current) if current == replayed)
            },
        )
        .await?;
        replayed
    } else {
        nonce
    };
    hold_nonce(base, &account, settled, BLACKOUT_WINDOW, phase).await?;
    for rule in confirmations {
        rule.remove();
    }
    let receipt = settled_receipt(
        base,
        private.ctx(),
        &commit_signature,
        restart.is_some(),
        phase,
    )
    .await?;
    if restart.is_none() {
        check!(
            receipt.base_signatures.contains(&landed),
            "{phase}: the receipt must name the base transaction that landed \
             ({landed}), got {:?}",
            receipt.base_signatures
        )?;
    }
    hold_nonce(base, &account, settled, SETTLE_WINDOW, phase).await?;
    let on_base = base
        .account(&account)
        .await?
        .ok_or("the delegated pda vanished from base")?;
    check!(
        on_base.data == snapshot,
        "{phase}: the base copy must still carry the settled er snapshot"
    )?;
    Ok(Settled {
        landed,
        nonce: settled,
        receipt_base_signatures: receipt.base_signatures.len(),
        seconds: started.elapsed().as_secs_f64(),
        restart,
    })
}

async fn follow_up_commit(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    account: Pubkey,
    commit_id: u64,
    write: u64,
    phase: &str,
) -> Result<u64> {
    let nonce_before = crate::last_commit_id(base, &account).await?;
    let (snapshot, commit_signature) =
        write_and_commit(er, payer, commit_id, write, &account).await?;
    settled_receipt(base, er, &commit_signature, false, phase).await?;
    check::poll(
        "the base copy matches the er snapshot after the follow-up commit",
        BASE_STATE_TIMEOUT,
        || async {
            matches!(base.account(&account).await, Ok(Some(acc)) if acc.data == snapshot)
        },
    )
    .await?;
    let nonce = crate::last_commit_id(base, &account).await?;
    check_eq!(
        nonce,
        nonce_before + 1,
        "{phase}: the follow-up commit advances the base nonce exactly once"
    )?;
    Ok(nonce)
}

async fn action_blackout(
    proxies: &BaseProxies,
    base: &BaseCtx,
    er: &ErCtx,
) -> Result<ActionOutcome> {
    let started = Instant::now();
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let (init, counter) = flexi::init_counter(payer.pubkey(), LABEL);
    base.submit_and_confirm(
        &payer,
        &[
            init,
            dlp::top_up_ephemeral_balance(
                &payer.pubkey(),
                ACTION_ESCROW_LAMPORTS,
                ACTOR_ESCROW_INDEX,
            ),
        ],
    )
    .await?;
    check_eq!(
        base_count(base, &counter).await?,
        0,
        "the base counter starts at zero"
    )?;
    let submission = proxies.intercept(
        Selector::method("sendTransaction")
            .http()
            .response()
            .account(&counter),
    );
    let intent_signature = er
        .submit_and_confirm(
            &payer,
            &[flexi::create_action_intent(
                payer.pubkey(),
                counter,
                ACTION_COUNT,
                ACTION_COMPUTE_UNITS,
            )],
        )
        .await?;
    let held = submission.wait(INTERCEPT_TIMEOUT).await?;
    let landed = held.operation.signature()?;
    let base_tx = base
        .api()
        .await_transaction(&landed, BASE_CONFIRM_TIMEOUT)
        .await?;
    check!(
        base_tx.err.is_none(),
        "the intercepted base action {landed} must succeed on base, got {:?}",
        base_tx.err
    )?;
    check::poll(
        "the base counter shows the action once before the fault is applied",
        BASE_STATE_TIMEOUT,
        || async { matches!(base_count(base, &counter).await, Ok(1)) },
    )
    .await?;
    let confirmations = withhold_confirmations(proxies, &landed);
    held.discard();
    hold_count(base, &counter, 1, BLACKOUT_WINDOW, "action blackout").await?;
    for rule in confirmations {
        rule.remove();
    }
    let receipt = receipt::fetch_commit_receipt(
        er.api(),
        &intent_signature,
        RECEIPT_TIMEOUT,
    )
    .await?;
    if receipt.succeeded() {
        check!(
            !receipt.base_signatures.is_empty(),
            "a succeeded action receipt must name the base transaction it \
             confirmed"
        )?;
        receipt::confirm_base_signatures(
            base.api(),
            &receipt,
            BASE_CONFIRM_TIMEOUT,
        )
        .await?;
    }
    hold_count(base, &counter, 1, SETTLE_WINDOW, "action settle").await?;
    Ok(ActionOutcome {
        landed,
        receipt_succeeded: receipt.succeeded(),
        receipt_error: receipt.error_message,
        seconds: started.elapsed().as_secs_f64(),
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

fn restart_settings(
    report: ScenarioReport,
    timing: &RestartTiming,
) -> ScenarioReport {
    report
        .setting("restart needed sigkill", timing.needed_sigkill)
        .setting(
            "restart exit",
            format!(
                "code={:?} signal={:?}",
                timing.exit_code, timing.exit_signal
            ),
        )
        .metric(
            "restart shutdown s",
            Unit::Seconds,
            timing.shutdown.as_secs_f64(),
        )
        .metric(
            "restart startup s",
            Unit::Seconds,
            timing.startup.as_secs_f64(),
        )
}

#[async_trait(?Send)]
impl PrivateErScenario for CommitExactlyOnce {
    fn name(&self) -> &str {
        "redshift/commit_exactly_once"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let proxies = BaseProxies::spawn(base).await?;
        let mut private = topology::private_er(
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
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let account =
            crate::init_delegated_account(base, &payer, 0, identity).await?;
        let on_base = base.account(&account).await?.ok_or("pda not on base")?;
        check_eq!(
            on_base.owner,
            DELEGATION_PROGRAM_ID,
            "a delegated pda must be dlp-owned on base"
        )?;
        await_clone(private.ctx(), &account).await?;

        let warmup_nonce = follow_up_commit(
            base,
            private.ctx(),
            &payer,
            account,
            1,
            WARMUP_WRITE,
            "warm-up",
        )
        .await?;
        let blackout = commit_blackout(
            &proxies,
            base,
            &mut private,
            &payer,
            account,
            FaultedCommit {
                phase: "blackout",
                commit_id: 2,
                write: BLACKOUT_WRITE,
                restart: false,
            },
        )
        .await?;
        let recovery_nonce = follow_up_commit(
            base,
            private.ctx(),
            &payer,
            account,
            3,
            RECOVERY_WRITE,
            "post-blackout",
        )
        .await?;
        let action = action_blackout(&proxies, base, private.ctx()).await?;
        let restarted = commit_blackout(
            &proxies,
            base,
            &mut private,
            &payer,
            account,
            FaultedCommit {
                phase: "restart",
                commit_id: 4,
                write: RESTART_WRITE,
                restart: true,
            },
        )
        .await?;
        let restart_recovery_nonce = follow_up_commit(
            base,
            private.ctx(),
            &payer,
            account,
            5,
            RESTART_RECOVERY_WRITE,
            "post-restart",
        )
        .await?;

        let events = proxies.finish()?;
        private.finish().await?;

        let mut report = ScenarioReport::ok(self.name())
            .setting("er", LABEL)
            .setting("delegated account", account)
            .setting(
                "fault",
                "sendTransaction response discarded, getSignatureStatuses and \
                 signatureNotification withheld",
            )
            .setting("warm-up nonce", warmup_nonce)
            .setting("blackout landed", blackout.landed)
            .setting("blackout nonce", blackout.nonce)
            .setting(
                "blackout receipt base sigs",
                blackout.receipt_base_signatures,
            )
            .setting("post-blackout nonce", recovery_nonce)
            .setting("restart landed", restarted.landed)
            .setting("restart nonce", restarted.nonce)
            .setting(
                "restart receipt base sigs",
                restarted.receipt_base_signatures,
            )
            .setting("post-restart nonce", restart_recovery_nonce)
            .setting("action landed", action.landed)
            .setting("action receipt succeeded", action.receipt_succeeded)
            .setting(
                "action receipt error",
                action.receipt_error.unwrap_or_else(|| "none".to_owned()),
            )
            .metric("blackout settlement s", Unit::Seconds, blackout.seconds)
            .metric("restart settlement s", Unit::Seconds, restarted.seconds)
            .metric("action settlement s", Unit::Seconds, action.seconds)
            .metric("fault events", Unit::Count, events.len() as f64);
        if let Some(timing) = &restarted.restart {
            report = restart_settings(report, timing);
        }
        Ok(netfault::report_events(report, &events))
    }
}
