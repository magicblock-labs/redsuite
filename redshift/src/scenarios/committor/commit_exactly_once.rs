use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::flexi::{
    build as flexi, FlexiCounter, ACTOR_ESCROW_INDEX,
};
use redsuite_core::{
    check, check_eq, dlp,
    netfault::{BaseProxies, RuleHandle, Selector},
    prep,
    receipt::{self, CommitReceipt},
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signature::Signature;
use signer::Signer;

use crate::program::DELEGATION_PROGRAM_ID;

use super::{
    hold_nonces, prove_landed, write_and_commit, BASE_CONFIRM_TIMEOUT,
    BASE_STATE_TIMEOUT,
};

const LABEL: &str = "commit-exactly-once";
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
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

async fn base_count(base: &BaseCtx, counter: &Pubkey) -> Result<u64> {
    let account = base
        .account(counter)
        .await?
        .ok_or("the base counter is missing")?;
    Ok(FlexiCounter::try_decode(&account.data)?.count)
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
    restart_intent_id: Option<u64>,
    phase: &str,
) -> Result<CommitReceipt> {
    let receipt = if let Some(intent_id) = restart_intent_id {
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
    super::require_settled(&receipt, phase)?;
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
) -> Result<()> {
    let FaultedCommit {
        phase,
        commit_id,
        write,
        restart,
    } = faulted;
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
    let restart_intent_id = if restart {
        Some(
            receipt::scheduled_intent_id(
                private.ctx().api(),
                &commit_signature,
            )
            .await?,
        )
    } else {
        None
    };
    let confirmations = withhold_confirmations(proxies, &landed);
    if restart {
        private.restart(RestartConfig::default()).await?;
    }
    held.discard();
    // The base transaction already landed and advanced the DLP nonce. Recovery
    // must reconcile that landed transaction instead of producing another base
    // effect for the same intent.
    let settled = nonce;
    hold_nonces(base, &[account], &[settled], BLACKOUT_WINDOW, phase).await?;
    for rule in confirmations {
        rule.remove();
    }
    let receipt = settled_receipt(
        base,
        private.ctx(),
        &commit_signature,
        restart_intent_id,
        phase,
    )
    .await?;
    check!(
        receipt.base_signatures.contains(&landed),
        "{phase}: the receipt must name the base transaction that landed \
         ({landed}), got {:?}",
        receipt.base_signatures
    )?;
    hold_nonces(base, &[account], &[settled], SETTLE_WINDOW, phase).await?;
    let on_base = base
        .account(&account)
        .await?
        .ok_or("the delegated pda vanished from base")?;
    check!(
        on_base.data == snapshot,
        "{phase}: the base copy must still carry the settled er snapshot"
    )?;
    Ok(())
}

async fn follow_up_commit(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    account: Pubkey,
    commit_id: u64,
    write: u64,
    phase: &str,
) -> Result<()> {
    let nonce_before = crate::last_commit_id(base, &account).await?;
    let (snapshot, commit_signature) =
        write_and_commit(er, payer, commit_id, write, &account).await?;
    settled_receipt(base, er, &commit_signature, None, phase).await?;
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
    Ok(())
}

async fn action_blackout(
    proxies: &BaseProxies,
    base: &BaseCtx,
    er: &ErCtx,
) -> Result<()> {
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
    Ok(())
}

#[async_trait(?Send)]
impl PrivateErScenario for CommitExactlyOnce {
    fn name(&self) -> &str {
        "redshift/commit_exactly_once"
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
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
        prep::await_clones(
            private.ctx(),
            &[account],
            crate::ACCOUNT_SPACE as usize,
            CLONE_TIMEOUT,
        )
        .await?;

        follow_up_commit(
            base,
            private.ctx(),
            &payer,
            account,
            1,
            WARMUP_WRITE,
            "warm-up",
        )
        .await?;
        commit_blackout(
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
        follow_up_commit(
            base,
            private.ctx(),
            &payer,
            account,
            3,
            RECOVERY_WRITE,
            "post-blackout",
        )
        .await?;
        action_blackout(&proxies, base, private.ctx()).await?;
        commit_blackout(
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
        follow_up_commit(
            base,
            private.ctx(),
            &payer,
            account,
            5,
            RESTART_RECOVERY_WRITE,
            "post-restart",
        )
        .await?;

        proxies.finish()?;
        private.finish().await?;
        Ok(())
    }
}
