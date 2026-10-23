use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, dlp,
    netfault::{BaseProxies, RuleHandle, Selector},
    programs::{Action, CellHeader, CellId, IntentBundle, REDLINE, REDSHIFT},
    receipt::{self, CommitReceipt},
    topology::RestartConfig,
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use tokio::time;

use super::{
    hold_nonces, prove_landed, write_and_commit, BASE_CONFIRM_TIMEOUT, BASE_STATE_TIMEOUT,
    PROXIED_GROUP,
};
use crate::{program::DELEGATION_PROGRAM_ID, ACCOUNT_SPACE, PAYER_LAMPORTS};

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

struct FaultedCommit<'a> {
    phase: &'a str,
    commit_id: u64,
    write: u64,
    restart: bool,
}

async fn base_count(base: &BaseCtx, cell: &Pubkey) -> Result<u64> {
    let account = base.account(cell).await?.ok_or("the base cell is missing")?;
    Ok(CellHeader::try_decode(&account.data)?.value)
}

async fn hold_count(
    base: &BaseCtx,
    cell: &Pubkey,
    expected: u64,
    window: Duration,
    phase: &str,
) -> Result<()> {
    let deadline = Instant::now() + window;
    loop {
        let count = base_count(base, cell).await?;
        check_eq!(
            count,
            expected,
            "{phase}: the base cell must not move once the action has \
             executed"
        )?;
        if Instant::now() >= deadline {
            return Ok(());
        }
        time::sleep(SAMPLE_INTERVAL).await;
    }
}

fn withhold_confirmations(proxies: &BaseProxies, signature: &Signature) -> [RuleHandle; 2] {
    [
        proxies
            .stall(Selector::method("getSignatureStatuses").http().response().signature(signature)),
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
        receipt::fetch_commit_receipt(er.api(), commit_signature, RECEIPT_TIMEOUT).await?
    };
    check!(
        !receipt.failure_is_duplicate_rejection(),
        "{phase}: a duplicate-rejection receipt does not pass on base bytes \
         alone, got {:?}",
        receipt.error_message
    )?;
    receipt.check_success(phase)?;
    receipt.confirm(base.api(), BASE_CONFIRM_TIMEOUT).await?;
    Ok(receipt)
}

async fn commit_blackout(
    proxies: &BaseProxies,
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    account: Pubkey,
    faulted: FaultedCommit<'_>,
) -> Result<()> {
    let FaultedCommit { phase, commit_id, write, restart } = faulted;
    let nonce_before = crate::last_commit_id(base, &account).await?;
    let submission =
        proxies.intercept(Selector::method("sendTransaction").http().response().account(&account));
    let (snapshot, commit_signature) =
        write_and_commit(er, payer, commit_id, write, &account).await?;
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
        Some(receipt::scheduled_intent_id(er.api(), &commit_signature).await?)
    } else {
        None
    };
    let confirmations = withhold_confirmations(proxies, &landed);
    if restart {
        er.restart(RestartConfig::default()).await?;
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
    let receipt = settled_receipt(base, er, &commit_signature, restart_intent_id, phase).await?;
    check!(
        receipt.base_signatures.contains(&landed),
        "{phase}: the receipt must name the base transaction that landed \
         ({landed}), got {:?}",
        receipt.base_signatures
    )?;
    hold_nonces(base, &[account], &[settled], SETTLE_WINDOW, phase).await?;
    let on_base = base.account(&account).await?.ok_or("the delegated pda vanished from base")?;
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
        || async { matches!(base.account(&account).await, Ok(Some(acc)) if acc.data == snapshot) },
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

async fn action_blackout(proxies: &BaseProxies, base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let payer = base.funded_payer(PAYER_LAMPORTS).await?;
    let (init, cell) = REDSHIFT.create(
        payer.pubkey(),
        CellId::new(payer.pubkey(), 0),
        CellHeader::SIZE as u32,
    );
    base.submit_and_confirm(
        &payer,
        &[init, dlp::top_up_ephemeral_balance(&payer.pubkey(), ACTION_ESCROW_LAMPORTS, 1)],
    )
    .await?;
    check_eq!(
        base_count(base, &cell).await?,
        0,
        "the base cell starts at zero"
    )?;
    let submission =
        proxies.intercept(Selector::method("sendTransaction").http().response().account(&cell));
    let action = REDSHIFT
        .batch()
        .add(&[cell], i64::from(ACTION_COUNT))
        .action(cell, REDSHIFT.id());
    let mut bundle = IntentBundle::new(payer.pubkey());
    bundle.actions.push(Action::new(payer.pubkey(), action, ACTION_COMPUTE_UNITS));
    let intent_signature = er
        .submit_and_confirm(&payer, &[REDSHIFT.batch().intents(bundle).build()])
        .await?;
    let held = submission.wait(INTERCEPT_TIMEOUT).await?;
    let landed = held.operation.signature()?;
    let base_tx = base.api().await_transaction(&landed, BASE_CONFIRM_TIMEOUT).await?;
    check!(
        base_tx.err.is_none(),
        "the intercepted base action {landed} must succeed on base, got {:?}",
        base_tx.err
    )?;
    check::poll(
        "the base cell shows the action once before the fault is applied",
        BASE_STATE_TIMEOUT,
        || async { matches!(base_count(base, &cell).await, Ok(1)) },
    )
    .await?;
    let confirmations = withhold_confirmations(proxies, &landed);
    held.discard();
    hold_count(base, &cell, 1, BLACKOUT_WINDOW, "action blackout").await?;
    for rule in confirmations {
        rule.remove();
    }
    let receipt =
        receipt::fetch_commit_receipt(er.api(), &intent_signature, RECEIPT_TIMEOUT).await?;
    if receipt.succeeded() {
        check!(
            !receipt.base_signatures.is_empty(),
            "a succeeded action receipt must name the base transaction it \
             confirmed"
        )?;
        receipt.confirm(base.api(), BASE_CONFIRM_TIMEOUT).await?;
    }
    hold_count(base, &cell, 1, SETTLE_WINDOW, "action settle").await?;
    Ok(())
}

pub struct CommitExactlyOnce {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
    account: Pubkey,
}

impl Scenario for CommitExactlyOnce {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        er.proxies().ok_or(PROXIED_GROUP)?;
        let identity = er.identity();
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let account =
            base.cells(REDLINE, &payer).create_delegated(ACCOUNT_SPACE, identity, 0).await?;
        let on_base = base.account(&account).await?.ok_or("pda not on base")?;
        check_eq!(
            on_base.owner,
            DELEGATION_PROGRAM_ID,
            "a delegated pda must be dlp-owned on base"
        )?;
        er.await_clones(&[account], ACCOUNT_SPACE as usize, CLONE_TIMEOUT).await?;

        Ok(Self { base, er, payer, account })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er, payer, account } = self;
        let proxies = er.proxies().ok_or(PROXIED_GROUP)?;
        follow_up_commit(&base, &er, &payer, account, 1, WARMUP_WRITE, "warm-up").await?;
        commit_blackout(
            proxies,
            &base,
            &er,
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
            &base,
            &er,
            &payer,
            account,
            3,
            RECOVERY_WRITE,
            "post-blackout",
        )
        .await?;
        action_blackout(proxies, &base, &er).await?;
        commit_blackout(
            proxies,
            &base,
            &er,
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
            &base,
            &er,
            &payer,
            account,
            5,
            RESTART_RECOVERY_WRITE,
            "post-restart",
        )
        .await?;

        super::scheduled_intents::run(&base, &er).await
    }
}
