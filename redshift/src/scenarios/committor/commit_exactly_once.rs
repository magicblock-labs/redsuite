use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    dlp,
    netfault::{BaseProxies, RuleHandle, Selector},
    programs::{Action, CellHeader, CellId, IntentBundle, REDLINE, REDSHIFT},
    receipt::{self, CommitReceipt},
    topology::RestartConfig,
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use tokio::time;

use super::{hold_nonces, prove_landed, write_and_commit, PROXIED_GROUP};
use crate::{program::DELEGATION_PROGRAM_ID, ACCOUNT_SPACE, PAYER_LAMPORTS};

const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
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
    let account = base.account(cell).await?.context("the base cell is missing")?;
    Ok(CellHeader::try_decode(&account.data)?.value)
}

async fn hold_count(base: &BaseCtx, cell: &Pubkey, expected: u64, window: Duration) -> Result<()> {
    let deadline = Instant::now() + window;
    loop {
        let count = base_count(base, cell).await?;
        ensure!(count == expected);
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
        receipt::fetch_commit_receipt_by_intent(er.api(), &er.identity(), intent_id).await?
    } else {
        receipt::fetch_commit_receipt(er.api(), commit_signature).await?
    };
    ensure!(!receipt.failure_is_duplicate_rejection());
    receipt.check_success(phase)?;
    receipt.confirm(base.api()).await?;
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
    ensure!(nonce == (nonce_before + 1));
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
    hold_nonces(base, &[account], &[settled], BLACKOUT_WINDOW).await?;
    for rule in confirmations {
        rule.remove();
    }
    let receipt = settled_receipt(base, er, &commit_signature, restart_intent_id, phase).await?;
    ensure!(receipt.base_signatures.contains(&landed));
    hold_nonces(base, &[account], &[settled], SETTLE_WINDOW).await?;
    let on_base = base.account(&account).await?.context("the delegated pda vanished from base")?;
    ensure!(on_base.data == snapshot);
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
    base.api()
        .await_accounts(
            &[account],
            "the base copy matches the er snapshot after the follow-up commit",
            async || {
                let account = base.account(&account).await?.context("base account missing")?;
                ensure!(account.data == snapshot);
                Ok(())
            },
        )
        .await?;
    let nonce = crate::last_commit_id(base, &account).await?;
    ensure!(nonce == (nonce_before + 1));
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
    ensure!(base_count(base, &cell).await? == 0);
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
    let base_tx = base.api().await_transaction(&landed).await?;
    let meta = transaction_meta(&base_tx)?;
    ensure!(meta.err == None);
    base.api()
        .await_accounts(
            &[cell],
            "the base cell shows the action once before the fault is applied",
            async || {
                ensure!(base_count(base, &cell).await? == 1);
                Ok(())
            },
        )
        .await?;
    let confirmations = withhold_confirmations(proxies, &landed);
    held.discard();
    hold_count(base, &cell, 1, BLACKOUT_WINDOW).await?;
    for rule in confirmations {
        rule.remove();
    }
    let receipt = receipt::fetch_commit_receipt(er.api(), &intent_signature).await?;
    if receipt.succeeded() {
        ensure!(!receipt.base_signatures.is_empty());
        receipt.confirm(base.api()).await?;
    }
    hold_count(base, &cell, 1, SETTLE_WINDOW).await?;
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
        er.proxies().context(PROXIED_GROUP)?;
        let identity = er.identity();
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let account =
            base.cells(REDLINE, &payer).create_delegated(ACCOUNT_SPACE, identity, 0).await?;
        let on_base = base.account(&account).await?.context("pda not on base")?;
        ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
        er.await_clones(&[account], ACCOUNT_SPACE as usize).await?;

        Ok(Self { base, er, payer, account })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er, payer, account } = self;
        let proxies = er.proxies().context(PROXIED_GROUP)?;
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
