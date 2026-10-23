use std::{rc::Rc, time::Duration};

use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq,
    netfault::{BaseProxies, Selector},
    programs::{SettlementMode, REDLINE},
    receipt::{self, CommitReceipt},
    topology::{self, ErOptions, PrivateEr},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signature::Signature;

use super::{hold_nonces, write_snapshot};
use crate::{program::DELEGATION_PROGRAM_ID, ACCOUNT_SPACE, PAYER_LAMPORTS};

const LABEL: &str = "commit-settlement-order";
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);
const BUFFER_CLEANUP_TIMEOUT: Duration = Duration::from_secs(60);
const HOLD_WINDOW: Duration = Duration::from_secs(5);
const HISTORY_LIMIT: usize = 32;
const SMALL_SPACE: u32 = ACCOUNT_SPACE;
const LARGE_SPACE: u32 = 2048;

#[derive(Clone, Copy)]
struct Variant {
    name: &'static str,
    space: u32,
    seed_base: u8,
    value_base: u64,
    buffered: bool,
}

const VARIANTS: [Variant; 2] = [
    Variant {
        name: "inline",
        space: SMALL_SPACE,
        seed_base: 10,
        value_base: 100,
        buffered: false,
    },
    Variant {
        name: "buffered",
        space: LARGE_SPACE,
        seed_base: 20,
        value_base: 200,
        buffered: true,
    },
];

struct Bundle {
    receipt: CommitReceipt,
    slot: u64,
}

async fn buffer_accounts(base: &BaseCtx) -> Result<usize> {
    let committor: Pubkey = topology::COMMITTOR_ID;
    Ok(base.api().get_program_accounts(&committor).await?.len())
}

async fn schedule(
    er: &ErCtx,
    payer: &Keypair,
    commit_id: u64,
    accounts: &[Pubkey],
) -> Result<Signature> {
    er.cells(REDLINE, payer)
        .settle(commit_id, accounts, SettlementMode::Commit)
        .await
}

async fn base_data(base: &BaseCtx, account: &Pubkey) -> Result<Vec<u8>> {
    Ok(base
        .account(account)
        .await?
        .ok_or("the delegated account is missing on base")?
        .data)
}

async fn await_base(base: &BaseCtx, account: &Pubkey, snapshot: &[u8], what: &str) -> Result<()> {
    check::poll(what, BASE_STATE_TIMEOUT, || async {
        matches!(base.account(account).await, Ok(Some(acc)) if acc.data == snapshot)
    })
    .await?;
    Ok(())
}

async fn settled(
    base: &BaseCtx,
    er: &ErCtx,
    commit_signature: &Signature,
    expected: &[Pubkey],
    phase: &str,
) -> Result<Bundle> {
    let receipt =
        receipt::fetch_commit_receipt(er.api(), commit_signature, RECEIPT_TIMEOUT).await?;
    receipt.check_success(phase)?;
    let mut included = receipt.included.clone();
    included.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    check_eq!(
        included,
        expected,
        "{phase}: the receipt must list exactly the bundled accounts"
    )?;
    receipt.confirm(base.api(), BASE_CONFIRM_TIMEOUT).await?;
    let last = receipt.base_signatures[receipt.base_signatures.len() - 1];
    let slot = base.api().await_transaction(&last, BASE_CONFIRM_TIMEOUT).await?.slot;
    Ok(Bundle { receipt, slot })
}

async fn history_position(
    base: &BaseCtx,
    account: &Pubkey,
    signature: &Signature,
) -> Result<usize> {
    let history = base.api().get_signatures_for_address(account, HISTORY_LIMIT).await?;
    history
        .iter()
        .position(|entry| entry == signature)
        .ok_or_else(|| format!("base history of {account} does not list {signature}").into())
}

async fn prepare_accounts(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    variant: Variant,
) -> Result<[Pubkey; 4]> {
    let mut keys = [Pubkey::default(); 4];
    for (index, key) in keys.iter_mut().enumerate() {
        let account = base
            .cells(REDLINE, payer)
            .create_delegated(
                variant.space,
                er.identity(),
                variant.seed_base + index as u8,
            )
            .await?;
        let on_base = base.account(&account).await?.ok_or("pda not on base")?;
        check_eq!(
            on_base.owner,
            DELEGATION_PROGRAM_ID,
            "a delegated pda must be dlp-owned on base"
        )?;
        er.await_clones(&[account], variant.space as usize, CLONE_TIMEOUT).await?;
        *key = account;
    }
    Ok(keys)
}

async fn run_variant(
    proxies: &BaseProxies,
    base: &BaseCtx,
    er: &ErCtx,
    variant: Variant,
) -> Result<()> {
    let phase = variant.name;
    let payer = base.funded_payer(PAYER_LAMPORTS).await?;
    let accounts = prepare_accounts(base, er, &payer, variant).await?;
    let [a, b, c, d] = accounts;
    let value = |offset: u64| variant.value_base + offset;
    let buffers_before = buffer_accounts(base).await?;

    let a1 = write_snapshot(er, &payer, value(1), &a).await?;
    let b1 = write_snapshot(er, &payer, value(2), &b).await?;
    let nonce_a = crate::last_commit_id(base, &a).await?;
    let nonce_b = crate::last_commit_id(base, &b).await?;
    let nonce_c = crate::last_commit_id(base, &c).await?;
    let nonce_d = crate::last_commit_id(base, &d).await?;
    let first_submission =
        proxies.intercept(Selector::method("sendTransaction").http().request().account(&a));
    let first = schedule(er, &payer, 1, &[a, b]).await?;
    let held_first = first_submission.wait(INTERCEPT_TIMEOUT).await?;

    let b2 = write_snapshot(er, &payer, value(3), &b).await?;
    let c1 = write_snapshot(er, &payer, value(4), &c).await?;
    let second_submission =
        proxies.intercept(Selector::method("sendTransaction").http().request().account(&c));
    let second = schedule(er, &payer, 2, &[b, c]).await?;
    let d1 = write_snapshot(er, &payer, value(5), &d).await?;
    let third = schedule(er, &payer, 3, &[d]).await?;

    for (offset, account) in accounts.iter().enumerate() {
        write_snapshot(er, &payer, value(11 + offset as u64), account).await?;
    }

    settled(base, er, &third, &[d], &format!("{phase} {{D}}")).await?;
    await_base(
        base,
        &d,
        &d1,
        "the independent bundle lands its staged value while the first \
         bundle is held",
    )
    .await?;
    check_eq!(
        crate::last_commit_id(base, &d).await?,
        nonce_d + 1,
        "{phase}: the independent bundle must advance D's commit nonce \
         exactly once"
    )?;
    hold_nonces(base, &[a, b], &[nonce_a, nonce_b], HOLD_WINDOW, phase).await?;
    let buffers_during_hold = buffer_accounts(base).await?.saturating_sub(buffers_before);
    if variant.buffered {
        check!(
            buffers_during_hold > 0,
            "{phase}: large accounts must be staged in temporary commit \
             buffers on base while the first bundle is held"
        )?;
    } else {
        check_eq!(
            buffers_during_hold,
            0,
            "{phase}: small accounts must not need commit buffers"
        )?;
    }

    let retry_submission =
        proxies.intercept(Selector::method("sendTransaction").http().request().account(&a));
    held_first.discard();
    let held_retry = retry_submission.wait(INTERCEPT_TIMEOUT).await?;
    let retry_held_at = held_retry.held_at;
    held_retry.release();

    let first_bundle = settled(base, er, &first, &[a, b], &format!("{phase} {{A,B}}")).await?;
    check_eq!(
        base_data(base, &a).await?,
        a1,
        "{phase}: A must carry the first bundle's staged value after its \
         retry settles"
    )?;
    check_eq!(
        base_data(base, &b).await?,
        b1,
        "{phase}: B must carry the first bundle's staged value before the \
         conflicting bundle is submitted"
    )?;
    check_eq!(
        crate::last_commit_id(base, &a).await?,
        nonce_a + 1,
        "{phase}: the retried bundle must advance A's commit nonce exactly \
         once"
    )?;
    check_eq!(
        crate::last_commit_id(base, &b).await?,
        nonce_b + 1,
        "{phase}: the retried bundle must advance B's commit nonce exactly \
         once before the conflicting bundle is submitted"
    )?;

    let held_second = second_submission.wait(INTERCEPT_TIMEOUT).await?;
    check!(
        held_second.held_at > retry_held_at,
        "{phase}: the conflicting bundle must not be submitted before the \
         delayed bundle retried (submitted at +{:.3}s, retry at +{:.3}s)",
        held_second.held_at.as_secs_f64(),
        retry_held_at.as_secs_f64()
    )?;
    held_second.release();
    let second_bundle = settled(base, er, &second, &[b, c], &format!("{phase} {{B,C}}")).await?;
    await_base(
        base,
        &b,
        &b2,
        "B carries the second bundle's staged value once it settles",
    )
    .await?;
    check_eq!(
        base_data(base, &c).await?,
        c1,
        "{phase}: C must carry the second bundle's staged value"
    )?;
    check_eq!(
        crate::last_commit_id(base, &b).await?,
        nonce_b + 2,
        "{phase}: the conflicting bundle must advance B's commit nonce \
         exactly once on top of the delayed bundle"
    )?;
    check_eq!(
        crate::last_commit_id(base, &c).await?,
        nonce_c + 1,
        "{phase}: the conflicting bundle must advance C's commit nonce \
         exactly once"
    )?;
    check!(
        first_bundle.slot <= second_bundle.slot,
        "{phase}: the delayed bundle must land no later than the \
         conflicting one (slots {} and {})",
        first_bundle.slot,
        second_bundle.slot
    )?;
    let first_position =
        history_position(base, &b, &first_bundle.receipt.base_signatures[0]).await?;
    let second_position =
        history_position(base, &b, &second_bundle.receipt.base_signatures[0]).await?;
    check!(
        first_position > second_position,
        "{phase}: B's base history must show the delayed bundle before the \
         conflicting one"
    )?;

    let mut finals = Vec::with_capacity(4);
    for (offset, account) in accounts.iter().enumerate() {
        finals.push(write_snapshot(er, &payer, value(21 + offset as u64), account).await?);
    }
    let fourth = schedule(er, &payer, 4, &accounts).await?;
    settled(
        base,
        er,
        &fourth,
        &accounts,
        &format!("{phase} {{A,B,C,D}}"),
    )
    .await?;
    for (account, snapshot) in accounts.iter().zip(&finals) {
        await_base(
            base,
            account,
            snapshot,
            "the follow-up bundle lands every account's final value",
        )
        .await?;
    }
    check::poll(
        "the temporary commit buffers are closed once every bundle settled",
        BUFFER_CLEANUP_TIMEOUT,
        || async { matches!(buffer_accounts(base).await, Ok(count) if count <= buffers_before) },
    )
    .await?;
    Ok(())
}

pub struct CommitSettlementOrder {
    base: Rc<BaseCtx>,
    proxies: BaseProxies,
    private: PrivateEr,
}

impl PrivateErScenario for CommitSettlementOrder {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let proxies = BaseProxies::spawn(&base).await?;
        let private = topology::private_er(
            &base,
            ErOptions {
                label: LABEL.to_owned(),
                base_endpoints: Some(proxies.endpoints()),
                ..Default::default()
            },
        )
        .await?;
        Ok(Self { base, proxies, private })
    }

    async fn run(self) -> Result<()> {
        let Self { base, proxies, private } = self;
        for variant in VARIANTS {
            run_variant(&proxies, &base, private.ctx(), variant).await?;
        }

        proxies.finish()?;
        private.finish().await?;
        Ok(())
    }
}
