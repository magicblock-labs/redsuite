use std::{rc::Rc, slice, time::Duration};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
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
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
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
    Ok(base.api().get_program_accounts(&topology::COMMITTOR_ID).await?.len())
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
        .context("the delegated account is missing on base")?
        .data)
}

async fn await_base(base: &BaseCtx, account: &Pubkey, snapshot: &[u8], what: &str) -> Result<()> {
    base.api()
        .await_accounts(slice::from_ref(account), what, async || {
            let account = base.account(account).await?.context("base account missing")?;
            ensure!(account.data == snapshot);
            Ok(())
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
    let receipt = receipt::fetch_commit_receipt(er.api(), commit_signature).await?;
    receipt.check_success(phase)?;
    receipt.check_included(expected)?;
    receipt.confirm(base.api()).await?;
    let last = receipt.base_signatures[receipt.base_signatures.len() - 1];
    let slot = base.api().await_transaction(&last).await?.slot;
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
        .context("settlement missing from base history")
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
        let on_base = base.account(&account).await?.context("pda not on base")?;
        ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
        er.await_clones(&[account], variant.space as usize).await?;
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

    settled(base, er, &third, &[d], "independent bundle").await?;
    await_base(
        base,
        &d,
        &d1,
        "independent snapshot settles while first bundle held",
    )
    .await?;
    ensure!(crate::last_commit_id(base, &d).await? == (nonce_d + 1));
    hold_nonces(base, &[a, b], &[nonce_a, nonce_b], HOLD_WINDOW).await?;
    let buffers_during_hold = buffer_accounts(base).await?.saturating_sub(buffers_before);
    if variant.buffered {
        ensure!(buffers_during_hold > 0);
    } else {
        ensure!(buffers_during_hold == 0);
    }

    let retry_submission =
        proxies.intercept(Selector::method("sendTransaction").http().request().account(&a));
    held_first.discard();
    let held_retry = retry_submission.wait(INTERCEPT_TIMEOUT).await?;
    let retry_held_at = held_retry.held_at;
    held_retry.release();

    let first_bundle = settled(base, er, &first, &[a, b], "delayed bundle").await?;
    ensure!(base_data(base, &a).await? == a1);
    ensure!(base_data(base, &b).await? == b1);
    ensure!(crate::last_commit_id(base, &a).await? == (nonce_a + 1));
    ensure!(crate::last_commit_id(base, &b).await? == (nonce_b + 1));

    let held_second = second_submission.wait(INTERCEPT_TIMEOUT).await?;
    ensure!(held_second.held_at > retry_held_at);
    held_second.release();
    let second_bundle = settled(base, er, &second, &[b, c], "conflicting bundle").await?;
    await_base(base, &b, &b2, "conflicting bundle: B snapshot").await?;
    ensure!(base_data(base, &c).await? == c1);
    ensure!(crate::last_commit_id(base, &b).await? == (nonce_b + 2));
    ensure!(crate::last_commit_id(base, &c).await? == (nonce_c + 1));
    ensure!(first_bundle.slot <= second_bundle.slot);
    let first_position =
        history_position(base, &b, &first_bundle.receipt.base_signatures[0]).await?;
    let second_position =
        history_position(base, &b, &second_bundle.receipt.base_signatures[0]).await?;
    ensure!(first_position > second_position);

    let mut finals = Vec::with_capacity(4);
    for (offset, account) in accounts.iter().enumerate() {
        finals.push(write_snapshot(er, &payer, value(21 + offset as u64), account).await?);
    }
    let fourth = schedule(er, &payer, 4, &accounts).await?;
    settled(base, er, &fourth, &accounts, "follow-up bundle").await?;
    for (account, snapshot) in accounts.iter().zip(&finals) {
        await_base(base, account, snapshot, "follow-up bundle: final snapshot").await?;
    }
    base.api()
        .await_query("commit buffers closed after settlement", async || {
            ensure!(buffer_accounts(base).await? <= buffers_before);
            Ok(())
        })
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
            crate::phase(
                variant.name,
                run_variant(&proxies, &base, private.ctx(), variant),
            )
            .await?;
        }

        proxies.finish()?;
        private.finish().await?;
        Ok(())
    }
}
