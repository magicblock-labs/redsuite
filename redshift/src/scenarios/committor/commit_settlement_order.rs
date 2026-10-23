use std::{rc::Rc, slice, time::Duration};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    netfault::{BaseProxies, Selector},
    programs::{SettlementMode, REDLINE},
    receipt::{self, CommitReceipt},
    topology::{self, ErOptions, PrivateEr},
    BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use signature::Signature;
use tracing::instrument;

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
        for variant in VARIANTS {
            self.run_variant(variant).await?;
        }

        self.proxies.finish()?;
        self.private.finish().await?;
        Ok(())
    }
}

impl CommitSettlementOrder {
    async fn buffer_accounts(&self) -> Result<usize> {
        Ok(self.base.api().get_program_accounts(&topology::COMMITTOR_ID).await?.len())
    }

    async fn base_data(&self, account: &Pubkey) -> Result<Vec<u8>> {
        Ok(self
            .base
            .account(account)
            .await?
            .context("the delegated account is missing on base")?
            .data)
    }

    async fn await_base(&self, account: &Pubkey, snapshot: &[u8], what: &str) -> Result<()> {
        self.base
            .api()
            .await_accounts(slice::from_ref(account), what, async || {
                let account = self.base.account(account).await?.context("base account missing")?;
                ensure!(account.data == snapshot);
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn history_position(&self, account: &Pubkey, signature: &Signature) -> Result<usize> {
        let history = self.base.api().get_signatures_for_address(account, HISTORY_LIMIT).await?;
        history
            .iter()
            .position(|entry| entry == signature)
            .context("settlement missing from base history")
    }

    async fn schedule(
        &self,
        payer: &Keypair,
        commit_id: u64,
        accounts: &[Pubkey],
    ) -> Result<Signature> {
        self.private
            .ctx()
            .cells(REDLINE, payer)
            .settle(commit_id, accounts, SettlementMode::Commit)
            .await
    }

    async fn settled(
        &self,
        commit_signature: &Signature,
        expected: &[Pubkey],
        phase: &str,
    ) -> Result<Bundle> {
        let receipt =
            receipt::fetch_commit_receipt(self.private.ctx().api(), commit_signature).await?;
        receipt.check_success(phase)?;
        receipt.check_included(expected)?;
        receipt.confirm(self.base.api()).await?;
        let last = receipt.base_signatures[receipt.base_signatures.len() - 1];
        let slot = self.base.api().await_transaction(&last).await?.slot;
        Ok(Bundle { receipt, slot })
    }

    async fn prepare_accounts(&self, payer: &Keypair, variant: Variant) -> Result<[Pubkey; 4]> {
        let mut keys = [Pubkey::default(); 4];
        for (index, key) in keys.iter_mut().enumerate() {
            let account = self
                .base
                .cells(REDLINE, payer)
                .create_delegated(
                    variant.space,
                    self.private.ctx().identity(),
                    variant.seed_base + index as u8,
                )
                .await?;
            let on_base = self.base.account(&account).await?.context("pda not on base")?;
            ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
            self.private.ctx().await_clones(&[account], variant.space as usize).await?;
            *key = account;
        }
        Ok(keys)
    }

    #[instrument(skip_all, err, fields(variant = variant.name))]
    async fn run_variant(&self, variant: Variant) -> Result<()> {
        let payer = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let accounts = self.prepare_accounts(&payer, variant).await?;
        let [a, b, c, d] = accounts;
        let value = |offset: u64| variant.value_base + offset;
        let buffers_before = self.buffer_accounts().await?;

        let a1 = write_snapshot(self.private.ctx(), &payer, value(1), &a).await?;
        let b1 = write_snapshot(self.private.ctx(), &payer, value(2), &b).await?;
        let nonce_a = crate::last_commit_id(self.base.as_ref(), &a).await?;
        let nonce_b = crate::last_commit_id(self.base.as_ref(), &b).await?;
        let nonce_c = crate::last_commit_id(self.base.as_ref(), &c).await?;
        let nonce_d = crate::last_commit_id(self.base.as_ref(), &d).await?;
        let first_submission = self
            .proxies
            .intercept(Selector::method("sendTransaction").http().request().account(&a));
        let first = self.schedule(&payer, 1, &[a, b]).await?;
        let held_first = first_submission.wait(INTERCEPT_TIMEOUT).await?;

        let b2 = write_snapshot(self.private.ctx(), &payer, value(3), &b).await?;
        let c1 = write_snapshot(self.private.ctx(), &payer, value(4), &c).await?;
        let second_submission = self
            .proxies
            .intercept(Selector::method("sendTransaction").http().request().account(&c));
        let second = self.schedule(&payer, 2, &[b, c]).await?;
        let d1 = write_snapshot(self.private.ctx(), &payer, value(5), &d).await?;
        let third = self.schedule(&payer, 3, &[d]).await?;

        for (offset, account) in accounts.iter().enumerate() {
            write_snapshot(
                self.private.ctx(),
                &payer,
                value(11 + offset as u64),
                account,
            )
            .await?;
        }

        self.settled(&third, &[d], "independent bundle").await?;
        self.await_base(
            &d,
            &d1,
            "independent snapshot settles while first bundle held",
        )
        .await?;
        ensure!(crate::last_commit_id(self.base.as_ref(), &d).await? == (nonce_d + 1));
        hold_nonces(
            self.base.as_ref(),
            &[a, b],
            &[nonce_a, nonce_b],
            HOLD_WINDOW,
        )
        .await?;
        let buffers_during_hold = self.buffer_accounts().await?.saturating_sub(buffers_before);
        if variant.buffered {
            ensure!(buffers_during_hold > 0);
        } else {
            ensure!(buffers_during_hold == 0);
        }

        let retry_submission = self
            .proxies
            .intercept(Selector::method("sendTransaction").http().request().account(&a));
        held_first.discard();
        let held_retry = retry_submission.wait(INTERCEPT_TIMEOUT).await?;
        let retry_held_at = held_retry.held_at;
        held_retry.release();

        let first_bundle = self.settled(&first, &[a, b], "delayed bundle").await?;
        ensure!(self.base_data(&a).await? == a1);
        ensure!(self.base_data(&b).await? == b1);
        ensure!(crate::last_commit_id(self.base.as_ref(), &a).await? == (nonce_a + 1));
        ensure!(crate::last_commit_id(self.base.as_ref(), &b).await? == (nonce_b + 1));

        let held_second = second_submission.wait(INTERCEPT_TIMEOUT).await?;
        ensure!(held_second.held_at > retry_held_at);
        held_second.release();
        let second_bundle = self.settled(&second, &[b, c], "conflicting bundle").await?;
        self.await_base(&b, &b2, "conflicting bundle: B snapshot").await?;
        ensure!(self.base_data(&c).await? == c1);
        ensure!(crate::last_commit_id(self.base.as_ref(), &b).await? == (nonce_b + 2));
        ensure!(crate::last_commit_id(self.base.as_ref(), &c).await? == (nonce_c + 1));
        ensure!(first_bundle.slot <= second_bundle.slot);
        let first_position =
            self.history_position(&b, &first_bundle.receipt.base_signatures[0]).await?;
        let second_position =
            self.history_position(&b, &second_bundle.receipt.base_signatures[0]).await?;
        ensure!(first_position > second_position);

        let mut finals = Vec::with_capacity(4);
        for (offset, account) in accounts.iter().enumerate() {
            finals.push(
                write_snapshot(
                    self.private.ctx(),
                    &payer,
                    value(21 + offset as u64),
                    account,
                )
                .await?,
            );
        }
        let fourth = self.schedule(&payer, 4, &accounts).await?;
        self.settled(&fourth, &accounts, "follow-up bundle").await?;
        for (account, snapshot) in accounts.iter().zip(&finals) {
            self.await_base(account, snapshot, "follow-up bundle: final snapshot").await?;
        }
        self.base
            .api()
            .await_query("commit buffers closed after settlement", async || {
                ensure!(self.buffer_accounts().await? <= buffers_before);
                Ok(())
            })
            .await?;
        Ok(())
    }
}
