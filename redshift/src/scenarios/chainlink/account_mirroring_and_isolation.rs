use std::{rc::Rc, time::Duration};

use account::Account;
use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{system, BaseCtx, ChainCtx, ErCtx, Result, Scenario};
use signer::Signer;
use tokio::time;
use tracing::instrument;

use crate::{ACCOUNT_SPACE, PAYER_LAMPORTS};
use redsuite_core::programs::{CellHeader, REDLINE};
const PRE_CLONE_WRITE: u64 = 11;
const POST_CLONE_WRITE: u64 = 12;
const UNDELEGATED_WRITE: u64 = 13;

const FREEZE_SETTLE: Duration = Duration::from_millis(800);
const FIRST_AIRDROP: u64 = 2_000_000_000;
const SECOND_AIRDROP: u64 = 1_000_000_000;
const CHAIN_TOPUP: u64 = 1_000_000_000;
const EXECUTED_TRANSFER: u64 = 300_000_000;
const RENT_EXEMPT: u64 = 890_880;

/// Checks cold reads, negative-cache invalidation, mirror refresh, and delegated isolation.
pub struct AccountMirroringAndIsolation {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

impl Scenario for AccountMirroringAndIsolation {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        self.check_cold_reads().await?;
        self.check_plain_clone().await?;

        self.check_absent().await?;

        let wallet = self.check_refresh().await?;

        let funder = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let receiver = self.base.delegated_payer(&funder, self.er.identity(), RENT_EXEMPT).await?;
        self.await_balance(receiver.pubkey(), RENT_EXEMPT).await?;

        let spender = self.check_balance_isolation(&funder).await?;

        self.check_batch_reads(&wallet, &receiver, &spender).await?;

        self.transfer(&spender, &receiver).await
    }
}

impl AccountMirroringAndIsolation {
    #[instrument(skip_all, err)]
    async fn check_cold_reads(&self) -> Result<()> {
        let keys: Vec<_> = (0..3).map(|_| Keypair::new().pubkey()).collect();
        for key in &keys {
            self.base.airdrop(key, FIRST_AIRDROP).await?;
        }
        let (single, batch) = tokio::join!(self.er.account(&keys[0]), self.er.accounts(&keys[1..]));
        ensure!(single?.is_some_and(|account| account.lamports == FIRST_AIRDROP));
        let batch = batch?;
        ensure!(batch.len() == 2);
        ensure!(batch
            .iter()
            .all(|account| account.as_ref().is_some_and(|a| a.lamports == FIRST_AIRDROP)));
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_absent(&self) -> Result<()> {
        let key = Keypair::new().pubkey();
        ensure!(self.er.account(&key).await?.is_none());
        self.base.airdrop(&key, FIRST_AIRDROP).await?;
        self.await_balance(key, FIRST_AIRDROP).await?;
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_refresh(&self) -> Result<Keypair> {
        let wallet = self.base.funded_payer(FIRST_AIRDROP).await?;
        self.await_balance(wallet.pubkey(), FIRST_AIRDROP).await?;
        self.base.airdrop(&wallet.pubkey(), SECOND_AIRDROP).await?;
        self.await_balance(wallet.pubkey(), FIRST_AIRDROP + SECOND_AIRDROP).await?;
        Ok(wallet)
    }

    #[instrument(skip_all, err)]
    async fn check_balance_isolation(&self, funder: &Keypair) -> Result<Keypair> {
        let spender = self.base.delegated_payer(funder, self.er.identity(), PAYER_LAMPORTS).await?;
        let initial = self.await_balance(spender.pubkey(), PAYER_LAMPORTS).await?;
        self.base.airdrop(&spender.pubkey(), CHAIN_TOPUP).await?;
        ensure!(self.er.account(&spender.pubkey()).await?.as_ref() == Some(&initial));
        time::sleep(FREEZE_SETTLE).await;
        ensure!(self.er.account(&spender.pubkey()).await?.as_ref() == Some(&initial));
        ensure!(
            self.base.api().get_balance(&spender.pubkey()).await? == (PAYER_LAMPORTS + CHAIN_TOPUP)
        );
        Ok(spender)
    }

    #[instrument(skip_all, err)]
    async fn check_batch_reads(
        &self,
        wallet: &Keypair,
        receiver: &Keypair,
        spender: &Keypair,
    ) -> Result<()> {
        let mixed = [wallet.pubkey(), Keypair::new().pubkey(), receiver.pubkey(), spender.pubkey()];
        let batch = self.er.accounts(&mixed).await?;
        let balances: Vec<_> =
            batch.iter().map(|entry| entry.as_ref().map(|a| a.lamports)).collect();
        ensure!(
            balances
                == [
                    Some(FIRST_AIRDROP + SECOND_AIRDROP),
                    None,
                    Some(RENT_EXEMPT),
                    Some(PAYER_LAMPORTS),
                ]
        );
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn transfer(&self, spender: &Keypair, receiver: &Keypair) -> Result<()> {
        self.er
            .submit_and_confirm(
                spender,
                &[system::transfer(&spender.pubkey(), &receiver.pubkey(), EXECUTED_TRANSFER)],
            )
            .await?;
        self.await_balance(receiver.pubkey(), RENT_EXEMPT + EXECUTED_TRANSFER).await?;
        ensure!(
            self.er.api().get_balance(&spender.pubkey()).await?
                == (PAYER_LAMPORTS - EXECUTED_TRANSFER)
        );
        ensure!(
            self.base.api().get_balance(&spender.pubkey()).await? == (PAYER_LAMPORTS + CHAIN_TOPUP)
        );
        Ok(())
    }

    async fn await_balance(&self, address: Pubkey, expected: u64) -> Result<Account> {
        self.er
            .await_account(&address, |account| {
                ensure!(account.lamports == expected);
                Ok(())
            })
            .await
    }
}

impl AccountMirroringAndIsolation {
    #[instrument(skip_all, err)]
    async fn check_plain_clone(&self) -> Result<()> {
        let payer = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let plain = self
            .base
            .cells(REDLINE, &payer)
            .create(ACCOUNT_SPACE, self.er.identity(), 0)
            .await?;
        self.base.cells(REDLINE, &payer).write(PRE_CLONE_WRITE, &[plain]).await?;
        let plain_clone = self
            .er
            .api()
            .await_account(&plain, |account| {
                ensure!(account.data.len() == ACCOUNT_SPACE as usize);
                Ok(())
            })
            .await
            .context("plain account cloned")?;
        let plain_on_base = self.base.account(&plain).await?.context("plain gone on base")?;
        ensure!(plain_clone.lamports == plain_on_base.lamports);
        ensure!(plain_clone.owner == crate::program::id());
        ensure!(CellHeader::written_id(&plain_clone.data) == Some(PRE_CLONE_WRITE));
        self.base.cells(REDLINE, &payer).write(POST_CLONE_WRITE, &[plain]).await?;
        self.er
            .api()
            .await_accounts(&[plain], "base write propagated", |accounts| {
                let account = accounts[0].as_ref().context("plain clone missing")?;
                ensure!(CellHeader::written_id(&account.data) == Some(POST_CLONE_WRITE));
                Ok(())
            })
            .await?;
        let undelegated_write =
            self.er.cells(REDLINE, &payer).write(UNDELEGATED_WRITE, &[plain]).await;
        ensure!(undelegated_write.is_err());
        Ok(())
    }
}
