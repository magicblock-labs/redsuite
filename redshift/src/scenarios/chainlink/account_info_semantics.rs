use std::{rc::Rc, time::Duration};

use account::Account;
use anyhow::ensure;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{check, system, BaseCtx, ChainCtx, ErCtx, Result, Scenario};
use signer::Signer;
use tokio::time;
use tracing::instrument;

use crate::PAYER_LAMPORTS;

const FREEZE_SETTLE: Duration = Duration::from_millis(800);
const FIRST_AIRDROP: u64 = 2_000_000_000;
const SECOND_AIRDROP: u64 = 1_000_000_000;
const TRANSFER_ATTEMPT: u64 = 500_000_000;
const CHAIN_TOPUP: u64 = 1_000_000_000;
const EXECUTED_TRANSFER: u64 = 300_000_000;
const RENT_EXEMPT: u64 = 890_880;
const COLD_WALLETS: usize = 10;

pub struct AccountInfoSemantics {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

impl Scenario for AccountInfoSemantics {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        self.check_cold_reads().await?;

        self.check_absent().await?;

        let wallet = self.check_refresh().await?;

        let funder = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let receiver = self.base.delegated_payer(&funder, self.er.identity(), RENT_EXEMPT).await?;
        self.await_balance(receiver.pubkey(), RENT_EXEMPT).await?;
        self.check_readonly_payer(&wallet, &receiver).await?;

        let spender = self.check_balance_isolation(&funder).await?;

        self.check_batch_reads(&wallet, &receiver, &spender).await?;

        self.transfer(&spender, &receiver).await
    }
}

impl AccountInfoSemantics {
    #[instrument(skip_all, err)]
    async fn check_cold_reads(&self) -> Result<()> {
        // These keys must stay cold until all five requests are in flight.
        let wallets: Vec<_> = (0..COLD_WALLETS).map(|_| Keypair::new().pubkey()).collect();
        for wallet in &wallets {
            self.base.airdrop(wallet, FIRST_AIRDROP).await?;
        }
        let (a, single_a, b, c, single_b) = tokio::join!(
            self.er.accounts(&wallets[0..3]),
            self.er.account(&wallets[3]),
            self.er.accounts(&wallets[4..6]),
            self.er.accounts(&wallets[6..9]),
            self.er.account(&wallets[9]),
        );
        for (entries, keys) in [
            (a?, &wallets[0..3]),
            (vec![single_a?], &wallets[3..4]),
            (b?, &wallets[4..6]),
            (c?, &wallets[6..9]),
            (vec![single_b?], &wallets[9..10]),
        ] {
            ensure!(entries.len() == keys.len());
            for entry in &entries {
                ensure!(entry.as_ref().map(|account| account.lamports) == Some(FIRST_AIRDROP));
            }
        }
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_absent(&self) -> Result<()> {
        let ghost = Keypair::new().pubkey();
        ensure!(self.er.account(&ghost).await?.is_none());
        ensure!(self.base.account(&ghost).await?.is_none());
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
    async fn check_readonly_payer(&self, wallet: &Keypair, receiver: &Keypair) -> Result<()> {
        let accounts = [wallet.pubkey(), receiver.pubkey()];
        let before = self.er.accounts(&accounts).await?;
        let attempt = self
            .er
            .submit_and_confirm(
                wallet,
                &[system::transfer(&accounts[0], &accounts[1], TRANSFER_ATTEMPT)],
            )
            .await;
        let error = check::expect_error(attempt, "non-delegated payer cannot spend")?;
        crate::rejection_code(
            "non-delegated debit",
            &["InvalidAccountForFee", "Immutable"],
            &format!("{error:#}"),
        )?;
        ensure!(self.er.accounts(&accounts).await? == before);
        Ok(())
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
        let all_missing =
            [Keypair::new().pubkey(), Keypair::new().pubkey(), Keypair::new().pubkey()];
        let misses = self.er.accounts(&all_missing).await?;
        ensure!(misses.len() == all_missing.len());
        ensure!(misses.iter().all(Option::is_none));

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
