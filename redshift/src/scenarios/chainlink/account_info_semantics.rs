use std::{rc::Rc, time::Duration};

use account::Account;
use anyhow::ensure;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{check, system, BaseCtx, ChainCtx, ErCtx, Result, Scenario};
use signer::Signer;
use tokio::time;

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

async fn await_balance(er: &ErCtx, address: Pubkey, expected: u64) -> Result<Account> {
    er.await_account(&address, |account| {
        ensure!(account.lamports == expected);
        Ok(())
    })
    .await
}

impl Scenario for AccountInfoSemantics {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er } = self;
        crate::phase("cold reads", async {
            // These keys must stay cold until all five requests are in flight.
            let wallets: Vec<_> = (0..COLD_WALLETS).map(|_| Keypair::new().pubkey()).collect();
            for wallet in &wallets {
                base.airdrop(wallet, FIRST_AIRDROP).await?;
            }
            let (a, single_a, b, c, single_b) = tokio::join!(
                er.accounts(&wallets[0..3]),
                er.account(&wallets[3]),
                er.accounts(&wallets[4..6]),
                er.accounts(&wallets[6..9]),
                er.account(&wallets[9]),
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
        })
        .await?;

        crate::phase("absent", async {
            let ghost = Keypair::new().pubkey();
            ensure!(er.account(&ghost).await?.is_none());
            ensure!(base.account(&ghost).await?.is_none());
            Ok(())
        })
        .await?;

        let wallet = crate::phase("refresh", async {
            let wallet = base.funded_payer(FIRST_AIRDROP).await?;
            await_balance(&er, wallet.pubkey(), FIRST_AIRDROP).await?;
            base.airdrop(&wallet.pubkey(), SECOND_AIRDROP).await?;
            await_balance(&er, wallet.pubkey(), FIRST_AIRDROP + SECOND_AIRDROP).await?;
            Ok(wallet)
        })
        .await?;

        let funder = base.funded_payer(PAYER_LAMPORTS).await?;
        let receiver = base.delegated_payer(&funder, er.identity(), RENT_EXEMPT).await?;
        await_balance(&er, receiver.pubkey(), RENT_EXEMPT).await?;
        crate::phase("readonly payer", async {
            let accounts = [wallet.pubkey(), receiver.pubkey()];
            let before = er.accounts(&accounts).await?;
            let attempt = er
                .submit_and_confirm(
                    &wallet,
                    &[system::transfer(&accounts[0], &accounts[1], TRANSFER_ATTEMPT)],
                )
                .await;
            let error = check::expect_error(attempt, "non-delegated payer cannot spend")?;
            crate::rejection_code(
                "non-delegated debit",
                &["InvalidAccountForFee", "Immutable"],
                &format!("{error:#}"),
            )?;
            ensure!(er.accounts(&accounts).await? == before);
            Ok(())
        })
        .await?;

        let spender = crate::phase("delegated balance isolation", async {
            let spender = base.delegated_payer(&funder, er.identity(), PAYER_LAMPORTS).await?;
            let initial = await_balance(&er, spender.pubkey(), PAYER_LAMPORTS).await?;
            base.airdrop(&spender.pubkey(), CHAIN_TOPUP).await?;
            ensure!(er.account(&spender.pubkey()).await?.as_ref() == Some(&initial));
            time::sleep(FREEZE_SETTLE).await;
            ensure!(er.account(&spender.pubkey()).await?.as_ref() == Some(&initial));
            ensure!(
                base.api().get_balance(&spender.pubkey()).await? == (PAYER_LAMPORTS + CHAIN_TOPUP)
            );
            Ok(spender)
        })
        .await?;

        crate::phase("batch reads", async {
            let all_missing =
                [Keypair::new().pubkey(), Keypair::new().pubkey(), Keypair::new().pubkey()];
            let misses = er.accounts(&all_missing).await?;
            ensure!(misses.len() == all_missing.len());
            ensure!(misses.iter().all(Option::is_none));

            let mixed =
                [wallet.pubkey(), Keypair::new().pubkey(), receiver.pubkey(), spender.pubkey()];
            let batch = er.accounts(&mixed).await?;
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
        })
        .await?;

        crate::phase("delegated transfer", async {
            er.submit_and_confirm(
                &spender,
                &[system::transfer(&spender.pubkey(), &receiver.pubkey(), EXECUTED_TRANSFER)],
            )
            .await?;
            await_balance(&er, receiver.pubkey(), RENT_EXEMPT + EXECUTED_TRANSFER).await?;
            ensure!(
                er.api().get_balance(&spender.pubkey()).await?
                    == (PAYER_LAMPORTS - EXECUTED_TRANSFER)
            );
            ensure!(
                base.api().get_balance(&spender.pubkey()).await? == (PAYER_LAMPORTS + CHAIN_TOPUP)
            );
            Ok(())
        })
        .await
    }
}
