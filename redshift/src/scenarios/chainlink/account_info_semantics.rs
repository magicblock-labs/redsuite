use std::time::Duration;

use async_trait::async_trait;
use keypair::Keypair;
use redsuite_core::{
    check, check_eq, prep, system, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const REFRESH_TIMEOUT: Duration = Duration::from_secs(15);
const FREEZE_SETTLE: Duration = Duration::from_millis(800);
const FIRST_AIRDROP: u64 = 2_000_000_000;
const SECOND_AIRDROP: u64 = 1_000_000_000;
const ESCROW_FUNDING: u64 = 4_000_000_000;
const TRANSFER_ATTEMPT: u64 = 500_000_000;
const CHAIN_TOPUP: u64 = 1_000_000_000;
const EXECUTED_TRANSFER: u64 = 300_000_000;
const RENT_EXEMPT: u64 = 890_880;
const COLD_WALLETS: usize = 10;

pub struct AccountInfoSemantics;

#[async_trait(?Send)]
impl Scenario for AccountInfoSemantics {
    fn name(&self) -> &str {
        "redshift/account_info_semantics"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<()> {
        crate::phase("cold reads", async {
            // These keys must stay cold until all five requests are in flight.
            let wallets: Vec<_> =
                (0..COLD_WALLETS).map(|_| Keypair::new().pubkey()).collect();
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
            for (case, entries, keys) in [
                ("batch a", a?, &wallets[0..3]),
                ("single a", vec![single_a?], &wallets[3..4]),
                ("batch b", b?, &wallets[4..6]),
                ("batch c", c?, &wallets[6..9]),
                ("single b", vec![single_b?], &wallets[9..10]),
            ] {
                check_eq!(
                    entries.len(),
                    keys.len(),
                    "cold {case}: response length"
                )?;
                for (entry, key) in entries.iter().zip(keys) {
                    check_eq!(
                        entry.as_ref().map(|account| account.lamports),
                        Some(FIRST_AIRDROP),
                        "cold {case}: wallet {key} carries its airdrop"
                    )?;
                }
            }
            Ok(())
        })
        .await?;

        crate::phase("absent", async {
            let ghost = Keypair::new().pubkey();
            check!(
                er.account(&ghost).await?.is_none(),
                "a never-existing account must read as absent on the ER"
            )?;
            check!(
                base.account(&ghost).await?.is_none(),
                "the negative ER read must not create the account on base"
            )?;
            Ok(())
        })
        .await?;

        let wallet = crate::phase("refresh", async {
            let wallet = Keypair::new().pubkey();
            base.airdrop(&wallet, FIRST_AIRDROP).await?;
            check::poll(
                "the ER clones the airdropped wallet on first read",
                CLONE_TIMEOUT,
                || async {
                    matches!(er.account(&wallet).await, Ok(Some(clone)) if clone.lamports == FIRST_AIRDROP)
                },
            )
            .await?;
            base.airdrop(&wallet, SECOND_AIRDROP).await?;
            check::poll(
                "the non-delegated wallet clone refreshes to the new balance",
                REFRESH_TIMEOUT,
                || async {
                    matches!(er.account(&wallet).await, Ok(Some(clone)) if clone.lamports == FIRST_AIRDROP + SECOND_AIRDROP)
                },
            )
            .await?;
            Ok(wallet)
        })
        .await?;

        let escrowed = crate::phase("escrow", async {
            let escrowed =
                prep::escrowed_payer(base, er.identity(), ESCROW_FUNDING)
                    .await?;
            let escrowed_pubkey = escrowed.payer.pubkey();
            check::poll(
                "the ER clones the escrowed payer wallet",
                CLONE_TIMEOUT,
                || async {
                    matches!(er.account(&escrowed_pubkey).await, Ok(Some(_)))
                },
            )
            .await?;
            let initial = er
                .account(&escrowed.escrow)
                .await?
                .ok_or("escrow pda unreadable on the ER")?;
            check_eq!(
                initial.lamports,
                escrowed.escrow_lamports,
                "the cloned escrow must hold the top-up plus the rent-exempt minimum"
            )?;

            let transfer_result = er
                .submit_and_confirm(
                    &escrowed.payer,
                    &[system::transfer(
                        &escrowed_pubkey,
                        &Keypair::new().pubkey(),
                        TRANSFER_ATTEMPT,
                    )],
                )
                .await;
            check!(
                transfer_result.is_err(),
                "the access gate must reject a transfer that moves lamports out \
                 of a non-delegated payer wallet (InvalidAccountForFee even at \
                 base fee 0 — the escrow is never consulted as a fee source); \
                 got {transfer_result:?}"
            )?;
            let after_tx = er
                .account(&escrowed.escrow)
                .await?
                .ok_or("escrow clone vanished after the ER transfer attempt")?;
            check_eq!(
                after_tx.lamports,
                initial.lamports,
                "paying or attempting an ER tx must not move escrow lamports"
            )?;
            check_eq!(
                after_tx.data,
                initial.data,
                "the escrow clone's data must stay untouched"
            )?;
            check_eq!(
                after_tx.owner,
                initial.owner,
                "the escrow clone's presented owner must stay untouched"
            )?;

            base.airdrop(&escrowed.escrow, CHAIN_TOPUP).await?;
            let immediately_after = er
                .account(&escrowed.escrow)
                .await?
                .ok_or("escrow clone vanished after the chain top-up")?;
            check_eq!(
                immediately_after.lamports,
                initial.lamports,
                "a chain-side write to a delegated escrow must not appear on the ER"
            )?;
            tokio::time::sleep(FREEZE_SETTLE).await;
            let after_settle = er
                .account(&escrowed.escrow)
                .await?
                .ok_or("escrow clone vanished after the settle wait")?;
            check_eq!(
                after_settle.lamports,
                initial.lamports,
                "the delegated escrow must stay frozen even after the update had time to propagate"
            )?;
            let escrow_on_base = base
                .account(&escrowed.escrow)
                .await?
                .ok_or("escrow gone on base")?;
            check_eq!(
                escrow_on_base.lamports,
                escrowed.escrow_lamports + CHAIN_TOPUP,
                "the chain top-up must exist on base for the freeze assert to mean anything"
            )?;
            Ok(escrowed)
        })
        .await?;

        crate::phase("batch reads", async {
            let all_missing = [
                Keypair::new().pubkey(),
                Keypair::new().pubkey(),
                Keypair::new().pubkey(),
            ];
            let misses = er.accounts(&all_missing).await?;
            check_eq!(
                misses.len(),
                all_missing.len(),
                "the batch read must return one entry per requested account"
            )?;
            check!(
                misses.iter().all(Option::is_none),
                "a batch of unknown accounts must come back all-None without error"
            )?;

            let mixed = [
                wallet,
                Keypair::new().pubkey(),
                escrowed.payer.pubkey(),
                escrowed.escrow,
            ];
            let batch = er.accounts(&mixed).await?;
            check_eq!(
                batch.len(),
                mixed.len(),
                "the mixed batch read must return one entry per requested account"
            )?;
            let wallet_entry =
                batch[0].as_ref().ok_or("wallet missing from batch read")?;
            check_eq!(
                wallet_entry.lamports,
                FIRST_AIRDROP + SECOND_AIRDROP,
                "the batch read must see the wallet's refreshed balance"
            )?;
            check!(
                batch[1].is_none(),
                "the unknown entry of a mixed batch must stay None"
            )?;
            check!(
                batch[2].is_some(),
                "the escrowed payer wallet must be readable in a batch"
            )?;
            let escrow_entry = batch[3]
                .as_ref()
                .ok_or("escrow pda missing from batch read")?;
            check_eq!(
                escrow_entry.lamports,
                escrowed.escrow_lamports,
                "the batch read must see the exact escrow balance"
            )?;
            Ok(())
        })
        .await?;

        crate::phase("escrowed spender", async {
            let funder =
                prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
            let spender =
                prep::escrowed_payer(base, er.identity(), ESCROW_FUNDING)
                    .await?;
            prep::delegate_payer(base, &funder, &spender.payer, er.identity())
                .await?;
            let receiver = prep::delegated_payer(
                base,
                &funder,
                er.identity(),
                RENT_EXEMPT,
            )
            .await?;
            check::poll(
                "the ER clones the delegated receiver at its rent-exempt balance",
                CLONE_TIMEOUT,
                || async {
                    matches!(
                        er.api().get_balance(&receiver.pubkey()).await,
                        Ok(balance) if balance == RENT_EXEMPT
                    )
                },
            )
            .await?;
            check::poll(
                "the ER clones the spender escrow pda",
                CLONE_TIMEOUT,
                || async {
                    matches!(er.account(&spender.escrow).await, Ok(Some(_)))
                },
            )
            .await?;
            let escrow_before = er
                .account(&spender.escrow)
                .await?
                .ok_or("the spender escrow clone vanished")?;
            let wallet_before =
                er.api().get_balance(&spender.payer.pubkey()).await?;

            er.submit_and_confirm(
                &spender.payer,
                &[system::transfer(
                    &spender.payer.pubkey(),
                    &receiver.pubkey(),
                    EXECUTED_TRANSFER,
                )],
            )
            .await?;
            check::poll(
                "the executed ER transfer lands on the receiver",
                CLONE_TIMEOUT,
                || async {
                    matches!(
                        er.api().get_balance(&receiver.pubkey()).await,
                        Ok(balance) if balance == RENT_EXEMPT + EXECUTED_TRANSFER
                    )
                },
            )
            .await?;
            check_eq!(
                er.api().get_balance(&spender.payer.pubkey()).await?,
                wallet_before - EXECUTED_TRANSFER,
                "the delegated wallet must pay exactly the transfer amount at \
                 base fee 0"
            )?;
            let escrow_after = er
                .account(&spender.escrow)
                .await?
                .ok_or("the spender escrow clone vanished after the transfer")?;
            check_eq!(
                escrow_after.lamports,
                escrow_before.lamports,
                "an EXECUTED er transfer must not move escrow lamports"
            )?;
            check_eq!(
                escrow_after.data,
                escrow_before.data,
                "an EXECUTED er transfer must not touch escrow data"
            )?;
            Ok(())
        })
        .await
    }
}
