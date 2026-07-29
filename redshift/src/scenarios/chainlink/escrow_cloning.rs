use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use redsuite_core::{
    assert::poll_until, dlp, prep, system, BaseCtx, ChainCtx, ErCtx, Result,
    Scenario, ScenarioReport,
};
use signer::Signer;

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const FREEZE_SETTLE: Duration = Duration::from_millis(800);
const ESCROW_FUNDING: u64 = 2_000_000_000;
const TRANSFER_ATTEMPT: u64 = 500_000_000;
const CHAIN_TOPUP: u64 = 1_000_000_000;
const EXECUTED_TRANSFER: u64 = 300_000_000;
const RENT_EXEMPT: u64 = 890_880;

pub struct EscrowCloning;

#[async_trait(?Send)]
impl Scenario for EscrowCloning {
    fn name(&self) -> &str {
        "redshift/escrow_cloning"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let escrowed =
            prep::escrowed_payer(base, er.identity(), ESCROW_FUNDING).await?;

        let first_access = Instant::now();
        poll_until(CLONE_TIMEOUT, || async {
            matches!(er.account(&escrowed.escrow).await, Ok(Some(_)))
        })
        .await;
        let clone_visibility_ms = first_access.elapsed().as_secs_f64() * 1e3;
        let initial = er
            .account(&escrowed.escrow)
            .await?
            .ok_or("escrow clone vanished")?;
        assert_eq!(
            initial.lamports, escrowed.escrow_lamports,
            "the cloned escrow must hold the top-up plus the rent-exempt minimum"
        );

        let drain_target = Keypair::new().pubkey();
        let transfer_result = er
            .send(
                &escrowed.payer,
                &[system::transfer(
                    &escrowed.payer.pubkey(),
                    &drain_target,
                    TRANSFER_ATTEMPT,
                )],
            )
            .await;
        assert!(
            transfer_result.is_err(),
            "the access gate must reject a transfer that moves lamports out \
             of a non-delegated payer wallet (InvalidAccountForFee even at \
             base fee 0 — the escrow is never consulted as a fee source); \
             got {transfer_result:?}"
        );

        let after_tx = er
            .account(&escrowed.escrow)
            .await?
            .ok_or("escrow clone vanished after the ER transfer attempt")?;
        assert_eq!(
            after_tx.lamports, initial.lamports,
            "paying or attempting an ER tx must not move escrow lamports"
        );
        assert_eq!(
            after_tx.data, initial.data,
            "the escrow clone's data must stay untouched"
        );
        assert_eq!(
            after_tx.owner, initial.owner,
            "the escrow clone's presented owner must stay untouched"
        );

        base.airdrop(&escrowed.escrow, CHAIN_TOPUP).await?;
        let immediately_after = er
            .account(&escrowed.escrow)
            .await?
            .ok_or("escrow clone vanished after the chain top-up")?;
        assert_eq!(
            immediately_after.lamports, after_tx.lamports,
            "a chain-side write to a delegated escrow must not appear on the ER"
        );
        tokio::time::sleep(FREEZE_SETTLE).await;
        let after_settle = er
            .account(&escrowed.escrow)
            .await?
            .ok_or("escrow clone vanished after the settle wait")?;
        assert_eq!(
            after_settle.lamports, after_tx.lamports,
            "the delegated escrow must stay frozen even after the update had time to propagate"
        );
        let escrow_on_base = base
            .account(&escrowed.escrow)
            .await?
            .ok_or("escrow gone on base")?;
        assert_eq!(
            escrow_on_base.lamports,
            escrowed.escrow_lamports + CHAIN_TOPUP,
            "the chain top-up must exist on base for the freeze assert to mean anything"
        );

        // executed-transfer cell — a wallet-delegated escrowed payer CAN
        // spend on the ER, and the executed transfer still leaves the escrow
        // untouched (the freeze assert with a transaction that actually ran).
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let spender =
            prep::escrowed_payer(base, er.identity(), ESCROW_FUNDING).await?;
        let delegate_spender = [
            system::assign(&spender.payer.pubkey(), &dlp::dlp_id()),
            dlp::delegate_account(
                &funder.pubkey(),
                &spender.payer.pubkey(),
                &er.identity(),
            ),
        ];
        base.send_with(&funder, &[&spender.payer], &delegate_spender)
            .await?;

        let receiver = Keypair::new();
        base.airdrop(&receiver.pubkey(), RENT_EXEMPT).await?;
        let delegate_receiver = [
            system::assign(&receiver.pubkey(), &dlp::dlp_id()),
            dlp::delegate_account(
                &funder.pubkey(),
                &receiver.pubkey(),
                &er.identity(),
            ),
        ];
        base.send_with(&funder, &[&receiver], &delegate_receiver)
            .await?;
        poll_until(CLONE_TIMEOUT, || async {
            matches!(
                er.api().get_balance(&receiver.pubkey()).await,
                Ok(balance) if balance == RENT_EXEMPT
            )
        })
        .await;

        poll_until(CLONE_TIMEOUT, || async {
            matches!(er.account(&spender.escrow).await, Ok(Some(_)))
        })
        .await;
        let spender_escrow_before = er
            .account(&spender.escrow)
            .await?
            .ok_or("the spender escrow clone vanished")?;
        let wallet_before =
            er.api().get_balance(&spender.payer.pubkey()).await?;

        er.send(
            &spender.payer,
            &[system::transfer(
                &spender.payer.pubkey(),
                &receiver.pubkey(),
                EXECUTED_TRANSFER,
            )],
        )
        .await?;
        poll_until(CLONE_TIMEOUT, || async {
            matches!(
                er.api().get_balance(&receiver.pubkey()).await,
                Ok(balance) if balance == RENT_EXEMPT + EXECUTED_TRANSFER
            )
        })
        .await;
        let wallet_after =
            er.api().get_balance(&spender.payer.pubkey()).await?;
        assert_eq!(
            wallet_after,
            wallet_before - EXECUTED_TRANSFER,
            "the delegated wallet must pay exactly the transfer amount at \
             base fee 0"
        );
        let spender_escrow_after = er
            .account(&spender.escrow)
            .await?
            .ok_or("the spender escrow clone vanished after the transfer")?;
        assert_eq!(
            spender_escrow_after.lamports, spender_escrow_before.lamports,
            "an EXECUTED er transfer must not move escrow lamports"
        );
        assert_eq!(
            spender_escrow_after.data, spender_escrow_before.data,
            "an EXECUTED er transfer must not touch escrow data"
        );

        Ok(ScenarioReport::ok(self.name())
            .setting("escrow funding lamports", ESCROW_FUNDING)
            .setting("executed transfer lamports", EXECUTED_TRANSFER)
            .metric("escrow clone visibility ms", clone_visibility_ms))
    }
}
