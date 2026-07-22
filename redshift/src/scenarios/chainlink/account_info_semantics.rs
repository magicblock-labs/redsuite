use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use redsuite_core::{
    assert::poll_until, prep, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
    ScenarioReport,
};
use signer::Signer;

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const REFRESH_TIMEOUT: Duration = Duration::from_secs(15);
const FIRST_AIRDROP: u64 = 2_000_000_000;
const SECOND_AIRDROP: u64 = 1_000_000_000;
const ESCROW_FUNDING: u64 = 4_000_000_000;

pub struct AccountInfoSemantics;

#[async_trait(?Send)]
impl Scenario for AccountInfoSemantics {
    fn name(&self) -> &str {
        "redshift/account_info_semantics"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let ghost = Keypair::new().pubkey();
        assert!(
            er.account(&ghost).await?.is_none(),
            "a never-existing account must read as absent on the ER"
        );
        assert!(
            base.account(&ghost).await?.is_none(),
            "the negative ER read must not create the account on base"
        );

        let wallet = Keypair::new().pubkey();
        base.airdrop(&wallet, FIRST_AIRDROP).await?;
        let first_read = Instant::now();
        poll_until(CLONE_TIMEOUT, || async {
            matches!(er.account(&wallet).await, Ok(Some(clone)) if clone.lamports == FIRST_AIRDROP)
        })
        .await;
        let first_read_ms = first_read.elapsed().as_secs_f64() * 1e3;
        base.airdrop(&wallet, SECOND_AIRDROP).await?;
        let refresh_started = Instant::now();
        poll_until(REFRESH_TIMEOUT, || async {
            matches!(er.account(&wallet).await, Ok(Some(clone)) if clone.lamports == FIRST_AIRDROP + SECOND_AIRDROP)
        })
        .await;
        let refresh_ms = refresh_started.elapsed().as_secs_f64() * 1e3;

        let escrowed =
            prep::escrowed_payer(base, er.identity(), ESCROW_FUNDING).await?;
        let escrowed_pubkey = escrowed.payer.pubkey();
        poll_until(CLONE_TIMEOUT, || async {
            matches!(er.account(&escrowed_pubkey).await, Ok(Some(_)))
        })
        .await;
        let escrow_clone = er
            .account(&escrowed.escrow)
            .await?
            .ok_or("escrow pda unreadable on the ER")?;
        assert_eq!(
            escrow_clone.lamports, escrowed.escrow_lamports,
            "the cloned escrow must hold the top-up plus the rent-exempt minimum"
        );

        let all_missing = [
            Keypair::new().pubkey(),
            Keypair::new().pubkey(),
            Keypair::new().pubkey(),
        ];
        let misses = er.accounts(&all_missing).await?;
        assert_eq!(misses.len(), all_missing.len());
        assert!(
            misses.iter().all(Option::is_none),
            "a batch of unknown accounts must come back all-None without error"
        );

        let mixed = [
            wallet,
            Keypair::new().pubkey(),
            escrowed_pubkey,
            escrowed.escrow,
        ];
        let batch = er.accounts(&mixed).await?;
        assert_eq!(batch.len(), mixed.len());
        let wallet_entry =
            batch[0].as_ref().ok_or("wallet missing from batch read")?;
        assert_eq!(
            wallet_entry.lamports,
            FIRST_AIRDROP + SECOND_AIRDROP,
            "the batch read must see the wallet's refreshed balance"
        );
        assert!(
            batch[1].is_none(),
            "the unknown entry of a mixed batch must stay None"
        );
        assert!(
            batch[2].is_some(),
            "the escrowed payer wallet must be readable in a batch"
        );
        let escrow_entry = batch[3]
            .as_ref()
            .ok_or("escrow pda missing from batch read")?;
        assert_eq!(
            escrow_entry.lamports, escrowed.escrow_lamports,
            "the batch read must see the exact escrow balance"
        );

        Ok(ScenarioReport::ok(self.name())
            .setting("escrow funding lamports", ESCROW_FUNDING)
            .metric("first clone read ms", first_read_ms)
            .metric("non-delegated refresh ms", refresh_ms))
    }
}
