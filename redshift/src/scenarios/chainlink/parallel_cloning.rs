use std::time::Instant;

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use signer::Signer;

const WALLET_COUNT: usize = 10;
const WALLET_LAMPORTS: u64 = 2_000_000_000;

pub struct ParallelCloning;

#[async_trait(?Send)]
impl Scenario for ParallelCloning {
    fn name(&self) -> &str {
        "redshift/parallel_cloning"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let wallets: Vec<Pubkey> =
            (0..WALLET_COUNT).map(|_| Keypair::new().pubkey()).collect();
        for wallet in &wallets {
            base.airdrop(wallet, WALLET_LAMPORTS).await?;
        }

        let fan_out = Instant::now();
        let (batch_a, single_a, batch_b, batch_c, single_b) = tokio::join!(
            er.accounts(&wallets[0..3]),
            er.account(&wallets[3]),
            er.accounts(&wallets[4..6]),
            er.accounts(&wallets[6..9]),
            er.account(&wallets[9]),
        );
        let concurrent_wall_ms = fan_out.elapsed().as_secs_f64() * 1e3;

        let batch_a = batch_a?;
        let batch_b = batch_b?;
        let batch_c = batch_c?;
        assert_eq!(batch_a.len(), 3);
        assert_eq!(batch_b.len(), 2);
        assert_eq!(batch_c.len(), 3);
        for (index, entry) in batch_a
            .iter()
            .chain(batch_b.iter())
            .chain(batch_c.iter())
            .enumerate()
        {
            let clone = entry.as_ref().unwrap_or_else(|| {
                panic!("concurrent batch entry {index} came back None")
            });
            assert_eq!(
                clone.lamports, WALLET_LAMPORTS,
                "every concurrently cloned wallet must show its airdrop"
            );
        }
        for single in [single_a?, single_b?] {
            let clone =
                single.ok_or("concurrent single fetch came back None")?;
            assert_eq!(
                clone.lamports, WALLET_LAMPORTS,
                "concurrent single fetches must show the airdrop"
            );
        }

        Ok(ScenarioReport::ok(self.name())
            .setting("wallets", WALLET_COUNT)
            .metric("concurrent first-touch wall ms", concurrent_wall_ms))
    }
}
