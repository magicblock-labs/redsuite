//! Account prep: airdrop / `InitAccount` / `Delegate` — always on the base;
//! the ER discovers delegation by clone-on-access.

use keypair::Keypair;
use signer::Signer;

use crate::{context::ChainCtx, Result};

pub async fn funded_payer(
    ctx: &impl ChainCtx,
    lamports: u64,
) -> Result<Keypair> {
    let payer = Keypair::new();
    ctx.airdrop(&payer.pubkey(), lamports).await?;
    Ok(payer)
}

pub async fn funded_payers(
    ctx: &impl ChainCtx,
    count: usize,
    lamports: u64,
) -> Result<Vec<Keypair>> {
    let mut payers = Vec::with_capacity(count);
    for _ in 0..count {
        payers.push(funded_payer(ctx, lamports).await?);
    }
    Ok(payers)
}
