pub mod account_mirroring_and_isolation;
pub mod activation_single_shot;
pub mod cache_lifecycle;
pub mod post_delegation_risk_gate;
pub mod program_materialization;
pub mod program_upgrade_consistency;
pub mod projected_token_lifecycle;
pub mod settlement_clone_controls;
pub mod spl;

use redsuite_core::{wait, BaseCtx, ChainCtx, ErCtx, Result};
use signer::Signer;

/// A fresh mirror proves replacement-socket acquisition without relying on delegated subscriptions.
async fn await_mirror_recovery(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let canary = base.funded_payer(1_000_000).await?;
    er.api()
        .await_query("replacement socket acquires a fresh mirror", async || {
            let present = er.account(&canary.pubkey()).await?.is_some();
            Ok(wait::when(present, present))
        })
        .await?;
    Ok(())
}
