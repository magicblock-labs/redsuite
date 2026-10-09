use async_trait::async_trait;
use redsuite_core::{
    check, dlp, topology, BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use signer::Signer;

const TEST_FEE_LAMPORTS: u64 = 1_000_000;

pub struct ClaimFees;

#[async_trait(?Send)]
impl PrivateErScenario for ClaimFees {
    fn name(&self) -> &str {
        "redshift/claim_fees"
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let validator = topology::er_identity_keypair()?;
        base.airdrop(&validator.pubkey(), crate::PAYER_LAMPORTS)
            .await?;
        let vault = dlp::validator_fees_vault_pda(&validator.pubkey());
        let vault_at_boot = base.api().get_balance(&vault).await?;
        check!(
            vault_at_boot > 0,
            "validator-fees-vault absent on base — it should have been \
             injected at genesis"
        )?;

        base.airdrop(&vault, TEST_FEE_LAMPORTS).await?;
        let vault_before = base.api().get_balance(&vault).await?;
        check!(
            vault_before >= TEST_FEE_LAMPORTS,
            "vault holds {vault_before}, expected at least the test fee \
             amount {TEST_FEE_LAMPORTS}"
        )?;

        let claimer_before =
            base.api().get_balance(&validator.pubkey()).await?;
        base.submit_and_confirm(
            &validator,
            &[dlp::validator_claim_fees(&validator.pubkey(), None)],
        )
        .await?;
        let vault_after = base.api().get_balance(&vault).await?;
        let claimer_after = base.api().get_balance(&validator.pubkey()).await?;

        let claimed = vault_before.saturating_sub(vault_after);
        check!(claimed > 0, "should have claimed some fees")?;
        check!(
            vault_after > 0,
            "claim drained the vault below rent exemption"
        )?;
        check!(
            claimer_after > claimer_before,
            "claimed fees never reached the validator: {claimer_before} -> \
             {claimer_after}"
        )?;

        Ok(())
    }
}
