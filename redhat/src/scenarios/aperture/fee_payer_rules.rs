use std::time::Duration;

use async_trait::async_trait;
use redshift_program::schedulecommit::build;
use redsuite_core::{
    dlp, prep, receipt, system, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
    ScenarioReport,
};
use signer::Signer;

const RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);

pub struct FeePayerRules;

#[async_trait(?Send)]
impl Scenario for FeePayerRules {
    fn name(&self) -> &str {
        "redhat/fee_payer_rules"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;

        // A delegated (mapped-signing) fee payer may commit ITSELF directly
        // via the magic program: it is the transaction signer and is delegated
        // to this ER, so the pipeline maps the signature and schedules the
        // commit. The vault is required because the payer is delegated.
        let delegated_payer = prep::delegated_payer(
            base,
            &funder,
            er.identity(),
            crate::PAYER_LAMPORTS,
        )
        .await?;
        let vault = dlp::magic_fee_vault_pda(&er.identity());
        assert!(
            er.account(&vault).await?.is_some(),
            "the magic fee vault for the booted er identity must exist"
        );

        let signature = er
            .send(
                &delegated_payer,
                &[build::direct_schedule_commit(
                    delegated_payer.pubkey(),
                    Some(vault),
                    &[delegated_payer.pubkey()],
                )],
            )
            .await?;
        let commit_receipt = receipt::fetch_commit_receipt(
            er.api(),
            &signature,
            RECEIPT_TIMEOUT,
        )
        .await?;
        if let Some(message) = &commit_receipt.error_message {
            return Err(format!(
                "the mapped-signing self-commit failed: {message}"
            )
            .into());
        }
        assert!(
            commit_receipt.included.contains(&delegated_payer.pubkey()),
            "the mapped-signing payer must commit itself, receipt included {:?}",
            commit_receipt.included
        );
        receipt::confirm_base_signatures(
            base.api(),
            &commit_receipt,
            BASE_CONFIRM_TIMEOUT,
        )
        .await?;

        // A non-delegated payer cannot pay ER fees for a transaction that
        // touches no delegated account. This is the fee gate outside gasless
        // mode: only delegated (or otherwise privileged) accounts may pay. The
        // transfer is to the payer itself, so it lands on an account that
        // already exists (no rent path) and touches nothing delegated — the
        // fee gate is the only thing left to fail on.
        let outsider = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let fee_only = er
            .send(
                &outsider,
                &[system::transfer(&outsider.pubkey(), &outsider.pubkey(), 1)],
            )
            .await;
        assert!(
            fee_only.is_err(),
            "a non-delegated payer must not pay ER fees for a tx touching no \
             delegated account"
        );
        let fee_error = format!("{:?}", fee_only.unwrap_err());
        assert!(
            fee_error.contains("InvalidAccountForFee"),
            "expected InvalidAccountForFee for the non-delegated payer, got \
             {fee_error}"
        );

        Ok(ScenarioReport::ok(self.name())
            .setting("magic fee vault", vault)
            .setting(
                "self commit base sigs",
                commit_receipt.base_signatures.len(),
            )
            .setting("non-delegated fee refusal", "InvalidAccountForFee"))
    }
}
