use super::SettlementAuthorization;
use crate::PAYER_LAMPORTS;
use anyhow::{ensure, Context};
use redsuite_core::{
    api::transaction_meta,
    dlp,
    programs::{CellHeader, SettlementMode, MAGIC, REDSHIFT},
    random::{self, Rng},
    rpc_types::response::TransactionError,
    ChainCtx, Result,
};
use signer::Signer;
use solana_instruction_error::InstructionError;
use transaction::Transaction;

impl SettlementAuthorization {
    /// Rejects absent/substituted/readonly fee vaults, then checks duplicate committee keys.
    /// Neither refusal nor duplicate membership may introduce extra balance effects.
    pub(super) async fn check_fee_roles(&self) -> Result<()> {
        let mut rng = random::rng(self.base.config().seed, &["redhat", "commit_fee_roles"], 0);
        let owner = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let payer = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        self.base.delegate_payer(&owner, &payer, self.er.identity()).await?;
        let cell = self
            .base
            .cells(REDSHIFT, &owner)
            .create_delegated(CellHeader::SIZE as u32 + 8, self.er.identity(), 0)
            .await?;
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        let tracked =
            [cell, vault, payer.pubkey(), owner.pubkey(), magic_api::MAGIC_CONTEXT_PUBKEY];
        self.er.accounts(&tracked).await?;
        let base = self.base.accounts(&tracked).await?;
        let before = self.er.local_images(&tracked).await?;
        for (index, (replacement, readonly, reason)) in [
            (None, false, InstructionError::MissingAccount),
            (
                Some(owner.pubkey()),
                false,
                InstructionError::MissingAccount,
            ),
            (Some(vault), true, InstructionError::IllegalOwner),
        ]
        .into_iter()
        .enumerate()
        {
            let mut native =
                MAGIC.commit(payer.pubkey(), replacement, &[cell], SettlementMode::Commit);
            if readonly {
                native.accounts[2].is_writable = false;
            }
            let tx = Transaction::new_signed_with_payer(
                &[
                    REDSHIFT.batch().invoke(native).build(),
                    REDSHIFT.batch().nonce(index as u64).build(),
                ],
                Some(&payer.pubkey()),
                &[&payer],
                self.er.api().get_latest_blockhash().await?,
            );
            let signature = self.er.api().send_transaction(&tx).await?;
            let record = self.er.api().await_transaction(&signature).await?;
            let meta = transaction_meta(&record)?;
            ensure!(
                meta.err == Some(TransactionError::InstructionError(0, reason).into()),
                "fee vault case {index}: {:?}",
                meta.err
            );
            ensure!(meta.fee == 0 && meta.pre_balances == meta.post_balances);
            ensure!(
                self.er.local_images(&tracked).await? == before,
                "fee vault case {index} changed local state"
            );
            ensure!(
                self.base.accounts(&tracked).await? == base,
                "fee vault case {index} changed base state"
            );
        }
        let value = rng.gen_range(1..1_000_000);
        let duplicate = MAGIC.commit(
            payer.pubkey(),
            Some(vault),
            &[cell, cell],
            SettlementMode::Commit,
        );
        let signature = self
            .er
            .submit_and_confirm(
                &payer,
                &[REDSHIFT.batch().set(&[cell], value).invoke(duplicate).build()],
            )
            .await?;
        let record = self.er.api().await_transaction(&signature).await?;
        let meta = transaction_meta(&record)?;
        ensure!(meta.fee == 0 && meta.pre_balances == meta.post_balances);
        // Successful scheduling changes Magic Context, unlike the refused cases above.
        ensure!(self.er.local_images(&tracked[1..4]).await? == before[1..4]);
        ensure!(self.base.accounts(&tracked[1..]).await? == base[1..]);
        self.er
            .check_commit(&self.base, &signature, &[cell], false)
            .await?
            .check_success("duplicate committee commits one account once")?;
        let mut expected = before[0].clone().context("fee role cell missing")?;
        let mut header = CellHeader::try_decode(&expected.data)?;
        header.value = value;
        header.revision += 1;
        header.write(&mut expected.data)?;
        ensure!(self.er.local_images(&[cell]).await? == vec![Some(expected.clone())]);
        let mut committed = base[0].clone().context("base fee role cell missing")?;
        committed.data = expected.data;
        ensure!(self.base.account(&cell).await? == Some(committed));
        let next = self
            .er
            .cells(REDSHIFT, &owner)
            .settle(253, &[cell], SettlementMode::Commit)
            .await?;
        self.er
            .check_commit(&self.base, &next, &[cell], false)
            .await?
            .check_success("commit after fee vault role refusals")?;
        Ok(())
    }
}
