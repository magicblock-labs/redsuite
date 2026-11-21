use std::rc::Rc;

use anyhow::{ensure, Context};
use redsuite_core::{
    api::transaction_meta,
    dlp,
    programs::{
        Action, CellHeader, Commit, IntentBundle, ResultCallback, SettlementMode, MAGIC, REDSHIFT,
    },
    random::{self, Rng},
    rpc_types::response::TransactionError,
    signed::audit_signatures,
    topology::{self, ReplicatedTopology, RestartConfig},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;
use solana_instruction_error::InstructionError;

use crate::PAYER_LAMPORTS;

use super::observations::{fence, CONNECT_TIMEOUT};

pub struct ReplicationSettlementRecovery {
    base: Rc<BaseCtx>,
    topology: ReplicatedTopology,
}

impl Scenario for ReplicationSettlementRecovery {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let topology = topology::replicated(&base, er).await?;
        Ok(Self { base, topology })
    }

    async fn run(self) -> Result<()> {
        let outcome = self.check_settlement_replay().await;
        outcome.and(self.topology.finish().await)
    }
}

impl ReplicationSettlementRecovery {
    /// Replicates successful settlement/callback work and a rollback, then restarts the leader.
    /// Retained records and account images must stay unchanged before a
    /// fresh commit proves progress.
    async fn check_settlement_replay(&self) -> Result<()> {
        self.topology.wait_verifiers_connected(CONNECT_TIMEOUT).await?;
        let mut rng = random::rng(
            self.base.config().seed,
            &["redhat", "replication_settlement"],
            0,
        );
        let marker = rng.gen_range(1..1_000_000u64);
        let funder = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let payer = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let er = self.topology.leader();
        let cell = self
            .base
            .cells(REDSHIFT, &payer)
            .create_primary(er.identity(), u32::MAX)
            .await?;
        self.base
            .submit_and_confirm(
                &payer,
                &[dlp::top_up_ephemeral_balance(&payer.pubkey(), 500_000_000, 1)],
            )
            .await?;
        self.base.delegate_payer(&funder, &payer, er.identity()).await?;
        er.await_delegated(&cell, "replicated settlement cell").await?;
        er.await_delegated(&payer.pubkey(), "replicated settlement payer").await?;
        let before = er
            .local_accounts(&[REDSHIFT.id()], &[cell])
            .await?
            .pop()
            .flatten()
            .context("replicated cell missing")?;
        let mut expected = before.clone();
        let mut header = CellHeader::try_decode(&expected.data)?;
        header.value = marker;
        header.revision += 2;
        header.write(&mut expected.data)?;
        let mut commit = Commit::new(SettlementMode::Commit, &[cell]);
        commit.after_commit.push(
            Action::new(
                payer.pubkey(),
                REDSHIFT.batch().nonce(marker).action(cell, dlp::dlp_id()),
                100_000,
            )
            .then(ResultCallback {
                cell,
                refund: None,
                compute_units: 20_000,
            }),
        );
        let mut bundle = IntentBundle::new(payer.pubkey());
        bundle.validator = Some(er.identity());
        bundle.commits.push(commit);
        let start = er.api().get_slot().await?;
        let scheduling = er
            .submit_and_confirm(
                &payer,
                &[REDSHIFT.batch().nonce(marker).set(&[cell], marker).intents(bundle).build()],
            )
            .await?;
        let receipt = er.check_commit(&self.base, &scheduling, &[cell], false).await?;
        receipt.check_success("replicated callback settlement")?;
        er.api()
            .await_account(&cell, |account| {
                ensure!(account == &expected);
                Ok(())
            })
            .await?;
        let failure = er
            .sender(Rc::new(payer.insecure_clone()))
            .submit(&[REDSHIFT.batch().nonce(marker + 1).add(&[cell], 99).fail(0x247).build()])
            .await?;
        let failed = er.api().await_transaction(&failure).await?;
        ensure!(
            transaction_meta(&failed)?.err
                == Some(
                    TransactionError::InstructionError(0, InstructionError::Custom(0x247)).into()
                )
        );
        ensure!(
            er.local_accounts(&[REDSHIFT.id()], &[cell]).await? == vec![Some(expected.clone())]
        );
        let settled = er
            .submit_and_confirm(
                &payer,
                &[REDSHIFT
                    .batch()
                    .nonce(marker + 2)
                    .invoke(MAGIC.commit(
                        payer.pubkey(),
                        Some(dlp::magic_fee_vault_pda(&er.identity())),
                        &[cell],
                        SettlementMode::Commit,
                    ))
                    .build()],
            )
            .await?;
        let final_receipt = er.check_commit(&self.base, &settled, &[cell], false).await?;
        final_receipt.check_success("replicated final snapshot")?;
        let base_image =
            self.base.account(&cell).await?.context("settled replication cell missing")?;
        ensure!(
            base_image.data == expected.data
                && base_image.lamports == expected.lamports
                && base_image.owner == dlp::dlp_id()
        );
        // Keep the pre-restart evidence: re-fetching alone would miss altered execution metadata.
        let signatures = [scheduling, receipt.signature, failure, settled, final_receipt.signature];
        let mut records = Vec::with_capacity(signatures.len());
        for signature in signatures {
            records.push(serde_json::to_value(
                er.api().await_transaction(&signature).await?,
            )?);
        }
        fence(&self.topology).await?;
        let restart = er.restart(RestartConfig::default()).await?;
        ensure!(restart.exit_code == Some(0) && !restart.needed_sigkill);
        self.topology.wait_verifiers_connected(CONNECT_TIMEOUT).await?;
        fence(&self.topology).await?;
        ensure!(
            er.local_accounts(&[REDSHIFT.id()], &[cell]).await? == vec![Some(expected.clone())]
        );
        ensure!(self.base.account(&cell).await? == Some(base_image));
        for (signature, record) in signatures.iter().zip(records) {
            ensure!(serde_json::to_value(er.api().await_transaction(signature).await?)? == record);
            ensure!(er.api().get_signature_status(signature).await?.is_some());
        }
        audit_signatures(
            er,
            start,
            &signatures.iter().map(|signature| (signature, 1)).collect::<Vec<_>>(),
        )
        .await?;
        let next = er
            .submit_and_confirm(
                &payer,
                &[REDSHIFT
                    .batch()
                    .nonce(marker + 3)
                    .invoke(MAGIC.commit(
                        payer.pubkey(),
                        Some(dlp::magic_fee_vault_pda(&er.identity())),
                        &[cell],
                        SettlementMode::Commit,
                    ))
                    .build()],
            )
            .await?;
        er.check_commit(&self.base, &next, &[cell], false)
            .await?
            .check_success("owner settlement after replicated recovery")?;
        ensure!(self
            .base
            .account(&cell)
            .await?
            .is_some_and(|account| account.data == expected.data));
        Ok(())
    }
}
