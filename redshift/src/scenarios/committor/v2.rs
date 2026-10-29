use std::{rc::Rc, slice};

use anyhow::{ensure, Context};
use delegation_program_interface::{self as dpi, DelegationState};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    programs::{CellHeader, CellId, SettlementMode, REDSHIFT},
    random,
    receipt::CommitReceipt,
    BaseCtx, ChainCtx, ErCtx, Result,
};
use signer::Signer;
use wheels::layout::Decodable;

pub(super) struct Fixture {
    pub base: Rc<BaseCtx>,
    pub er: ErCtx,
    pub payer: Keypair,
    pub cluster: Pubkey,
}

impl Fixture {
    pub async fn new(base: Rc<BaseCtx>, er: ErCtx, scenario: &str) -> Result<Self> {
        let mut rng = random::rng(base.config().seed, &["redshift", scenario], 0);
        let payer = base.seeded_payer(&mut rng, crate::PAYER_LAMPORTS).await?;
        let cluster = random::keypair(&mut rng);
        base.submit_and_confirm_with(
            &payer,
            &[&cluster],
            &[dpi::instruction_builder::init_delegation_cluster(
                payer.pubkey(),
                cluster.pubkey(),
                dpi::InitDelegationClusterArgs {
                    current_validator: er.identity(),
                    validator_rotation_authority: payer.pubkey(),
                    challenge_config: None,
                },
            )],
        )
        .await
        .context("create direct-mode v2 cluster; use REDSUITE_DLP_SO with the pinned DLP build")?;
        Ok(Self {
            base,
            er,
            payer,
            cluster: dpi::delegation_cluster_pda(&cluster.pubkey()),
        })
    }

    pub async fn create_cell(&self, index: u64, space: u32) -> Result<Pubkey> {
        let id = CellId::new(self.payer.pubkey(), index);
        let (create, cell) = REDSHIFT.create(self.payer.pubkey(), id, space);
        self.base.submit_and_confirm(&self.payer, &[create]).await?;
        self.base
            .submit_and_confirm(
                &self.payer,
                &[REDSHIFT.delegate_v2(self.payer.pubkey(), id, self.cluster)],
            )
            .await?;
        let account = self
            .base
            .account(&dpi::delegation_state_pda(&cell))
            .await?
            .context("v2 DelegationState missing")?;
        ensure!(account.owner == dpi::id());
        let state = DelegationState::decode(&account.data)?;
        ensure!(state.discriminator() == DelegationState::DISCRIMINATOR);
        ensure!(*state.delegated_account() == cell);
        ensure!(*state.delegation_cluster() == self.cluster);
        ensure!(*state.original_owner() == REDSHIFT.id());
        ensure!(state.last_commit_id() == 0);
        let legacy = self
            .base
            .accounts(&[
                dpi::legacy::pda::delegation_record_pda_from_delegated_account(&cell),
                dpi::legacy::pda::delegation_metadata_pda_from_delegated_account(&cell),
            ])
            .await?;
        ensure!(
            legacy.iter().all(Option::is_none),
            "v2 fixture created legacy delegation PDAs"
        );
        self.er.await_clones(&[cell], space as usize).await?;
        self.er.await_delegated(&cell, "v2 account becomes writable on the ER").await?;
        Ok(cell)
    }

    pub async fn commit_id(&self, cell: &Pubkey) -> Result<u64> {
        let account = self
            .base
            .account(&dpi::delegation_state_pda(cell))
            .await?
            .context("v2 DelegationState missing")?;
        Ok(DelegationState::decode(&account.data)?.last_commit_id())
    }

    pub async fn write(&self, cell: Pubkey, value: u64) -> Result<Vec<u8>> {
        self.er.cells(REDSHIFT, &self.payer).write(value, &[cell]).await?;
        let account = self.er.account(&cell).await?.context("ER cell missing after write")?;
        ensure!(CellHeader::written_id(&account.data) == Some(value));
        Ok(account.data)
    }

    pub async fn settle(
        &self,
        nonce: u64,
        cells: &[Pubkey],
        mode: SettlementMode,
    ) -> Result<CommitReceipt> {
        let signature = self.er.cells(REDSHIFT, &self.payer).settle(nonce, cells, mode).await?;
        let receipt = self
            .er
            .check_commit(
                &self.base,
                &signature,
                cells,
                matches!(mode, SettlementMode::Undelegate),
            )
            .await?;
        receipt.check_success("v2 settlement")?;
        Ok(receipt)
    }

    pub async fn await_base(&self, cell: Pubkey, data: &[u8], owner: Pubkey) -> Result<()> {
        self.base
            .api()
            .await_accounts(
                slice::from_ref(&cell),
                "v2 settlement data and owner",
                |accounts| {
                    let account = accounts[0].as_ref().context("base cell missing")?;
                    ensure!(account.data == data);
                    ensure!(account.owner == owner);
                    Ok(())
                },
            )
            .await?;
        Ok(())
    }
}
