use std::rc::Rc;

use anyhow::ensure;
use delegation_program_interface::{self as dpi, DlpV2Instruction};
use redsuite_core::{
    programs::{CellHeader, SettlementMode, REDSHIFT},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};

use super::v2::Fixture;

pub struct DlpMixedSettlement(Fixture);

impl Scenario for DlpMixedSettlement {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self(Fixture::new(base, er, "dlp_mixed_settlement").await?))
    }

    async fn run(self) -> Result<()> {
        let fixture = self.0;
        let legacy = fixture
            .base
            .cells(REDSHIFT, &fixture.payer)
            .create_primary(fixture.er.identity(), u32::MAX)
            .await?;
        fixture.er.await_clones(&[legacy], CellHeader::SIZE).await?;
        fixture
            .er
            .await_delegated(&legacy, "legacy cell becomes writable on the ER")
            .await?;
        let v2 = fixture.create_cell(1, CellHeader::SIZE as u32).await?;
        let cells = [legacy, v2];

        for (index, mode) in
            [SettlementMode::Commit, SettlementMode::Undelegate].into_iter().enumerate()
        {
            let value = index as u64 + 11;
            let snapshots = [fixture.write(legacy, value).await?, fixture.write(v2, value).await?];
            let receipt = fixture.settle(index as u64 + 1, &cells, mode).await?;
            let instructions = fixture.dlp_instructions(&receipt.base_signatures).await?;
            for discriminator in [
                dpi::legacy::discriminator::DlpDiscriminator::CommitFinalize.to_vec(),
                DlpV2Instruction::CommitFinalize.to_vec(),
            ] {
                ensure!(
                    instructions.iter().any(|data| data.starts_with(&discriminator)),
                    "mixed intent did not settle through both DLP versions"
                );
            }
            let undelegate = matches!(mode, SettlementMode::Undelegate);
            for (cell, snapshot) in cells.into_iter().zip(&snapshots) {
                fixture
                    .await_base(
                        cell,
                        snapshot,
                        if undelegate { REDSHIFT.id() } else { dpi::id() },
                    )
                    .await?;
            }
            if undelegate {
                ensure!(
                    instructions
                        .iter()
                        .any(|data| data.starts_with(&DlpV2Instruction::Undelegate.to_vec())),
                    "missing v2 undelegation"
                );
                ensure!(fixture.base.account(&dpi::delegation_state_pda(&v2)).await?.is_none());
            } else {
                ensure!(crate::last_commit_id(&fixture.base, &legacy).await? == 1);
                ensure!(fixture.commit_id(&v2).await? == 1);
            }
        }
        Ok(())
    }
}
