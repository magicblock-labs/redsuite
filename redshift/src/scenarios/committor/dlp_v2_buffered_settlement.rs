use std::rc::Rc;

use anyhow::ensure;
use delegation_program_interface::{self as dpi, DlpV2Instruction};
use redsuite_core::{
    programs::{SettlementMode, REDSHIFT},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};

use super::v2::Fixture;

pub struct DlpV2BufferedSettlement(Fixture);

impl Scenario for DlpV2BufferedSettlement {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self(
            Fixture::new(base, er, "dlp_v2_buffered_settlement").await?,
        ))
    }

    async fn run(self) -> Result<()> {
        let fixture = self.0;
        for (index, mode) in
            [SettlementMode::Commit, SettlementMode::Undelegate].into_iter().enumerate()
        {
            // Rewrite the whole payload so a small inline diff cannot avoid the buffer.
            let cell = fixture.create_cell(index as u64, 8 * 1024).await?;
            let snapshot = fixture.write(cell, u64::MAX).await?;
            let receipt = fixture.settle(index as u64 + 1, &[cell], mode).await?;
            let instructions = fixture.dlp_instructions(&receipt.base_signatures).await?;
            ensure!(
                instructions
                    .iter()
                    .any(|data| data
                        .starts_with(&DlpV2Instruction::CommitFinalizeFromBuffer.to_vec())),
                "settlement did not use a v2 StateBuffer"
            );

            let undelegate = matches!(mode, SettlementMode::Undelegate);
            fixture
                .await_base(
                    cell,
                    &snapshot,
                    if undelegate { REDSHIFT.id() } else { dpi::id() },
                )
                .await?;
            if undelegate {
                ensure!(fixture.base.account(&dpi::delegation_state_pda(&cell)).await?.is_none());
            } else {
                ensure!(fixture.commit_id(&cell).await? == 1);
            }

            let buffer = dpi::state_buffer_pda(&cell, 1, &fixture.er.identity());
            fixture
                .base
                .api()
                .await_query("v2 buffer writes and cleanup", async || {
                    let signatures =
                        fixture.base.api().get_signatures_for_address(&buffer, 64).await?;
                    let instructions = fixture.dlp_instructions(&signatures).await?;
                    for instruction in
                        [DlpV2Instruction::WriteStateBuffer, DlpV2Instruction::CloseStateBuffer]
                    {
                        ensure!(
                            instructions.iter().any(|data| data.starts_with(&instruction.to_vec())),
                            "missing {instruction:?}"
                        );
                    }
                    ensure!(
                        fixture.base.account(&buffer).await?.is_none(),
                        "StateBuffer was not closed"
                    );
                    Ok(())
                })
                .await?;
        }
        Ok(())
    }
}
