use std::rc::Rc;

use anyhow::ensure;
use delegation_program_interface as dpi;
use redsuite_core::{
    programs::{CellHeader, SettlementMode, REDSHIFT},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};

use super::v2::Fixture;

pub struct DlpV2Lifecycle(Fixture);

impl Scenario for DlpV2Lifecycle {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self(Fixture::new(base, er, "dlp_v2_lifecycle").await?))
    }

    async fn run(self) -> Result<()> {
        let fixture = self.0;
        let cell = fixture.create_cell(0, CellHeader::SIZE as u32).await?;
        let snapshot = fixture.write(cell, 11).await?;
        fixture.settle(1, &[cell], SettlementMode::Commit).await?;
        fixture.await_base(cell, &snapshot, dpi::id()).await?;
        ensure!(fixture.commit_id(&cell).await? == 1);

        let snapshot = fixture.write(cell, 22).await?;
        fixture.settle(2, &[cell], SettlementMode::Undelegate).await?;
        fixture.await_base(cell, &snapshot, REDSHIFT.id()).await?;
        ensure!(fixture.base.account(&dpi::delegation_state_pda(&cell)).await?.is_none());
        Ok(())
    }
}
