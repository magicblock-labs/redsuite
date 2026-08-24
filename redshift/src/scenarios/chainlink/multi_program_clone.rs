use std::time::Instant;

use async_trait::async_trait;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, topology, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
    ScenarioReport,
};

use crate::program::instruction::build;

const FIRST_WRITE: u64 = 61;
const SECOND_WRITE: u64 = 62;

pub struct MultiProgramClone;

#[async_trait(?Send)]
impl Scenario for MultiProgramClone {
    fn name(&self) -> &str {
        "redshift/multi_program_clone"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let [(first_program, _), (second_program, _)] =
            topology::redline_loader_v3_pair();

        let payer =
            redsuite_core::prep::funded_payer(base, crate::PAYER_LAMPORTS)
                .await?;
        let first_pda = crate::init_delegated_account_at(
            base,
            first_program,
            &payer,
            0,
            er.identity(),
        )
        .await?;
        let second_pda = crate::init_delegated_account_at(
            base,
            second_program,
            &payer,
            1,
            er.identity(),
        )
        .await?;

        let multi_clone_tx = Instant::now();
        er.send(
            &payer,
            &[
                build::simple_byte_set_at(
                    first_program,
                    FIRST_WRITE,
                    &[first_pda],
                ),
                build::simple_byte_set_at(
                    second_program,
                    SECOND_WRITE,
                    &[second_pda],
                ),
            ],
        )
        .await?;
        let multi_clone_ms = multi_clone_tx.elapsed().as_secs_f64() * 1e3;

        for (program, label) in
            [(first_program, "first"), (second_program, "second")]
        {
            let clone = er.account(&program).await?.ok_or_else(|| {
                check::CheckError::new(format!(
                    "the {label} program must be cloned to the ER after the tx"
                ))
            })?;
            check!(
                clone.executable,
                "the {label} cloned program must be executable on the ER"
            )?;
        }
        let first_clone = er
            .account(&first_pda)
            .await?
            .ok_or("first program's account missing on the ER")?;
        check_eq!(
            crate::written_id(&first_clone.data),
            Some(FIRST_WRITE),
            "the first program must have executed its write"
        )?;
        let second_clone = er
            .account(&second_pda)
            .await?
            .ok_or("second program's account missing on the ER")?;
        check_eq!(
            crate::written_id(&second_clone.data),
            Some(SECOND_WRITE),
            "the second program must have executed its write"
        )?;

        Ok(ScenarioReport::ok(self.name())
            .setting("loaders", "v3,v3")
            .metric(
                "two-program clone tx wall ms",
                Unit::Millis,
                multi_clone_ms,
            ))
    }
}
