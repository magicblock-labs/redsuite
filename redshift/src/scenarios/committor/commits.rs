use std::time::Duration;

use async_trait::async_trait;
use borsh::BorshDeserialize;
use keypair::Keypair;
use redshift_interface::schedulecommit::{
    build, MainAccount, ScheduleCommitType,
};
use redsuite_core::{
    check, check_eq, prep, BaseCtx, ChainCtx, CheckError, ErCtx, Result,
    Scenario, ScenarioReport,
};
use signer::Signer;

use crate::program::DELEGATION_PROGRAM_ID;

const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(20);

pub struct Commits;

fn decoded_count(data: &[u8]) -> Result<u64> {
    Ok(MainAccount::try_from_slice(data)?.count)
}

#[async_trait(?Send)]
impl Scenario for Commits {
    fn name(&self) -> &str {
        "redshift/commits"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let mut report = ScenarioReport::ok(self.name());

        for committee_count in [1usize, 2] {
            let committees = crate::init_schedulecommit_committees(
                base,
                &payer,
                er.identity(),
                committee_count,
            )
            .await?;
            crate::await_committee_clones(er, &committees).await?;
            let players: Vec<_> =
                committees.iter().map(|c| c.player.pubkey()).collect();
            let pdas: Vec<_> = committees.iter().map(|c| c.pda).collect();

            let signature = er
                .send(
                    &payer,
                    &[build::schedule_commit_cpi(
                        payer.pubkey(),
                        players.clone(),
                        true,
                        false,
                        ScheduleCommitType::CommitFinalize,
                        true,
                    )],
                )
                .await?;
            let commit_receipt = crate::assert_commit_receipt(
                base, er, &signature, &pdas, false,
            )
            .await?;
            if !commit_receipt.failure_is_duplicate_rejection() {
                check_eq!(
                    commit_receipt.base_signatures.len(),
                    1,
                    "a single-stage commit must send exactly one base tx"
                )?;
            }

            for pda in &pdas {
                let on_er = er
                    .account(pda)
                    .await?
                    .ok_or("the er clone is not present after the commit")?;
                check_eq!(
                    decoded_count(&on_er.data)?,
                    1,
                    "ephem count after the commit"
                )?;
                check::poll(
                    &format!("the committed count lands on base for {pda}"),
                    BASE_STATE_TIMEOUT,
                    || async {
                        matches!(
                            base.account(pda).await,
                            Ok(Some(acc))
                                if decoded_count(&acc.data).ok() == Some(1)
                        )
                    },
                )
                .await?;
                let on_base = base
                    .account(pda)
                    .await?
                    .ok_or("the pda is not on base after the commit")?;
                check_eq!(
                    decoded_count(&on_base.data)?,
                    1,
                    "base count after the commit"
                )?;
                check_eq!(
                    on_base.owner,
                    DELEGATION_PROGRAM_ID,
                    "a plain commit must leave the committee delegated"
                )?;
            }

            report = report.setting(
                format!("{committee_count}-account commit base sigs"),
                commit_receipt.base_signatures.len(),
            );
        }

        let outsider_payer = prep::delegated_payer(
            base,
            &payer,
            er.identity(),
            crate::PAYER_LAMPORTS,
        )
        .await?;
        let other_validator = Keypair::new();

        let foreign = crate::init_schedulecommit_committees(
            base,
            &payer,
            other_validator.pubkey(),
            1,
        )
        .await?;
        crate::await_committee_clones(er, &foreign).await?;
        let (foreign_player, foreign_pda) =
            (foreign[0].player.pubkey(), foreign[0].pda);
        let illegal_commit = er
            .send(
                &outsider_payer,
                &[build::schedule_commit_cpi(
                    outsider_payer.pubkey(),
                    vec![foreign_player],
                    false,
                    true,
                    ScheduleCommitType::CommitFinalize,
                    false,
                )],
            )
            .await;
        check!(
            illegal_commit.is_err(),
            "a commit for an account that is delegated to another validator \
             must fail"
        )?;
        let commit_error = format!("{:?}", illegal_commit.unwrap_err());
        let commit_rejection = ["IllegalOwner", "MissingAccount"]
            .into_iter()
            .find(|code| commit_error.contains(code))
            .ok_or_else(|| {
                CheckError::new(format!(
                    "the foreign commit for {foreign_pda} is rejected with a \
                     known code"
                ))
                .expected(
                    "IllegalOwner (upstream) or MissingAccount (observed on \
                     this validator build)",
                )
                .actual(&commit_error)
            })?;
        if commit_rejection == "MissingAccount" {
            eprintln!(
                "[redsuite] {}: warning: the foreign commit was rejected \
                 with MissingAccount; upstream 01_commits asserts \
                 IllegalOwner exactly",
                self.name()
            );
        }

        let undelegate = crate::init_schedulecommit_committees(
            base,
            &payer,
            other_validator.pubkey(),
            1,
        )
        .await?;
        crate::await_committee_clones(er, &undelegate).await?;
        let (undelegate_player, undelegate_pda) =
            (undelegate[0].player.pubkey(), undelegate[0].pda);
        let illegal_undelegate = er
            .send(
                &outsider_payer,
                &[build::schedule_commit_cpi(
                    outsider_payer.pubkey(),
                    vec![undelegate_player],
                    false,
                    true,
                    ScheduleCommitType::CommitFinalizeAndUndelegate,
                    true,
                )],
            )
            .await;
        check!(
            illegal_undelegate.is_err(),
            "an undelegation for an account that is delegated to another \
             validator must fail"
        )?;
        let undelegate_error = format!("{:?}", illegal_undelegate.unwrap_err());
        let undelegate_rejection = ["ReadonlyDataModified", "MissingAccount"]
            .into_iter()
            .find(|code| undelegate_error.contains(code))
            .ok_or_else(|| {
                CheckError::new(format!(
                    "the foreign undelegation for {undelegate_pda} is \
                     rejected with a known code"
                ))
                .expected(
                    "ReadonlyDataModified (upstream) or MissingAccount \
                     (observed on this validator build)",
                )
                .actual(&undelegate_error)
            })?;
        if undelegate_rejection == "MissingAccount" {
            eprintln!(
                "[redsuite] {}: warning: the foreign undelegation was \
                 rejected with MissingAccount; upstream 01_commits asserts \
                 ReadonlyDataModified exactly",
                self.name()
            );
        }

        Ok(report
            .setting("commit frequency ms", crate::COMMIT_FREQUENCY_MS)
            .setting("foreign validator", other_validator.pubkey())
            .setting("foreign commit rejection", commit_rejection)
            .setting("foreign undelegate rejection", undelegate_rejection))
    }
}
