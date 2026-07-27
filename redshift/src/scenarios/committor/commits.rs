use std::time::Duration;

use async_trait::async_trait;
use borsh::BorshDeserialize;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_program::schedulecommit::{
    build, MainAccount, ScheduleCommitType,
};
use redsuite_core::{
    assert::poll_until, prep, receipt, BaseCtx, ChainCtx, ErCtx, Result,
    Scenario, ScenarioReport,
};
use signer::Signer;

use crate::program::DELEGATION_PROGRAM_ID;

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(20);
const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;

pub struct Commits;

async fn init_delegated_committee(
    base: &BaseCtx,
    payer: &Keypair,
    validator: Pubkey,
) -> Result<(Keypair, Pubkey)> {
    let player = Keypair::new();
    let (init, pda) = build::init_account(payer.pubkey(), player.pubkey());
    let delegate = build::delegate_cpi(
        payer.pubkey(),
        player.pubkey(),
        COMMIT_FREQUENCY_MS,
        Some(validator),
    );
    base.send_with(payer, &[&player], &[init, delegate]).await?;
    let on_base = base
        .account(&pda)
        .await?
        .ok_or("the committee pda is not on base after init and delegate")?;
    assert_eq!(
        on_base.owner, DELEGATION_PROGRAM_ID,
        "dlp must own a delegated committee on base"
    );
    Ok((player, pda))
}

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
            let mut players = Vec::new();
            let mut pdas = Vec::new();
            for _ in 0..committee_count {
                let (player, pda) =
                    init_delegated_committee(base, &payer, er.identity())
                        .await?;
                players.push(player.pubkey());
                pdas.push(pda);
            }
            for pda in &pdas {
                poll_until(CLONE_TIMEOUT, || async {
                    matches!(
                        er.account(pda).await,
                        Ok(Some(clone)) if clone.data.len() == MainAccount::SIZE
                    )
                })
                .await;
            }

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
            let commit_receipt = receipt::fetch_commit_receipt(
                er.api(),
                &signature,
                RECEIPT_TIMEOUT,
            )
            .await?;
            if let Some(message) = &commit_receipt.error_message {
                return Err(format!(
                    "{committee_count}-account commit intent failed: {message}"
                )
                .into());
            }
            let mut included = commit_receipt.included.clone();
            included.sort();
            let mut expected = pdas.clone();
            expected.sort();
            assert_eq!(
                included, expected,
                "the receipt must list exactly the committed accounts"
            );
            assert!(
                commit_receipt.excluded.is_empty(),
                "the receipt must exclude no accounts"
            );
            assert!(
                !commit_receipt.requested_undelegation,
                "a plain commit must not request undelegation"
            );
            assert_eq!(
                commit_receipt.base_signatures.len(),
                1,
                "a single-stage commit must send exactly one base tx"
            );
            receipt::confirm_base_signatures(
                base.api(),
                &commit_receipt,
                BASE_CONFIRM_TIMEOUT,
            )
            .await?;

            for pda in &pdas {
                let on_er = er
                    .account(pda)
                    .await?
                    .ok_or("the er clone is not present after the commit")?;
                assert_eq!(
                    decoded_count(&on_er.data)?,
                    1,
                    "ephem count after the commit"
                );
                poll_until(BASE_STATE_TIMEOUT, || async {
                    matches!(
                        base.account(pda).await,
                        Ok(Some(acc))
                            if decoded_count(&acc.data).ok() == Some(1)
                    )
                })
                .await;
                let on_base = base
                    .account(pda)
                    .await?
                    .ok_or("the pda is not on base after the commit")?;
                assert_eq!(
                    decoded_count(&on_base.data)?,
                    1,
                    "base count after the commit"
                );
                assert_eq!(
                    on_base.owner, DELEGATION_PROGRAM_ID,
                    "a plain commit must leave the committee delegated"
                );
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

        let (foreign_player, foreign_pda) =
            init_delegated_committee(base, &payer, other_validator.pubkey())
                .await?;
        poll_until(CLONE_TIMEOUT, || async {
            matches!(
                er.account(&foreign_pda).await,
                Ok(Some(clone)) if clone.data.len() == MainAccount::SIZE
            )
        })
        .await;
        let illegal_commit = er
            .send(
                &outsider_payer,
                &[build::schedule_commit_cpi(
                    outsider_payer.pubkey(),
                    vec![foreign_player.pubkey()],
                    false,
                    true,
                    ScheduleCommitType::CommitFinalize,
                    false,
                )],
            )
            .await;
        assert!(
            illegal_commit.is_err(),
            "a commit for an account that is delegated to another validator \
             must fail"
        );
        let commit_error = format!("{:?}", illegal_commit.unwrap_err());
        let commit_rejection = ["IllegalOwner", "MissingAccount"]
            .into_iter()
            .find(|code| commit_error.contains(code))
            .ok_or_else(|| {
                format!(
                    "expected IllegalOwner (upstream) or MissingAccount \
                     (observed on this validator build) for {foreign_pda}, \
                     got {commit_error}"
                )
            })?;

        let (undelegate_player, undelegate_pda) =
            init_delegated_committee(base, &payer, other_validator.pubkey())
                .await?;
        poll_until(CLONE_TIMEOUT, || async {
            matches!(
                er.account(&undelegate_pda).await,
                Ok(Some(clone)) if clone.data.len() == MainAccount::SIZE
            )
        })
        .await;
        let illegal_undelegate = er
            .send(
                &outsider_payer,
                &[build::schedule_commit_cpi(
                    outsider_payer.pubkey(),
                    vec![undelegate_player.pubkey()],
                    false,
                    true,
                    ScheduleCommitType::CommitFinalizeAndUndelegate,
                    true,
                )],
            )
            .await;
        assert!(
            illegal_undelegate.is_err(),
            "an undelegation for an account that is delegated to another \
             validator must fail"
        );
        let undelegate_error = format!("{:?}", illegal_undelegate.unwrap_err());
        let undelegate_rejection = ["ReadonlyDataModified", "MissingAccount"]
            .into_iter()
            .find(|code| undelegate_error.contains(code))
            .ok_or_else(|| {
                format!(
                    "expected ReadonlyDataModified (upstream) or \
                     MissingAccount (observed on this validator build) for \
                     {undelegate_pda}, got {undelegate_error}"
                )
            })?;

        Ok(report
            .setting("commit frequency ms", COMMIT_FREQUENCY_MS)
            .setting("foreign validator", other_validator.pubkey())
            .setting("foreign commit rejection", commit_rejection)
            .setting("foreign undelegate rejection", undelegate_rejection))
    }
}
