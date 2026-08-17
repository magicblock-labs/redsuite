use std::time::{Duration, Instant};

use async_trait::async_trait;
use redsuite_core::{
    check, check_eq, prep, receipt, BaseCtx, ChainCtx, CheckError, ErCtx,
    Result, Scenario, ScenarioReport,
};
use signer::Signer;

use crate::program::{instruction::build, layout, DELEGATION_PROGRAM_ID};

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(20);
const UNDELEGATE_TIMEOUT: Duration = Duration::from_secs(30);
const FIRST_WRITE: u64 = 21;
const SECOND_WRITE: u64 = 22;
const LOCKOUT_WRITE: u64 = 23;

pub struct CommitRoundtrip;

#[async_trait(?Send)]
impl Scenario for CommitRoundtrip {
    fn name(&self) -> &str {
        "redshift/commit_roundtrip"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let committed =
            crate::init_delegated_account(base, &payer, 0, er.identity())
                .await?;
        let sibling =
            crate::init_delegated_account(base, &payer, 1, er.identity())
                .await?;
        let accounts = [committed, sibling];

        let clone_started = Instant::now();
        for pda in &accounts {
            let on_base =
                base.account(pda).await?.ok_or("pda missing on base")?;
            check_eq!(
                on_base.owner,
                DELEGATION_PROGRAM_ID,
                "a delegated pda must be dlp-owned on base"
            )?;
            check::poll(
                &format!("the ER clones the delegated pda {pda}"),
                CLONE_TIMEOUT,
                || async {
                    matches!(er.account(pda).await, Ok(Some(clone)) if clone.data.len() == crate::ACCOUNT_SPACE as usize)
                },
            )
            .await?;
        }
        let clone_visibility_ms = clone_started.elapsed().as_secs_f64() * 1e3;

        er.send(&payer, &[build::simple_byte_set(FIRST_WRITE, &accounts)])
            .await?;
        let committed_state = er
            .account(&committed)
            .await?
            .ok_or("er copy vanished after the write")?
            .data;
        check_eq!(
            crate::written_id(&committed_state),
            Some(FIRST_WRITE),
            "the er write must land before the commit"
        )?;
        for pda in &accounts {
            let on_base = base.account(pda).await?.ok_or("pda gone on base")?;
            check!(
                on_base.data[layout::DATA_OFFSET..]
                    .iter()
                    .all(|&byte| byte == 0),
                "base copies must stay untouched before the commit"
            )?;
        }

        let commit_started = Instant::now();
        let commit_signature = er
            .send(
                &payer,
                &[build::commit_accounts(1, payer.pubkey(), &accounts[..1])],
            )
            .await?;
        let commit_receipt = receipt::fetch_commit_receipt(
            er.api(),
            &commit_signature,
            RECEIPT_TIMEOUT,
        )
        .await?;
        let commit_tolerated = match &commit_receipt.error_message {
            Some(message)
                if commit_receipt.failure_is_duplicate_rejection() =>
            {
                receipt::warn_duplicate_rejection(self.name(), message);
                true
            }
            Some(message) => {
                return Err(CheckError::new("the commit intent succeeds")
                    .actual(message)
                    .into());
            }
            None => false,
        };
        if !commit_tolerated {
            check_eq!(
                commit_receipt.included,
                vec![committed],
                "the receipt must list exactly the committed account"
            )?;
            check!(
                commit_receipt.excluded.is_empty(),
                "nothing was eligible for exclusion"
            )?;
            check!(
                !commit_receipt.requested_undelegation,
                "a plain commit must not request undelegation"
            )?;
            check_eq!(
                commit_receipt.payer,
                Some(payer.pubkey()),
                "the receipt must name the scheduling payer"
            )?;
            check!(
                !commit_receipt.base_signatures.is_empty(),
                "a commit receipt must name at least one base tx"
            )?;
            receipt::confirm_base_signatures(
                base.api(),
                &commit_receipt,
                BASE_CONFIRM_TIMEOUT,
            )
            .await?;
        }
        check::poll_for(
            "the committed base copy matches the er snapshot",
            BASE_STATE_TIMEOUT,
            || async {
                match base.account(&committed).await {
                    Ok(Some(acc)) if acc.data == committed_state => Ok(()),
                    Ok(Some(acc)) => Err(format!(
                        "owner {} data[..48] {:02x?}",
                        acc.owner,
                        &acc.data[..48.min(acc.data.len())]
                    )),
                    Ok(None) => Err("absent".to_owned()),
                    Err(error) => Err(format!("read failed: {error}")),
                }
            },
        )
        .await
        .map_err(|error| {
            error.expected(format!(
                "data[..48] {:02x?}",
                &committed_state[..48.min(committed_state.len())]
            ))
        })?;
        let commit_roundtrip_s = commit_started.elapsed().as_secs_f64();

        let committed_on_base = base
            .account(&committed)
            .await?
            .ok_or("committed pda gone on base")?;
        check_eq!(
            committed_on_base.owner,
            DELEGATION_PROGRAM_ID,
            "a commit without undelegation must leave the pda delegated"
        )?;
        let sibling_on_base = base
            .account(&sibling)
            .await?
            .ok_or("sibling pda gone on base")?;
        check!(
            sibling_on_base.data[layout::DATA_OFFSET..]
                .iter()
                .all(|&byte| byte == 0),
            "committing one account must not touch its sibling on base"
        )?;

        er.send(&payer, &[build::simple_byte_set(SECOND_WRITE, &accounts)])
            .await?;
        let committed_final = er
            .account(&committed)
            .await?
            .ok_or("er copy vanished before the undelegating commit")?;
        let sibling_final = er
            .account(&sibling)
            .await?
            .ok_or("sibling er copy vanished before the undelegating commit")?;
        check_eq!(
            crate::written_id(&committed_final.data),
            Some(SECOND_WRITE),
            "the second er write must land before the undelegating commit"
        )?;

        let undelegate_started = Instant::now();
        let undelegate_signature = er
            .send(
                &payer,
                &[build::commit_and_undelegate_accounts(
                    2,
                    payer.pubkey(),
                    &accounts,
                )],
            )
            .await?;
        let undelegate_receipt = receipt::fetch_commit_receipt(
            er.api(),
            &undelegate_signature,
            RECEIPT_TIMEOUT,
        )
        .await?;
        let undelegate_tolerated = match &undelegate_receipt.error_message {
            Some(message)
                if undelegate_receipt.failure_is_duplicate_rejection() =>
            {
                receipt::warn_duplicate_rejection(self.name(), message);
                true
            }
            Some(message) => {
                return Err(CheckError::new(
                    "the commit-undelegate intent succeeds",
                )
                .actual(message)
                .into());
            }
            None => false,
        };
        if !undelegate_tolerated {
            let mut included = undelegate_receipt.included.clone();
            included.sort();
            let mut expected = accounts.to_vec();
            expected.sort();
            check_eq!(
                included,
                expected,
                "the receipt must list both undelegated accounts"
            )?;
            check!(
                undelegate_receipt.excluded.is_empty(),
                "nothing was eligible for exclusion"
            )?;
            check!(
                undelegate_receipt.requested_undelegation,
                "the receipt must record the undelegation request"
            )?;
            check!(
                !undelegate_receipt.base_signatures.is_empty(),
                "an undelegating commit must name at least one base tx"
            )?;
            receipt::confirm_base_signatures(
                base.api(),
                &undelegate_receipt,
                BASE_CONFIRM_TIMEOUT,
            )
            .await?;
        }

        for (pda, expected) in
            [(committed, &committed_final), (sibling, &sibling_final)]
        {
            check::poll_for(
                &format!(
                    "{pda} undelegates on base with matching \
                     owner/data/lamports"
                ),
                UNDELEGATE_TIMEOUT,
                || async {
                    match base.account(&pda).await {
                        Ok(Some(acc))
                            if acc.owner == crate::program::id()
                                && acc.data == expected.data
                                && acc.lamports == expected.lamports =>
                        {
                            Ok(())
                        }
                        Ok(Some(acc)) => Err(format!(
                            "owner {} lamports {} data[..48] {:02x?}",
                            acc.owner,
                            acc.lamports,
                            &acc.data[..48.min(acc.data.len())]
                        )),
                        Ok(None) => Err("absent".to_owned()),
                        Err(error) => Err(format!("read failed: {error}")),
                    }
                },
            )
            .await
            .map_err(|error| {
                error.expected(format!(
                    "owner {} lamports {} data[..48] {:02x?}",
                    crate::program::id(),
                    expected.lamports,
                    &expected.data[..48.min(expected.data.len())]
                ))
            })?;
        }
        let undelegate_roundtrip_s = undelegate_started.elapsed().as_secs_f64();

        check::poll(
            "the er re-clones the undelegated account with its base owner",
            UNDELEGATE_TIMEOUT,
            || async {
                matches!(
                    er.account(&committed).await,
                    Ok(Some(clone)) if clone.owner == crate::program::id()
                )
            },
        )
        .await?;
        let lockout_payer = prep::delegated_payer(
            base,
            &payer,
            er.identity(),
            crate::PAYER_LAMPORTS,
        )
        .await?;
        let write_after_undelegate = er
            .send(
                &lockout_payer,
                &[build::simple_byte_set(LOCKOUT_WRITE, &[committed])],
            )
            .await;
        check!(
            write_after_undelegate.is_err(),
            "the ER copy of an undelegated account must reject writes \
             (locked out after undelegation), got {write_after_undelegate:?}"
        )?;
        let lockout_error =
            format!("{:?}", write_after_undelegate.unwrap_err());
        let lockout_rejection = [
            "InvalidWritableAccount",
            "ExternalAccountDataModified",
            "ProgramFailedToComplete",
        ]
        .into_iter()
        .find(|code| lockout_error.contains(code))
        .ok_or_else(|| {
            CheckError::new(
                "the lockout write is rejected with an upstream code",
            )
            .expected(
                "InvalidWritableAccount, ExternalAccountDataModified or \
                 ProgramFailedToComplete",
            )
            .actual(&lockout_error)
        })?;

        Ok(ScenarioReport::ok(self.name())
            .setting("account space", crate::ACCOUNT_SPACE)
            .setting("accounts", accounts.len())
            .setting("commit base sigs", commit_receipt.base_signatures.len())
            .setting(
                "undelegate base sigs",
                undelegate_receipt.base_signatures.len(),
            )
            .setting("commit id", commit_receipt.commit_id.unwrap_or_default())
            .setting("lockout rejection", lockout_rejection)
            .metric("clone visibility ms", clone_visibility_ms)
            .metric("commit roundtrip s", commit_roundtrip_s)
            .metric("commit-undelegate roundtrip s", undelegate_roundtrip_s))
    }
}
