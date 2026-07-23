use std::time::{Duration, Instant};

use async_trait::async_trait;
use redsuite_core::{
    assert::poll_until, prep, receipt, BaseCtx, ChainCtx, ErCtx, Result,
    Scenario, ScenarioReport,
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
            assert_eq!(
                on_base.owner, DELEGATION_PROGRAM_ID,
                "a delegated pda must be dlp-owned on base"
            );
            poll_until(CLONE_TIMEOUT, || async {
                matches!(er.account(pda).await, Ok(Some(clone)) if clone.data.len() == crate::ACCOUNT_SPACE as usize)
            })
            .await;
        }
        let clone_visibility_ms = clone_started.elapsed().as_secs_f64() * 1e3;

        er.send(&payer, &[build::simple_byte_set(FIRST_WRITE, &accounts)])
            .await?;
        let committed_state = er
            .account(&committed)
            .await?
            .ok_or("er copy vanished after the write")?
            .data;
        assert_eq!(crate::written_id(&committed_state), Some(FIRST_WRITE));
        for pda in &accounts {
            let on_base = base.account(pda).await?.ok_or("pda gone on base")?;
            assert!(
                on_base.data[layout::DATA_OFFSET..]
                    .iter()
                    .all(|&byte| byte == 0),
                "base copies must stay untouched before the commit"
            );
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
        if let Some(message) = &commit_receipt.error_message {
            return Err(format!("commit intent failed: {message}").into());
        }
        assert_eq!(
            commit_receipt.included,
            vec![committed],
            "the receipt must list exactly the committed account"
        );
        assert!(
            commit_receipt.excluded.is_empty(),
            "nothing was eligible for exclusion"
        );
        assert!(
            !commit_receipt.requested_undelegation,
            "a plain commit must not request undelegation"
        );
        assert_eq!(
            commit_receipt.payer,
            Some(payer.pubkey()),
            "the receipt must name the scheduling payer"
        );
        assert!(
            !commit_receipt.base_signatures.is_empty(),
            "a commit receipt must name at least one base tx"
        );
        receipt::confirm_base_signatures(
            base.api(),
            &commit_receipt,
            BASE_CONFIRM_TIMEOUT,
        )
        .await?;
        let state_deadline = tokio::time::Instant::now() + BASE_STATE_TIMEOUT;
        loop {
            let on_base = base.account(&committed).await?;
            if matches!(&on_base, Some(acc) if acc.data == committed_state) {
                break;
            }
            if tokio::time::Instant::now() >= state_deadline {
                let observed = on_base
                    .map(|acc| {
                        format!(
                            "owner {} data[..48] {:02x?}",
                            acc.owner,
                            &acc.data[..48.min(acc.data.len())]
                        )
                    })
                    .unwrap_or_else(|| "absent".to_owned());
                return Err(format!(
                    "base state never matched; expected data[..48] {:02x?}, observed {observed}",
                    &committed_state[..48]
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let commit_roundtrip_s = commit_started.elapsed().as_secs_f64();

        let committed_on_base = base
            .account(&committed)
            .await?
            .ok_or("committed pda gone on base")?;
        assert_eq!(
            committed_on_base.owner, DELEGATION_PROGRAM_ID,
            "a commit without undelegation must leave the pda delegated"
        );
        let sibling_on_base = base
            .account(&sibling)
            .await?
            .ok_or("sibling pda gone on base")?;
        assert!(
            sibling_on_base.data[layout::DATA_OFFSET..]
                .iter()
                .all(|&byte| byte == 0),
            "committing one account must not touch its sibling on base"
        );

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
        assert_eq!(
            crate::written_id(&committed_final.data),
            Some(SECOND_WRITE)
        );

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
        if let Some(message) = &undelegate_receipt.error_message {
            return Err(
                format!("commit-undelegate intent failed: {message}").into()
            );
        }
        let mut included = undelegate_receipt.included.clone();
        included.sort();
        let mut expected = accounts.to_vec();
        expected.sort();
        assert_eq!(
            included, expected,
            "the receipt must list both undelegated accounts"
        );
        assert!(
            undelegate_receipt.excluded.is_empty(),
            "nothing was eligible for exclusion"
        );
        assert!(
            undelegate_receipt.requested_undelegation,
            "the receipt must record the undelegation request"
        );
        assert!(
            !undelegate_receipt.base_signatures.is_empty(),
            "an undelegating commit must name at least one base tx"
        );
        receipt::confirm_base_signatures(
            base.api(),
            &undelegate_receipt,
            BASE_CONFIRM_TIMEOUT,
        )
        .await?;

        for (pda, expected) in
            [(committed, &committed_final), (sibling, &sibling_final)]
        {
            let undelegate_deadline =
                tokio::time::Instant::now() + UNDELEGATE_TIMEOUT;
            loop {
                let on_base = base.account(&pda).await?;
                if matches!(
                    &on_base,
                    Some(acc) if acc.owner == crate::program::id()
                        && acc.data == expected.data
                        && acc.lamports == expected.lamports
                ) {
                    break;
                }
                if tokio::time::Instant::now() >= undelegate_deadline {
                    let observed = on_base
                        .map(|acc| {
                            format!(
                                "owner {} lamports {} data[..48] {:02x?}",
                                acc.owner,
                                acc.lamports,
                                &acc.data[..48.min(acc.data.len())]
                            )
                        })
                        .unwrap_or_else(|| "absent".to_owned());
                    return Err(format!(
                        "{pda} never undelegated on base with matching \
                         owner/data/lamports; expected owner {} lamports {} \
                         data[..48] {:02x?}, observed {observed}",
                        crate::program::id(),
                        expected.lamports,
                        &expected.data[..48]
                    )
                    .into());
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        let undelegate_roundtrip_s = undelegate_started.elapsed().as_secs_f64();

        poll_until(UNDELEGATE_TIMEOUT, || async {
            matches!(
                er.account(&committed).await,
                Ok(Some(clone)) if clone.owner == crate::program::id()
            )
        })
        .await;
        let write_after_undelegate = er
            .send(
                &payer,
                &[build::simple_byte_set(LOCKOUT_WRITE, &[committed])],
            )
            .await;
        assert!(
            write_after_undelegate.is_err(),
            "the ER copy of an undelegated account must reject writes \
             (locked out after undelegation), got {write_after_undelegate:?}"
        );

        Ok(ScenarioReport::ok(self.name())
            .setting("account space", crate::ACCOUNT_SPACE)
            .setting("accounts", accounts.len())
            .setting("commit base sigs", commit_receipt.base_signatures.len())
            .setting(
                "undelegate base sigs",
                undelegate_receipt.base_signatures.len(),
            )
            .setting("commit id", commit_receipt.commit_id.unwrap_or_default())
            .metric("clone visibility ms", clone_visibility_ms)
            .metric("commit roundtrip s", commit_roundtrip_s)
            .metric("commit-undelegate roundtrip s", undelegate_roundtrip_s))
    }
}
