use std::time::Duration;

use async_trait::async_trait;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_program::schedulecommit::{
    build, order_book_view, ScheduleCommitType,
};
use redsuite_core::{
    assert::poll_until,
    dlp, prep,
    receipt::{COMMIT_LIMIT_ERR, INTENT_TOO_LARGE_ERR, SPONSORED_COMMIT_LIMIT},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use signer::Signer;

const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const ACTION_TIMEOUT: Duration = Duration::from_secs(30);
const ACTUAL_COMMIT_LIMIT: u64 = 25;
const COMMIT_FEE_LAMPORTS: u64 = 100_000;
const ACTION_FEE_LAMPORTS: u64 = 2_500;
const FITTING_ACCOUNTS: usize = 3;
const REFUSED_ACCOUNTS: usize = 12;

pub struct CommitLimitAndFees;

fn marker(text: String) -> Instruction {
    Instruction {
        program_id: redshift_program::id(),
        accounts: vec![],
        data: redshift_program::log_msg_data(&text),
    }
}

fn assert_error_code(
    attempt: Result<signature::Signature>,
    code: u32,
    context: &str,
) -> Result<()> {
    assert!(attempt.is_err(), "{context} must fail");
    let text = format!("{:?}", attempt.unwrap_err());
    if !text.contains(&code.to_string()) {
        return Err(
            format!("{context}: expected Custom({code}), got {text}").into()
        );
    }
    Ok(())
}

async fn er_balance(er: &ErCtx, account: &Pubkey) -> Result<u64> {
    er.api().get_balance(account).await
}

async fn test_sponsored_quota(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let committees =
        crate::init_schedulecommit_committees(base, &payer, er.identity(), 2)
            .await?;
    crate::await_committee_clones(er, &committees).await;
    let players: Vec<_> =
        committees.iter().map(|c| c.player.pubkey()).collect();
    let pdas: Vec<_> = committees.iter().map(|c| c.pda).collect();

    for round in 0..SPONSORED_COMMIT_LIMIT {
        let signature = er
            .send(
                &payer,
                &[
                    build::schedule_commit_cpi(
                        payer.pubkey(),
                        players.clone(),
                        true,
                        false,
                        ScheduleCommitType::CommitFinalize,
                        true,
                    ),
                    marker(format!("sponsored {round}")),
                ],
            )
            .await?;
        crate::assert_commit_receipt(base, er, &signature, &pdas, false)
            .await?;
    }

    let over_limit = er
        .send(
            &payer,
            &[build::schedule_commit_cpi(
                payer.pubkey(),
                vec![players[0]],
                true,
                false,
                ScheduleCommitType::CommitFinalize,
                true,
            )],
        )
        .await;
    assert_error_code(
        over_limit,
        COMMIT_LIMIT_ERR,
        "a plain commit past the sponsored limit",
    )?;

    let signature = er
        .send(
            &payer,
            &[build::schedule_commit_cpi(
                payer.pubkey(),
                vec![players[1]],
                true,
                false,
                ScheduleCommitType::CommitFinalizeAndUndelegate,
                true,
            )],
        )
        .await?;
    crate::assert_commit_receipt(base, er, &signature, &[pdas[1]], true)
        .await?;
    poll_until(BASE_STATE_TIMEOUT, || async {
        matches!(
            base.account(&pdas[1]).await,
            Ok(Some(acc)) if acc.owner == redshift_program::id()
        )
    })
    .await;
    Ok(())
}

async fn test_fee_vault_quota(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let vault_payer = prep::delegated_payer(
        base,
        &payer,
        er.identity(),
        crate::PAYER_LAMPORTS,
    )
    .await?;
    let vault = dlp::magic_fee_vault_pda(&er.identity());
    assert!(
        er.account(&vault).await?.is_some(),
        "the magic fee vault for the booted er identity must exist"
    );

    let vault_committees =
        crate::init_schedulecommit_committees(base, &payer, er.identity(), 3)
            .await?;
    crate::await_committee_clones(er, &vault_committees).await;
    let missing_vault = &vault_committees[0];
    let within_limit = &vault_committees[1];
    let charged = &vault_committees[2];

    for round in 0..ACTUAL_COMMIT_LIMIT {
        let signature = er
            .send(
                &vault_payer,
                &[
                    build::schedule_commit_with_vault(
                        vault_payer.pubkey(),
                        Some(vault),
                        vec![charged.player.pubkey()],
                        false,
                    ),
                    marker(format!("vault {round}")),
                ],
            )
            .await?;
        crate::assert_commit_receipt(
            base,
            er,
            &signature,
            &[charged.pda],
            false,
        )
        .await?;
    }

    let vault_absent = er
        .send(
            &vault_payer,
            &[build::schedule_commit_with_vault(
                vault_payer.pubkey(),
                None,
                vec![missing_vault.player.pubkey()],
                false,
            )],
        )
        .await;
    assert!(
        vault_absent.is_err(),
        "an escrowed payer without the fee vault must fail"
    );
    let vault_absent_error = format!("{:?}", vault_absent.unwrap_err());
    assert!(
        vault_absent_error.contains("MissingAccount"),
        "expected MissingAccount for the vault-absent commit, got \
         {vault_absent_error}"
    );

    let payer_before = er_balance(er, &vault_payer.pubkey()).await?;
    let vault_before = er_balance(er, &vault).await?;
    let signature = er
        .send(
            &vault_payer,
            &[build::schedule_commit_with_vault(
                vault_payer.pubkey(),
                Some(vault),
                vec![within_limit.player.pubkey()],
                false,
            )],
        )
        .await?;
    crate::assert_commit_receipt(
        base,
        er,
        &signature,
        &[within_limit.pda],
        false,
    )
    .await?;
    assert_eq!(
        er_balance(er, &vault_payer.pubkey()).await?,
        payer_before,
        "the payer balance must not change within the actual commit limit"
    );
    assert_eq!(
        er_balance(er, &vault).await?,
        vault_before,
        "the vault balance must not change within the actual commit limit"
    );

    let payer_before = er_balance(er, &vault_payer.pubkey()).await?;
    let vault_before = er_balance(er, &vault).await?;
    let signature = er
        .send(
            &vault_payer,
            &[build::schedule_commit_with_vault(
                vault_payer.pubkey(),
                Some(vault),
                vec![charged.player.pubkey()],
                false,
            )],
        )
        .await?;
    crate::assert_commit_receipt(base, er, &signature, &[charged.pda], false)
        .await?;
    assert_eq!(
        er_balance(er, &vault_payer.pubkey()).await?,
        payer_before - COMMIT_FEE_LAMPORTS,
        "the payer must pay the commit fee past the actual commit limit"
    );
    assert_eq!(
        er_balance(er, &vault).await?,
        vault_before + COMMIT_FEE_LAMPORTS,
        "the vault must receive the commit fee past the actual commit \
         limit"
    );

    let manager = Keypair::new();
    let (init_book, book) =
        build::init_order_book(payer.pubkey(), manager.pubkey());
    base.send_with(&payer, &[&manager], &[init_book]).await?;
    let payer_before = er_balance(er, &vault_payer.pubkey()).await?;
    let vault_before = er_balance(er, &vault).await?;
    let signature = er
        .send(
            &vault_payer,
            &[build::schedule_commit_with_vault_and_order_book(
                vault_payer.pubkey(),
                vault,
                manager.pubkey(),
                vec![charged.player.pubkey()],
                true,
            )],
        )
        .await?;
    crate::assert_commit_receipt(base, er, &signature, &[charged.pda], false)
        .await?;
    assert_eq!(
        er_balance(er, &vault_payer.pubkey()).await?,
        payer_before - COMMIT_FEE_LAMPORTS - ACTION_FEE_LAMPORTS,
        "the payer must pay the commit fee and the action fee"
    );
    assert_eq!(
        er_balance(er, &vault).await?,
        vault_before + COMMIT_FEE_LAMPORTS + ACTION_FEE_LAMPORTS,
        "the vault must receive the commit fee and the action fee"
    );
    poll_until(ACTION_TIMEOUT, || async {
        matches!(
            base.api().get_signatures_for_address(&book, 5).await,
            Ok(sigs) if sigs.len() >= 2
        )
    })
    .await;
    let book_signatures =
        base.api().get_signatures_for_address(&book, 5).await?;
    let action_signature: signature::Signature = book_signatures[0].parse()?;
    let action_tx = base
        .api()
        .await_transaction(&action_signature, ACTION_TIMEOUT)
        .await?;
    assert!(
        action_tx.err.is_some(),
        "the order book action tx must fail on base (the call-handler \
         convention signs the escrow LAST while the handler wants a \
         signer FIRST)"
    );
    let action_logs = action_tx.logs.join("\n");
    assert!(
        action_logs.contains("missing required signature"),
        "the action must fail on the handler signer check, got: \
         {action_logs}"
    );
    let book_on_base = base
        .account(&book)
        .await?
        .ok_or("the order book is not on base after the action")?;
    assert_eq!(
        book_on_base.owner,
        redshift_program::id(),
        "the post-commit action target stays a plain chain account"
    );
    let (bids, asks) = order_book_view(&book_on_base.data)
        .ok_or("the order book data is not valid")?;
    assert!(
        bids.is_empty() && asks.is_empty(),
        "the refused action must not change the order book"
    );
    Ok(())
}

async fn test_size_budget_limits(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let fitting = crate::init_schedulecommit_committees(
        base,
        &payer,
        er.identity(),
        FITTING_ACCOUNTS,
    )
    .await?;
    crate::await_committee_clones(er, &fitting).await;
    let fitting_players: Vec<_> =
        fitting.iter().map(|c| c.player.pubkey()).collect();
    let fitting_pdas: Vec<_> = fitting.iter().map(|c| c.pda).collect();
    let signature = er
        .send(
            &payer,
            &[build::schedule_commit_cpi(
                payer.pubkey(),
                fitting_players,
                true,
                false,
                ScheduleCommitType::CommitAndUndelegate,
                true,
            )],
        )
        .await?;
    crate::assert_commit_receipt(base, er, &signature, &fitting_pdas, true)
        .await?;
    for pda in &fitting_pdas {
        poll_until(BASE_STATE_TIMEOUT, || async {
            matches!(
                base.account(pda).await,
                Ok(Some(acc)) if acc.owner == redshift_program::id()
            )
        })
        .await;
    }

    let refused = crate::init_schedulecommit_committees(
        base,
        &payer,
        er.identity(),
        REFUSED_ACCOUNTS,
    )
    .await?;
    crate::await_committee_clones(er, &refused).await;
    let refused_players: Vec<_> =
        refused.iter().map(|c| c.player.pubkey()).collect();
    let too_large = er
        .send(
            &payer,
            &[build::schedule_commit_cpi(
                payer.pubkey(),
                refused_players,
                true,
                false,
                ScheduleCommitType::CommitAndUndelegate,
                true,
            )],
        )
        .await;
    assert_error_code(
        too_large,
        INTENT_TOO_LARGE_ERR,
        "a commit and undelegate intent over the size budget",
    )?;
    Ok(())
}

#[async_trait(?Send)]
impl Scenario for CommitLimitAndFees {
    fn name(&self) -> &str {
        "redshift/commit_limit_and_fees"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        tokio::try_join!(
            test_sponsored_quota(base, er),
            test_fee_vault_quota(base, er),
            test_size_budget_limits(base, er),
        )?;

        let report = ScenarioReport::ok(self.name())
            .setting("sponsored commit limit", SPONSORED_COMMIT_LIMIT)
            .setting("actual commit limit", ACTUAL_COMMIT_LIMIT)
            .setting("commit fee lamports", COMMIT_FEE_LAMPORTS)
            .setting("action fee lamports", ACTION_FEE_LAMPORTS)
            .setting("fitting intent accounts", FITTING_ACCOUNTS)
            .setting("refused intent accounts", REFUSED_ACCOUNTS);
        Ok(report)
    }
}
