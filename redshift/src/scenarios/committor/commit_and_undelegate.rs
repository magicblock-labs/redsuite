use std::{rc::Rc, time::Duration};

use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, prep,
    programs::{CellHeader, CellId, SettlementMode, UndelegationOutcome, MAGIC, REDSHIFT},
    receipt::{self, CommitReceipt},
    BaseCtx, ChainCtx, DynError, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use tokio::time;

use crate::{
    program::DELEGATION_PROGRAM_ID, LARGE_ACCOUNT_SPACE, LOCKOUT_REJECTIONS, PAYER_LAMPORTS,
};

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const ER_WRITE_TIMEOUT: Duration = Duration::from_secs(20);
const REDELEGATE_SETTLE: Duration = Duration::from_secs(2);
const ROLLBACK_SETTLE: Duration = Duration::from_millis(1500);
const MOD_AFTER_REFUSAL: &str = "instruction modified data of an account it does not own";
const TWICE_REFUSAL: &str = "is required to be writable and delegated in order to be undelegated";
const FOREIGN_REJECTION: &str = "MissingAccount";

pub struct CommitAndUndelegate {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

async fn base_requested_undelegation(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let payer = base.funded_payer(PAYER_LAMPORTS).await?;
    let cells = base.cells(REDSHIFT, &payer).create_owned(er.identity(), 1).await?;
    er.await_cells(&cells).await?;
    let cell = &cells[0];
    let request = sdk::pda::undelegation_request_pda_from_delegated_account(&cell.address);
    check!(
        base.account(&request).await?.is_none(),
        "request {request} starts absent"
    )?;
    er.submit_and_confirm(&payer, &[REDSHIFT.batch().add(&[cell.address], 1).build()])
        .await?;
    let snapshot = er.account(&cell.address).await?.ok_or("missing ER cell")?;
    check_eq!(
        decoded_count(&snapshot.data)?,
        1,
        "latest ER state before base request"
    )?;
    check_eq!(
        decoded_count(&base.account(&cell.address).await?.ok_or("missing base cell")?.data)?,
        0,
        "the request must settle an ER write absent from base"
    )?;
    let signature = base
        .submit_and_confirm(
            &payer,
            &[REDSHIFT
                .request_undelegation(payer.pubkey(), CellId::new(cell.authority.pubkey(), 0))],
        )
        .await?;
    check::poll_for(
        &format!(
            "base request {signature} settles {} and removes {request}",
            cell.address
        ),
        BASE_STATE_TIMEOUT,
        || async {
            let account = base.account(&cell.address).await?.ok_or("missing cell")?;
            check_eq!(
                account.owner,
                redshift_interface::ID,
                "base-requested ownership restored"
            )?;
            check_eq!(
                account.data,
                snapshot.data,
                "base-requested undelegation settles the latest bytes"
            )?;
            check_eq!(
                account.lamports,
                snapshot.lamports,
                "base-requested undelegation preserves lamports"
            )?;
            check!(
                base.account(&request).await?.is_none(),
                "completed request {request} is removed"
            )?;
            Ok::<_, DynError>(())
        },
    )
    .await?;
    Ok(())
}

fn decoded_count(data: &[u8]) -> Result<u64> {
    Ok(CellHeader::try_decode(data)?.value)
}

async fn commit(
    er: &ErCtx,
    payer: &Keypair,
    cells: &[Pubkey],
    mode: SettlementMode,
) -> Result<Signature> {
    er.submit_and_confirm(
        payer,
        &[REDSHIFT.batch().add(cells, 1).settle(payer.pubkey(), cells, mode).build()],
    )
    .await
}

async fn settled(
    base: &BaseCtx,
    er: &ErCtx,
    pdas: &[Pubkey],
    signature: &Signature,
    mode: SettlementMode,
    count: u64,
) -> Result<CommitReceipt> {
    let undelegates = matches!(mode, SettlementMode::Undelegate);
    let owner = if undelegates { redshift_interface::id() } else { DELEGATION_PROGRAM_ID };
    let commit_receipt = er.check_commit(base, signature, pdas, undelegates).await?;
    if !commit_receipt.failure_is_duplicate_rejection() {
        check_eq!(
            commit_receipt.base_signatures.len(),
            1,
            "a single-stage commit must send exactly one base tx"
        )?;
    }
    for pda in pdas {
        check::poll_for(
            &format!("{pda} settles on base after the commit"),
            BASE_STATE_TIMEOUT,
            || async {
                let account = base.account(pda).await?.ok_or("settled cell absent from base")?;
                check_eq!(account.owner, owner, "settled cell owner")?;
                check_eq!(decoded_count(&account.data)?, count, "settled cell value")?;
                Ok::<_, DynError>(())
            },
        )
        .await
        .map_err(|error| error.expected(format!("owner {owner} count {count}")))?;
        let on_er = er.account(pda).await?.ok_or("the er clone is not present after the commit")?;
        check_eq!(
            decoded_count(&on_er.data)?,
            count,
            "ephem count after the commit for {pda}"
        )?;
    }
    Ok(commit_receipt)
}

async fn commit_undelegate_lifecycle(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    er_fee_payer: &Keypair,
    base_negative_payer: &Keypair,
    count: usize,
) -> Result<()> {
    let cells = base.cells(REDSHIFT, payer).create_owned(er.identity(), count).await?;
    er.await_cells(&cells).await?;
    let pdas: Vec<_> = cells.iter().map(|c| c.address).collect();
    let plain = commit(er, payer, &pdas, SettlementMode::Commit).await?;
    settled(base, er, &pdas, &plain, SettlementMode::Commit, 1).await?;

    let signature = commit(er, payer, &pdas, SettlementMode::Undelegate).await?;

    for &pda in &pdas {
        let attempt = er
            .submit_and_confirm(er_fee_payer, &[REDSHIFT.batch().add(&[pda], 1).build()])
            .await;
        let error = check::expect_error(attempt, "ER write rejected after undelegation request")?;
        crate::rejection_code("the write", &LOCKOUT_REJECTIONS, &format!("{error:?}"))?;
    }

    settled(base, er, &pdas, &signature, SettlementMode::Undelegate, 2).await?;

    for &pda in &pdas {
        base.submit_and_confirm(payer, &[REDSHIFT.batch().add(&[pda], 1).build()])
            .await?;
    }
    for pda in &pdas {
        let on_base =
            base.account(pda).await?.ok_or("the pda is not on base after the chain write")?;
        check_eq!(
            decoded_count(&on_base.data)?,
            3,
            "an undelegated account must accept chain writes"
        )?;
    }

    time::sleep(REDELEGATE_SETTLE).await;
    let redelegate: Vec<_> = cells
        .iter()
        .map(|cell| {
            REDSHIFT.delegate(
                payer.pubkey(),
                CellId::new(cell.authority.pubkey(), 0),
                Some(er.identity()),
                prep::COMMIT_FREQUENCY_MS,
            )
        })
        .collect();
    let signers: Vec<_> = cells.iter().map(|cell| &cell.authority).collect();
    base.submit_and_confirm_with(payer, &signers, &redelegate).await?;
    for pda in &pdas {
        let on_base = base
            .account(pda)
            .await?
            .ok_or("the pda is not on base after the redelegation")?;
        check_eq!(
            on_base.owner,
            DELEGATION_PROGRAM_ID,
            "dlp must own a redelegated cell on base"
        )?;
    }

    for &pda in &pdas {
        let attempt = base
            .submit_and_confirm(
                base_negative_payer,
                &[REDSHIFT.batch().add(&[pda], 1).build()],
            )
            .await;
        let error = check::expect_error(attempt, "base write rejected after redelegation")?;
        crate::rejection_code("the write", &LOCKOUT_REJECTIONS, &format!("{error:?}"))?;
    }

    for cell in &cells {
        check::poll(
            &format!(
                "the er accepts writes to {} after the redelegation",
                cell.address
            ),
            ER_WRITE_TIMEOUT,
            || async {
                er.submit_and_confirm(
                    er_fee_payer,
                    &[REDSHIFT.batch().add(&[cell.address], 1).build()],
                )
                .await
                .is_ok()
            },
        )
        .await?;
        let on_er = er
            .account(&cell.address)
            .await?
            .ok_or("the er clone is not present after the redelegation")?;
        check_eq!(
            decoded_count(&on_er.data)?,
            4,
            "ephem count after the redelegated write"
        )?;
    }

    Ok(())
}

async fn large_account_cell(base: &BaseCtx, er: &ErCtx, commit_type: SettlementMode) -> Result<()> {
    let undelegates = matches!(commit_type, SettlementMode::Undelegate);
    let payer = base.funded_payer(PAYER_LAMPORTS).await?;
    let cells = base
        .cells(REDSHIFT, &payer)
        .create_owned_sized(er.identity(), 1, LARGE_ACCOUNT_SPACE)
        .await?;
    let cell = &cells[0];
    let pda = cell.address;
    let on_base = base.account(&pda).await?.ok_or("large account missing after delegation")?;
    check_eq!(
        on_base.data.len(),
        LARGE_ACCOUNT_SPACE as usize,
        "large account size on base"
    )?;
    er.await_clones(&[pda], LARGE_ACCOUNT_SPACE as usize, CLONE_TIMEOUT).await?;

    let seed = u64::from(undelegates) + 1;
    let mut expected = on_base.data;
    CellHeader::fill_payload(&mut expected, seed)?;
    let mut header = CellHeader::try_decode(&expected)?;
    header.revision += 1;
    header.write(&mut expected)?;
    let signature = er
        .submit_and_confirm(
            &payer,
            &[
                REDSHIFT.batch().fill(&[pda], seed, 1).require_signer(payer.pubkey()).build(),
                REDSHIFT.batch().settle(payer.pubkey(), &[pda], commit_type).build(),
            ],
        )
        .await?;
    er.check_commit(base, &signature, &[pda], undelegates).await?;
    let on_er = er.account(&pda).await?.ok_or("large account missing after commit")?;
    check!(
        on_er.data == expected,
        "ER must retain the entire large-account payload after commit"
    )?;
    let expected_owner = if undelegates { redshift_interface::id() } else { DELEGATION_PROGRAM_ID };
    check::poll(
        "large-account data and ownership settle on base",
        BASE_STATE_TIMEOUT,
        || async {
            matches!(base.account(&pda).await, Ok(Some(account))
                if account.owner == expected_owner && account.data == expected)
        },
    )
    .await
    .map_err(Into::into)
}

async fn rejected_intent_cell(
    er: &ErCtx,
    payer: &Rc<Keypair>,
    instruction: Instruction,
    refusal: &str,
) -> Result<()> {
    let sender = er.sender(payer.clone());
    let signature = sender.submit(&[instruction]).await?;
    let tx = er.api().await_transaction(&signature, RECEIPT_TIMEOUT).await?;
    let error = tx.err.as_ref().ok_or_else(|| format!("{signature}: expected ER rejection"))?;
    let observed = format!("{}\n{error:?}", tx.logs.join("\n"));
    check!(
        observed.contains(refusal),
        "the refusal must name '{refusal}', got: {observed}"
    )?;

    let sent_signature = receipt::receipt_signature_in_logs(&tx.logs)
        .ok_or("the failed tx logs carry no ScheduledCommitSent signature")?;
    time::sleep(ROLLBACK_SETTLE).await;
    check!(
        er.api().get_transaction(&sent_signature).await?.is_none(),
        "a failed transaction must not schedule a commit"
    )?;
    Ok(())
}

async fn test_lifecycle_cell(base: &BaseCtx, er: &ErCtx, count: usize) -> Result<()> {
    let payer = base.funded_payer(PAYER_LAMPORTS).await?;
    let er_fee_payer = base.delegated_payer(&payer, er.identity(), PAYER_LAMPORTS).await?;
    let base_negative_payer = base.funded_payer(PAYER_LAMPORTS).await?;

    commit_undelegate_lifecycle(base, er, &payer, &er_fee_payer, &base_negative_payer, count).await
}

async fn test_foreign_ownership(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let payer = base.funded_payer(PAYER_LAMPORTS).await?;
    let outsider = base.delegated_payer(&payer, er.identity(), PAYER_LAMPORTS).await?;
    let validator = Keypair::new().pubkey();
    for (commit_type, what) in
        [(SettlementMode::Commit, "commit"), (SettlementMode::Undelegate, "undelegation")]
    {
        let cells = base.cells(REDSHIFT, &payer).create_owned(validator, 1).await?;
        er.await_cells(&cells).await?;
        let attempt = er
            .submit_and_confirm(
                &outsider,
                &[REDSHIFT
                    .batch()
                    .settle(
                        outsider.pubkey(),
                        &[cells[0].address, outsider.pubkey()],
                        commit_type,
                    )
                    .build()],
            )
            .await;

        let error = check::expect_error(attempt, &format!("foreign {what} rejected"))?;
        let error = format!("{error:?}");
        check!(
            error.contains(FOREIGN_REJECTION),
            "the foreign {what} for {} must be rejected with \
             {FOREIGN_REJECTION}, got {error}",
            cells[0].address
        )?;
    }
    Ok(())
}

async fn test_mod_after_rejection(base: &BaseCtx, er: &ErCtx, count: usize) -> Result<()> {
    let payer = Rc::new(base.funded_payer(PAYER_LAMPORTS).await?);
    let cells = base.cells(REDSHIFT, &payer).create_owned(er.identity(), count).await?;
    er.await_cells(&cells).await?;
    let addresses: Vec<_> = cells.iter().map(|cell| cell.address).collect();
    rejected_intent_cell(
        er,
        &payer,
        REDSHIFT
            .batch()
            .settle(payer.pubkey(), &addresses, SettlementMode::Undelegate)
            .add(&addresses, 1)
            .build(),
        MOD_AFTER_REFUSAL,
    )
    .await?;
    for cell in &cells {
        let on_er = er
            .account(&cell.address)
            .await?
            .ok_or("the er clone is gone after the failed tx")?;
        check_eq!(
            decoded_count(&on_er.data)?,
            0,
            "a failed tx must not modify the cell"
        )?;
    }
    Ok(())
}

async fn test_twice_rejection(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let payer = Rc::new(base.funded_payer(PAYER_LAMPORTS).await?);
    let cells = base.cells(REDSHIFT, &payer).create_owned(er.identity(), 2).await?;
    er.await_cells(&cells).await?;
    let addresses: Vec<_> = cells.iter().map(|cell| cell.address).collect();
    let signers: Vec<_> =
        cells.iter().map(|cell| CellId::new(cell.authority.pubkey(), 0)).collect();
    let mut repeated = MAGIC.commit(payer.pubkey(), None, &addresses, SettlementMode::Undelegate);
    for meta in repeated.accounts.iter_mut().skip(2) {
        meta.is_signer = true;
    }
    let instruction = REDSHIFT
        .batch()
        .settle(payer.pubkey(), &addresses, SettlementMode::Undelegate)
        .invoke_signed(repeated, &signers)
        .build();
    rejected_intent_cell(er, &payer, instruction, TWICE_REFUSAL).await
}

async fn test_failed_undelegation_lockout(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let payer = base.funded_payer(PAYER_LAMPORTS).await?;
    let er_fee_payer = base.delegated_payer(&payer, er.identity(), PAYER_LAMPORTS).await?;

    let cells = base.cells(REDSHIFT, &payer).create_owned(er.identity(), 1).await?;
    er.await_cells(&cells).await?;
    let pda = cells[0].address;

    er.submit_and_confirm(
        &payer,
        &[REDSHIFT.batch().undelegation(&[pda], UndelegationOutcome::Reject).build()],
    )
    .await?;
    let on_er = er
        .account(&pda)
        .await?
        .ok_or("ER cell missing after setting rejection policy")?;
    check_eq!(
        CellHeader::try_decode(&on_er.data)?.undelegation,
        UndelegationOutcome::Reject,
        "explicit undelegation rejection on the ER"
    )?;

    let signature = er
        .submit_and_confirm(
            &payer,
            &[REDSHIFT
                .batch()
                .settle(payer.pubkey(), &[pda], SettlementMode::Undelegate)
                .build()],
        )
        .await?;
    er.check_commit(base, &signature, &[pda], true).await?;

    check::poll(
        "the rejection policy settles on base",
        BASE_STATE_TIMEOUT,
        || async {
            matches!(
                base.account(&pda).await,
                Ok(Some(acc))
                    if CellHeader::try_decode(&acc.data).ok().map(|cell| cell.undelegation)
                        == Some(UndelegationOutcome::Reject)
            )
        },
    )
    .await?;
    let on_base = base
        .account(&pda)
        .await?
        .ok_or("the pda is not on base after the patched commit")?;
    check_eq!(
        on_base.owner,
        DELEGATION_PROGRAM_ID,
        "a failed undelegation must leave the account delegated on \
         base"
    )?;

    let attempt = er
        .submit_and_confirm(&er_fee_payer, &[REDSHIFT.batch().set(&[pda], 2222).build()])
        .await;
    let error = check::expect_error(attempt, "ER lockout survives failed base undelegation")?;
    crate::rejection_code(
        "the write after the failed undelegation",
        &LOCKOUT_REJECTIONS,
        &format!("{error:?}"),
    )?;
    Ok(())
}

impl Scenario for CommitAndUndelegate {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er } = self;

        tokio::try_join!(
            crate::phase(
                "base-requested undelegation",
                base_requested_undelegation(&base, &er)
            ),
            test_lifecycle_cell(&base, &er, 1),
            test_lifecycle_cell(&base, &er, 2),
            test_foreign_ownership(&base, &er),
            crate::phase(
                "large account commit",
                large_account_cell(&base, &er, SettlementMode::Commit),
            ),
            crate::phase(
                "large account undelegation",
                large_account_cell(&base, &er, SettlementMode::Undelegate),
            ),
            test_mod_after_rejection(&base, &er, 1),
            test_mod_after_rejection(&base, &er, 2),
            test_twice_rejection(&base, &er),
            test_failed_undelegation_lockout(&base, &er),
        )?;
        Ok(())
    }
}
