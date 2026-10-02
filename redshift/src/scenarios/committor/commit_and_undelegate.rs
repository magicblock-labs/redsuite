use std::{rc::Rc, time::Duration};

use async_trait::async_trait;
use borsh::BorshDeserialize;
use keypair::Keypair;
use pubkey::Pubkey;
use rand::{
    rngs::{OsRng, StdRng},
    Rng, RngCore, SeedableRng,
};
use redshift_interface::schedulecommit::{
    build, order_book_view, BookUpdate, MainAccount, OrderLevel,
    ScheduleCommitType, FAIL_UNDELEGATION_COUNT, ORDER_BOOK_INIT_SIZE,
};
use redsuite_core::{
    check, check_eq, prep, receipt, BaseCtx, ChainCtx, CheckError, ErCtx,
    Result, Scenario, ScenarioReport,
};
use signature::Signature;
use signer::Signer;

use crate::program::DELEGATION_PROGRAM_ID;

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const ER_WRITE_TIMEOUT: Duration = Duration::from_secs(20);
const REDELEGATE_SETTLE: Duration = Duration::from_secs(2);
const ROLLBACK_SETTLE: Duration = Duration::from_millis(1500);
const MOD_AFTER_REFUSAL: &str =
    "instruction modified data of an account it does not own";
const TWICE_REFUSAL: &str =
    "is required to be writable and delegated in order to be undelegated";
const FOREIGN_REJECTION: &str = "MissingAccount";
const WRITE_REJECTIONS: [&str; 3] = [
    "InvalidWritableAccount",
    "ExternalAccountDataModified",
    "ProgramFailedToComplete",
];

pub struct CommitAndUndelegate;

fn decoded_count(data: &[u8]) -> Result<u64> {
    Ok(MainAccount::try_from_slice(data)?.count)
}

struct LifecycleOutcome {
    commit_base_sigs: usize,
    er_lockout: &'static str,
    base_frozen: &'static str,
}

async fn commit(
    er: &ErCtx,
    payer: &Keypair,
    players: &[Pubkey],
    undelegates: bool,
) -> Result<Signature> {
    er.submit_and_confirm(
        payer,
        &[build::schedule_commit_cpi(
            payer.pubkey(),
            players.to_vec(),
            true,
            false,
            if undelegates {
                ScheduleCommitType::CommitFinalizeAndUndelegate
            } else {
                ScheduleCommitType::CommitFinalize
            },
            true,
        )],
    )
    .await
}

async fn settled(
    base: &BaseCtx,
    er: &ErCtx,
    pdas: &[Pubkey],
    signature: &Signature,
    undelegates: bool,
    count: u64,
) -> Result<receipt::CommitReceipt> {
    let owner = if undelegates {
        redshift_interface::id()
    } else {
        DELEGATION_PROGRAM_ID
    };
    let commit_receipt =
        crate::assert_commit_receipt(base, er, signature, pdas, undelegates)
            .await?;
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
                match base.account(pda).await {
                    Ok(Some(acc)) => match decoded_count(&acc.data) {
                        Ok(seen) if acc.owner == owner && seen == count => {
                            Ok(())
                        }
                        Ok(seen) => {
                            Err(format!("owner {} count {seen}", acc.owner))
                        }
                        Err(error) => Err(format!(
                            "owner {} undecodable state: {error}",
                            acc.owner
                        )),
                    },
                    Ok(None) => Err("absent from base".to_owned()),
                    Err(error) => Err(format!("base read failed: {error}")),
                }
            },
        )
        .await
        .map_err(|error| {
            error.expected(format!("owner {owner} count {count}"))
        })?;
        let on_er = er
            .account(pda)
            .await?
            .ok_or("the er clone is not present after the commit")?;
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
) -> Result<LifecycleOutcome> {
    let committees =
        prep::init_committees(base, payer, er.identity(), count).await?;
    prep::await_committee_clones(er, &committees).await?;
    let players: Vec<_> =
        committees.iter().map(|c| c.player.pubkey()).collect();
    let pdas: Vec<_> = committees.iter().map(|c| c.pda).collect();
    let plain = commit(er, payer, &players, false).await?;
    let commit_base_sigs = settled(base, er, &pdas, &plain, false, 1)
        .await?
        .base_signatures
        .len();

    let signature = commit(er, payer, &players, true).await?;

    let mut er_lockout = "";
    for player in &players {
        let attempt = er
            .submit_and_confirm(er_fee_payer, &[build::increase_count(*player)])
            .await;
        check!(
            attempt.is_err(),
            "an er write must be rejected after undelegation is requested"
        )?;
        er_lockout = crate::rejection_code(
            "the write",
            &WRITE_REJECTIONS,
            &format!("{:?}", attempt.unwrap_err()),
        )?;
    }

    settled(base, er, &pdas, &signature, true, 2).await?;

    for player in &players {
        base.submit_and_confirm(payer, &[build::increase_count(*player)])
            .await?;
    }
    for pda in &pdas {
        let on_base = base
            .account(pda)
            .await?
            .ok_or("the pda is not on base after the chain write")?;
        check_eq!(
            decoded_count(&on_base.data)?,
            3,
            "an undelegated account must accept chain writes"
        )?;
    }

    tokio::time::sleep(REDELEGATE_SETTLE).await;
    let redelegate: Vec<_> = players
        .iter()
        .map(|player| {
            build::delegate_cpi(
                payer.pubkey(),
                *player,
                prep::COMMIT_FREQUENCY_MS,
                Some(er.identity()),
            )
        })
        .collect();
    base.submit_and_confirm(payer, &redelegate).await?;
    for pda in &pdas {
        let on_base = base
            .account(pda)
            .await?
            .ok_or("the pda is not on base after the redelegation")?;
        check_eq!(
            on_base.owner,
            DELEGATION_PROGRAM_ID,
            "dlp must own a redelegated committee on base"
        )?;
    }

    let mut base_frozen = "";
    for player in &players {
        let attempt = base
            .submit_and_confirm(
                base_negative_payer,
                &[build::increase_count(*player)],
            )
            .await;
        check!(
            attempt.is_err(),
            "a chain write must be rejected after the redelegation"
        )?;
        base_frozen = crate::rejection_code(
            "the write",
            &WRITE_REJECTIONS,
            &format!("{:?}", attempt.unwrap_err()),
        )?;
    }

    for committee in &committees {
        check::poll(
            &format!(
                "the er accepts writes to {} after the redelegation",
                committee.pda
            ),
            ER_WRITE_TIMEOUT,
            || async {
                er.submit_and_confirm(
                    er_fee_payer,
                    &[build::increase_count(committee.player.pubkey())],
                )
                .await
                .is_ok()
            },
        )
        .await?;
        let on_er = er
            .account(&committee.pda)
            .await?
            .ok_or("the er clone is not present after the redelegation")?;
        check_eq!(
            decoded_count(&on_er.data)?,
            4,
            "ephem count after the redelegated write"
        )?;
    }

    Ok(LifecycleOutcome {
        commit_base_sigs,
        er_lockout,
        base_frozen,
    })
}

fn random_book_update(seed: u64) -> BookUpdate {
    let mut rng = StdRng::seed_from_u64(seed);
    let bids = (0..rng.gen_range(5..100))
        .map(|_| OrderLevel {
            price: rng.gen_range(75_000..90_000),
            size: rng.gen_range(1..10),
        })
        .collect();
    let asks = (0..rng.gen_range(5..100))
        .map(|_| OrderLevel {
            price: rng.gen_range(125_000..150_000),
            size: rng.gen_range(1..10),
        })
        .collect();
    BookUpdate { bids, asks }
}

fn check_book_matches(
    data: &[u8],
    update: &BookUpdate,
    seed: u64,
    side: &str,
) -> Result<()> {
    let (bids, asks) = order_book_view(data).ok_or_else(|| {
        CheckError::new(format!("{side}: the order book data is not valid"))
    })?;
    check_eq!(
        bids,
        update.bids,
        "{side}: the bids must match the update (seed {seed})"
    )?;
    check_eq!(
        asks,
        update.asks,
        "{side}: the asks must match the update (seed {seed})"
    )?;
    Ok(())
}

async fn order_book_cell(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    commit_type: ScheduleCommitType,
    undelegates: bool,
    seed: u64,
) -> Result<usize> {
    let manager = Keypair::new();
    let (init, book) = build::init_order_book(payer.pubkey(), manager.pubkey());
    let delegate = build::delegate_order_book(
        payer.pubkey(),
        manager.pubkey(),
        prep::COMMIT_FREQUENCY_MS,
        Some(er.identity()),
    );
    base.submit_and_confirm_with(payer, &[&manager], &[init, delegate])
        .await?;
    let on_base = base
        .account(&book)
        .await?
        .ok_or("the order book is not on base after init and delegate")?;
    check_eq!(
        on_base.owner,
        DELEGATION_PROGRAM_ID,
        "dlp must own the delegated order book on base"
    )?;
    check_eq!(
        on_base.data.len(),
        ORDER_BOOK_INIT_SIZE,
        "the order book size on base"
    )?;
    check::poll(
        "the ER clones the delegated order book",
        CLONE_TIMEOUT,
        || async {
            matches!(
                er.account(&book).await,
                Ok(Some(clone)) if clone.data.len() == ORDER_BOOK_INIT_SIZE
            )
        },
    )
    .await?;

    let update = random_book_update(seed);
    let signature = er
        .submit_and_confirm(
            payer,
            &[
                build::update_order_book(
                    payer.pubkey(),
                    manager.pubkey(),
                    update.clone(),
                ),
                build::schedule_commit_for_order_book(
                    payer.pubkey(),
                    manager.pubkey(),
                    commit_type,
                ),
            ],
        )
        .await?;

    let commit_receipt = crate::assert_commit_receipt(
        base,
        er,
        &signature,
        &[book],
        undelegates,
    )
    .await?;

    let on_er = er
        .account(&book)
        .await?
        .ok_or("the order book clone is not present after the commit")?;
    check_book_matches(&on_er.data, &update, seed, "ephem")?;

    let expected_owner = if undelegates {
        redshift_interface::id()
    } else {
        DELEGATION_PROGRAM_ID
    };
    check::poll(
        "the committed order book lands on base with its expected owner",
        BASE_STATE_TIMEOUT,
        || async {
            match base.account(&book).await {
                Ok(Some(acc)) => {
                    acc.owner == expected_owner
                        && order_book_view(&acc.data)
                            .is_some_and(|(bids, _)| bids == update.bids)
                }
                _ => false,
            }
        },
    )
    .await?;
    let on_base = base
        .account(&book)
        .await?
        .ok_or("the order book is not on base after the commit")?;
    check_book_matches(&on_base.data, &update, seed, "base")?;
    check_eq!(
        on_base.owner,
        expected_owner,
        "the order book owner on base after the commit (seed {seed})"
    )?;

    Ok(commit_receipt.base_signatures.len())
}

async fn rejected_intent_cell(
    er: &ErCtx,
    payer: &Rc<Keypair>,
    instruction: instruction::Instruction,
    refusal: &str,
) -> Result<()> {
    let sender = er.sender(payer.clone());
    let signature = sender.submit(&[instruction]).await?;
    let tx = er
        .api()
        .await_transaction(&signature, RECEIPT_TIMEOUT)
        .await?;
    check!(
        tx.err.is_some(),
        "the transaction must fail on the ephemeral"
    )?;
    let observed =
        format!("{}\n{:?}", tx.logs.join("\n"), tx.err.as_ref().unwrap());
    check!(
        observed.contains(refusal),
        "the refusal must name '{refusal}', got: {observed}"
    )?;

    let sent_signature = receipt::receipt_signature_in_logs(&tx.logs)
        .ok_or("the failed tx logs carry no ScheduledCommitSent signature")?;
    tokio::time::sleep(ROLLBACK_SETTLE).await;
    check!(
        er.api().get_transaction(&sent_signature).await?.is_none(),
        "a failed transaction must not schedule a commit"
    )?;
    Ok(())
}

async fn test_lifecycle_cell(
    base: &BaseCtx,
    er: &ErCtx,
    count: usize,
) -> Result<LifecycleOutcome> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let er_fee_payer = prep::delegated_payer(
        base,
        &payer,
        er.identity(),
        crate::PAYER_LAMPORTS,
    )
    .await?;
    let base_negative_payer =
        prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;

    commit_undelegate_lifecycle(
        base,
        er,
        &payer,
        &er_fee_payer,
        &base_negative_payer,
        count,
    )
    .await
}

async fn test_foreign_ownership(base: &BaseCtx, er: &ErCtx) -> Result<Pubkey> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let outsider = prep::delegated_payer(
        base,
        &payer,
        er.identity(),
        crate::PAYER_LAMPORTS,
    )
    .await?;
    let validator = Keypair::new().pubkey();
    for (commit_type, what) in [
        (ScheduleCommitType::CommitFinalize, "commit"),
        (
            ScheduleCommitType::CommitFinalizeAndUndelegate,
            "undelegation",
        ),
    ] {
        let committees =
            prep::init_committees(base, &payer, validator, 1).await?;
        prep::await_committee_clones(er, &committees).await?;
        let attempt = er
            .submit_and_confirm(
                &outsider,
                &[build::schedule_commit_cpi(
                    outsider.pubkey(),
                    vec![committees[0].player.pubkey()],
                    false,
                    true,
                    commit_type,
                    what == "undelegation",
                )],
            )
            .await;

        check!(
            attempt.is_err(),
            "a {what} for an account delegated to another validator must fail"
        )?;
        let error = format!("{:?}", attempt.unwrap_err());
        check!(
            error.contains(FOREIGN_REJECTION),
            "the foreign {what} for {} must be rejected with \
             {FOREIGN_REJECTION}, got {error}",
            committees[0].pda
        )?;
    }
    Ok(validator)
}

async fn test_order_book_cell(
    base: &BaseCtx,
    er: &ErCtx,
    commit_type: ScheduleCommitType,
    undelegates: bool,
    seed: u64,
) -> Result<usize> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    order_book_cell(base, er, &payer, commit_type, undelegates, seed).await
}

async fn test_mod_after_rejection(
    base: &BaseCtx,
    er: &ErCtx,
    count: usize,
) -> Result<()> {
    let payer = Rc::new(prep::funded_payer(base, crate::PAYER_LAMPORTS).await?);
    let committees =
        prep::init_committees(base, &payer, er.identity(), count).await?;
    prep::await_committee_clones(er, &committees).await?;
    let players: Vec<_> =
        committees.iter().map(|c| c.player.pubkey()).collect();
    rejected_intent_cell(
        er,
        &payer,
        build::schedule_commit_and_undelegate_mod_after(
            payer.pubkey(),
            players,
        ),
        MOD_AFTER_REFUSAL,
    )
    .await?;
    for committee in &committees {
        let on_er = er
            .account(&committee.pda)
            .await?
            .ok_or("the er clone is gone after the failed tx")?;
        check_eq!(
            decoded_count(&on_er.data)?,
            0,
            "a failed tx must not modify the committee"
        )?;
    }
    Ok(())
}

async fn test_twice_rejection(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let payer = Rc::new(prep::funded_payer(base, crate::PAYER_LAMPORTS).await?);
    let committees =
        prep::init_committees(base, &payer, er.identity(), 2).await?;
    prep::await_committee_clones(er, &committees).await?;
    let players: Vec<_> =
        committees.iter().map(|c| c.player.pubkey()).collect();
    rejected_intent_cell(
        er,
        &payer,
        build::schedule_commit_and_undelegate_twice(payer.pubkey(), players),
        TWICE_REFUSAL,
    )
    .await
}

async fn test_failed_undelegation_lockout(
    base: &BaseCtx,
    er: &ErCtx,
) -> Result<&'static str> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let er_fee_payer = prep::delegated_payer(
        base,
        &payer,
        er.identity(),
        crate::PAYER_LAMPORTS,
    )
    .await?;

    let committees =
        prep::init_committees(base, &payer, er.identity(), 1).await?;
    prep::await_committee_clones(er, &committees).await?;
    let player = committees[0].player.pubkey();
    let pda = committees[0].pda;

    er.submit_and_confirm(
        &payer,
        &[build::set_count(player, FAIL_UNDELEGATION_COUNT)],
    )
    .await?;
    let on_er = er
        .account(&pda)
        .await?
        .ok_or("the er clone is not present after set_count")?;
    check_eq!(
        decoded_count(&on_er.data)?,
        FAIL_UNDELEGATION_COUNT,
        "the poison count on the ephemeral"
    )?;

    let signature = er
        .submit_and_confirm(
            &payer,
            &[build::schedule_commit_cpi(
                payer.pubkey(),
                vec![player],
                false,
                false,
                ScheduleCommitType::CommitFinalizeAndUndelegate,
                true,
            )],
        )
        .await?;
    crate::assert_commit_receipt(base, er, &signature, &[pda], true).await?;

    check::poll(
        "the poison count lands on base after the patched commit",
        BASE_STATE_TIMEOUT,
        || async {
            matches!(
                base.account(&pda).await,
                Ok(Some(acc))
                    if decoded_count(&acc.data).ok()
                        == Some(FAIL_UNDELEGATION_COUNT)
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
        .submit_and_confirm(&er_fee_payer, &[build::set_count(player, 2222)])
        .await;
    check!(
        attempt.is_err(),
        "the ephemeral must reject writes after the undelegation \
         request even when the base undelegation failed"
    )?;
    crate::rejection_code(
        "the write after the failed undelegation",
        &WRITE_REJECTIONS,
        &format!("{:?}", attempt.unwrap_err()),
    )
}

#[async_trait(?Send)]
impl Scenario for CommitAndUndelegate {
    fn name(&self) -> &str {
        "redshift/commit_and_undelegate"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let seed_commit = OsRng.next_u64();
        let seed_undelegate = OsRng.next_u64();

        let (
            lifecycle_1,
            lifecycle_2,
            foreign_validator,
            sigs_commit,
            sigs_undelegate,
            _,
            _,
            _,
            failed_undelegation_lockout,
        ) = tokio::try_join!(
            test_lifecycle_cell(base, er, 1),
            test_lifecycle_cell(base, er, 2),
            test_foreign_ownership(base, er),
            test_order_book_cell(
                base,
                er,
                ScheduleCommitType::CommitFinalize,
                false,
                seed_commit
            ),
            test_order_book_cell(
                base,
                er,
                ScheduleCommitType::CommitFinalizeAndUndelegate,
                true,
                seed_undelegate
            ),
            test_mod_after_rejection(base, er, 1),
            test_mod_after_rejection(base, er, 2),
            test_twice_rejection(base, er),
            test_failed_undelegation_lockout(base, er),
        )?;

        let report = ScenarioReport::ok(self.name())
            .setting("1-account er lockout rejection", lifecycle_1.er_lockout)
            .setting("1-account base frozen rejection", lifecycle_1.base_frozen)
            .setting("2-account er lockout rejection", lifecycle_2.er_lockout)
            .setting("2-account base frozen rejection", lifecycle_2.base_frozen)
            .setting("1-account commit base sigs", lifecycle_1.commit_base_sigs)
            .setting("2-account commit base sigs", lifecycle_2.commit_base_sigs)
            .setting("foreign validator", foreign_validator)
            .setting("foreign rejection", FOREIGN_REJECTION)
            .setting("commit book seed", seed_commit)
            .setting("commit book base sigs", sigs_commit)
            .setting("undelegate book seed", seed_undelegate)
            .setting("undelegate book base sigs", sigs_undelegate)
            .setting("failed undelegation lockout", failed_undelegation_lockout)
            .setting("commit frequency ms", prep::COMMIT_FREQUENCY_MS);

        Ok(report)
    }
}
