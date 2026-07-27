use std::{rc::Rc, time::Duration};

use async_trait::async_trait;
use borsh::BorshDeserialize;
use keypair::Keypair;
use rand::{
    rngs::{OsRng, StdRng},
    Rng, RngCore, SeedableRng,
};
use redshift_program::schedulecommit::{
    build, order_book_view, BookUpdate, MainAccount, OrderLevel,
    ScheduleCommitType, FAIL_UNDELEGATION_COUNT, ORDER_BOOK_INIT_SIZE,
};
use redsuite_core::{
    assert::poll_until, prep, receipt, BaseCtx, ChainCtx, ErCtx, Result,
    Scenario, ScenarioReport,
};
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
const WRITE_REJECTIONS: [&str; 3] = [
    "InvalidWritableAccount",
    "ExternalAccountDataModified",
    "ProgramFailedToComplete",
];

pub struct CommitAndUndelegate;

fn decoded_count(data: &[u8]) -> Result<u64> {
    Ok(MainAccount::try_from_slice(data)?.count)
}

fn rejection_code(error_text: &str) -> Result<&'static str> {
    WRITE_REJECTIONS
        .into_iter()
        .find(|code| error_text.contains(code))
        .ok_or_else(|| {
            format!("expected one of {WRITE_REJECTIONS:?}, got {error_text}")
                .into()
        })
}

struct LifecycleOutcome {
    er_lockout: &'static str,
    base_frozen: &'static str,
}

async fn commit_undelegate_lifecycle(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    er_fee_payer: &Keypair,
    base_negative_payer: &Keypair,
    count: usize,
) -> Result<LifecycleOutcome> {
    let committees = crate::init_schedulecommit_committees(
        base,
        payer,
        er.identity(),
        count,
    )
    .await?;
    crate::await_committee_clones(er, &committees).await;
    let players: Vec<_> =
        committees.iter().map(|c| c.player.pubkey()).collect();
    let pdas: Vec<_> = committees.iter().map(|c| c.pda).collect();

    let signature = er
        .send(
            payer,
            &[build::schedule_commit_cpi(
                payer.pubkey(),
                players.clone(),
                true,
                false,
                ScheduleCommitType::CommitFinalizeAndUndelegate,
                true,
            )],
        )
        .await?;

    let mut er_lockout = "";
    for player in &players {
        let attempt = er
            .send(er_fee_payer, &[build::increase_count(*player)])
            .await;
        assert!(
            attempt.is_err(),
            "an er write must be rejected after undelegation is requested"
        );
        er_lockout = rejection_code(&format!("{:?}", attempt.unwrap_err()))?;
    }

    let commit_receipt =
        crate::assert_commit_receipt(base, er, &signature, &pdas, true).await?;
    assert_eq!(
        commit_receipt.base_signatures.len(),
        1,
        "a single-stage commit and undelegate must send exactly one base tx"
    );

    for pda in &pdas {
        poll_until(BASE_STATE_TIMEOUT, || async {
            matches!(
                base.account(pda).await,
                Ok(Some(acc)) if acc.owner == redshift_program::id()
            )
        })
        .await;
        let on_base = base
            .account(pda)
            .await?
            .ok_or("the pda is not on base after the undelegation")?;
        assert_eq!(
            decoded_count(&on_base.data)?,
            1,
            "base count after the commit and undelegate"
        );
        let on_er = er
            .account(pda)
            .await?
            .ok_or("the er clone is not present after the undelegation")?;
        assert_eq!(
            decoded_count(&on_er.data)?,
            1,
            "ephem count after the commit and undelegate"
        );
    }

    for player in &players {
        base.send(payer, &[build::increase_count(*player)]).await?;
    }
    for pda in &pdas {
        let on_base = base
            .account(pda)
            .await?
            .ok_or("the pda is not on base after the chain write")?;
        assert_eq!(
            decoded_count(&on_base.data)?,
            2,
            "an undelegated account must accept chain writes"
        );
    }

    tokio::time::sleep(REDELEGATE_SETTLE).await;
    let redelegate: Vec<_> = players
        .iter()
        .map(|player| {
            build::delegate_cpi(
                payer.pubkey(),
                *player,
                crate::COMMIT_FREQUENCY_MS,
                Some(er.identity()),
            )
        })
        .collect();
    base.send(payer, &redelegate).await?;
    for pda in &pdas {
        let on_base = base
            .account(pda)
            .await?
            .ok_or("the pda is not on base after the redelegation")?;
        assert_eq!(
            on_base.owner, DELEGATION_PROGRAM_ID,
            "dlp must own a redelegated committee on base"
        );
    }

    let mut base_frozen = "";
    for player in &players {
        let attempt = base
            .send(base_negative_payer, &[build::increase_count(*player)])
            .await;
        assert!(
            attempt.is_err(),
            "a chain write must be rejected after the redelegation"
        );
        base_frozen = rejection_code(&format!("{:?}", attempt.unwrap_err()))?;
    }

    for committee in &committees {
        poll_until(ER_WRITE_TIMEOUT, || async {
            er.send(
                er_fee_payer,
                &[build::increase_count(committee.player.pubkey())],
            )
            .await
            .is_ok()
        })
        .await;
        let on_er = er
            .account(&committee.pda)
            .await?
            .ok_or("the er clone is not present after the redelegation")?;
        assert_eq!(
            decoded_count(&on_er.data)?,
            3,
            "ephem count after the redelegated write"
        );
    }

    Ok(LifecycleOutcome {
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

fn assert_book_matches(
    data: &[u8],
    update: &BookUpdate,
    seed: u64,
    side: &str,
) {
    let (bids, asks) = order_book_view(data)
        .unwrap_or_else(|| panic!("{side}: the order book data is not valid"));
    assert_eq!(
        bids, update.bids,
        "{side}: the bids must match the update (seed {seed})"
    );
    assert_eq!(
        asks, update.asks,
        "{side}: the asks must match the update (seed {seed})"
    );
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
        crate::COMMIT_FREQUENCY_MS,
        Some(er.identity()),
    );
    base.send_with(payer, &[&manager], &[init, delegate])
        .await?;
    let on_base = base
        .account(&book)
        .await?
        .ok_or("the order book is not on base after init and delegate")?;
    assert_eq!(
        on_base.owner, DELEGATION_PROGRAM_ID,
        "dlp must own the delegated order book on base"
    );
    assert_eq!(
        on_base.data.len(),
        ORDER_BOOK_INIT_SIZE,
        "the order book size on base"
    );
    poll_until(CLONE_TIMEOUT, || async {
        matches!(
            er.account(&book).await,
            Ok(Some(clone)) if clone.data.len() == ORDER_BOOK_INIT_SIZE
        )
    })
    .await;

    let update = random_book_update(seed);
    let signature = er
        .send(
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
    assert_book_matches(&on_er.data, &update, seed, "ephem");

    let expected_owner = if undelegates {
        redshift_program::id()
    } else {
        DELEGATION_PROGRAM_ID
    };
    poll_until(BASE_STATE_TIMEOUT, || async {
        match base.account(&book).await {
            Ok(Some(acc)) => {
                acc.owner == expected_owner
                    && order_book_view(&acc.data)
                        .is_some_and(|(bids, _)| bids == update.bids)
            }
            _ => false,
        }
    })
    .await;
    let on_base = base
        .account(&book)
        .await?
        .ok_or("the order book is not on base after the commit")?;
    assert_book_matches(&on_base.data, &update, seed, "base");
    assert_eq!(
        on_base.owner, expected_owner,
        "the order book owner on base after the commit (seed {seed})"
    );

    Ok(commit_receipt.base_signatures.len())
}

async fn rejected_intent_cell(
    er: &ErCtx,
    payer: &Rc<Keypair>,
    instruction: instruction::Instruction,
    refusal: &str,
) -> Result<()> {
    let sender = er.sender(payer.clone());
    let signature = sender.send(&[instruction]).await?;
    let tx = er
        .api()
        .await_transaction(&signature, RECEIPT_TIMEOUT)
        .await?;
    assert!(
        tx.err.is_some(),
        "the transaction must fail on the ephemeral"
    );
    let observed =
        format!("{}\n{:?}", tx.logs.join("\n"), tx.err.as_ref().unwrap());
    assert!(
        observed.contains(refusal),
        "the refusal must name '{refusal}', got: {observed}"
    );

    let sent_signature = receipt::receipt_signature_in_logs(&tx.logs)
        .ok_or("the failed tx logs carry no ScheduledCommitSent signature")?;
    tokio::time::sleep(ROLLBACK_SETTLE).await;
    assert!(
        er.api().get_transaction(&sent_signature).await?.is_none(),
        "a failed transaction must not schedule a commit"
    );
    Ok(())
}

#[async_trait(?Send)]
impl Scenario for CommitAndUndelegate {
    fn name(&self) -> &str {
        "redshift/commit_and_undelegate"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let payer =
            Rc::new(prep::funded_payer(base, crate::PAYER_LAMPORTS).await?);
        let er_fee_payer = prep::delegated_payer(
            base,
            &payer,
            er.identity(),
            crate::PAYER_LAMPORTS,
        )
        .await?;
        let base_negative_payer =
            prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let mut report = ScenarioReport::ok(self.name());

        for count in [1usize, 2] {
            let outcome = commit_undelegate_lifecycle(
                base,
                er,
                &payer,
                &er_fee_payer,
                &base_negative_payer,
                count,
            )
            .await?;
            report = report
                .setting(
                    format!("{count}-account er lockout rejection"),
                    outcome.er_lockout,
                )
                .setting(
                    format!("{count}-account base frozen rejection"),
                    outcome.base_frozen,
                );
        }

        for (commit_type, undelegates, label) in [
            (ScheduleCommitType::CommitFinalize, false, "commit book"),
            (
                ScheduleCommitType::CommitFinalizeAndUndelegate,
                true,
                "undelegate book",
            ),
        ] {
            let seed = OsRng.next_u64();
            let base_signatures = order_book_cell(
                base,
                er,
                &payer,
                commit_type,
                undelegates,
                seed,
            )
            .await?;
            report = report
                .setting(format!("{label} seed"), seed)
                .setting(format!("{label} base sigs"), base_signatures);
        }

        for count in [1usize, 2] {
            let committees = crate::init_schedulecommit_committees(
                base,
                &payer,
                er.identity(),
                count,
            )
            .await?;
            crate::await_committee_clones(er, &committees).await;
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
                assert_eq!(
                    decoded_count(&on_er.data)?,
                    0,
                    "a failed tx must not modify the committee"
                );
            }
        }

        {
            let committees = crate::init_schedulecommit_committees(
                base,
                &payer,
                er.identity(),
                2,
            )
            .await?;
            crate::await_committee_clones(er, &committees).await;
            let players: Vec<_> =
                committees.iter().map(|c| c.player.pubkey()).collect();
            rejected_intent_cell(
                er,
                &payer,
                build::schedule_commit_and_undelegate_twice(
                    payer.pubkey(),
                    players,
                ),
                TWICE_REFUSAL,
            )
            .await?;
        }

        {
            let committees = crate::init_schedulecommit_committees(
                base,
                &payer,
                er.identity(),
                1,
            )
            .await?;
            crate::await_committee_clones(er, &committees).await;
            let player = committees[0].player.pubkey();
            let pda = committees[0].pda;

            er.send(
                &payer,
                &[build::set_count(player, FAIL_UNDELEGATION_COUNT)],
            )
            .await?;
            let on_er = er
                .account(&pda)
                .await?
                .ok_or("the er clone is not present after set_count")?;
            assert_eq!(
                decoded_count(&on_er.data)?,
                FAIL_UNDELEGATION_COUNT,
                "the poison count on the ephemeral"
            );

            let signature = er
                .send(
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
            crate::assert_commit_receipt(base, er, &signature, &[pda], true)
                .await?;

            poll_until(BASE_STATE_TIMEOUT, || async {
                matches!(
                    base.account(&pda).await,
                    Ok(Some(acc))
                        if decoded_count(&acc.data).ok()
                            == Some(FAIL_UNDELEGATION_COUNT)
                )
            })
            .await;
            let on_base = base
                .account(&pda)
                .await?
                .ok_or("the pda is not on base after the patched commit")?;
            assert_eq!(
                on_base.owner, DELEGATION_PROGRAM_ID,
                "a failed undelegation must leave the account delegated on \
                 base"
            );

            let attempt = er
                .send(&er_fee_payer, &[build::set_count(player, 2222)])
                .await;
            assert!(
                attempt.is_err(),
                "the ephemeral must reject writes after the undelegation \
                 request even when the base undelegation failed"
            );
            let lockout =
                rejection_code(&format!("{:?}", attempt.unwrap_err()))?;
            report = report.setting("failed undelegation lockout", lockout);
        }

        Ok(report.setting("commit frequency ms", crate::COMMIT_FREQUENCY_MS))
    }
}
