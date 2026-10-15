use std::{rc::Rc, time::Duration};

use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::flexi::{
    build as flexi, FlexiCounter, IntentMode, ACTION_PRIZE, ACTOR_ESCROW_INDEX,
};
use redsuite_core::{
    api::custom_error_code, check, check_eq, dlp, prep, receipt, BaseCtx,
    ChainCtx, ErCtx, Result,
};
use signature::Signature;
use signer::Signer;

const TIMEOUT: Duration = Duration::from_secs(60);
const PAID_COMMIT_LIMIT: u64 = 25;
const ACTION_FEE: u64 = 5_000;
const CALLBACK_FEE: u64 = 5_000;

async fn redelegate(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    phase: &str,
) -> Result<()> {
    base.submit_and_confirm(
        payer,
        &[
            flexi::delegate_counter(
                payer.pubkey(),
                prep::COMMIT_FREQUENCY_MS,
                Some(er.identity()),
            ),
            instruction::Instruction {
                program_id: redshift_interface::ID,
                accounts: vec![],
                data: redshift_interface::log_msg_data(phase),
            },
        ],
    )
    .await?;
    Ok(())
}

pub(super) async fn run(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let delegated = async |key: &Pubkey| {
        super::await_delegated(
            er,
            key,
            TIMEOUT,
            &format!("{key} is delegated locally"),
        )
        .await
    };
    let settled =
        async |signature: &Signature, accounts: &[Pubkey], undelegates| {
            let receipt = crate::assert_commit_receipt(
                base,
                er,
                signature,
                accounts,
                undelegates,
            )
            .await?;
            super::require_settled(&receipt, "scheduled intent")
        };
    let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let payers = prep::funded_payers(base, 3, crate::PAYER_LAMPORTS).await?;
    let keys: Vec<_> = payers.iter().map(Signer::pubkey).collect();
    let mut counters = Vec::new();
    let mut expected = Vec::new();
    for payer in &payers {
        let (counter, setup) = prep::flexi_counter(
            payer.pubkey(),
            "scheduled-intents",
            er.identity(),
            prep::COMMIT_FREQUENCY_MS,
        );
        base.submit_and_confirm(
            payer,
            &[
                setup[0].clone(),
                setup[1].clone(),
                dlp::top_up_ephemeral_balance(
                    &payer.pubkey(),
                    500_000_000,
                    ACTOR_ESCROW_INDEX,
                ),
            ],
        )
        .await?;
        expected.push(FlexiCounter::try_decode(
            &base.account(&counter).await?.ok_or("counter missing")?.data,
        )?);
        counters.push(counter);
    }
    let mut is_delegated = [true; 3];
    use IntentMode::{Commit, Finalize, Undelegate};
    for (label, modes, additions, actions) in [
        ("single commit action", vec![Commit], vec![101], true),
        ("repeated commit action", vec![Commit], vec![2], true),
        ("undelegate action", vec![Undelegate(-100)], vec![0], true),
        (
            "action after redelegation",
            vec![Undelegate(102)],
            vec![0],
            true,
        ),
        (
            "two-payer commit actions",
            vec![Commit, Commit],
            vec![0, 200],
            true,
        ),
        (
            "two-payer undelegate actions",
            vec![Undelegate(-50), Undelegate(25)],
            vec![0, 0],
            true,
        ),
        (
            "mixed commit/undelegate bundle",
            vec![Commit, Commit, Undelegate(-25)],
            vec![0, 0, 200],
            true,
        ),
        (
            "mixed commit/finalize bundle",
            vec![Commit, Finalize],
            vec![0, 0],
            false,
        ),
    ] {
        crate::phase(label, async {
            let n = modes.len();
            let destination = Keypair::new().pubkey();
            for i in 0..n {
                if !is_delegated[i] {
                    redelegate(base, er, &payers[i], label).await?;
                    is_delegated[i] = true;
                }
                delegated(&counters[i]).await?;
                check::poll_for(
                    &format!("{} restored for {label}", counters[i]),
                    TIMEOUT,
                    || async {
                        let account = er
                            .account(&counters[i])
                            .await?
                            .ok_or("ER counter missing")?;
                        check_eq!(
                            account.owner,
                            redshift_interface::ID,
                            "ER counter ownership"
                        )?;
                        check_eq!(
                            FlexiCounter::try_decode(&account.data)?,
                            expected[i],
                            "ER sees the latest counter before another intent"
                        )?;
                        Ok::<_, redsuite_core::DynError>(())
                    },
                )
                .await?;
                if additions[i] != 0 {
                    er.submit_and_confirm(
                        &payers[i],
                        &[flexi::add(keys[i], additions[i])],
                    )
                    .await?;
                    expected[i].count += additions[i] as u64;
                    expected[i].updates += 1;
                }
            }
            let signature = er
                .submit_and_confirm_with(
                    &payers[0],
                    &payers[1..n].iter().collect::<Vec<_>>(),
                    &[flexi::schedule_intents(
                        &keys[..n],
                        er.identity(),
                        destination,
                        modes.clone(),
                        actions,
                    )],
                )
                .await?;
            let undelegates =
                modes.iter().any(|mode| matches!(mode, Undelegate(_)));
            settled(&signature, &counters[..n], undelegates).await?;
            let mut prizes = 0;
            for (i, mode) in modes.iter().enumerate() {
                prizes += usize::from(actions);
                if let Undelegate(change) = mode {
                    expected[i].count = expected[i]
                        .count
                        .checked_add_signed(*change)
                        .ok_or("counter model underflow")?;
                    expected[i].updates += 1;
                    is_delegated[i] = false;
                    prizes += 1;
                }
                let account = base
                    .account(&counters[i])
                    .await?
                    .ok_or("committed counter missing")?;
                check_eq!(
                    FlexiCounter::try_decode(&account.data)?,
                    expected[i],
                    "{label} {signature}: exact counter {}",
                    counters[i]
                )?;
                check_eq!(
                    account.owner,
                    if is_delegated[i] {
                        dlp::dlp_id()
                    } else {
                        redshift_interface::ID
                    },
                    "{label}: base ownership of {}",
                    counters[i]
                )?;
            }
            check_eq!(
                base.api().get_balance(&destination).await?,
                prizes as u64 * ACTION_PRIZE,
                "{label} {signature}: exactly one prize per routed action"
            )?;
            Ok(())
        })
        .await?;
    }

    crate::phase("sponsored quota and undelegation", async {
        let payer = &payers[0];
        let initial_nonce = crate::last_commit_id(base, &counters[0]).await?;
        check!(
            initial_nonce < receipt::SPONSORED_COMMIT_LIMIT,
            "quota preparation starts below the sponsored limit"
        )?;
        for nonce in initial_nonce..receipt::SPONSORED_COMMIT_LIMIT {
            let signature = er
                .submit_and_confirm(
                    payer,
                    &[
                        flexi::add(keys[0], nonce as u8 + 1),
                        flexi::schedule_intents(
                            &keys[..1],
                            er.identity(),
                            keys[1],
                            vec![Commit],
                            false,
                        ),
                    ],
                )
                .await?;
            settled(&signature, &counters[..1], false).await?;
            check_eq!(
                crate::last_commit_id(base, &counters[0]).await?,
                nonce + 1,
                "sponsored commit advances exactly one nonce"
            )?;
        }
        let before = er.account(&counters[0]).await?;
        let signature = er
            .sender(Rc::new(prep::payer_from_bytes(&payer.to_bytes())))
            .submit(&[
                flexi::add(keys[0], 99),
                flexi::schedule_intents(
                    &keys[..1],
                    er.identity(),
                    keys[1],
                    vec![Commit],
                    false,
                ),
            ])
            .await?;
        let tx = er.api().await_transaction(&signature, TIMEOUT).await?;
        check_eq!(
            tx.err.as_ref().and_then(custom_error_code),
            Some(receipt::COMMIT_LIMIT_ERR),
            "{signature}: precise sponsored-limit refusal"
        )?;
        check_eq!(
            er.account(&counters[0]).await?,
            before,
            "quota refusal rolls back the preceding write"
        )?;
        let signature = er
            .submit_and_confirm(
                payer,
                &[flexi::schedule_intents(
                    &keys[..1],
                    er.identity(),
                    keys[1],
                    vec![Undelegate(0)],
                    false,
                )],
            )
            .await?;
        settled(&signature, &counters[..1], true).await?;
        let account = base
            .account(&counters[0])
            .await?
            .ok_or("undelegated counter missing")?;
        check_eq!(
            account.owner,
            redshift_interface::ID,
            "quota exhaustion still permits undelegation"
        )?;
        check_eq!(
            account.data,
            before.ok_or("ER counter missing")?.data,
            "quota undelegation settles unchanged bytes"
        )?;
        redelegate(base, er, payer, "after sponsored quota").await?;
        Ok(())
    })
    .await?;

    prep::delegate_payer(base, &funder, &payers[0], er.identity()).await?;
    delegated(&keys[0]).await?;
    delegated(&counters[0]).await?;
    let vault = dlp::magic_fee_vault_pda(&er.identity());
    crate::phase("paid commit threshold and combined action charge", async {
        let initial_nonce = crate::last_commit_id(base, &counters[0]).await?;
        check!(
            initial_nonce < PAID_COMMIT_LIMIT,
            "paid threshold starts above sponsored coverage"
        )?;
        for nonce in initial_nonce..=PAID_COMMIT_LIMIT + 1 {
            let actions = nonce == PAID_COMMIT_LIMIT + 1;
            let destination = Keypair::new().pubkey();
            let payer_before = er.api().get_balance(&keys[0]).await?;
            let vault_before = er.api().get_balance(&vault).await?;
            let signature = er
                .submit_and_confirm(
                    &payers[0],
                    &[
                        flexi::add(keys[0], nonce as u8 + 1),
                        flexi::schedule_intents(
                            &keys[..1],
                            er.identity(),
                            destination,
                            vec![Commit],
                            actions,
                        ),
                    ],
                )
                .await?;
            settled(&signature, &counters[..1], false).await?;
            let fee = if nonce >= PAID_COMMIT_LIMIT {
                sdk::dlp_api::consts::COMMIT_FEE_LAMPORTS
            } else {
                0
            } + if actions { ACTION_FEE } else { 0 };
            check_eq!(
                er.api().get_balance(&keys[0]).await?,
                payer_before - fee,
                "{signature}: exact payer debit at nonce {nonce}"
            )?;
            check_eq!(
                er.api().get_balance(&vault).await?,
                vault_before + fee,
                "{signature}: exact vault credit at nonce {nonce}"
            )?;
            check_eq!(
                crate::last_commit_id(base, &counters[0]).await?,
                nonce + 1,
                "paid commit nonce"
            )?;
            check_eq!(
                base.api().get_balance(&destination).await?,
                if actions { ACTION_PRIZE } else { 0 },
                "combined charge retains action execution"
            )?;
        }
        Ok(())
    })
    .await?;

    prep::delegate_payer(base, &funder, &payers[1], er.identity()).await?;
    delegated(&keys[1]).await?;
    for fail in [false, true] {
        crate::phase(
            if fail {
                "transfer callback refund"
            } else {
                "transfer callback success"
            },
            async {
                const AMOUNT: u64 = 890_880;
                let destination = Keypair::new().pubkey();
                let before = er
                    .account(&counters[1])
                    .await?
                    .ok_or("transfer counter missing")?;
                let state = FlexiCounter::try_decode(&before.data)?;
                let payer_before = er.api().get_balance(&keys[1]).await?;
                let vault_before = er.api().get_balance(&vault).await?;
                let signature = er
                    .submit_and_confirm(
                        &payers[1],
                        &[flexi::transfer_intent(
                            keys[1],
                            er.identity(),
                            destination,
                            AMOUNT,
                            fail,
                        )],
                    )
                    .await?;
                settled(&signature, &counters[1..2], false).await?;
                check::poll_for(
                    &format!("{signature}: callback completes exactly once"),
                    TIMEOUT,
                    || async {
                        let account = er
                            .account(&counters[1])
                            .await?
                            .ok_or("callback counter missing")?;
                        let actual = FlexiCounter::try_decode(&account.data)?;
                        check_eq!(
                            actual.updates,
                            state.updates + 1,
                            "callback delivery count"
                        )?;
                        check_eq!(
                            actual.count,
                            state.count + u64::from(fail),
                            "callback success/failure result"
                        )?;
                        check_eq!(
                            account.lamports,
                            before.lamports + if fail { 0 } else { AMOUNT },
                            "callback retains or refunds prepayment"
                        )?;
                        check_eq!(
                            er.api().get_balance(&keys[1]).await?,
                            payer_before
                                - ACTION_FEE
                                - CALLBACK_FEE
                                - if fail { 0 } else { AMOUNT },
                            "exact payer balance after callback"
                        )?;
                        Ok::<_, redsuite_core::DynError>(())
                    },
                )
                .await?;
                check_eq!(
                    base.api().get_balance(&destination).await?,
                    if fail { 0 } else { AMOUNT },
                    "{signature}: transferred amount or zero on rollback"
                )?;
                check_eq!(
                    er.api().get_balance(&vault).await?,
                    vault_before + ACTION_FEE + CALLBACK_FEE,
                    "{signature}: action and callback fees are retained"
                )?;
                Ok(())
            },
        )
        .await?;
    }
    Ok(())
}
