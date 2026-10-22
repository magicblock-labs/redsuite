use std::{rc::Rc, time::Duration};

use anyhow::{ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use magic_api::args::{CommitTypeArgs, MagicIntentBundleArgs};
use pubkey::Pubkey;
use redsuite_core::{
    api::custom_error_code,
    check, dlp, prep,
    programs::{
        Action, CellHeader, CellId, Commit, IntentBundle, Refund, ResultCallback, SettlementMode,
        MAGIC, REDSHIFT,
    },
    receipt, BaseCtx, ChainCtx, DynError, ErCtx, Result,
};
use sdk::dlp_api::consts::COMMIT_FEE_LAMPORTS;
use signature::Signature;
use signer::Signer;

use crate::PAYER_LAMPORTS;

const TIMEOUT: Duration = Duration::from_secs(60);
const PAID_COMMIT_LIMIT: u64 = 25;
const ACTION_FEE: u64 = 5_000;
const CALLBACK_FEE: u64 = 5_000;
const TRANSFER_LAMPORTS: u64 = 1_000_000;
const ACTION_COMPUTE_UNITS: u32 = 100_000;

#[derive(Clone, Copy)]
enum Mode {
    Commit,
    Undelegate(i64),
    Finalize,
}

fn intents(
    authorities: &[Pubkey],
    validator: Pubkey,
    destination: Pubkey,
    modes: &[Mode],
    actions: bool,
) -> Instruction {
    let cells = REDSHIFT.primaries(authorities);
    if matches!(modes, [Mode::Commit, Mode::Finalize]) {
        let args = MagicIntentBundleArgs {
            commit: Some(CommitTypeArgs::Standalone(vec![3])),
            commit_finalize: Some(CommitTypeArgs::Standalone(vec![4])),
            ..Default::default()
        };
        return REDSHIFT
            .batch()
            .invoke(MAGIC.intents(
                authorities[0],
                dlp::magic_fee_vault_pda(&validator),
                &cells,
                args,
            ))
            .build();
    }
    let mut bundle = IntentBundle::new(authorities[0]);
    bundle.validator = Some(validator);
    for ((&authority, &cell), &mode) in authorities.iter().zip(&cells).zip(modes) {
        let settlement = match mode {
            Mode::Undelegate(_) => SettlementMode::Undelegate,
            _ => SettlementMode::Commit,
        };
        let mut commit = Commit::new(settlement, &[cell]);
        if actions {
            let transfer = REDSHIFT
                .batch()
                .transfer_from_escrow(destination, TRANSFER_LAMPORTS)
                .action(cell, dlp::dlp_id());
            commit.after_commit.push(Action::new(authority, transfer, ACTION_COMPUTE_UNITS));
            if let Mode::Undelegate(change) = mode {
                let transfer = REDSHIFT
                    .batch()
                    .add(&[cell], change)
                    .transfer_from_escrow(destination, TRANSFER_LAMPORTS)
                    .action(cell, REDSHIFT.id());
                commit.after_undelegate.push(Action::new(
                    authority,
                    transfer,
                    ACTION_COMPUTE_UNITS,
                ));
            }
        }
        bundle.commits.push(commit);
    }
    REDSHIFT.batch().intents(bundle).build()
}

fn transfer_with_refund(
    authority: Pubkey,
    validator: Pubkey,
    destination: Pubkey,
    amount: u64,
    fail: bool,
) -> Instruction {
    let cell = REDSHIFT.primary(authority);
    let mut action = REDSHIFT.batch();
    if fail {
        action = action.fail(0xFA11);
    }
    let transfer = action.transfer_from_escrow(destination, amount).action(cell, dlp::dlp_id());
    let callback = ResultCallback {
        cell,
        refund: Some(Refund { recipient: authority, amount }),
        compute_units: 20_000,
    };
    let mut commit = Commit::new(SettlementMode::Commit, &[cell]);
    commit
        .after_commit
        .push(Action::new(authority, transfer, ACTION_COMPUTE_UNITS).then(callback));
    let mut bundle = IntentBundle::new(authority);
    bundle.validator = Some(validator);
    bundle.commits.push(commit);
    REDSHIFT
        .batch()
        .invoke(redsuite_core::system::transfer(&authority, &cell, amount))
        .intents(bundle)
        .build()
}

async fn redelegate(base: &BaseCtx, er: &ErCtx, payer: &Keypair, phase: &str) -> Result<()> {
    base.submit_and_confirm(
        payer,
        &[
            REDSHIFT.delegate(
                payer.pubkey(),
                CellId::new(payer.pubkey(), 0),
                Some(er.identity()),
                prep::COMMIT_FREQUENCY_MS,
            ),
            REDSHIFT.log(phase),
        ],
    )
    .await?;
    Ok(())
}

pub(super) async fn run(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let delegated = async |key: &Pubkey| {
        er.await_delegated(key, TIMEOUT, &format!("{key} is delegated locally")).await
    };
    let settled = async |signature: &Signature, accounts: &[Pubkey], undelegates| {
        let receipt = er.check_commit(base, signature, accounts, undelegates).await?;
        receipt.check_success("scheduled intent")
    };
    let funder = base.funded_payer(PAYER_LAMPORTS).await?;
    let payers = base.funded_payers(3, PAYER_LAMPORTS).await?;
    let keys: Vec<_> = payers.iter().map(Signer::pubkey).collect();
    let mut cells = Vec::new();
    let mut expected = Vec::new();
    for payer in &payers {
        let (cell, [create, delegate]) = base
            .cells(REDSHIFT, payer)
            .prepare_delegated(er.identity(), prep::COMMIT_FREQUENCY_MS);
        base.submit_and_confirm(
            payer,
            &[create, delegate, dlp::top_up_ephemeral_balance(&payer.pubkey(), 500_000_000, 1)],
        )
        .await?;
        expected.push(CellHeader::try_decode(
            &base.account(&cell).await?.context("cell missing")?.data,
        )?);
        cells.push(cell);
    }
    let mut is_delegated = [true; 3];
    use Mode::{Commit, Finalize, Undelegate};
    for (label, modes, additions, actions) in [
        ("single commit action", &[Commit][..], &[101][..], true),
        ("repeated commit action", &[Commit], &[2], true),
        ("undelegate action", &[Undelegate(-100)], &[0], true),
        ("action after redelegation", &[Undelegate(102)], &[0], true),
        (
            "two-payer commit actions",
            &[Commit, Commit],
            &[0, 200],
            true,
        ),
        (
            "two-payer undelegate actions",
            &[Undelegate(-50), Undelegate(25)],
            &[0, 0],
            true,
        ),
        (
            "mixed commit/undelegate bundle",
            &[Commit, Commit, Undelegate(-25)],
            &[0, 0, 200],
            true,
        ),
        (
            "mixed commit/finalize bundle",
            &[Commit, Finalize],
            &[0, 0],
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
                delegated(&cells[i]).await?;
                check::poll_for(
                    &format!("{} restored for {label}", cells[i]),
                    TIMEOUT,
                    || async {
                        let account = er.account(&cells[i]).await?.context("ER cell missing")?;
                        ensure!(account.owner == redshift_interface::ID);
                        ensure!(CellHeader::try_decode(&account.data)? == expected[i]);
                        Ok::<_, DynError>(())
                    },
                )
                .await?;
                if additions[i] != 0 {
                    er.submit_and_confirm(
                        &payers[i],
                        &[REDSHIFT
                            .batch()
                            .add(&[REDSHIFT.primary(keys[i])], i64::from(additions[i]))
                            .require_signer(keys[i])
                            .build()],
                    )
                    .await?;
                    expected[i].value += additions[i] as u64;
                    expected[i].revision += 1;
                }
            }
            let signature = er
                .submit_and_confirm_with(
                    &payers[0],
                    &payers[1..n].iter().collect::<Vec<_>>(),
                    &[intents(&keys[..n], er.identity(), destination, modes, actions)],
                )
                .await?;
            let undelegates = modes.iter().any(|mode| matches!(mode, Undelegate(_)));
            settled(&signature, &cells[..n], undelegates).await?;
            let mut transfers = 0;
            for (i, mode) in modes.iter().enumerate() {
                transfers += usize::from(actions);
                if let Undelegate(change) = mode {
                    expected[i].value = expected[i]
                        .value
                        .checked_add_signed(*change)
                        .context("cell model underflow")?;
                    expected[i].revision += 1;
                    is_delegated[i] = false;
                    transfers += 1;
                }
                let account = base.account(&cells[i]).await?.context("committed cell missing")?;
                ensure!(CellHeader::try_decode(&account.data)? == expected[i]);
                ensure!(
                    account.owner
                        == if is_delegated[i] { dlp::dlp_id() } else { redshift_interface::ID }
                );
            }
            ensure!(
                base.api().get_balance(&destination).await?
                    == (transfers as u64 * TRANSFER_LAMPORTS)
            );
            Ok(())
        })
        .await?;
    }

    crate::phase("sponsored quota and undelegation", async {
        let payer = &payers[0];
        let initial_nonce = crate::last_commit_id(base, &cells[0]).await?;
        ensure!(
            initial_nonce < receipt::SPONSORED_COMMIT_LIMIT,
            "quota preparation starts below the sponsored limit"
        );
        for nonce in initial_nonce..receipt::SPONSORED_COMMIT_LIMIT {
            let signature = er
                .submit_and_confirm(
                    payer,
                    &[
                        REDSHIFT
                            .batch()
                            .add(&[REDSHIFT.primary(keys[0])], i64::from(nonce as u8 + 1))
                            .require_signer(keys[0])
                            .build(),
                        intents(&keys[..1], er.identity(), keys[1], &[Commit], false),
                    ],
                )
                .await?;
            settled(&signature, &cells[..1], false).await?;
            ensure!(crate::last_commit_id(base, &cells[0]).await? == (nonce + 1));
        }
        let before = er.account(&cells[0]).await?;
        let signature = er
            .sender(Rc::new(prep::payer_from_bytes(&payer.to_bytes())))
            .submit(&[
                REDSHIFT.batch().add(&[cells[0]], 99).require_signer(keys[0]).build(),
                intents(&keys[..1], er.identity(), keys[1], &[Commit], false),
            ])
            .await?;
        let tx = er.api().await_transaction(&signature, TIMEOUT).await?;
        ensure!(tx.err.as_ref().and_then(custom_error_code) == Some(receipt::COMMIT_LIMIT_ERR));
        ensure!(er.account(&cells[0]).await? == before);
        let signature = er
            .submit_and_confirm(
                payer,
                &[intents(&keys[..1], er.identity(), keys[1], &[Undelegate(0)], false)],
            )
            .await?;
        settled(&signature, &cells[..1], true).await?;
        let account = base.account(&cells[0]).await?.context("undelegated cell missing")?;
        ensure!(account.owner == redshift_interface::ID);
        ensure!(account.data == before.context("ER cell missing")?.data);
        redelegate(base, er, payer, "after sponsored quota").await?;
        Ok(())
    })
    .await?;

    base.delegate_payer(&funder, &payers[0], er.identity()).await?;
    delegated(&keys[0]).await?;
    delegated(&cells[0]).await?;
    let vault = dlp::magic_fee_vault_pda(&er.identity());
    crate::phase("paid commit threshold and combined action charge", async {
        let initial_nonce = crate::last_commit_id(base, &cells[0]).await?;
        ensure!(
            initial_nonce < PAID_COMMIT_LIMIT,
            "paid threshold starts above sponsored coverage"
        );
        for nonce in initial_nonce..=PAID_COMMIT_LIMIT + 1 {
            let actions = nonce == PAID_COMMIT_LIMIT + 1;
            let destination = Keypair::new().pubkey();
            let payer_before = er.api().get_balance(&keys[0]).await?;
            let vault_before = er.api().get_balance(&vault).await?;
            let signature = er
                .submit_and_confirm(
                    &payers[0],
                    &[
                        REDSHIFT
                            .batch()
                            .add(&[REDSHIFT.primary(keys[0])], i64::from(nonce as u8 + 1))
                            .require_signer(keys[0])
                            .build(),
                        intents(&keys[..1], er.identity(), destination, &[Commit], actions),
                    ],
                )
                .await?;
            settled(&signature, &cells[..1], false).await?;
            let fee = if nonce >= PAID_COMMIT_LIMIT { COMMIT_FEE_LAMPORTS } else { 0 }
                + if actions { ACTION_FEE } else { 0 };
            ensure!(er.api().get_balance(&keys[0]).await? == (payer_before - fee));
            ensure!(er.api().get_balance(&vault).await? == (vault_before + fee));
            ensure!(crate::last_commit_id(base, &cells[0]).await? == (nonce + 1));
            ensure!(
                base.api().get_balance(&destination).await?
                    == if actions { TRANSFER_LAMPORTS } else { 0 }
            );
        }
        Ok(())
    })
    .await?;

    base.delegate_payer(&funder, &payers[1], er.identity()).await?;
    delegated(&keys[1]).await?;
    for fail in [false, true] {
        crate::phase(
            if fail { "transfer callback refund" } else { "transfer callback success" },
            async {
                const AMOUNT: u64 = 890_880;
                let destination = Keypair::new().pubkey();
                let before = er.account(&cells[1]).await?.context("transfer cell missing")?;
                let state = CellHeader::try_decode(&before.data)?;
                let payer_before = er.api().get_balance(&keys[1]).await?;
                let vault_before = er.api().get_balance(&vault).await?;
                let signature = er
                    .submit_and_confirm(
                        &payers[1],
                        &[transfer_with_refund(keys[1], er.identity(), destination, AMOUNT, fail)],
                    )
                    .await?;
                settled(&signature, &cells[1..2], false).await?;
                check::poll_for(
                    &format!("{signature}: callback completes exactly once"),
                    TIMEOUT,
                    || async {
                        let account =
                            er.account(&cells[1]).await?.context("callback cell missing")?;
                        let actual = CellHeader::try_decode(&account.data)?;
                        ensure!(actual.revision == (state.revision + 1));
                        ensure!(actual.value == (state.value + u64::from(fail)));
                        ensure!(
                            account.lamports == (before.lamports + if fail { 0 } else { AMOUNT })
                        );
                        ensure!(
                            er.api().get_balance(&keys[1]).await?
                                == (payer_before
                                    - ACTION_FEE
                                    - CALLBACK_FEE
                                    - if fail { 0 } else { AMOUNT })
                        );
                        Ok::<_, DynError>(())
                    },
                )
                .await?;
                ensure!(
                    base.api().get_balance(&destination).await? == if fail { 0 } else { AMOUNT }
                );
                ensure!(
                    er.api().get_balance(&vault).await?
                        == (vault_before + ACTION_FEE + CALLBACK_FEE)
                );
                Ok(())
            },
        )
        .await?;
    }
    Ok(())
}
