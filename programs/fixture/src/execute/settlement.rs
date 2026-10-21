use instruction::AccountMeta;
use magic_api::{pda::CALLBACK_SIGNER, response::MagicResponse};
use pubkey::Pubkey;
use sdk::{
    ephem::{
        ActionCallback, CallHandler, CommitAndUndelegateIntentBuilder, FoldableCauIntentBuilder,
        FoldableCommitIntentBuilder, FoldableIntentBuilder, MagicIntentBundleBuilder,
    },
    ActionArgs, ShortAccountMeta,
};
use solana_program::{
    account_info::AccountInfo, entrypoint::ProgramResult, program_error::ProgramError,
};

use super::{adjust, info};
use crate::{Action, Instruction, Intent, ResultCallback, Settlement, SettlementMode};

fn handler<'info>(
    accounts: &[AccountInfo<'info>],
    action: &Action,
) -> Result<CallHandler<'info>, ProgramError> {
    let authority = accounts
        .iter()
        .find(|account| account.key == &action.authority)
        .ok_or(ProgramError::NotEnoughAccountKeys)?;
    Ok(CallHandler {
        args: ActionArgs {
            data: action.instruction.data.clone(),
            escrow_index: action.escrow_index,
        },
        compute_units: action.compute_units,
        escrow_authority: authority.clone(),
        destination_program: action.instruction.program_id,
        accounts: action
            .instruction
            .accounts
            .iter()
            .map(|meta| ShortAccountMeta {
                pubkey: meta.pubkey,
                is_writable: meta.is_writable,
            })
            .collect(),
    })
}

fn callback(program_id: Pubkey, effect: &ResultCallback) -> Result<ActionCallback, ProgramError> {
    let mut accounts = vec![
        AccountMeta::new_readonly(CALLBACK_SIGNER, false),
        AccountMeta::new(effect.cell, false),
    ];
    accounts.extend(effect.refund.as_ref().map(|refund| AccountMeta::new(refund.recipient, false)));
    Ok(ActionCallback {
        destination_program: program_id,
        discriminator: Instruction::HandleResult.into_data(),
        payload: wincode::serialize(effect).map_err(|_| ProgramError::InvalidInstructionData)?,
        compute_units: effect.compute_units,
        accounts: accounts.into_iter().map(ShortAccountMeta::from).collect(),
    })
}

// SDK intent construction needs its own bounded SBF stack frame.
#[inline(never)]
pub(super) fn invoke(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    settlement: &Settlement,
) -> ProgramResult {
    let mut builder = MagicIntentBundleBuilder::new(
        info(accounts, settlement.payer)?.clone(),
        info(accounts, settlement.context)?.clone(),
        info(accounts, settlement.magic)?.clone(),
    );
    if let Some(index) = settlement.vault {
        builder = builder.magic_fee_vault(info(accounts, index)?.clone());
    }
    for intent in &settlement.intents {
        builder = match intent.mode {
            SettlementMode::Commit => add_commit(program_id, accounts, builder, intent)?,
            SettlementMode::Undelegate => add_undelegation(program_id, accounts, builder, intent)?,
        };
    }
    for action in &settlement.actions {
        builder = add_action(program_id, accounts, builder, action)?;
    }
    send(builder)
}

#[inline(never)]
fn add_commit<'info>(
    program_id: &Pubkey,
    accounts: &[AccountInfo<'info>],
    builder: MagicIntentBundleBuilder<'info>,
    intent: &Intent,
) -> Result<MagicIntentBundleBuilder<'info>, ProgramError> {
    let cells = intent
        .cells
        .iter()
        .map(|&index| info(accounts, index).cloned())
        .collect::<Result<Vec<_>, _>>()?;
    let mut commit = builder.commit(&cells);
    for action in &intent.after_commit {
        let mut next = commit.add_post_commit_action(handler(accounts, action)?);
        if let Some(effect) = &action.callback {
            next = next.then(callback(*program_id, effect)?);
        }
        commit = next.fold_commit_builder();
    }
    Ok(commit.fold_builder())
}

#[inline(never)]
fn add_undelegation<'info>(
    program_id: &Pubkey,
    accounts: &[AccountInfo<'info>],
    builder: MagicIntentBundleBuilder<'info>,
    intent: &Intent,
) -> Result<MagicIntentBundleBuilder<'info>, ProgramError> {
    let cells = intent
        .cells
        .iter()
        .map(|&index| info(accounts, index).cloned())
        .collect::<Result<Vec<_>, _>>()?;
    let mut commit = builder.commit_and_undelegate(&cells);
    for action in &intent.after_commit {
        let mut next = commit.add_post_commit_action(handler(accounts, action)?);
        if let Some(effect) = &action.callback {
            next = next.then(callback(*program_id, effect)?);
        }
        commit = next.fold_cau_builder();
    }
    after_undelegation(program_id, accounts, commit, &intent.after_undelegate)
}

#[inline(never)]
fn after_undelegation<'info>(
    program_id: &Pubkey,
    accounts: &[AccountInfo<'info>],
    mut commit: CommitAndUndelegateIntentBuilder<'info>,
    actions: &[Action],
) -> Result<MagicIntentBundleBuilder<'info>, ProgramError> {
    for action in actions {
        let mut next = commit.add_post_undelegate_action(handler(accounts, action)?);
        if let Some(effect) = &action.callback {
            next = next.then(callback(*program_id, effect)?);
        }
        commit = next.fold_cau_builder();
    }
    Ok(commit.fold_builder())
}

#[inline(never)]
fn add_action<'info>(
    program_id: &Pubkey,
    accounts: &[AccountInfo<'info>],
    builder: MagicIntentBundleBuilder<'info>,
    action: &Action,
) -> Result<MagicIntentBundleBuilder<'info>, ProgramError> {
    let mut next = builder.add_standalone_action(handler(accounts, action)?);
    if let Some(effect) = &action.callback {
        next = next.then(callback(*program_id, effect)?);
    }
    Ok(next.fold_builder())
}

// Separate SDK serialization/CPI temporaries from the intent-construction frame.
#[inline(never)]
fn send(builder: MagicIntentBundleBuilder<'_>) -> ProgramResult {
    builder.build_and_invoke()
}

pub(super) fn handle_result(accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    let [signer, cell, remaining @ ..] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if signer.key != &CALLBACK_SIGNER || !signer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    let response: MagicResponse =
        bincode::deserialize(data).map_err(|_| ProgramError::InvalidInstructionData)?;
    let effect: ResultCallback = wincode::deserialize_exact(response.data())
        .map_err(|_| ProgramError::InvalidInstructionData)?;
    if cell.key != &effect.cell {
        return Err(ProgramError::InvalidArgument);
    }
    if !response.ok() {
        if let Some(refund) = effect.refund {
            let [recipient] = remaining else {
                return Err(ProgramError::NotEnoughAccountKeys);
            };
            if recipient.key != &refund.recipient {
                return Err(ProgramError::InvalidArgument);
            }
            let debit = cell
                .lamports()
                .checked_sub(refund.amount)
                .ok_or(ProgramError::InsufficientFunds)?;
            let credit = recipient
                .lamports()
                .checked_add(refund.amount)
                .ok_or(ProgramError::ArithmeticOverflow)?;
            **cell.try_borrow_mut_lamports()? = debit;
            **recipient.try_borrow_mut_lamports()? = credit;
        }
    }
    adjust(cell, i64::from(!response.ok()))
}
