use magic_api::{
    args::{CommitTypeArgs, MagicIntentBundleArgs},
    instruction::MagicBlockInstruction,
    pda::CALLBACK_SIGNER,
    response::MagicResponse,
};
use sdk::ephem::{ActionCallback, FoldableIntentBuilder};

use super::*;

fn transfer_lamports<'a>(
    from: &AccountInfo<'a>,
    to: &AccountInfo<'a>,
    system: &AccountInfo<'a>,
    amount: u64,
) -> ProgramResult {
    invoke(
        &system_instruction::transfer(from.key, to.key, amount),
        &[from.clone(), to.clone(), system.clone()],
    )
}

fn handler<'a>(
    owner: &AccountInfo<'a>,
    counter: &AccountInfo,
    destination: &AccountInfo,
    system: &AccountInfo,
    amount: u64,
    change: Option<i64>,
    fail: bool,
) -> CallHandler<'a> {
    CallHandler {
        args: ActionArgs {
            data: tagged(&FlexiInstruction::BaseAction {
                amount,
                change,
                fail,
            }),
            escrow_index: ACTOR_ESCROW_INDEX,
        },
        compute_units: ACTION_COMPUTE_UNITS,
        escrow_authority: owner.clone(),
        destination_program: crate::ID,
        accounts: vec![
            ShortAccountMeta {
                pubkey: *counter.key,
                is_writable: change.is_some(),
            },
            ShortAccountMeta {
                pubkey: *destination.key,
                is_writable: true,
            },
            system.into(),
        ],
    }
}

pub(super) fn schedule(
    accounts: &[AccountInfo],
    modes: &[IntentMode],
    actions: bool,
) -> ProgramResult {
    let [payer, context, magic, vault, destination, system, pairs @ ..] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if pairs.len() != modes.len() * 2 {
        return Err(ProgramError::NotEnoughAccountKeys);
    }
    if matches!(modes, [IntentMode::Commit, IntentMode::Finalize]) && !actions {
        let instruction = Instruction::new_with_bincode(
            *magic.key,
            &MagicBlockInstruction::ScheduleIntentBundle(
                MagicIntentBundleArgs {
                    commit: Some(CommitTypeArgs::Standalone(vec![3])),
                    commit_finalize: Some(CommitTypeArgs::Standalone(vec![4])),
                    ..Default::default()
                },
            ),
            [payer, context, vault, &pairs[1], &pairs[3]]
                .into_iter()
                .map(|account| {
                    AccountMeta::new(*account.key, account.is_signer)
                })
                .collect(),
        );
        return invoke(&instruction, accounts);
    }
    let mut builder = MagicIntentBundleBuilder::new(
        payer.clone(),
        context.clone(),
        magic.clone(),
    )
    .magic_fee_vault(vault.clone());
    for (pair, mode) in pairs.chunks_exact(2).zip(modes) {
        let owner = &pair[0];
        let counter = &pair[1];
        if counter.key != &FlexiCounter::pda_and_bump(owner.key).0 {
            return Err(ProgramError::InvalidSeeds);
        }
        let post_commit = || {
            handler(
                owner,
                counter,
                destination,
                system,
                ACTION_PRIZE,
                None,
                false,
            )
        };
        builder = match mode {
            IntentMode::Undelegate(change) => {
                let mut intent = builder
                    .commit_and_undelegate(std::slice::from_ref(counter));
                if actions {
                    intent = intent
                        .add_post_commit_actions([post_commit()])
                        .add_post_undelegate_actions([handler(
                            owner,
                            counter,
                            destination,
                            system,
                            ACTION_PRIZE,
                            Some(*change),
                            false,
                        )]);
                }
                intent.fold_builder()
            }
            IntentMode::Commit => {
                let mut intent = builder.commit(std::slice::from_ref(counter));
                if actions {
                    intent = intent.add_post_commit_actions([post_commit()]);
                }
                intent.fold_builder()
            }
            IntentMode::Finalize => {
                return Err(ProgramError::InvalidInstructionData)
            }
        };
    }
    builder.build_and_invoke()
}

pub(super) fn action(
    accounts: &[AccountInfo],
    amount: u64,
    change: Option<i64>,
    fail: bool,
) -> ProgramResult {
    let [counter, destination, system, source, _, escrow] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if source.key != &crate::ID {
        return Err(ProgramError::IncorrectProgramId);
    }
    if !escrow.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    let owner = if change.is_some() {
        crate::ID
    } else {
        sdk::cpi::DELEGATION_PROGRAM_ID
    };
    if counter.owner != &owner {
        return Err(ProgramError::InvalidAccountOwner);
    }
    if fail {
        return Err(ProgramError::Custom(TRANSFER_FAILURE));
    }
    if let Some(change) = change {
        adjust(counter, change)?;
    }
    transfer_lamports(escrow, destination, system, amount)
}

pub(super) fn transfer(
    accounts: &[AccountInfo],
    amount: u64,
    fail: bool,
) -> ProgramResult {
    let [payer, context, magic, vault, destination, system, _, counter] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    transfer_lamports(payer, counter, system, amount)?;
    let callback = ActionCallback {
        destination_program: crate::ID,
        discriminator: TRANSFER_CALLBACK.to_vec(),
        payload: amount.to_le_bytes().to_vec(),
        compute_units: 20_000,
        accounts: vec![
            ShortAccountMeta {
                pubkey: CALLBACK_SIGNER,
                is_writable: false,
            },
            ShortAccountMeta {
                pubkey: *counter.key,
                is_writable: true,
            },
            ShortAccountMeta {
                pubkey: *payer.key,
                is_writable: true,
            },
        ],
    };
    MagicIntentBundleBuilder::new(payer.clone(), context.clone(), magic.clone())
        .magic_fee_vault(vault.clone())
        .commit(std::slice::from_ref(counter))
        .add_post_commit_action(handler(
            payer,
            counter,
            destination,
            system,
            amount,
            None,
            fail,
        ))
        .then(callback)
        .build_and_invoke()
}

pub(super) fn callback(accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    let [signer, counter, payer] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if signer.key != &CALLBACK_SIGNER || !signer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    let response: MagicResponse = bincode::deserialize(data)
        .map_err(|_| ProgramError::InvalidInstructionData)?;
    if !response.ok() {
        let amount = u64::from_le_bytes(
            response
                .data()
                .try_into()
                .map_err(|_| ProgramError::InvalidInstructionData)?,
        );
        **counter.try_borrow_mut_lamports()? -= amount;
        **payer.try_borrow_mut_lamports()? += amount;
    }
    adjust(counter, i64::from(!response.ok()))
}
