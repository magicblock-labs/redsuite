use borsh::{to_vec, BorshDeserialize};
use magic_api::{pda::CALLBACK_SIGNER, response::MagicResponse};
pub use redshift_interface::flexi::*;
// process_create_intent mirrors upstream's fixture on the sdk's older
// intent API; migrating it would change the verified .so behavior.
#[allow(deprecated)]
use sdk::ephem::{
    CommitAndUndelegate, CommitType, MagicAction, MagicInstructionBuilder,
    UndelegateType,
};
use sdk::{
    ephem::{
        ActionCallback, CallHandler, FoldableIntentBuilder,
        MagicIntentBundleBuilder,
    },
    ActionArgs, ShortAccountMeta,
};
use solana_program::{
    account_info::{next_account_info, next_account_infos, AccountInfo},
    entrypoint::ProgramResult,
    instruction::{AccountMeta, Instruction},
    msg,
    program::{invoke, invoke_signed},
    program_error::ProgramError,
    pubkey::Pubkey,
    rent::Rent,
    sysvar::Sysvar,
};
use solana_sdk_ids::system_program;

mod system_instruction {
    use super::*;

    pub fn create_account(
        from: &Pubkey,
        to: &Pubkey,
        lamports: u64,
        space: u64,
        owner: &Pubkey,
    ) -> Instruction {
        let mut data = 0u32.to_le_bytes().to_vec();
        data.extend_from_slice(&lamports.to_le_bytes());
        data.extend_from_slice(&space.to_le_bytes());
        data.extend_from_slice(owner.as_ref());
        Instruction {
            program_id: system_program::ID,
            accounts: vec![
                AccountMeta::new(*from, true),
                AccountMeta::new(*to, true),
            ],
            data,
        }
    }

    pub fn transfer(from: &Pubkey, to: &Pubkey, lamports: u64) -> Instruction {
        let mut data = 2u32.to_le_bytes().to_vec();
        data.extend_from_slice(&lamports.to_le_bytes());
        Instruction {
            program_id: system_program::ID,
            accounts: vec![
                AccountMeta::new(*from, true),
                AccountMeta::new(*to, false),
            ],
            data,
        }
    }
}

pub fn process(accounts: &[AccountInfo], payload: &[u8]) -> ProgramResult {
    let instruction =
        FlexiInstruction::try_from_slice(payload).map_err(|err| {
            msg!("cannot parse the flexi instruction: {}", err);
            ProgramError::InvalidInstructionData
        })?;

    use FlexiInstruction::*;
    match instruction {
        Init { label, bump } => process_init(accounts, label, bump),
        Delegate {
            commit_frequency_ms,
            validator,
        } => process_delegate(accounts, commit_frequency_ms, validator),
        Add { count } => process_add(accounts, count),
        CreateIntent {
            num_committees,
            counter_diffs,
            is_undelegate,
            compute_units,
        } => process_create_intent(
            accounts,
            num_committees,
            counter_diffs,
            is_undelegate,
            compute_units,
        ),
        CommitActionHandler { amount } => {
            process_commit_action_handler(accounts, amount)
        }
        UndelegateActionHandler {
            amount,
            counter_diff,
        } => process_undelegate_action_handler(accounts, amount, counter_diff),
        CreateIntentBundle {
            num_commit_only,
            num_undelegate,
            counter_diffs,
            compute_units,
        } => process_create_intent_bundle(
            accounts,
            num_commit_only,
            num_undelegate,
            counter_diffs,
            compute_units,
        ),
        CreateIntentBundleCommitAndFinalize {
            num_commit,
            num_commit_finalize,
        } => process_create_intent_bundle_commit_and_finalize(
            accounts,
            num_commit,
            num_commit_finalize,
        ),
        CreateTransferIntent {
            amount,
            fail,
            compute_units,
        } => process_create_transfer_intent(
            accounts,
            amount,
            fail,
            compute_units,
        ),
        TransferActionHandler { amount, fail } => {
            process_transfer_action_handler(accounts, amount, fail)
        }
        AddUnsigned { count } => process_add_unsigned(accounts, count),
        AddError { count } => process_add_error(accounts, count),
        ScheduleCounterTask {
            task_id,
            execution_interval_millis,
            iterations,
            error,
            signer,
        } => process_schedule_counter_task(
            accounts,
            task_id,
            execution_interval_millis,
            iterations,
            error,
            signer,
        ),
        CancelCounterTask { task_id } => {
            process_cancel_counter_task(accounts, task_id)
        }
        Mul { multiplier } => process_mul(accounts, multiplier),
        AddAndScheduleCommit {
            count,
            undelegate,
            has_magic_vault,
        } => process_add_and_schedule_commit(
            accounts,
            count,
            undelegate,
            has_magic_vault,
        ),
    }
}

fn add(counter_account: &AccountInfo, count: u8) -> ProgramResult {
    let mut counter =
        FlexiCounter::try_from_slice(&counter_account.data.borrow())?;
    counter.count += count as u64;
    counter.updates += 1;
    let size = counter_account.data_len();
    let counter_data = to_vec(&counter)?;
    counter_account.data.borrow_mut()[..size].copy_from_slice(&counter_data);
    Ok(())
}

fn process_init(
    accounts: &[AccountInfo],
    label: String,
    bump: u8,
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let counter = next_account_info(iter)?;
    let system = next_account_info(iter)?;

    let (expected_pda, _) = FlexiCounter::pda_and_bump(payer.key);
    if counter.key != &expected_pda {
        return Err(ProgramError::InvalidSeeds);
    }

    let bump_slice = [bump];
    let seeds: [&[u8]; 4] = [
        crate::ID.as_ref(),
        FLEXI_SEED,
        payer.key.as_ref(),
        &bump_slice,
    ];
    let state = FlexiCounter::new(label);
    let data = to_vec(&state)?;
    let size = data.len();
    let create = system_instruction::create_account(
        payer.key,
        counter.key,
        Rent::get()?.minimum_balance(size),
        size as u64,
        &crate::ID,
    );
    invoke_signed(
        &create,
        &[payer.clone(), counter.clone(), system.clone()],
        &[&seeds],
    )?;
    counter.data.borrow_mut()[..size].copy_from_slice(&data);
    Ok(())
}

fn process_delegate(
    accounts: &[AccountInfo],
    commit_frequency_ms: u32,
    validator: Option<Pubkey>,
) -> ProgramResult {
    use sdk::cpi::{delegate_account, DelegateAccounts, DelegateConfig};
    let [payer, counter, owner_program, buffer, delegation_record, delegation_metadata, delegation_program, system] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    let seeds: [&[u8]; 3] =
        [crate::ID.as_ref(), FLEXI_SEED, payer.key.as_ref()];
    delegate_account(
        DelegateAccounts {
            payer,
            pda: counter,
            owner_program,
            buffer,
            delegation_record,
            delegation_metadata,
            delegation_program,
            system_program: system,
        },
        &seeds,
        DelegateConfig {
            commit_frequency_ms,
            validator,
        },
    )
}

fn process_add(accounts: &[AccountInfo], count: u8) -> ProgramResult {
    let iter = &mut accounts.iter();
    let _payer = next_account_info(iter)?;
    let counter = next_account_info(iter)?;
    add(counter, count)
}

#[allow(deprecated)]
fn process_create_intent(
    accounts: &[AccountInfo],
    num_committees: u8,
    counter_diffs: Vec<i64>,
    is_undelegate: bool,
    compute_units: u32,
) -> ProgramResult {
    let num_committees = num_committees as usize;
    if accounts.len() != 2 * num_committees + 5 {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let iter = &mut accounts.iter();
    let destination_program = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let transfer_destination = next_account_info(iter)?;
    let system = next_account_info(iter)?;
    let escrow_authorities = next_account_infos(iter, num_committees)?;
    let committees = next_account_infos(iter, num_committees)?;

    let commit_action = FlexiInstruction::CommitActionHandler { amount: PRIZE };
    let commit_handlers = committees
        .iter()
        .zip(escrow_authorities.iter().cloned())
        .map(|(committee, escrow_authority)| CallHandler {
            args: ActionArgs {
                data: tagged(&commit_action),
                escrow_index: ACTOR_ESCROW_INDEX,
            },
            compute_units,
            escrow_authority,
            destination_program: *destination_program.key,
            accounts: vec![
                committee.into(),
                ShortAccountMeta {
                    pubkey: *transfer_destination.key,
                    is_writable: true,
                },
                system.into(),
            ],
        })
        .collect::<Vec<_>>();
    let commit_type = CommitType::WithHandler {
        commited_accounts: committees.to_vec(),
        callbacks: vec![],
        call_handlers: commit_handlers,
    };

    let magic_action = if is_undelegate {
        let undelegate_handlers = committees
            .iter()
            .zip(escrow_authorities.iter().cloned())
            .zip(counter_diffs.iter().copied())
            .map(|((committee, escrow_authority), counter_diff)| {
                let action = FlexiInstruction::UndelegateActionHandler {
                    counter_diff,
                    amount: PRIZE,
                };
                CallHandler {
                    args: ActionArgs {
                        data: tagged(&action),
                        escrow_index: ACTOR_ESCROW_INDEX,
                    },
                    compute_units,
                    escrow_authority,
                    destination_program: *destination_program.key,
                    accounts: vec![
                        committee.into(),
                        ShortAccountMeta {
                            pubkey: *transfer_destination.key,
                            is_writable: true,
                        },
                        system.into(),
                    ],
                }
            })
            .collect::<Vec<_>>();
        MagicAction::CommitAndUndelegate(CommitAndUndelegate {
            commit_type,
            undelegate_type: UndelegateType::WithHandler {
                call_handlers: undelegate_handlers,
                callbacks: vec![],
            },
        })
    } else {
        MagicAction::Commit(commit_type)
    };

    MagicInstructionBuilder {
        payer: escrow_authorities[0].clone(),
        magic_context: magic_context.clone(),
        magic_program: magic_program.clone(),
        magic_fee_vault: None,
        magic_action,
    }
    .build_and_invoke()
}

fn action_handler<'info>(
    committee: &AccountInfo<'info>,
    transfer_destination: &AccountInfo<'info>,
    system: &AccountInfo<'info>,
    destination_program: &Pubkey,
    escrow_authority: AccountInfo<'info>,
    instruction: &FlexiInstruction,
    compute_units: u32,
) -> CallHandler<'info> {
    CallHandler {
        args: ActionArgs {
            data: tagged(instruction),
            escrow_index: ACTOR_ESCROW_INDEX,
        },
        compute_units,
        escrow_authority,
        destination_program: *destination_program,
        accounts: vec![
            committee.into(),
            ShortAccountMeta {
                pubkey: *transfer_destination.key,
                is_writable: true,
            },
            system.into(),
        ],
    }
}

fn process_create_intent_bundle(
    accounts: &[AccountInfo],
    num_commit_only: u8,
    num_undelegate: u8,
    counter_diffs: Vec<i64>,
    compute_units: u32,
) -> ProgramResult {
    let num_commit_only = num_commit_only as usize;
    let num_undelegate = num_undelegate as usize;
    if accounts.len() != 5 + 2 * num_commit_only + 2 * num_undelegate {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let iter = &mut accounts.iter();
    let destination_program = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let transfer_destination = next_account_info(iter)?;
    let system = next_account_info(iter)?;
    let commit_only_escrows = next_account_infos(iter, num_commit_only)?;
    let commit_only_counters = next_account_infos(iter, num_commit_only)?;
    let undelegate_escrows = next_account_infos(iter, num_undelegate)?;
    let undelegate_counters = next_account_infos(iter, num_undelegate)?;

    let payer = if !commit_only_escrows.is_empty() {
        commit_only_escrows[0].clone()
    } else if !undelegate_escrows.is_empty() {
        undelegate_escrows[0].clone()
    } else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let mut builder = MagicIntentBundleBuilder::new(
        payer,
        magic_context.clone(),
        magic_program.clone(),
    );

    if !commit_only_counters.is_empty() {
        let handlers = commit_only_counters
            .iter()
            .zip(commit_only_escrows.iter().cloned())
            .map(|(counter, escrow_authority)| {
                action_handler(
                    counter,
                    transfer_destination,
                    system,
                    destination_program.key,
                    escrow_authority,
                    &FlexiInstruction::CommitActionHandler { amount: PRIZE },
                    compute_units,
                )
            })
            .collect::<Vec<_>>();
        builder = builder
            .commit(commit_only_counters)
            .add_post_commit_actions(handlers)
            .fold_builder();
    }

    if !undelegate_counters.is_empty() {
        let commit_handlers = undelegate_counters
            .iter()
            .zip(undelegate_escrows.iter().cloned())
            .map(|(counter, escrow_authority)| {
                action_handler(
                    counter,
                    transfer_destination,
                    system,
                    destination_program.key,
                    escrow_authority,
                    &FlexiInstruction::CommitActionHandler { amount: PRIZE },
                    compute_units,
                )
            })
            .collect::<Vec<_>>();
        let undelegate_handlers = undelegate_counters
            .iter()
            .zip(undelegate_escrows.iter().cloned())
            .zip(counter_diffs.iter().copied())
            .map(|((counter, escrow_authority), counter_diff)| {
                action_handler(
                    counter,
                    transfer_destination,
                    system,
                    destination_program.key,
                    escrow_authority,
                    &FlexiInstruction::UndelegateActionHandler {
                        counter_diff,
                        amount: PRIZE,
                    },
                    compute_units,
                )
            })
            .collect::<Vec<_>>();
        builder = builder
            .commit_and_undelegate(undelegate_counters)
            .add_post_commit_actions(commit_handlers)
            .add_post_undelegate_actions(undelegate_handlers)
            .fold_builder();
    }

    builder.build_and_invoke()
}

fn process_create_intent_bundle_commit_and_finalize(
    accounts: &[AccountInfo],
    num_commit: u8,
    num_commit_finalize: u8,
) -> ProgramResult {
    use magic_api::{
        args::{CommitTypeArgs, MagicIntentBundleArgs},
        instruction::MagicBlockInstruction,
    };

    let num_commit = num_commit as usize;
    let num_commit_finalize = num_commit_finalize as usize;
    if accounts.len() != 3 + num_commit + num_commit_finalize {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let iter = &mut accounts.iter();
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let payer = next_account_info(iter)?;
    let commit_accounts = next_account_infos(iter, num_commit)?;
    let commit_finalize_accounts =
        next_account_infos(iter, num_commit_finalize)?;

    let mut all_accounts = Vec::new();
    all_accounts.extend(commit_accounts.iter().cloned());
    all_accounts.extend(commit_finalize_accounts.iter().cloned());

    let mut dedup_keys = Vec::new();
    for account in &all_accounts {
        if !dedup_keys.iter().any(|key| key == account.key) {
            dedup_keys.push(*account.key);
        }
    }

    let account_index = |pubkey: &Pubkey| -> u8 {
        dedup_keys
            .iter()
            .position(|key| key == pubkey)
            .map(|index| (2 + index) as u8)
            .unwrap()
    };

    let args = MagicIntentBundleArgs {
        commit: Some(CommitTypeArgs::Standalone(
            commit_accounts
                .iter()
                .map(|account| account_index(account.key))
                .collect(),
        )),
        commit_and_undelegate: None,
        commit_finalize: Some(CommitTypeArgs::Standalone(
            commit_finalize_accounts
                .iter()
                .map(|account| account_index(account.key))
                .collect(),
        )),
        commit_finalize_and_undelegate: None,
        standalone_actions: vec![],
    };

    let mut metas = vec![
        AccountMeta::new(*payer.key, true),
        AccountMeta::new(*magic_context.key, false),
    ];
    metas.extend(
        dedup_keys
            .iter()
            .map(|pubkey| AccountMeta::new(*pubkey, false)),
    );

    let mut cpi_accounts =
        vec![magic_program.clone(), payer.clone(), magic_context.clone()];
    cpi_accounts.extend(dedup_keys.iter().filter_map(|pubkey| {
        all_accounts
            .iter()
            .find(|account| account.key == pubkey)
            .cloned()
    }));

    let ix = Instruction::new_with_bincode(
        *magic_program.key,
        &MagicBlockInstruction::ScheduleIntentBundle(args),
        metas,
    );
    invoke(&ix, &cpi_accounts)
}

fn process_create_transfer_intent(
    accounts: &[AccountInfo],
    amount: u64,
    fail: bool,
    compute_units: u32,
) -> ProgramResult {
    let [payer, counter, destination, system, magic_context, magic_program, magic_fee_vault] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    invoke(
        &system_instruction::transfer(payer.key, counter.key, amount),
        &[payer.clone(), counter.clone(), system.clone()],
    )?;

    let action = FlexiInstruction::TransferActionHandler { amount, fail };
    let call_handler = CallHandler {
        args: ActionArgs {
            data: tagged(&action),
            escrow_index: ACTOR_ESCROW_INDEX,
        },
        compute_units,
        escrow_authority: payer.clone(),
        destination_program: crate::ID,
        accounts: vec![
            ShortAccountMeta {
                pubkey: *destination.key,
                is_writable: true,
            },
            system.into(),
        ],
    };

    let callback = ActionCallback {
        destination_program: crate::ID,
        discriminator: TRANSFER_CALLBACK_DISCRIMINATOR.to_vec(),
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
            system.into(),
        ],
    };

    MagicIntentBundleBuilder::new(
        payer.clone(),
        magic_context.clone(),
        magic_program.clone(),
    )
    .magic_fee_vault(magic_fee_vault.clone())
    .commit(std::slice::from_ref(counter))
    .add_post_commit_action(call_handler)
    .then(callback)
    .build_and_invoke()
}

fn process_commit_action_handler(
    accounts: &[AccountInfo],
    amount: u64,
) -> ProgramResult {
    let [delegated_account, destination, system, source_program, _, escrow_account] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !escrow_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if delegated_account.owner != &sdk::id() {
        return Err(ProgramError::InvalidAccountOwner);
    }
    if source_program.key != &crate::ID {
        return Err(ProgramError::IncorrectProgramId);
    }
    invoke(
        &system_instruction::transfer(
            escrow_account.key,
            destination.key,
            amount,
        ),
        &[escrow_account.clone(), destination.clone(), system.clone()],
    )
}

fn process_undelegate_action_handler(
    accounts: &[AccountInfo],
    amount: u64,
    counter_diff: i64,
) -> ProgramResult {
    let [undelegated_counter, destination, system, source_program, _, escrow_account] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !escrow_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if undelegated_counter.owner == &sdk::id() {
        return Err(ProgramError::InvalidAccountOwner);
    }
    if source_program.key != &crate::ID {
        return Err(ProgramError::IncorrectProgramId);
    }

    let mut counter = {
        let data = undelegated_counter.data.borrow();
        FlexiCounter::deserialize(&mut data.as_ref())?
    };
    counter.count = u64::try_from(counter.count as i64 + counter_diff)
        .map_err(|_| ProgramError::ArithmeticOverflow)?;
    counter.updates += 1;
    let counter_data = to_vec(&counter)?;
    undelegated_counter.data.borrow_mut()[..counter_data.len()]
        .copy_from_slice(&counter_data);

    invoke(
        &system_instruction::transfer(
            escrow_account.key,
            destination.key,
            amount,
        ),
        &[escrow_account.clone(), destination.clone(), system.clone()],
    )
}

fn process_transfer_action_handler(
    accounts: &[AccountInfo],
    amount: u64,
    fail: bool,
) -> ProgramResult {
    let [destination, system, source_program, _, escrow_account] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if source_program.key != &crate::ID {
        return Err(ProgramError::IncorrectProgramId);
    }
    if !escrow_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if fail {
        return Err(ProgramError::Custom(TRANSFER_FAIL_CODE));
    }
    invoke(
        &system_instruction::transfer(
            escrow_account.key,
            destination.key,
            amount,
        ),
        &[escrow_account.clone(), destination.clone(), system.clone()],
    )
}

pub fn process_transfer_callback(
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let [callback_signer, counter, payer, _system] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !callback_signer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if callback_signer.key != &CALLBACK_SIGNER {
        return Err(ProgramError::IncorrectAuthority);
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
    Ok(())
}

fn process_add_unsigned(accounts: &[AccountInfo], count: u8) -> ProgramResult {
    let iter = &mut accounts.iter();
    let counter = next_account_info(iter)?;
    add(counter, count)
}

fn process_mul(accounts: &[AccountInfo], multiplier: u8) -> ProgramResult {
    let iter = &mut accounts.iter();
    let _payer = next_account_info(iter)?;
    let counter = next_account_info(iter)?;
    let mut state = FlexiCounter::try_from_slice(&counter.data.borrow())?;
    state.count *= multiplier as u64;
    state.updates += 1;
    let size = counter.data_len();
    let data = to_vec(&state)?;
    counter.data.borrow_mut()[..size].copy_from_slice(&data);
    Ok(())
}

fn process_add_error(_accounts: &[AccountInfo], _count: u8) -> ProgramResult {
    Err(ProgramError::Custom(0))
}

fn process_schedule_counter_task(
    accounts: &[AccountInfo],
    task_id: i64,
    execution_interval_millis: i64,
    iterations: i64,
    error: bool,
    signer: bool,
) -> ProgramResult {
    use magic_api::{
        args::ScheduleTaskArgs, instruction::MagicBlockInstruction,
    };

    let [magic_program, payer, counter] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    let (expected_pda, bump) = FlexiCounter::pda_and_bump(payer.key);
    if counter.key != &expected_pda {
        return Err(ProgramError::InvalidSeeds);
    }

    let task_instruction = match (error, signer) {
        (true, _) => build::add_error(*payer.key, 1),
        (false, true) => build::add(*payer.key, 1),
        _ => build::add_unsigned(*payer.key, 1),
    };
    let data = bincode::serialize(&MagicBlockInstruction::ScheduleTask(
        ScheduleTaskArgs {
            task_id,
            execution_interval_millis,
            iterations,
            instructions: vec![task_instruction],
        },
    ))
    .map_err(|_| ProgramError::InvalidArgument)?;

    let instruction = Instruction::new_with_bytes(
        *magic_program.key,
        &data,
        vec![
            AccountMeta::new(*payer.key, true),
            AccountMeta::new(*counter.key, true),
        ],
    );

    let bump_slice = [bump];
    let seeds: [&[u8]; 4] = [
        crate::ID.as_ref(),
        FLEXI_SEED,
        payer.key.as_ref(),
        &bump_slice,
    ];
    invoke_signed(&instruction, &[payer.clone(), counter.clone()], &[&seeds])
}

fn process_add_and_schedule_commit(
    accounts: &[AccountInfo],
    count: u8,
    undelegate: bool,
    has_magic_vault: bool,
) -> ProgramResult {
    use sdk::ephem::{commit_accounts, commit_and_undelegate_accounts};

    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let counter = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let magic_fee_vault = if has_magic_vault {
        Some(next_account_info(iter)?)
    } else {
        None
    };

    add(counter, count)?;
    if undelegate {
        commit_and_undelegate_accounts(
            payer,
            vec![counter],
            magic_context,
            magic_program,
            magic_fee_vault,
        )
    } else {
        commit_accounts(
            payer,
            vec![counter],
            magic_context,
            magic_program,
            magic_fee_vault,
        )
    }
}

fn process_cancel_counter_task(
    accounts: &[AccountInfo],
    task_id: i64,
) -> ProgramResult {
    use magic_api::instruction::MagicBlockInstruction;

    let [magic_program, payer] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    let data =
        bincode::serialize(&MagicBlockInstruction::CancelTask { task_id })
            .map_err(|_| ProgramError::InvalidArgument)?;
    let instruction = Instruction::new_with_bytes(
        *magic_program.key,
        &data,
        vec![AccountMeta::new(*payer.key, true)],
    );
    invoke(&instruction, std::slice::from_ref(payer))
}
