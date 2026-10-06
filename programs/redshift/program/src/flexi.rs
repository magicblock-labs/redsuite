use borsh::{to_vec, BorshDeserialize};
pub use redshift_interface::flexi::*;
use sdk::{
    cpi::{delegate_account, DelegateConfig},
    ephem::{CallHandler, MagicIntentBundleBuilder},
    ActionArgs, ShortAccountMeta,
};
use sdk_ids::system_program;
use solana_program::{
    account_info::{next_account_info, AccountInfo},
    entrypoint::ProgramResult,
    instruction::{AccountMeta, Instruction},
    msg,
    program::{invoke, invoke_signed},
    program_error::ProgramError,
    pubkey::Pubkey,
    rent::Rent,
    sysvar::Sysvar,
};

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
        AddUnsigned { count } => process_add_unsigned(accounts, count),
        AddError { count } => process_add_error(accounts, count),
        ScheduleCounterTask {
            task_id,
            execution_interval_millis,
            iterations,
            signer,
        } => process_schedule_counter_task(
            accounts,
            task_id,
            execution_interval_millis,
            iterations,
            signer,
        ),
        CancelCounterTask { task_id } => {
            process_cancel_counter_task(accounts, task_id)
        }
        CreateActionIntent {
            counter,
            count,
            compute_units,
        } => process_create_action_intent(
            accounts,
            counter,
            count,
            compute_units,
        ),
        AddActionHandler { count } => {
            process_add_action_handler(accounts, count)
        }
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
    let accounts = crate::schedulecommit::delegate_accounts(accounts)?;
    let seeds: [&[u8]; 3] =
        [crate::ID.as_ref(), FLEXI_SEED, accounts.payer.key.as_ref()];
    delegate_account(
        accounts,
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

fn process_create_action_intent(
    accounts: &[AccountInfo],
    counter: Pubkey,
    count: u8,
    compute_units: u32,
) -> ProgramResult {
    let [payer, magic_context, magic_program] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    let action = FlexiInstruction::AddActionHandler { count };
    let handler = CallHandler {
        args: ActionArgs {
            data: tagged(&action),
            escrow_index: ACTOR_ESCROW_INDEX,
        },
        compute_units,
        escrow_authority: payer.clone(),
        destination_program: crate::ID,
        accounts: vec![ShortAccountMeta {
            pubkey: counter,
            is_writable: true,
        }],
    };
    MagicIntentBundleBuilder::new(
        payer.clone(),
        magic_context.clone(),
        magic_program.clone(),
    )
    .add_standalone_actions([handler])
    .build_and_invoke()
}

fn process_add_action_handler(
    accounts: &[AccountInfo],
    count: u8,
) -> ProgramResult {
    let [counter, source_program, _, escrow_account] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !escrow_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if source_program.key != &crate::ID {
        return Err(ProgramError::IncorrectProgramId);
    }
    if counter.owner != &crate::ID {
        return Err(ProgramError::InvalidAccountOwner);
    }
    add(counter, count)
}

fn process_add_unsigned(accounts: &[AccountInfo], count: u8) -> ProgramResult {
    let iter = &mut accounts.iter();
    let counter = next_account_info(iter)?;
    add(counter, count)
}

fn process_add_error(_accounts: &[AccountInfo], _count: u8) -> ProgramResult {
    Err(ProgramError::Custom(0))
}

fn process_schedule_counter_task(
    accounts: &[AccountInfo],
    task_id: i64,
    execution_interval_millis: i64,
    iterations: i64,
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

    let task_instruction = if signer {
        build::add(*payer.key, 1)
    } else {
        build::add_unsigned(*payer.key, 1)
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
