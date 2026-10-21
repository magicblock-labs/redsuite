use std::slice;

use borsh::{BorshDeserialize, BorshSerialize};
use redshift_interface::schedulecommit::*;
use sdk::{
    cpi::{delegate_account, undelegate_account, DelegateAccounts, DelegateConfig},
    ephem::{
        commit_and_undelegate_accounts, CallHandler, FoldableIntentBuilder,
        MagicIntentBundleBuilder,
    },
    utils::create_pda,
    ActionArgs, ShortAccountMeta,
};
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
use system_interface::instruction as system_instruction;

mod intents;

fn invoke_commit<'a, 'info>(
    commit_type: ScheduleCommitType,
    payer: &'a AccountInfo<'info>,
    committees: Vec<&'a AccountInfo<'info>>,
    magic_context: &'a AccountInfo<'info>,
    magic_program: &'a AccountInfo<'info>,
) -> ProgramResult {
    match commit_type {
        ScheduleCommitType::Commit => {
            invoke_via_builder(payer, committees, magic_context, magic_program, false)
        }
        ScheduleCommitType::CommitAndUndelegate => {
            invoke_via_builder(payer, committees, magic_context, magic_program, true)
        }
        ScheduleCommitType::CommitFinalize => {
            invoke_schedule_commit_raw(payer, committees, magic_context, magic_program)
        }
        ScheduleCommitType::CommitFinalizeAndUndelegate => {
            commit_and_undelegate_accounts(payer, committees, magic_context, magic_program, None)
        }
    }
}

fn invoke_via_builder<'a, 'info>(
    payer: &'a AccountInfo<'info>,
    committees: Vec<&'a AccountInfo<'info>>,
    magic_context: &'a AccountInfo<'info>,
    magic_program: &'a AccountInfo<'info>,
    undelegate: bool,
) -> ProgramResult {
    let builder =
        MagicIntentBundleBuilder::new(payer.clone(), magic_context.clone(), magic_program.clone());
    let owned: Vec<_> = committees.into_iter().cloned().collect();
    if undelegate {
        builder.commit_and_undelegate(&owned).build_and_invoke()
    } else {
        builder.commit(&owned).build_and_invoke()
    }
}

fn invoke_schedule_commit_raw<'a, 'info>(
    payer: &'a AccountInfo<'info>,
    committees: Vec<&'a AccountInfo<'info>>,
    magic_context: &'a AccountInfo<'info>,
    magic_program: &'a AccountInfo<'info>,
) -> ProgramResult {
    // The payer keeps its incoming writability so a readonly crank
    // signer can pay a crank-executed commit.
    let mut metas = vec![
        AccountMeta {
            pubkey: *payer.key,
            is_signer: true,
            is_writable: payer.is_writable,
        },
        AccountMeta::new(*magic_context.key, false),
    ];
    metas.extend(committees.iter().map(|committee| AccountMeta {
        pubkey: *committee.key,
        is_signer: committee.is_signer,
        is_writable: committee.is_writable,
    }));

    let instruction = Instruction::new_with_bytes(
        *magic_program.key,
        &MAGIC_SCHEDULE_COMMIT_TAG.to_le_bytes(),
        metas,
    );

    let mut infos = vec![payer.clone(), magic_context.clone()];
    infos.extend(committees.into_iter().cloned());

    invoke(&instruction, &infos)
}

pub fn process(program_id: &Pubkey, accounts: &[AccountInfo], payload: &[u8]) -> ProgramResult {
    if let Some(response) = payload.strip_prefix(&TRANSFER_CALLBACK[1..]) {
        return intents::callback(accounts, response);
    }
    let instruction = ScheduleCommitInstruction::try_from_slice(payload).map_err(|err| {
        msg!("cannot parse the schedulecommit instruction: {}", err);
        ProgramError::InvalidInstructionData
    })?;

    use ScheduleCommitInstruction::*;
    match instruction {
        Init => process_init(program_id, accounts),
        DelegateCpi(args) => process_delegate_cpi(accounts, args),
        ScheduleCommitCpi(args, commit_type) => {
            process_schedulecommit_cpi(accounts, args, commit_type)
        }
        ScheduleCommitAndUndelegateCpiModAfter(players) => {
            process_commit_undelegate_mod_after(accounts, &players)
        }
        ScheduleCommitAndUndelegateCpiTwice(players) => {
            process_commit_undelegate_twice(accounts, &players)
        }
        AdjustCount(change) => {
            let counter = accounts.first().ok_or(ProgramError::NotEnoughAccountKeys)?;
            adjust(counter, change)
        }
        Fail => Err(ProgramError::Custom(0)),
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
        CancelCounterTask { task_id } => process_cancel_counter_task(accounts, task_id),
        CreateActionIntent { counter, count, compute_units } => {
            process_create_action_intent(accounts, counter, count, compute_units)
        }
        AddActionHandler { count } => process_add_action_handler(accounts, count),
        ScheduleIntents { modes, actions } => intents::schedule(accounts, &modes, actions),
        BaseAction { amount, change, fail } => intents::action(accounts, amount, change, fail),
        TransferIntent { amount, fail } => intents::transfer(accounts, amount, fail),
        SetCount(value) => process_set_count(accounts, value),
        InitOrderBook => process_init_order_book(accounts),
        GrowOrderBook(additional_space) => process_grow_order_book(accounts, additional_space),
        DelegateOrderBook(args) => process_delegate_order_book(accounts, args),
        UpdateOrderBook(update) => process_update_order_book(accounts, &update),
        ScheduleCommitForOrderBook(commit_type) => {
            process_commit_for_order_book(accounts, commit_type)
        }
        RequestUndelegation(player) => {
            let [payer, committee, ..] = accounts else {
                return Err(ProgramError::NotEnoughAccountKeys);
            };
            let (pda, bump) = pda_and_bump(&player);
            if committee.key != &pda {
                return Err(ProgramError::InvalidSeeds);
            }
            invoke_signed(
                &sdk::dlp_api::instruction_builder::request_undelegation(
                    *payer.key,
                    pda,
                    crate::ID,
                ),
                accounts,
                &[&[PDA_SEED, player.as_ref(), &[bump]]],
            )
        }
    }
}

fn process_init_order_book(accounts: &[AccountInfo]) -> ProgramResult {
    let [payer, book_manager, order_book, system_program] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !payer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let (expected_pda, bump) = order_book_pda_and_bump(book_manager.key);
    if order_book.key != &expected_pda {
        msg!(
            "the order book PDA {} is not correct for manager {}",
            order_book.key,
            book_manager.key
        );
        return Err(ProgramError::InvalidSeeds);
    }

    let bump_slice = [bump];
    let seeds: [&[u8]; 3] = [ORDER_BOOK_SEED, book_manager.key.as_ref(), &bump_slice];
    create_pda(
        order_book,
        &crate::ID,
        ORDER_BOOK_INIT_SIZE,
        &[&seeds],
        system_program,
        payer,
        true,
    )
}

fn process_grow_order_book(accounts: &[AccountInfo], additional_space: u64) -> ProgramResult {
    let [payer, book_manager, order_book, system_program] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !payer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let (expected_pda, _) = order_book_pda_and_bump(book_manager.key);
    if order_book.key != &expected_pda {
        return Err(ProgramError::InvalidSeeds);
    }

    let new_size = order_book.data_len() + additional_space as usize;
    let required = Rent::get()?.minimum_balance(new_size);
    let current = order_book.lamports();
    if current < required {
        let mut data = SYSTEM_TRANSFER_TAG.to_le_bytes().to_vec();
        data.extend_from_slice(&(required - current).to_le_bytes());
        let transfer = Instruction {
            program_id: *system_program.key,
            accounts: vec![
                AccountMeta::new(*payer.key, true),
                AccountMeta::new(*order_book.key, false),
            ],
            data,
        };
        invoke(
            &transfer,
            &[payer.clone(), order_book.clone(), system_program.clone()],
        )?;
    }

    order_book.resize(new_size)?;
    Ok(())
}

fn process_delegate_order_book(
    accounts: &[AccountInfo],
    args: DelegateOrderBookArgs,
) -> ProgramResult {
    let accounts = delegate_accounts(accounts)?;
    let seeds: [&[u8]; 2] = [ORDER_BOOK_SEED, args.book_manager.as_ref()];
    delegate_account(
        accounts,
        &seeds,
        DelegateConfig {
            commit_frequency_ms: args.commit_frequency_ms,
            validator: args.validator,
        },
    )?;
    Ok(())
}

fn process_update_order_book(accounts: &[AccountInfo], update: &BookUpdate) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let order_book = next_account_info(iter)?;
    if !payer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    order_book_apply(&mut order_book.try_borrow_mut_data()?, update)
}

fn process_commit_for_order_book(
    accounts: &[AccountInfo],
    commit_type: ScheduleCommitType,
) -> ProgramResult {
    let [payer, order_book, magic_context, magic_program] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !payer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    invoke_commit(
        commit_type,
        payer,
        vec![order_book],
        magic_context,
        magic_program,
    )
}

fn process_init(program_id: &Pubkey, accounts: &[AccountInfo]) -> ProgramResult {
    let [payer, player, pda, system_program] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !player.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let (expected_pda, bump) = pda_and_bump(player.key);
    if pda.key != &expected_pda {
        msg!(
            "the PDA {} is not correct for player {}",
            pda.key,
            player.key
        );
        return Err(ProgramError::InvalidSeeds);
    }

    let bump_slice = [bump];
    let seeds: [&[u8]; 3] = [PDA_SEED, player.key.as_ref(), &bump_slice];
    create_pda(
        pda,
        program_id,
        MainAccount::SIZE,
        &[&seeds],
        system_program,
        payer,
        true,
    )?;

    let account = MainAccount {
        player: *player.key,
        count: 0,
        updates: 0,
    };
    account.serialize(&mut &mut pda.try_borrow_mut_data()?.as_mut())?;
    Ok(())
}

fn process_delegate_cpi(accounts: &[AccountInfo], args: DelegateCpiArgs) -> ProgramResult {
    let accounts = delegate_accounts(accounts)?;
    let seeds: [&[u8]; 2] = [PDA_SEED, args.player.as_ref()];
    delegate_account(
        accounts,
        &seeds,
        DelegateConfig {
            commit_frequency_ms: args.commit_frequency_ms,
            validator: args.validator,
        },
    )?;
    Ok(())
}

fn update_count(
    account: &AccountInfo,
    update: impl FnOnce(u64) -> Result<u64, ProgramError>,
) -> ProgramResult {
    let mut state = MainAccount::try_from_slice(&account.try_borrow_data()?)?;
    state.count = update(state.count)?;
    state.updates = state.updates.checked_add(1).ok_or(ProgramError::ArithmeticOverflow)?;
    state.serialize(&mut account.try_borrow_mut_data()?.as_mut())?;
    Ok(())
}

fn adjust(account: &AccountInfo, change: i64) -> ProgramResult {
    update_count(account, |count| {
        count.checked_add_signed(change).ok_or(ProgramError::ArithmeticOverflow)
    })
}

fn increase_committee_counts(committees: &[AccountInfo]) -> ProgramResult {
    for committee in committees {
        adjust(committee, 1)?;
    }
    Ok(())
}

fn process_schedulecommit_cpi(
    accounts: &[AccountInfo],
    args: ScheduleCommitCpiArgs,
    commit_type: ScheduleCommitType,
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let remaining = iter.as_slice();

    if remaining.len() != args.players.len() {
        msg!(
            "players {} != committees {}",
            args.players.len(),
            remaining.len()
        );
        return Err(ProgramError::InvalidArgument);
    }

    if args.modify_accounts {
        increase_committee_counts(remaining)?;
    }

    let mut committees: Vec<_> = remaining.iter().collect();
    if args.commit_payer {
        committees.push(payer);
    }

    invoke_commit(commit_type, payer, committees, magic_context, magic_program)
}

fn process_commit_undelegate_mod_after(
    accounts: &[AccountInfo],
    players: &[Pubkey],
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let remaining = iter.as_slice();

    if remaining.len() != players.len() {
        return Err(ProgramError::InvalidArgument);
    }

    commit_and_undelegate_accounts(
        payer,
        remaining.iter().collect(),
        magic_context,
        magic_program,
        None,
    )?;

    increase_committee_counts(remaining)
}

fn process_commit_undelegate_twice(accounts: &[AccountInfo], players: &[Pubkey]) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let remaining = iter.as_slice();

    if remaining.len() != players.len() {
        return Err(ProgramError::InvalidArgument);
    }

    commit_and_undelegate_accounts(
        payer,
        remaining.iter().collect(),
        magic_context,
        magic_program,
        None,
    )?;

    let mut metas =
        vec![AccountMeta::new(*payer.key, true), AccountMeta::new(*magic_context.key, false)];
    metas.extend(remaining.iter().map(|committee| AccountMeta {
        pubkey: *committee.key,
        is_signer: true,
        is_writable: committee.is_writable,
    }));
    let instruction = Instruction::new_with_bytes(
        *magic_program.key,
        &MAGIC_SCHEDULE_COMMIT_AND_UNDELEGATE_TAG.to_le_bytes(),
        metas,
    );

    let mut infos = vec![payer.clone(), magic_context.clone()];
    infos.extend(remaining.iter().cloned());

    let all_seeds: Vec<(Vec<u8>, [u8; 1])> = players
        .iter()
        .map(|player| {
            let (_, bump) = pda_and_bump(player);
            (player.as_ref().to_vec(), [bump])
        })
        .collect();
    let seed_slices: Vec<[&[u8]; 3]> = all_seeds
        .iter()
        .map(|(player_bytes, bump)| [PDA_SEED, player_bytes.as_slice(), bump.as_slice()])
        .collect();
    let signer_seeds: Vec<&[&[u8]]> = seed_slices.iter().map(|seeds| seeds.as_slice()).collect();

    invoke_signed(&instruction, &infos, &signer_seeds)
}

fn process_set_count(accounts: &[AccountInfo], value: u64) -> ProgramResult {
    let iter = &mut accounts.iter();
    let account = next_account_info(iter)?;
    update_count(account, |_| Ok(value))
}

pub fn process_undelegate_request(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    seeds_data: &[u8],
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let delegated_account = next_account_info(iter)?;
    let buffer = next_account_info(iter)?;
    let payer = next_account_info(iter)?;
    let system_program = next_account_info(iter)?;

    let account_seeds = <Vec<Vec<u8>>>::try_from_slice(seeds_data).map_err(|err| {
        msg!("cannot parse the undelegation seeds: {}", err);
        ProgramError::InvalidArgument
    })?;

    undelegate_account(
        delegated_account,
        program_id,
        buffer,
        payer,
        system_program,
        account_seeds,
    )?;

    let data = delegated_account.try_borrow_data()?;
    if data.len() == MainAccount::SIZE {
        if let Ok(counter) = MainAccount::try_from_slice(&data) {
            if counter.count == FAIL_UNDELEGATION_COUNT {
                return Err(ProgramError::Custom(111));
            }
        }
    } else if book_lens(&data).is_none() {
        msg!("the undelegated data is not a valid order book");
    }
    Ok(())
}

pub(crate) fn delegate_accounts<'a, 'info>(
    accounts: &'a [AccountInfo<'info>],
) -> Result<DelegateAccounts<'a, 'info>, ProgramError> {
    let [payer, pda, owner_program, buffer, delegation_record, delegation_metadata, delegation_program, system_program] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    Ok(DelegateAccounts {
        payer,
        pda,
        owner_program,
        buffer,
        delegation_record,
        delegation_metadata,
        delegation_program,
        system_program,
    })
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
    let action = ScheduleCommitInstruction::AddActionHandler { count };
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
    MagicIntentBundleBuilder::new(payer.clone(), magic_context.clone(), magic_program.clone())
        .add_standalone_actions([handler])
        .build_and_invoke()
}

fn process_add_action_handler(accounts: &[AccountInfo], count: u8) -> ProgramResult {
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
    adjust(counter, i64::from(count))
}

fn process_schedule_counter_task(
    accounts: &[AccountInfo],
    task_id: i64,
    execution_interval_millis: i64,
    iterations: i64,
    signer: bool,
) -> ProgramResult {
    use magic_api::{args::ScheduleTaskArgs, instruction::MagicBlockInstruction};

    let [magic_program, payer, counter] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    let (expected_pda, bump) = pda_and_bump(payer.key);
    if counter.key != &expected_pda {
        return Err(ProgramError::InvalidSeeds);
    }

    let task_instruction = if signer {
        build::add(*payer.key, 1)
    } else {
        build::add_unsigned(*payer.key, 1)
    };
    let data = bincode::serialize(&MagicBlockInstruction::ScheduleTask(ScheduleTaskArgs {
        task_id,
        execution_interval_millis,
        iterations,
        instructions: vec![task_instruction],
    }))
    .map_err(|_| ProgramError::InvalidArgument)?;

    let instruction = Instruction::new_with_bytes(
        *magic_program.key,
        &data,
        vec![AccountMeta::new(*payer.key, true), AccountMeta::new(*counter.key, true)],
    );

    let bump_slice = [bump];
    let seeds: [&[u8]; 3] = [PDA_SEED, payer.key.as_ref(), &bump_slice];
    invoke_signed(&instruction, &[payer.clone(), counter.clone()], &[&seeds])
}

fn process_cancel_counter_task(accounts: &[AccountInfo], task_id: i64) -> ProgramResult {
    use magic_api::instruction::MagicBlockInstruction;

    let [magic_program, payer] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    let data = bincode::serialize(&MagicBlockInstruction::CancelTask { task_id })
        .map_err(|_| ProgramError::InvalidArgument)?;
    let instruction = Instruction::new_with_bytes(
        *magic_program.key,
        &data,
        vec![AccountMeta::new(*payer.key, true)],
    );
    invoke(&instruction, slice::from_ref(payer))
}
