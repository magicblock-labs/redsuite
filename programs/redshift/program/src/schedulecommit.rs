use borsh::{BorshDeserialize, BorshSerialize};
use redshift_interface::schedulecommit::*;
use sdk::{
    consts::DELEGATION_PROGRAM_ID,
    cpi::{
        delegate_account, undelegate_account, DelegateAccounts, DelegateConfig,
    },
    ephem::{
        commit_accounts, commit_and_undelegate_accounts, CallHandler,
        FoldableIntentBuilder, MagicIntentBundleBuilder,
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

fn invoke_commit<'a, 'info>(
    commit_type: ScheduleCommitType,
    payer: &'a AccountInfo<'info>,
    committees: Vec<&'a AccountInfo<'info>>,
    magic_context: &'a AccountInfo<'info>,
    magic_program: &'a AccountInfo<'info>,
    magic_fee_vault: Option<&'a AccountInfo<'info>>,
) -> ProgramResult {
    match commit_type {
        ScheduleCommitType::Commit => invoke_via_builder(
            payer,
            committees,
            magic_context,
            magic_program,
            magic_fee_vault,
            false,
        ),
        ScheduleCommitType::CommitAndUndelegate => invoke_via_builder(
            payer,
            committees,
            magic_context,
            magic_program,
            magic_fee_vault,
            true,
        ),
        ScheduleCommitType::CommitFinalize => invoke_schedule_commit_raw(
            payer,
            committees,
            magic_context,
            magic_program,
            magic_fee_vault,
        ),
        ScheduleCommitType::CommitFinalizeAndUndelegate => {
            commit_and_undelegate_accounts(
                payer,
                committees,
                magic_context,
                magic_program,
                magic_fee_vault,
            )
        }
    }
}

fn invoke_via_builder<'a, 'info>(
    payer: &'a AccountInfo<'info>,
    committees: Vec<&'a AccountInfo<'info>>,
    magic_context: &'a AccountInfo<'info>,
    magic_program: &'a AccountInfo<'info>,
    magic_fee_vault: Option<&'a AccountInfo<'info>>,
    undelegate: bool,
) -> ProgramResult {
    let builder = MagicIntentBundleBuilder::new(
        payer.clone(),
        magic_context.clone(),
        magic_program.clone(),
    );
    let builder = match magic_fee_vault {
        Some(vault) => builder.magic_fee_vault(vault.clone()),
        None => builder,
    };
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
    magic_fee_vault: Option<&'a AccountInfo<'info>>,
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
    if let Some(vault) = magic_fee_vault {
        metas.push(AccountMeta::new(*vault.key, false));
    }
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
    if let Some(vault) = magic_fee_vault {
        infos.push(vault.clone());
    }
    infos.extend(committees.into_iter().cloned());

    invoke(&instruction, &infos)
}

pub fn process(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    payload: &[u8],
) -> ProgramResult {
    let instruction = ScheduleCommitInstruction::try_from_slice(payload)
        .map_err(|err| {
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
        ScheduleCommitWithVaultCpi(args) => {
            process_schedulecommit_with_vault_cpi(accounts, args)
        }
        ScheduleCommitAndUndelegateCpiModAfter(players) => {
            process_commit_undelegate_mod_after(accounts, &players)
        }
        ScheduleCommitAndUndelegateCpiTwice(players) => {
            process_commit_undelegate_twice(accounts, &players)
        }
        IncreaseCount => process_increase_count(accounts),
        SetCount(value) => process_set_count(accounts, value),
        InitOrderBook => process_init_order_book(accounts),
        GrowOrderBook(additional_space) => {
            process_grow_order_book(accounts, additional_space)
        }
        DelegateOrderBook(args) => process_delegate_order_book(accounts, args),
        UpdateOrderBook(update) => process_update_order_book(accounts, &update),
        ScheduleCommitWithVaultAndOrderBookCpi(args) => {
            process_commit_with_vault_and_order_book(accounts, &args)
        }
        ScheduleCommitForOrderBook(commit_type) => {
            process_commit_for_order_book(accounts, commit_type)
        }
        RequestUndelegationCpi(player) => {
            process_request_undelegation_cpi(accounts, &player)
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
    let seeds: [&[u8]; 3] =
        [ORDER_BOOK_SEED, book_manager.key.as_ref(), &bump_slice];
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

fn process_grow_order_book(
    accounts: &[AccountInfo],
    additional_space: u64,
) -> ProgramResult {
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
    let [payer, order_book, owner_program, buffer, delegation_record, delegation_metadata, delegation_program, system_program] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let seeds: [&[u8]; 2] = [ORDER_BOOK_SEED, args.book_manager.as_ref()];
    delegate_account(
        DelegateAccounts {
            payer,
            pda: order_book,
            owner_program,
            buffer,
            delegation_record,
            delegation_metadata,
            delegation_program,
            system_program,
        },
        &seeds,
        DelegateConfig {
            commit_frequency_ms: args.commit_frequency_ms,
            validator: args.validator,
        },
    )?;
    Ok(())
}

fn process_update_order_book(
    accounts: &[AccountInfo],
    update: &BookUpdate,
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let order_book = next_account_info(iter)?;
    if !payer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    order_book_apply(&mut order_book.try_borrow_mut_data()?, update)
}

fn process_commit_with_vault_and_order_book(
    accounts: &[AccountInfo],
    args: &ScheduleCommitWithOrderBookArgs,
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let magic_fee_vault = next_account_info(iter)?;
    let order_book = next_account_info(iter)?;
    let committees: Vec<_> = iter.cloned().collect();

    if committees.len() != args.players.len() {
        msg!(
            "players {} != committees {}",
            args.players.len(),
            committees.len()
        );
        return Err(ProgramError::InvalidArgument);
    }

    let mut builder = MagicIntentBundleBuilder::new(
        payer.clone(),
        magic_context.clone(),
        magic_program.clone(),
    )
    .magic_fee_vault(magic_fee_vault.clone())
    .commit(&committees);

    if args.with_actions {
        let update = ScheduleCommitInstruction::UpdateOrderBook(BookUpdate {
            bids: vec![OrderLevel {
                price: 100,
                size: 10,
            }],
            asks: vec![],
        });
        let mut update_data = vec![crate::SCHEDULE_COMMIT_TAG];
        update_data.extend(
            borsh::to_vec(&update)
                .map_err(|_| ProgramError::InvalidInstructionData)?,
        );
        let call_handler = CallHandler {
            args: ActionArgs {
                data: update_data,
                escrow_index: 1,
            },
            compute_units: 50_000,
            escrow_authority: payer.clone(),
            destination_program: crate::ID,
            accounts: vec![ShortAccountMeta {
                pubkey: *order_book.key,
                is_writable: true,
            }],
        };
        builder = builder.add_post_commit_actions([call_handler]);
    }

    builder.build_and_invoke()
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
        None,
    )
}

fn process_request_undelegation_cpi(
    accounts: &[AccountInfo],
    player: &Pubkey,
) -> ProgramResult {
    let [payer, delegated_account, owner_program, undelegation_request, delegation_record, delegation_metadata, system_program, delegation_program] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !payer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if owner_program.key != &crate::ID
        || delegation_program.key != &DELEGATION_PROGRAM_ID
    {
        return Err(ProgramError::InvalidArgument);
    }

    let (expected_pda, bump) = pda_and_bump(player);
    if delegated_account.key != &expected_pda {
        return Err(ProgramError::InvalidSeeds);
    }

    let instruction = Instruction {
        program_id: *delegation_program.key,
        accounts: vec![
            AccountMeta::new(*payer.key, true),
            AccountMeta::new_readonly(*delegated_account.key, true),
            AccountMeta::new_readonly(*owner_program.key, false),
            AccountMeta::new(*undelegation_request.key, false),
            AccountMeta::new_readonly(*delegation_record.key, false),
            AccountMeta::new(*delegation_metadata.key, false),
            AccountMeta::new_readonly(*system_program.key, false),
        ],
        data: DLP_REQUEST_UNDELEGATION_TAG.to_le_bytes().to_vec(),
    };

    let bump_slice = [bump];
    let seeds: [&[u8]; 3] = [PDA_SEED, player.as_ref(), &bump_slice];
    invoke_signed(
        &instruction,
        &[
            payer.clone(),
            delegated_account.clone(),
            owner_program.clone(),
            undelegation_request.clone(),
            delegation_record.clone(),
            delegation_metadata.clone(),
            system_program.clone(),
            delegation_program.clone(),
        ],
        &[&seeds],
    )
}

fn process_init(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
) -> ProgramResult {
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
    };
    account.serialize(&mut &mut pda.try_borrow_mut_data()?.as_mut())?;
    Ok(())
}

fn process_delegate_cpi(
    accounts: &[AccountInfo],
    args: DelegateCpiArgs,
) -> ProgramResult {
    let [payer, pda, owner_program, buffer, delegation_record, delegation_metadata, delegation_program, system_program] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let seeds: [&[u8]; 2] = [PDA_SEED, args.player.as_ref()];
    delegate_account(
        DelegateAccounts {
            payer,
            pda,
            owner_program,
            buffer,
            delegation_record,
            delegation_metadata,
            delegation_program,
            system_program,
        },
        &seeds,
        DelegateConfig {
            commit_frequency_ms: args.commit_frequency_ms,
            validator: args.validator,
        },
    )?;
    Ok(())
}

fn increase_committee_counts(committees: &[AccountInfo]) -> ProgramResult {
    for committee in committees {
        let mut account = {
            let data = committee.try_borrow_data()?;
            MainAccount::try_from_slice(&data)?
        };
        account.count += 1;
        account
            .serialize(&mut &mut committee.try_borrow_mut_data()?.as_mut())?;
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
    let magic_fee_vault = if args.has_magic_vault {
        Some(next_account_info(iter)?)
    } else {
        None
    };
    let remaining: Vec<_> = iter.cloned().collect();

    if remaining.len() != args.players.len() {
        msg!(
            "players {} != committees {}",
            args.players.len(),
            remaining.len()
        );
        return Err(ProgramError::InvalidArgument);
    }

    if args.modify_accounts {
        increase_committee_counts(&remaining)?;
    }

    let mut committees: Vec<_> = remaining.iter().collect();
    if args.commit_payer {
        committees.push(payer);
    }

    invoke_commit(
        commit_type,
        payer,
        committees,
        magic_context,
        magic_program,
        magic_fee_vault,
    )
}

fn process_schedulecommit_with_vault_cpi(
    accounts: &[AccountInfo],
    args: ScheduleCommitCpiWithVaultArgs,
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let magic_fee_vault = if args.has_magic_vault {
        Some(next_account_info(iter)?)
    } else {
        None
    };
    let remaining: Vec<_> = iter.cloned().collect();

    if remaining.len() != args.players.len() {
        msg!(
            "players {} != committees {}",
            args.players.len(),
            remaining.len()
        );
        return Err(ProgramError::InvalidArgument);
    }

    let committees: Vec<_> = remaining.iter().collect();
    if args.undelegate {
        commit_and_undelegate_accounts(
            payer,
            committees,
            magic_context,
            magic_program,
            magic_fee_vault,
        )
    } else {
        commit_accounts(
            payer,
            committees,
            magic_context,
            magic_program,
            magic_fee_vault,
        )
    }
}

fn process_commit_undelegate_mod_after(
    accounts: &[AccountInfo],
    players: &[Pubkey],
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let remaining: Vec<_> = iter.cloned().collect();

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

    increase_committee_counts(&remaining)
}

fn process_commit_undelegate_twice(
    accounts: &[AccountInfo],
    players: &[Pubkey],
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let remaining: Vec<_> = iter.cloned().collect();

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

    let mut metas = vec![
        AccountMeta::new(*payer.key, true),
        AccountMeta::new(*magic_context.key, false),
    ];
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
        .map(|(player_bytes, bump)| {
            [PDA_SEED, player_bytes.as_slice(), bump.as_slice()]
        })
        .collect();
    let signer_seeds: Vec<&[&[u8]]> =
        seed_slices.iter().map(|seeds| seeds.as_slice()).collect();

    invoke_signed(&instruction, &infos, &signer_seeds)
}

fn process_increase_count(accounts: &[AccountInfo]) -> ProgramResult {
    let iter = &mut accounts.iter();
    let account = next_account_info(iter)?;
    let mut main_account = {
        let data = account.try_borrow_data()?;
        MainAccount::try_from_slice(&data)?
    };
    main_account.count += 1;
    main_account
        .serialize(&mut &mut account.try_borrow_mut_data()?.as_mut())?;
    Ok(())
}

fn process_set_count(accounts: &[AccountInfo], value: u64) -> ProgramResult {
    let iter = &mut accounts.iter();
    let account = next_account_info(iter)?;
    let mut main_account = {
        let data = account.try_borrow_data()?;
        MainAccount::try_from_slice(&data)?
    };
    main_account.count = value;
    main_account
        .serialize(&mut &mut account.try_borrow_mut_data()?.as_mut())?;
    Ok(())
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

    let account_seeds =
        <Vec<Vec<u8>>>::try_from_slice(seeds_data).map_err(|err| {
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
    } else if crate::flexi::undelegate_label_poison(&data) {
        return Err(ProgramError::Custom(crate::flexi::FAIL_UNDELEGATION_CODE));
    } else if book_lens(&data).is_none() {
        msg!("the undelegated data is not a valid order book");
    }
    Ok(())
}
