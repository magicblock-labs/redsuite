use delegation_program_interface as dpi;
use instruction::Instruction as SolanaInstruction;
use pubkey::Pubkey;
use sdk::{
    consts::EXTERNAL_UNDELEGATE_DISCRIMINATOR,
    cpi::{delegate_account, undelegate_account, DelegateAccounts, DelegateConfig},
    utils::create_pda,
};
use solana_program::{
    account_info::AccountInfo,
    entrypoint::ProgramResult,
    msg,
    program::{invoke, invoke_signed},
    program_error::ProgramError,
    rent::Rent,
    sysvar::Sysvar,
};
use system_interface::instruction as system_instruction;
use wincode::{
    config::{self, Configuration, DefaultConfig},
    SchemaRead,
};

use crate::{CellHeader, CellId, HashState, Instruction, Operation, UndelegationOutcome};

mod settlement;

pub fn process(program_id: &Pubkey, accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    if let Some(data) = data.strip_prefix(&EXTERNAL_UNDELEGATE_DISCRIMINATOR) {
        return undelegate(program_id, accounts, data);
    }
    let mut remaining = data;
    let instruction =
        <Instruction as SchemaRead<DefaultConfig>>::get(&mut remaining).map_err(|error| {
            msg!("invalid fixture instruction: {}", error);
            ProgramError::InvalidInstructionData
        })?;
    if !remaining.is_empty() && !matches!(instruction, Instruction::HandleResult) {
        return Err(ProgramError::InvalidInstructionData);
    }
    match instruction {
        Instruction::Create { id, space } => create(program_id, accounts, id, space as usize),
        Instruction::Delegate {
            id,
            validator,
            commit_frequency_ms,
        } => delegate(
            program_id,
            accounts,
            id,
            DelegateConfig { validator, commit_frequency_ms },
        ),
        Instruction::Resize { space } => resize(program_id, accounts, space as usize),
        Instruction::Execute { operations, .. } => run(program_id, accounts, &operations),
        Instruction::Action { guard, operations, .. } => {
            let [.., source, _, escrow] = accounts else {
                return Err(ProgramError::NotEnoughAccountKeys);
            };
            if source.key != program_id {
                return Err(ProgramError::IncorrectProgramId);
            }
            if !escrow.is_signer {
                return Err(ProgramError::MissingRequiredSignature);
            }
            let cell = accounts
                .iter()
                .find(|account| account.key == &guard.cell)
                .ok_or(ProgramError::NotEnoughAccountKeys)?;
            if cell.owner != &guard.owner {
                return Err(ProgramError::InvalidAccountOwner);
            }
            run(program_id, accounts, &operations)
        }
        Instruction::HandleResult => settlement::handle_result(accounts, remaining),
        Instruction::DelegateV2 { id } => delegate_v2(program_id, accounts, id),
    }
}

pub(crate) fn info<'a, 'info>(
    accounts: &'a [AccountInfo<'info>],
    index: u16,
) -> Result<&'a AccountInfo<'info>, ProgramError> {
    accounts.get(usize::from(index)).ok_or(ProgramError::NotEnoughAccountKeys)
}

fn update(
    account: &AccountInfo,
    f: impl FnOnce(&mut CellHeader, &mut [u8]) -> ProgramResult,
) -> ProgramResult {
    let mut data = account.try_borrow_mut_data()?;
    let mut cell = CellHeader::try_decode(&data).map_err(|_| ProgramError::InvalidAccountData)?;
    f(&mut cell, CellHeader::payload_mut(&mut data)?)?;
    cell.revision = cell.revision.checked_add(1).ok_or(ProgramError::ArithmeticOverflow)?;
    cell.write(&mut data)
}

pub(crate) fn adjust(account: &AccountInfo, change: i64) -> ProgramResult {
    update(account, |cell, _| {
        cell.value =
            cell.value.checked_add_signed(change).ok_or(ProgramError::ArithmeticOverflow)?;
        Ok(())
    })
}

fn each(
    accounts: &[AccountInfo],
    cells: &[u16],
    mut f: impl FnMut(&AccountInfo) -> ProgramResult,
) -> ProgramResult {
    for &index in cells {
        f(info(accounts, index)?)?;
    }
    Ok(())
}

fn run(program_id: &Pubkey, accounts: &[AccountInfo], operations: &[Operation]) -> ProgramResult {
    for (step, operation) in operations.iter().enumerate() {
        if let Err(error) = apply(program_id, accounts, operation) {
            msg!("batch step {} failed: {:?}", step, error);
            return Err(error);
        }
    }
    Ok(())
}

fn apply(program_id: &Pubkey, accounts: &[AccountInfo], operation: &Operation) -> ProgramResult {
    match operation {
        Operation::Set { cells, value } => each(accounts, cells, |account| {
            update(account, |cell, _| {
                cell.value = *value;
                Ok(())
            })
        })?,
        Operation::Add { cells, change } => {
            each(accounts, cells, |account| adjust(account, *change))?
        }
        Operation::Fill { cells, seed, step } => each(accounts, cells, |account| {
            update(account, |_, payload| {
                CellHeader::fill(payload, *seed, *step);
                Ok(())
            })
        })?,
        Operation::Hash { cells, initial, rounds } => {
            let hash = HashState::chain(*initial, *rounds);
            each(accounts, cells, |account| write_payload(account, &hash))?;
        }
        Operation::FoldHashes { cells, salt, rounds } => {
            let hashes = cells
                .iter()
                .map(|&index| {
                    let data = info(accounts, index)?.try_borrow_data()?;
                    CellHeader::payload(&data)?
                        .first_chunk::<{ HashState::SIZE }>()
                        .copied()
                        .ok_or(ProgramError::AccountDataTooSmall)
                })
                .collect::<Result<Vec<[u8; 32]>, ProgramError>>()?;
            let hash = HashState::fold(*salt, &hashes, *rounds);
            each(accounts, cells, |account| write_payload(account, &hash))?;
        }
        Operation::Copy { sources, destinations } => {
            if sources.is_empty() {
                return Err(ProgramError::NotEnoughAccountKeys);
            }
            for (position, &destination) in destinations.iter().enumerate() {
                let source = info(accounts, sources[position % sources.len()])?;
                let destination = info(accounts, destination)?;
                if source.key == destination.key {
                    update(destination, |_, _| Ok(()))?;
                    continue;
                }
                let source = source.try_borrow_data()?;
                let source = CellHeader::payload(&source)?;
                update(destination, |_, payload| {
                    let len = source.len().min(payload.len());
                    payload[..len].copy_from_slice(&source[..len]);
                    Ok(())
                })?;
            }
        }
        Operation::SumSizes { output, inputs } => {
            let mut sum = 0u64;
            for &index in inputs {
                sum += info(accounts, index)?.data_len() as u64;
            }
            write_payload(info(accounts, *output)?, &sum.to_le_bytes())?;
        }
        Operation::Inspect { accounts: indices, hash } => each(accounts, indices, |account| {
            if *hash {
                msg!(
                    "CellObservation: {} {} {} {}",
                    account.owner,
                    account.data_len(),
                    account.lamports(),
                    solana_program::hash::hash(&account.try_borrow_data()?)
                );
            } else {
                msg!(
                    "CellObservation: {} {} {}",
                    account.owner,
                    account.data_len(),
                    account.lamports()
                );
            }
            Ok(())
        })?,
        Operation::FillBytes { account, byte } => {
            info(accounts, *account)?.try_borrow_mut_data()?.fill(*byte)
        }
        Operation::Undelegation { cells, outcome } => each(accounts, cells, |account| {
            update(account, |cell, _| {
                cell.undelegation = *outcome;
                Ok(())
            })
        })?,
        Operation::Invoke {
            program,
            accounts: metas,
            data,
            signers,
        } => {
            let program = info(accounts, *program)?;
            let instruction = SolanaInstruction {
                program_id: *program.key,
                data: data.clone(),
                accounts: metas
                    .iter()
                    .map(|meta| Ok(meta.resolve(*info(accounts, meta.index)?.key)))
                    .collect::<Result<_, ProgramError>>()?,
            };
            let mut infos = metas
                .iter()
                .map(|meta| info(accounts, meta.index).cloned())
                .collect::<Result<Vec<_>, _>>()?;
            infos.push(program.clone());
            let seeds: Vec<_> = signers
                .iter()
                .map(|id| (id.authority, id.index.to_le_bytes(), [id.pda(program_id).1]))
                .collect();
            let slices: Vec<_> = seeds
                .iter()
                .map(|(authority, index, bump)| {
                    [CellId::SEED, authority.as_ref(), index.as_slice(), bump.as_slice()]
                })
                .collect();
            let slices: Vec<_> = slices.iter().map(|seed| seed.as_slice()).collect();
            invoke_signed(&instruction, &infos, &slices)?;
        }
        Operation::Settle(settlement) => settlement::invoke(program_id, accounts, settlement)?,
        Operation::TransferFromEscrow { destination, system, amount } => {
            let escrow = accounts.last().ok_or(ProgramError::NotEnoughAccountKeys)?;
            if !escrow.is_signer {
                return Err(ProgramError::MissingRequiredSignature);
            }
            let destination = info(accounts, *destination)?;
            let system = info(accounts, *system)?;
            invoke(
                &system_instruction::transfer(escrow.key, destination.key, *amount),
                &[escrow.clone(), destination.clone(), system.clone()],
            )?;
        }
        Operation::ResizeBytes { account, length, byte } => {
            let account = info(accounts, *account)?;
            account.resize(*length as usize)?;
            if let Some(first) = account.try_borrow_mut_data()?.first_mut() {
                *first = *byte;
            }
        }
        Operation::Log(message) => msg!("{}", message),
        Operation::Fail(code) => return Err(ProgramError::Custom(*code)),
    }
    Ok(())
}

fn write_payload(account: &AccountInfo, bytes: &[u8]) -> ProgramResult {
    update(account, |_, payload| {
        payload
            .get_mut(..bytes.len())
            .ok_or(ProgramError::AccountDataTooSmall)?
            .copy_from_slice(bytes);
        Ok(())
    })
}

fn create(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    id: CellId,
    space: usize,
) -> ProgramResult {
    let [payer, authority, cell, system] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !authority.is_signer || authority.key != &id.authority {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if space < CellHeader::SIZE {
        return Err(ProgramError::AccountDataTooSmall);
    }
    let (pda, bump) = id.pda(program_id);
    if cell.key != &pda {
        return Err(ProgramError::InvalidSeeds);
    }
    create_pda(
        cell,
        program_id,
        space,
        &[&[CellId::SEED, id.authority.as_ref(), &id.index.to_le_bytes(), &[bump]]],
        system,
        payer,
        true,
    )?;
    CellHeader::new(id).write(&mut cell.try_borrow_mut_data()?)
}

fn delegate(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    id: CellId,
    config: DelegateConfig,
) -> ProgramResult {
    let [payer, pda, owner_program, buffer, delegation_record, delegation_metadata, delegation_program, system_program, authority] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if authority.key != &id.authority || !authority.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if pda.key != &id.pda(program_id).0 {
        return Err(ProgramError::InvalidSeeds);
    }
    let cell = CellHeader::try_decode(&pda.try_borrow_data()?)
        .map_err(|_| ProgramError::InvalidAccountData)?;
    if cell.id != id {
        return Err(ProgramError::InvalidSeeds);
    }
    let delegate = if config.validator == Some(sdk_ids::system_program::ID) {
        sdk::cpi::delegate_account_with_any_validator
    } else {
        delegate_account
    };
    delegate(
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
        &[CellId::SEED, id.authority.as_ref(), &id.index.to_le_bytes()],
        config,
    )
}

fn delegate_v2(program_id: &Pubkey, accounts: &[AccountInfo], id: CellId) -> ProgramResult {
    let [payer, cell, owner, buffer, _state, cluster, system, dlp, authority] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if authority.key != &id.authority || !authority.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if owner.key != program_id
        || cell.owner != program_id
        || dlp.key != &dpi::id()
        || system.key != &sdk_ids::system_program::ID
    {
        return Err(ProgramError::IncorrectProgramId);
    }
    let (address, bump) = id.pda(program_id);
    if cell.key != &address {
        return Err(ProgramError::InvalidSeeds);
    }
    if CellHeader::try_decode(&cell.try_borrow_data()?)
        .map_err(|_| ProgramError::InvalidAccountData)?
        .id
        != id
    {
        return Err(ProgramError::InvalidSeeds);
    }
    let index = id.index.to_le_bytes();
    let seeds: &[&[u8]] = &[CellId::SEED, id.authority.as_ref(), &index];
    let cell_seeds = &[CellId::SEED, id.authority.as_ref(), &index, &[bump]];
    let buffer_seeds = &[dpi::legacy::pda::DELEGATE_BUFFER_TAG, cell.key.as_ref()];
    let (_, buffer_bump) = Pubkey::find_program_address(buffer_seeds, program_id);
    let buffer_seeds = &[buffer_seeds[0], buffer_seeds[1], &[buffer_bump]];
    create_pda(
        buffer,
        program_id,
        cell.data_len(),
        &[buffer_seeds],
        system,
        payer,
        false,
    )?;
    buffer.try_borrow_mut_data()?.copy_from_slice(&cell.try_borrow_data()?);
    cell.try_borrow_mut_data()?.fill(0);
    cell.assign(system.key);
    invoke_signed(
        &system_instruction::assign(cell.key, dlp.key),
        &[cell.clone(), system.clone()],
        &[cell_seeds],
    )?;
    let args = dpi::DelegateArgs::new(
        bump,
        seeds.iter().map(|seed| dpi::DelegationSeed { value: seed.to_vec() }).collect(),
    );
    let instruction =
        dpi::instruction_builder::delegate(*payer.key, *cell.key, *program_id, *cluster.key, args)
            .map_err(|_| ProgramError::InvalidInstructionData)?;
    invoke_signed(&instruction, accounts, &[cell_seeds])?;
    sdk::utils::close_pda_with_system_transfer(buffer, &[buffer_seeds], payer, system)
}

fn resize(program_id: &Pubkey, accounts: &[AccountInfo], space: usize) -> ProgramResult {
    let [payer, cell, system] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !payer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if space < CellHeader::SIZE {
        return Err(ProgramError::AccountDataTooSmall);
    }
    let state = CellHeader::try_decode(&cell.try_borrow_data()?)
        .map_err(|_| ProgramError::InvalidAccountData)?;
    if cell.key != &state.id.pda(program_id).0 {
        return Err(ProgramError::InvalidSeeds);
    }
    let required = Rent::get()?.minimum_balance(space);
    if cell.lamports() < required {
        invoke(
            &system_instruction::transfer(payer.key, cell.key, required - cell.lamports()),
            &[payer.clone(), cell.clone(), system.clone()],
        )?;
    }
    cell.resize(space)?;
    update(cell, |_, _| Ok(()))
}

fn undelegate(program_id: &Pubkey, accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    let [cell, buffer, payer, system, ..] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    let seeds = config::deserialize_exact::<Vec<Vec<u8>>, _>(
        data,
        Configuration::default().with_length_encoding::<u32>(),
    )
    .map_err(|_| ProgramError::InvalidInstructionData)?;
    undelegate_account(cell, program_id, buffer, payer, system, seeds)?;
    let state = CellHeader::try_decode(&cell.try_borrow_data()?)
        .map_err(|_| ProgramError::InvalidAccountData)?;
    if state.undelegation == UndelegationOutcome::Reject {
        return Err(ProgramError::Custom(111));
    }
    Ok(())
}
