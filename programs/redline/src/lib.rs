#![allow(unexpected_cfgs)]

use instruction::Instruction;
use processors::*;
use sdk::consts::EXTERNAL_UNDELEGATE_DISCRIMINATOR;
use solana_program::{
    account_info::AccountInfo,
    declare_id,
    entrypoint::{self, ProgramResult},
    msg,
    program_error::ProgramError,
    pubkey::Pubkey,
};

entrypoint::entrypoint!(process_instruction);
declare_id!("3JnJ727jWEmPVU8qfXwtH63sCNDX7nMgsLbg8qy8aaPX");

pub use sdk::{
    consts::DELEGATION_PROGRAM_ID,
    delegate_args::{DelegateAccountMetas, DelegateAccounts},
};

pub mod layout {
    pub const OWNER_PUBKEY_SIZE: usize = 32;
    pub const DATA_OFFSET: usize = OWNER_PUBKEY_SIZE;
    pub const ID_OFFSET: usize = DATA_OFFSET;
    pub const ID_SIZE: usize = 8;
    pub const HASH_OFFSET: usize = ID_OFFSET + ID_SIZE;
    pub const HASH_SIZE: usize = 32;
}

fn process_instruction(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    // The dlp undelegation callback arrives with its own discriminator and
    // must be intercepted before bincode deserialization.
    if instruction_data.len() >= EXTERNAL_UNDELEGATE_DISCRIMINATOR.len() {
        let (disc, data) =
            instruction_data.split_at(EXTERNAL_UNDELEGATE_DISCRIMINATOR.len());
        if disc == EXTERNAL_UNDELEGATE_DISCRIMINATOR {
            return undelegate(accounts, data);
        }
    }

    let instruction: Instruction = bincode::deserialize(instruction_data)
        .map_err(|err| {
            msg!("failed to bincode deserialize instruction data: {}", err);
            ProgramError::InvalidInstructionData
        })?;
    let mut iter = accounts.iter();

    match instruction {
        Instruction::InitAccount {
            space,
            seed,
            bump,
            authority,
        } => init_account(&mut iter, space, seed, bump, authority)?,
        Instruction::Delegate { seed, authority } => {
            delegate_account(accounts, seed, authority)?
        }
        Instruction::SimpleByteSet { id } => simple_byte_set(&mut iter, id)?,
        Instruction::MultiAccountRead { id } => {
            multi_account_read(&mut iter, accounts, id)?
        }
        Instruction::ExpensiveHashCompute { id, init, iters } => {
            expensive_hash_compute(&mut iter, id, init.to_bytes(), iters)?
        }
        Instruction::AccountDataCopy { id } => {
            account_data_copy(&mut iter, id)?
        }
        Instruction::ReadAccountsData { id } => {
            read_accounts_data(&mut iter, id)?
        }
        Instruction::CommitAccounts { id } => commit_accounts(&mut iter, id)?,
        Instruction::CommitAndUndelegateAccounts { id } => {
            commit_undelegate_accounts(&mut iter, id)?
        }
        Instruction::CloseAccount => close_account(&mut iter)?,
    }

    Ok(())
}

pub mod instruction;
mod processors;
pub mod utils;
