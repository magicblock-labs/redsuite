#![allow(unexpected_cfgs)]

use redline_interface::instruction::build;
use solana_program::{
    account_info::AccountInfo, entrypoint::ProgramResult, msg, program::invoke,
    program_error::ProgramError, pubkey::Pubkey,
};

#[cfg(not(feature = "no-entrypoint"))]
solana_program::entrypoint!(process_instruction);

pub const FAILURE_CODE: u32 = 7;

pub fn instruction_data(id: u64, fail: bool) -> [u8; 9] {
    let mut data = [0; 9];
    data[0] = u8::from(fail);
    data[1..].copy_from_slice(&id.to_le_bytes());
    data
}

pub fn process_instruction(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let (&fail, id) = data.split_first().ok_or(ProgramError::InvalidInstructionData)?;
    let id = u64::from_le_bytes(id.try_into().map_err(|_| ProgramError::InvalidInstructionData)?);
    let version = if cfg!(feature = "upgraded") { 2 } else { 1 };
    let value = id.wrapping_mul(10).wrapping_add(version);
    msg!("Upgrade: {}", version);
    let pair = accounts.get(..2).ok_or(ProgramError::NotEnoughAccountKeys)?;
    for (account, value) in pair.iter().zip([value, !value]) {
        invoke(&build::simple_byte_set(value, &[*account.key]), accounts)?;
    }
    if fail != 0 {
        return Err(ProgramError::Custom(FAILURE_CODE));
    }
    Ok(())
}
