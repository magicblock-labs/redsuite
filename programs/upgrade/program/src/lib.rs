#![allow(unexpected_cfgs)]

use redline_interface::PROGRAM as REDLINE;
use solana_program::{
    account_info::AccountInfo,
    entrypoint::ProgramResult,
    instruction::{AccountMeta, Instruction},
    msg,
    program::invoke,
    program_error::ProgramError,
    pubkey::Pubkey,
};
use wincode::{SchemaRead, SchemaWrite};

#[cfg(not(feature = "no-entrypoint"))]
solana_program::entrypoint!(process_instruction);

#[derive(Clone, Copy)]
pub struct Program {
    id: Pubkey,
}

impl Program {
    pub const FAILURE_CODE: u32 = 7;

    pub const fn at(id: Pubkey) -> Self {
        Self { id }
    }

    pub fn write(self, id: u64, fail: bool, accounts: &[Pubkey; 2]) -> Instruction {
        Instruction {
            program_id: self.id,
            accounts: vec![
                AccountMeta::new(accounts[0], false),
                AccountMeta::new(accounts[1], false),
                AccountMeta::new_readonly(redline_interface::ID, false),
            ],
            data: wincode::serialize(&VersionedWrite { id, fail })
                .expect("instruction serialization cannot fail"),
        }
    }
}

#[derive(SchemaRead, SchemaWrite)]
struct VersionedWrite {
    id: u64,
    fail: bool,
}

pub fn process_instruction(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let VersionedWrite { id, fail } = wincode::deserialize_exact(data).map_err(|err| {
        msg!("invalid upgrade probe: {}", err);
        ProgramError::InvalidInstructionData
    })?;
    let version = if cfg!(feature = "upgraded") { 2 } else { 1 };
    let value = id.wrapping_mul(10).wrapping_add(version);
    msg!("Upgrade: {}", version);
    let pair = accounts.get(..2).ok_or(ProgramError::NotEnoughAccountKeys)?;
    for (account, value) in pair.iter().zip([value, !value]) {
        invoke(&REDLINE.write(value, &[*account.key]), accounts)?;
    }
    if fail {
        return Err(ProgramError::Custom(Program::FAILURE_CODE));
    }
    Ok(())
}
