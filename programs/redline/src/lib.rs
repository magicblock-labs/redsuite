//! Load-target program; the redline program (`3JnJ72…aaPX`) is ported here.

#![allow(unexpected_cfgs)]

use solana_program::{
    account_info::AccountInfo,
    declare_id,
    entrypoint::{self, ProgramResult},
    pubkey::Pubkey,
};

entrypoint::entrypoint!(process_instruction);
declare_id!("3JnJ727jWEmPVU8qfXwtH63sCNDX7nMgsLbg8qy8aaPX");

fn process_instruction(
    _program_id: &Pubkey,
    _accounts: &[AccountInfo],
    _instruction_data: &[u8],
) -> ProgramResult {
    Ok(())
}
