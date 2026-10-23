#![allow(unexpected_cfgs)]

pub use redshift_interface::{id, ID};
use solana_program::{account_info::AccountInfo, entrypoint::ProgramResult, pubkey::Pubkey};

solana_program::entrypoint!(process_instruction);

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    fixture::execute::process(program_id, accounts, data)
}
