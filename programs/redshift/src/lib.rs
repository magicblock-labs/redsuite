//! Correctness-fixture program (counter-style state the redshift scenarios assert on).

#![allow(unexpected_cfgs)]

use solana_program::{
    account_info::AccountInfo,
    declare_id,
    entrypoint::{self, ProgramResult},
    pubkey::Pubkey,
};

entrypoint::entrypoint!(process_instruction);
declare_id!("AijneHkXJVVWyimuwfSJdrJktARZu2WiMaZBqHsq7CS5");

fn process_instruction(
    _program_id: &Pubkey,
    _accounts: &[AccountInfo],
    _instruction_data: &[u8],
) -> ProgramResult {
    Ok(())
}
