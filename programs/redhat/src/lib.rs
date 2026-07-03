//! Adversarial-fixture program (illegal writes, privilege escalation attempts).

#![allow(unexpected_cfgs)]

use solana_program::{
    account_info::AccountInfo,
    declare_id,
    entrypoint::{self, ProgramResult},
    pubkey::Pubkey,
};

entrypoint::entrypoint!(process_instruction);
declare_id!("BTczL2chGpVHw25pbmMtkFAD1t7rxoa8pVbaUjsybjiq");

fn process_instruction(
    _program_id: &Pubkey,
    _accounts: &[AccountInfo],
    _instruction_data: &[u8],
) -> ProgramResult {
    Ok(())
}
