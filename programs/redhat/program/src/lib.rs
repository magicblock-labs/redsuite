#![allow(unexpected_cfgs)]

use borsh::BorshDeserialize;
pub use redhat_interface::{id, SecurityInstruction, ID};
use redshift_interface::schedulecommit::{
    build::{direct_schedule_commit, schedule_commit_cpi},
    ScheduleCommitType,
};
use solana_program::{
    account_info::{next_account_info, AccountInfo},
    entrypoint::ProgramResult,
    msg,
    program::invoke,
    program_error::ProgramError,
    pubkey::Pubkey,
};

solana_program::entrypoint!(process_instruction);

pub fn process_instruction(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    let instruction = SecurityInstruction::try_from_slice(instruction_data)
        .map_err(|err| {
            msg!("cannot parse the security instruction: {}", err);
            ProgramError::InvalidInstructionData
        })?;

    use SecurityInstruction::*;
    match instruction {
        SiblingScheduleCommitCpis(players) => {
            process_sibling_cpis(accounts, &players)
        }
        NonCpi => Ok(()),
        DirectScheduleCommitCpi(players) => {
            process_direct_cpi(accounts, &players)
        }
    }
}

fn process_sibling_cpis(
    accounts: &[AccountInfo],
    players: &[Pubkey],
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let _magic_program = next_account_info(iter)?;
    let _schedulecommit_program = next_account_info(iter)?;
    let pdas: Vec<AccountInfo> = iter.cloned().collect();
    let pda_keys: Vec<Pubkey> = pdas.iter().map(|info| *info.key).collect();

    // 1. CPI into the program that owns the PDAs (legitimate path).
    let indirect = schedule_commit_cpi(
        *payer.key,
        players.to_vec(),
        false,
        false,
        ScheduleCommitType::CommitFinalize,
        true,
    );
    let mut indirect_infos = vec![payer.clone(), magic_context.clone()];
    indirect_infos.extend(pdas.iter().cloned());
    invoke(&indirect, &indirect_infos)?;

    // 2. CPI directly into the magic program (this program does not own the
    //    PDAs, so this must fail).
    let direct = direct_schedule_commit(*payer.key, None, &pda_keys);
    let mut direct_infos = vec![payer.clone(), magic_context.clone()];
    direct_infos.extend(pdas.iter().cloned());
    invoke(&direct, &direct_infos)
}

fn process_direct_cpi(
    accounts: &[AccountInfo],
    _players: &[Pubkey],
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let _magic_program = next_account_info(iter)?;
    let pdas: Vec<AccountInfo> = iter.cloned().collect();
    let pda_keys: Vec<Pubkey> = pdas.iter().map(|info| *info.key).collect();

    let direct = direct_schedule_commit(*payer.key, None, &pda_keys);
    let mut infos = vec![payer.clone(), magic_context.clone()];
    infos.extend(pdas.iter().cloned());
    invoke(&direct, &infos)
}
