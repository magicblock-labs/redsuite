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

    let players = match instruction {
        SecurityInstruction::NonCpi => return Ok(()),
        SecurityInstruction::SiblingScheduleCommitCpis(players) => {
            Some(players)
        }
        SecurityInstruction::DirectScheduleCommitCpi => None,
    };
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let _magic_program = next_account_info(iter)?;
    if players.is_some() {
        let _schedulecommit_program = next_account_info(iter)?;
    }
    let pdas = iter.as_slice();
    let mut infos = vec![payer.clone(), magic_context.clone()];
    infos.extend_from_slice(pdas);

    if let Some(players) = players {
        // First invoke the owning program; the direct CPI below must still fail.
        let indirect = schedule_commit_cpi(
            *payer.key,
            players,
            false,
            false,
            ScheduleCommitType::CommitFinalize,
            true,
        );
        invoke(&indirect, &infos)?;
    }

    let pda_keys: Vec<_> = pdas.iter().map(|info| *info.key).collect();
    let direct = direct_schedule_commit(*payer.key, None, &pda_keys);
    invoke(&direct, &infos)
}
