#![allow(unexpected_cfgs)]

use borsh::{BorshDeserialize, BorshSerialize};
use redshift_program::schedulecommit::{
    build::{direct_schedule_commit, schedule_commit_cpi},
    ScheduleCommitType,
};
use solana_program::{
    account_info::{next_account_info, AccountInfo},
    declare_id,
    entrypoint::ProgramResult,
    msg,
    program::invoke,
    program_error::ProgramError,
    pubkey::Pubkey,
};

#[cfg(not(feature = "no-entrypoint"))]
solana_program::entrypoint!(process_instruction);
declare_id!("BTczL2chGpVHw25pbmMtkFAD1t7rxoa8pVbaUjsybjiq");

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum SecurityInstruction {
    // Commits the PDAs twice: once via the owning program (legitimate) and
    // once directly via the magic program (must fail — this program does not
    // own the PDAs).
    SiblingScheduleCommitCpis(Vec<Pubkey>),
    // A no-op instruction. Used to try to confuse the CPI-parent detection.
    NonCpi,
    // Commits the PDAs directly via the magic program from a program that does
    // not own them (must fail).
    DirectScheduleCommitCpi(Vec<Pubkey>),
}

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

pub mod build {
    use redshift_program::schedulecommit::build::{
        magic_context_id, magic_program_id,
    };
    use solana_program::instruction::{AccountMeta, Instruction};

    use super::*;

    fn with_pdas(
        payer: Pubkey,
        pass_schedulecommit_program: bool,
        pdas: &[Pubkey],
    ) -> Vec<AccountMeta> {
        let mut metas = vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(magic_context_id(), false),
            AccountMeta::new_readonly(magic_program_id(), false),
        ];
        if pass_schedulecommit_program {
            metas
                .push(AccountMeta::new_readonly(redshift_program::id(), false));
        }
        metas.extend(pdas.iter().map(|key| AccountMeta::new(*key, false)));
        metas
    }

    fn borsh_ix(
        instruction: &SecurityInstruction,
        metas: Vec<AccountMeta>,
    ) -> Instruction {
        Instruction {
            program_id: crate::id(),
            accounts: metas,
            data: borsh::to_vec(instruction)
                .expect("instruction serialization cannot fail"),
        }
    }

    pub fn sibling_schedule_commit_cpis(
        payer: Pubkey,
        players: &[Pubkey],
        pdas: &[Pubkey],
    ) -> Instruction {
        borsh_ix(
            &SecurityInstruction::SiblingScheduleCommitCpis(players.to_vec()),
            with_pdas(payer, true, pdas),
        )
    }

    pub fn nested_schedule_commit_cpi(
        payer: Pubkey,
        players: &[Pubkey],
        pdas: &[Pubkey],
    ) -> Instruction {
        borsh_ix(
            &SecurityInstruction::DirectScheduleCommitCpi(players.to_vec()),
            with_pdas(payer, false, pdas),
        )
    }

    pub fn non_cpi(payer: Pubkey) -> Instruction {
        borsh_ix(
            &SecurityInstruction::NonCpi,
            vec![AccountMeta::new(payer, true)],
        )
    }
}
