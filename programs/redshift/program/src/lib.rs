#![allow(unexpected_cfgs)]

use sdk::consts::EXTERNAL_UNDELEGATE_DISCRIMINATOR;
use solana_program::{
    account_info::AccountInfo,
    entrypoint::ProgramResult,
    instruction::{AccountMeta, Instruction},
    msg,
    program::invoke,
    program_error::ProgramError,
    pubkey::Pubkey,
};

solana_program::entrypoint!(process_instruction);

pub use redshift_interface::{id, EPHEMERAL_TAG, ID, LOG_MSG_TAG, SCHEDULE_COMMIT_TAG};

pub mod schedulecommit;

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    if instruction_data.len() >= EXTERNAL_UNDELEGATE_DISCRIMINATOR.len() {
        let (discriminator, rest) =
            instruction_data.split_at(EXTERNAL_UNDELEGATE_DISCRIMINATOR.len());
        if discriminator == EXTERNAL_UNDELEGATE_DISCRIMINATOR {
            return schedulecommit::process_undelegate_request(program_id, accounts, rest);
        }
    }

    match instruction_data.split_first() {
        Some((&LOG_MSG_TAG, message)) => {
            let text =
                core::str::from_utf8(message).map_err(|_| ProgramError::InvalidInstructionData)?;
            msg!("LogMsg: {}", text);
            Ok(())
        }
        Some((&EPHEMERAL_TAG, payload)) => {
            let [sponsor, account, vault, native] = accounts else {
                return Err(ProgramError::NotEnoughAccountKeys);
            };
            let (&fill, data) =
                payload.split_first().ok_or(ProgramError::InvalidInstructionData)?;
            let observe = || {
                msg!(
                    "Ephemeral: {} {} {} {} {} {}",
                    account.owner,
                    account.data_len(),
                    account.lamports(),
                    sponsor.lamports(),
                    vault.lamports(),
                    solana_program::hash::hash(&account.data.borrow()),
                )
            };
            observe();
            invoke(
                &Instruction {
                    program_id: *native.key,
                    accounts: accounts[..3]
                        .iter()
                        .map(|a| AccountMeta::new(*a.key, a.is_signer))
                        .collect(),
                    data: data.to_vec(),
                },
                accounts,
            )?;
            if fill != 0 {
                account.try_borrow_mut_data()?.fill(fill);
            }
            observe();
            Ok(())
        }
        Some((&SCHEDULE_COMMIT_TAG, payload)) => {
            schedulecommit::process(program_id, accounts, payload)
        }
        _ => Ok(()),
    }
}
