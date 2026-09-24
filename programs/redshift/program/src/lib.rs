#![allow(unexpected_cfgs)]

use redline_interface::instruction::build;
#[cfg(feature = "schedulecommit")]
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

pub use redshift_interface::{
    id, EPHEMERAL_TAG, FLEXI_TAG, ID, LOG_MSG_TAG, SCHEDULE_COMMIT_TAG,
    UPGRADE_TAG,
};

#[cfg(feature = "schedulecommit")]
pub mod flexi;
#[cfg(feature = "schedulecommit")]
pub mod schedulecommit;

#[cfg(feature = "upgraded")]
const LOG_MSG_SUFFIX: &str = " upgraded";
#[cfg(not(feature = "upgraded"))]
const LOG_MSG_SUFFIX: &str = "";

#[cfg_attr(not(feature = "schedulecommit"), allow(unused_variables))]
pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    #[cfg(feature = "schedulecommit")]
    if instruction_data.len() >= EXTERNAL_UNDELEGATE_DISCRIMINATOR.len() {
        let (discriminator, rest) =
            instruction_data.split_at(EXTERNAL_UNDELEGATE_DISCRIMINATOR.len());
        if discriminator == EXTERNAL_UNDELEGATE_DISCRIMINATOR {
            return schedulecommit::process_undelegate_request(
                program_id, accounts, rest,
            );
        }
        if discriminator == flexi::TRANSFER_CALLBACK_DISCRIMINATOR {
            return flexi::process_transfer_callback(accounts, rest);
        }
    }

    match instruction_data.split_first() {
        Some((&LOG_MSG_TAG, message)) => {
            let text = core::str::from_utf8(message)
                .map_err(|_| ProgramError::InvalidInstructionData)?;
            msg!("LogMsg: {}{}", text, LOG_MSG_SUFFIX);
            Ok(())
        }
        Some((&EPHEMERAL_TAG, payload)) => {
            let [sponsor, account, vault, native] = accounts else {
                return Err(ProgramError::NotEnoughAccountKeys);
            };
            let (&fill, data) = payload
                .split_first()
                .ok_or(ProgramError::InvalidInstructionData)?;
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
        Some((&UPGRADE_TAG, payload)) => {
            let (&fail, id) = payload
                .split_first()
                .ok_or(ProgramError::InvalidInstructionData)?;
            let id = u64::from_le_bytes(
                id.try_into()
                    .map_err(|_| ProgramError::InvalidInstructionData)?,
            );
            let version = if cfg!(feature = "upgraded") { 2 } else { 1 };
            let value = id.wrapping_mul(10).wrapping_add(version);
            msg!("Upgrade: {}", version);
            let pair = accounts
                .get(..2)
                .ok_or(ProgramError::NotEnoughAccountKeys)?;
            for (account, value) in pair.iter().zip([value, !value]) {
                invoke(
                    &build::simple_byte_set(value, &[*account.key]),
                    accounts,
                )?;
            }
            if fail != 0 {
                return Err(ProgramError::Custom(UPGRADE_TAG as u32));
            }
            Ok(())
        }
        #[cfg(feature = "schedulecommit")]
        Some((&SCHEDULE_COMMIT_TAG, payload)) => {
            schedulecommit::process(program_id, accounts, payload)
        }
        #[cfg(feature = "schedulecommit")]
        Some((&FLEXI_TAG, payload)) => flexi::process(accounts, payload),
        _ => Ok(()),
    }
}
