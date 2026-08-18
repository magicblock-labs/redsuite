#![allow(unexpected_cfgs)]

#[cfg(feature = "schedulecommit")]
use sdk::consts::EXTERNAL_UNDELEGATE_DISCRIMINATOR;
use solana_program::{
    account_info::AccountInfo, entrypoint::ProgramResult, msg,
    program_error::ProgramError, pubkey::Pubkey,
};

solana_program::entrypoint!(process_instruction);

pub use redshift_interface::{
    id, FLEXI_TAG, ID, LOG_MSG_TAG, SCHEDULE_COMMIT_TAG,
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
        #[cfg(feature = "schedulecommit")]
        Some((&SCHEDULE_COMMIT_TAG, payload)) => {
            schedulecommit::process(program_id, accounts, payload)
        }
        #[cfg(feature = "schedulecommit")]
        Some((&FLEXI_TAG, payload)) => flexi::process(accounts, payload),
        _ => Ok(()),
    }
}
