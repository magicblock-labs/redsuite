//! Correctness-fixture program (counter-style state the redshift scenarios assert on).

#![allow(unexpected_cfgs)]

use solana_program::{
    account_info::AccountInfo, declare_id, entrypoint::ProgramResult, msg,
    program_error::ProgramError, pubkey::Pubkey,
};

#[cfg(not(feature = "no-entrypoint"))]
solana_program::entrypoint!(process_instruction);
declare_id!("AijneHkXJVVWyimuwfSJdrJktARZu2WiMaZBqHsq7CS5");

const LOG_MSG_TAG: u8 = 4;

#[cfg(feature = "upgraded")]
const LOG_MSG_SUFFIX: &str = " upgraded";
#[cfg(not(feature = "upgraded"))]
const LOG_MSG_SUFFIX: &str = "";

pub fn process_instruction(
    _program_id: &Pubkey,
    _accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    match instruction_data.split_first() {
        Some((&LOG_MSG_TAG, message)) => {
            let text = core::str::from_utf8(message)
                .map_err(|_| ProgramError::InvalidInstructionData)?;
            msg!("LogMsg: {}{}", text, LOG_MSG_SUFFIX);
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn log_msg_data(message: &str) -> Vec<u8> {
    let mut data = Vec::with_capacity(1 + message.len());
    data.push(LOG_MSG_TAG);
    data.extend_from_slice(message.as_bytes());
    data
}
