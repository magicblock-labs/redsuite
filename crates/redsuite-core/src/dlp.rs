//! Hand-built dlp instructions and PDAs for the ephemeral-balance escrow.

use instruction::{AccountMeta, Instruction};
use pubkey::Pubkey;

use crate::{system::system_id, topology::DLP_ID};

const UPGRADEABLE_LOADER_ID: &str =
    "BPFLoaderUpgradeab1e11111111111111111111111";
const BORSH_OPTION_NONE: u8 = 0;
const BORSH_OPTION_SOME: u8 = 1;

pub fn dlp_id() -> Pubkey {
    DLP_ID.parse().expect("dlp program id")
}

pub fn ephemeral_balance_pda(payer: &Pubkey, index: u8) -> Pubkey {
    Pubkey::find_program_address(
        &[b"balance", payer.as_ref(), &[index]],
        &dlp_id(),
    )
    .0
}

pub fn delegation_record_pda(delegated_account: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[b"delegation", delegated_account.as_ref()],
        &dlp_id(),
    )
    .0
}

pub fn delegation_metadata_pda(delegated_account: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[b"delegation-metadata", delegated_account.as_ref()],
        &dlp_id(),
    )
    .0
}

fn delegate_buffer_pda(
    delegated_account: &Pubkey,
    owner_program: &Pubkey,
) -> Pubkey {
    Pubkey::find_program_address(
        &[b"buffer", delegated_account.as_ref()],
        owner_program,
    )
    .0
}

pub fn protocol_fees_vault_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"fees-vault"], &dlp_id()).0
}

pub fn validator_fees_vault_pda(validator: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[b"v-fees-vault", validator.as_ref()],
        &dlp_id(),
    )
    .0
}

fn dlp_programdata_pda() -> Pubkey {
    let loader: Pubkey = UPGRADEABLE_LOADER_ID
        .parse()
        .expect("upgradeable loader id");
    Pubkey::find_program_address(&[dlp_id().as_ref()], &loader).0
}

pub fn delegate_account(
    payer: &Pubkey,
    delegatee: &Pubkey,
    validator: &Pubkey,
) -> Instruction {
    let system = system_id();
    let commit_frequency_ms = u32::MAX;
    let seeds_count = 0u32;
    let mut data = 0u64.to_le_bytes().to_vec();
    data.extend_from_slice(&commit_frequency_ms.to_le_bytes());
    data.extend_from_slice(&seeds_count.to_le_bytes());
    data.push(BORSH_OPTION_SOME);
    data.extend_from_slice(validator.as_ref());
    Instruction {
        program_id: dlp_id(),
        accounts: vec![
            AccountMeta::new(*payer, true),
            AccountMeta::new(*delegatee, true),
            AccountMeta::new_readonly(system, false),
            AccountMeta::new(delegate_buffer_pda(delegatee, &system), false),
            AccountMeta::new(delegation_record_pda(delegatee), false),
            AccountMeta::new(delegation_metadata_pda(delegatee), false),
            AccountMeta::new_readonly(system, false),
        ],
        data,
    }
}

pub fn init_validator_fees_vault(
    payer: &Pubkey,
    admin: &Pubkey,
    validator: &Pubkey,
) -> Instruction {
    Instruction {
        program_id: dlp_id(),
        accounts: vec![
            AccountMeta::new(*payer, true),
            AccountMeta::new(*admin, true),
            AccountMeta::new_readonly(dlp_programdata_pda(), false),
            AccountMeta::new(*validator, false),
            AccountMeta::new(validator_fees_vault_pda(validator), false),
            AccountMeta::new_readonly(system_id(), false),
        ],
        data: 6u64.to_le_bytes().to_vec(),
    }
}

pub fn validator_claim_fees(
    validator: &Pubkey,
    amount: Option<u64>,
) -> Instruction {
    let mut data = 7u64.to_le_bytes().to_vec();
    match amount {
        Some(lamports) => {
            data.push(BORSH_OPTION_SOME);
            data.extend_from_slice(&lamports.to_le_bytes());
        }
        None => data.push(BORSH_OPTION_NONE),
    }
    Instruction {
        program_id: dlp_id(),
        accounts: vec![
            AccountMeta::new(*validator, true),
            AccountMeta::new(protocol_fees_vault_pda(), false),
            AccountMeta::new(validator_fees_vault_pda(validator), false),
        ],
        data,
    }
}

pub fn top_up_ephemeral_balance(
    payer: &Pubkey,
    lamports: u64,
    index: u8,
) -> Instruction {
    let escrow = ephemeral_balance_pda(payer, index);
    let mut data = 9u64.to_le_bytes().to_vec();
    data.extend_from_slice(&lamports.to_le_bytes());
    data.push(index);
    Instruction {
        program_id: dlp_id(),
        accounts: vec![
            AccountMeta::new(*payer, true),
            AccountMeta::new_readonly(*payer, false),
            AccountMeta::new(escrow, false),
            AccountMeta::new_readonly(system_id(), false),
        ],
        data,
    }
}

pub fn delegate_ephemeral_balance(
    payer: &Pubkey,
    validator: &Pubkey,
    index: u8,
) -> Instruction {
    let escrow = ephemeral_balance_pda(payer, index);
    let system = system_id();
    let commit_frequency_ms = 0u32;
    let seeds_count = 0u32;
    let mut data = 10u64.to_le_bytes().to_vec();
    data.extend_from_slice(&commit_frequency_ms.to_le_bytes());
    data.extend_from_slice(&seeds_count.to_le_bytes());
    data.push(BORSH_OPTION_SOME);
    data.extend_from_slice(validator.as_ref());
    data.push(index);
    Instruction {
        program_id: dlp_id(),
        accounts: vec![
            AccountMeta::new(*payer, true),
            AccountMeta::new_readonly(*payer, true),
            AccountMeta::new(escrow, false),
            AccountMeta::new(delegate_buffer_pda(&escrow, &system), false),
            AccountMeta::new(delegation_record_pda(&escrow), false),
            AccountMeta::new(delegation_metadata_pda(&escrow), false),
            AccountMeta::new_readonly(system, false),
            AccountMeta::new_readonly(dlp_id(), false),
        ],
        data,
    }
}

pub fn close_ephemeral_balance(payer: &Pubkey, index: u8) -> Instruction {
    let mut data = 11u64.to_le_bytes().to_vec();
    data.push(index);
    Instruction {
        program_id: dlp_id(),
        accounts: vec![
            AccountMeta::new(*payer, true),
            AccountMeta::new(ephemeral_balance_pda(payer, index), false),
            AccountMeta::new_readonly(system_id(), false),
        ],
        data,
    }
}
