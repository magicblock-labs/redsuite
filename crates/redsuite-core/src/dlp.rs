// Convenience layer over dlp-api: shorter argument lists and the ids/pdas in
// one place. Nothing here encodes DLP instructions itself.
use anyhow::Context;
pub use dlp_api::{
    args::{CommitStateArgs, DelegateArgs},
    pda::{
        delegate_buffer_pda_from_delegated_account_and_owner_program as delegate_buffer_pda,
        delegation_metadata_pda_from_delegated_account as delegation_metadata_pda,
        delegation_record_pda_from_delegated_account as delegation_record_pda,
        magic_fee_vault_pda_from_validator as magic_fee_vault_pda,
        validator_fees_vault_pda_from_validator as validator_fees_vault_pda,
    },
    ID as DELEGATION_PROGRAM_ID,
};
use dlp_api::{
    instruction_builder::{self, Encryptable},
    state::DelegationMetadata,
};
use instruction::Instruction;
use pubkey::Pubkey;

use crate::Result;

pub fn dlp_id() -> Pubkey {
    DELEGATION_PROGRAM_ID
}

fn delegate_args(commit_frequency_ms: u32, validator: &Pubkey) -> DelegateArgs {
    DelegateArgs {
        commit_frequency_ms,
        seeds: vec![],
        validator: Some(*validator),
    }
}

pub fn delegate_account(payer: &Pubkey, delegatee: &Pubkey, validator: &Pubkey) -> Instruction {
    instruction_builder::delegate(*payer, *delegatee, None, delegate_args(u32::MAX, validator))
}

pub fn delegate_with_actions(
    payer: &Pubkey,
    delegated_account: &Pubkey,
    owner: Option<Pubkey>,
    delegate: DelegateArgs,
    actions: &[Instruction],
) -> Instruction {
    instruction_builder::delegate_with_actions(
        *payer,
        *delegated_account,
        owner,
        delegate,
        actions.iter().cloned().map(Encryptable::cleartext).collect(),
    )
}

pub fn commit_state(
    validator: &Pubkey,
    delegated_account: &Pubkey,
    delegated_account_owner: &Pubkey,
    args: CommitStateArgs,
) -> Instruction {
    instruction_builder::commit_state(
        *validator,
        *delegated_account,
        *delegated_account_owner,
        args,
    )
}

pub fn finalize(validator: &Pubkey, delegated_account: &Pubkey) -> Instruction {
    instruction_builder::finalize(*validator, *delegated_account)
}

pub fn undelegate(
    validator: &Pubkey,
    delegated_account: &Pubkey,
    owner_program: &Pubkey,
    delegation_rent_payer: &Pubkey,
) -> Instruction {
    instruction_builder::undelegate(
        *validator,
        *delegated_account,
        *owner_program,
        *delegation_rent_payer,
    )
}

pub fn top_up_ephemeral_balance(payer: &Pubkey, lamports: u64, index: u8) -> Instruction {
    instruction_builder::top_up_ephemeral_balance(*payer, *payer, Some(lamports), Some(index))
}

pub fn last_commit_id(delegation_metadata: &[u8]) -> Result<u64> {
    let metadata = DelegationMetadata::try_from_bytes_with_discriminator(delegation_metadata)
        .context("decoding delegation metadata")?;
    Ok(metadata.last_commit_id)
}
