// Convenience layer over dlp-api: shorter argument lists and the ids/pdas in
// one place. Nothing here encodes DLP instructions itself.
pub use dlp_api::args::{CommitStateArgs, DelegateArgs};
use dlp_api::{
    args::DelegateEphemeralBalanceArgs,
    instruction_builder::{self, Encryptable},
    pda,
};
pub use dlp_api::{
    pda::{
        delegate_buffer_pda_from_delegated_account_and_owner_program as delegate_buffer_pda,
        delegation_metadata_pda_from_delegated_account as delegation_metadata_pda,
        delegation_record_pda_from_delegated_account as delegation_record_pda,
    },
    ID as DELEGATION_PROGRAM_ID,
};
use instruction::Instruction;
use pubkey::Pubkey;

pub fn dlp_id() -> Pubkey {
    DELEGATION_PROGRAM_ID
}

pub fn ephemeral_balance_pda(payer: &Pubkey, index: u8) -> Pubkey {
    pda::ephemeral_balance_pda_from_payer(payer, index)
}

pub fn commit_state_pda(delegated_account: &Pubkey) -> Pubkey {
    pda::commit_state_pda_from_delegated_account(delegated_account)
}

pub fn commit_record_pda(delegated_account: &Pubkey) -> Pubkey {
    pda::commit_record_pda_from_delegated_account(delegated_account)
}

pub fn undelegate_buffer_pda(delegated_account: &Pubkey) -> Pubkey {
    pda::undelegate_buffer_pda_from_delegated_account(delegated_account)
}

pub fn program_config_pda(program_id: &Pubkey) -> Pubkey {
    pda::program_config_from_program_id(program_id)
}

pub fn protocol_fees_vault_pda() -> Pubkey {
    pda::fees_vault_pda()
}

pub fn validator_fees_vault_pda(validator: &Pubkey) -> Pubkey {
    pda::validator_fees_vault_pda_from_validator(validator)
}

pub fn magic_fee_vault_pda(validator: &Pubkey) -> Pubkey {
    pda::magic_fee_vault_pda_from_validator(validator)
}

fn delegate_args(commit_frequency_ms: u32, validator: &Pubkey) -> DelegateArgs {
    DelegateArgs {
        commit_frequency_ms,
        seeds: vec![],
        validator: Some(*validator),
    }
}

pub fn delegate_account(
    payer: &Pubkey,
    delegatee: &Pubkey,
    validator: &Pubkey,
) -> Instruction {
    instruction_builder::delegate(
        *payer,
        *delegatee,
        None,
        delegate_args(u32::MAX, validator),
    )
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
        actions
            .iter()
            .cloned()
            .map(Encryptable::cleartext)
            .collect(),
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

pub fn init_validator_fees_vault(
    payer: &Pubkey,
    admin: &Pubkey,
    validator: &Pubkey,
) -> Instruction {
    instruction_builder::init_validator_fees_vault(*payer, *admin, *validator)
}

pub fn validator_claim_fees(
    validator: &Pubkey,
    amount: Option<u64>,
) -> Instruction {
    instruction_builder::validator_claim_fees(*validator, amount)
}

pub fn top_up_ephemeral_balance(
    payer: &Pubkey,
    lamports: u64,
    index: u8,
) -> Instruction {
    top_up_ephemeral_balance_for(payer, payer, lamports, index)
}

pub fn top_up_ephemeral_balance_for(
    funder: &Pubkey,
    beneficiary: &Pubkey,
    lamports: u64,
    index: u8,
) -> Instruction {
    instruction_builder::top_up_ephemeral_balance(
        *funder,
        *beneficiary,
        Some(lamports),
        Some(index),
    )
}

pub fn delegate_ephemeral_balance(
    payer: &Pubkey,
    validator: &Pubkey,
    index: u8,
) -> Instruction {
    delegate_ephemeral_balance_for(payer, payer, validator, index)
}

pub fn delegate_ephemeral_balance_for(
    funder: &Pubkey,
    beneficiary: &Pubkey,
    validator: &Pubkey,
    index: u8,
) -> Instruction {
    instruction_builder::delegate_ephemeral_balance(
        *funder,
        *beneficiary,
        DelegateEphemeralBalanceArgs {
            delegate_args: delegate_args(0, validator),
            index,
        },
    )
}

pub fn close_ephemeral_balance(payer: &Pubkey, index: u8) -> Instruction {
    instruction_builder::close_ephemeral_balance(*payer, index)
}
