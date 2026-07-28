#![allow(deprecated)]

use borsh::{to_vec, BorshDeserialize, BorshSerialize};
use magic_api::{pda::CALLBACK_SIGNER, response::MagicResponse};
use sdk::{
    ephem::{
        ActionCallback, CallHandler, CommitAndUndelegate, CommitType,
        FoldableIntentBuilder, MagicAction, MagicInstructionBuilder,
        MagicIntentBundleBuilder, UndelegateType,
    },
    ActionArgs, ShortAccountMeta,
};
use solana_program::{
    account_info::{next_account_info, next_account_infos, AccountInfo},
    entrypoint::ProgramResult,
    instruction::{AccountMeta, Instruction},
    msg,
    program::{invoke, invoke_signed},
    program_error::ProgramError,
    pubkey::Pubkey,
    rent::Rent,
    sysvar::Sysvar,
};
use solana_sdk_ids::system_program;

mod system_instruction {
    use super::*;

    pub fn create_account(
        from: &Pubkey,
        to: &Pubkey,
        lamports: u64,
        space: u64,
        owner: &Pubkey,
    ) -> Instruction {
        let mut data = 0u32.to_le_bytes().to_vec();
        data.extend_from_slice(&lamports.to_le_bytes());
        data.extend_from_slice(&space.to_le_bytes());
        data.extend_from_slice(owner.as_ref());
        Instruction {
            program_id: system_program::ID,
            accounts: vec![
                AccountMeta::new(*from, true),
                AccountMeta::new(*to, true),
            ],
            data,
        }
    }

    pub fn transfer(from: &Pubkey, to: &Pubkey, lamports: u64) -> Instruction {
        let mut data = 2u32.to_le_bytes().to_vec();
        data.extend_from_slice(&lamports.to_le_bytes());
        Instruction {
            program_id: system_program::ID,
            accounts: vec![
                AccountMeta::new(*from, true),
                AccountMeta::new(*to, false),
            ],
            data,
        }
    }
}

pub const FLEXI_SEED: &[u8] = b"flexi_counter";
pub const PRIZE: u64 = 1_000_000;
pub const ACTOR_ESCROW_INDEX: u8 = 1;
pub const FAIL_UNDELEGATION_LABEL: &str = "undelegate_fail";
pub const FAIL_UNDELEGATION_CODE: u32 = 122;
pub const TRANSFER_FAIL_CODE: u32 = 0xFA11;
pub const TRANSFER_CALLBACK_DISCRIMINATOR: &[u8] =
    &[0xFE, 0xCA, 0xCB, 0x01, 0x00, 0x00, 0x00, 0x00];

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlexiCounter {
    pub count: u64,
    pub updates: u64,
    pub label: String,
}

impl FlexiCounter {
    pub fn new(label: String) -> Self {
        Self {
            count: 0,
            updates: 0,
            label,
        }
    }

    pub fn pda_and_bump(payer: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[crate::ID.as_ref(), FLEXI_SEED, payer.as_ref()],
            &crate::ID,
        )
    }

    pub fn try_decode(data: &[u8]) -> std::io::Result<Self> {
        Self::try_from_slice(data)
    }
}

fn tagged(instruction: &FlexiInstruction) -> Vec<u8> {
    let mut data = vec![crate::FLEXI_TAG];
    data.extend(to_vec(instruction).expect("action serialization"));
    data
}

pub fn undelegate_label_poison(data: &[u8]) -> bool {
    matches!(
        FlexiCounter::try_from_slice(data),
        Ok(counter) if counter.label == FAIL_UNDELEGATION_LABEL
    )
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum FlexiInstruction {
    Init {
        label: String,
        bump: u8,
    },
    Delegate {
        commit_frequency_ms: u32,
        validator: Option<Pubkey>,
    },
    Add {
        count: u8,
    },
    CreateIntent {
        num_committees: u8,
        counter_diffs: Vec<i64>,
        is_undelegate: bool,
        compute_units: u32,
    },
    CommitActionHandler {
        amount: u64,
    },
    UndelegateActionHandler {
        amount: u64,
        counter_diff: i64,
    },
    CreateIntentBundle {
        num_commit_only: u8,
        num_undelegate: u8,
        counter_diffs: Vec<i64>,
        compute_units: u32,
    },
    CreateIntentBundleCommitAndFinalize {
        num_commit: u8,
        num_commit_finalize: u8,
    },
    CreateTransferIntent {
        amount: u64,
        fail: bool,
        compute_units: u32,
    },
    TransferActionHandler {
        amount: u64,
        fail: bool,
    },
}

pub fn process(accounts: &[AccountInfo], payload: &[u8]) -> ProgramResult {
    let instruction =
        FlexiInstruction::try_from_slice(payload).map_err(|err| {
            msg!("cannot parse the flexi instruction: {}", err);
            ProgramError::InvalidInstructionData
        })?;

    use FlexiInstruction::*;
    match instruction {
        Init { label, bump } => process_init(accounts, label, bump),
        Delegate {
            commit_frequency_ms,
            validator,
        } => process_delegate(accounts, commit_frequency_ms, validator),
        Add { count } => process_add(accounts, count),
        CreateIntent {
            num_committees,
            counter_diffs,
            is_undelegate,
            compute_units,
        } => process_create_intent(
            accounts,
            num_committees,
            counter_diffs,
            is_undelegate,
            compute_units,
        ),
        CommitActionHandler { amount } => {
            process_commit_action_handler(accounts, amount)
        }
        UndelegateActionHandler {
            amount,
            counter_diff,
        } => process_undelegate_action_handler(accounts, amount, counter_diff),
        CreateIntentBundle {
            num_commit_only,
            num_undelegate,
            counter_diffs,
            compute_units,
        } => process_create_intent_bundle(
            accounts,
            num_commit_only,
            num_undelegate,
            counter_diffs,
            compute_units,
        ),
        CreateIntentBundleCommitAndFinalize {
            num_commit,
            num_commit_finalize,
        } => process_create_intent_bundle_commit_and_finalize(
            accounts,
            num_commit,
            num_commit_finalize,
        ),
        CreateTransferIntent {
            amount,
            fail,
            compute_units,
        } => process_create_transfer_intent(
            accounts,
            amount,
            fail,
            compute_units,
        ),
        TransferActionHandler { amount, fail } => {
            process_transfer_action_handler(accounts, amount, fail)
        }
    }
}

fn add(counter_account: &AccountInfo, count: u8) -> ProgramResult {
    let mut counter =
        FlexiCounter::try_from_slice(&counter_account.data.borrow())?;
    counter.count += count as u64;
    counter.updates += 1;
    let size = counter_account.data_len();
    let counter_data = to_vec(&counter)?;
    counter_account.data.borrow_mut()[..size].copy_from_slice(&counter_data);
    Ok(())
}

fn process_init(
    accounts: &[AccountInfo],
    label: String,
    bump: u8,
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let payer = next_account_info(iter)?;
    let counter = next_account_info(iter)?;
    let system = next_account_info(iter)?;

    let (expected_pda, _) = FlexiCounter::pda_and_bump(payer.key);
    if counter.key != &expected_pda {
        return Err(ProgramError::InvalidSeeds);
    }

    let bump_slice = [bump];
    let seeds: [&[u8]; 4] = [
        crate::ID.as_ref(),
        FLEXI_SEED,
        payer.key.as_ref(),
        &bump_slice,
    ];
    let state = FlexiCounter::new(label);
    let data = to_vec(&state)?;
    let size = data.len();
    let create = system_instruction::create_account(
        payer.key,
        counter.key,
        Rent::get()?.minimum_balance(size),
        size as u64,
        &crate::ID,
    );
    invoke_signed(
        &create,
        &[payer.clone(), counter.clone(), system.clone()],
        &[&seeds],
    )?;
    counter.data.borrow_mut()[..size].copy_from_slice(&data);
    Ok(())
}

fn process_delegate(
    accounts: &[AccountInfo],
    commit_frequency_ms: u32,
    validator: Option<Pubkey>,
) -> ProgramResult {
    use sdk::cpi::{delegate_account, DelegateAccounts, DelegateConfig};
    let [payer, counter, owner_program, buffer, delegation_record, delegation_metadata, delegation_program, system] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    let seeds: [&[u8]; 3] =
        [crate::ID.as_ref(), FLEXI_SEED, payer.key.as_ref()];
    delegate_account(
        DelegateAccounts {
            payer,
            pda: counter,
            owner_program,
            buffer,
            delegation_record,
            delegation_metadata,
            delegation_program,
            system_program: system,
        },
        &seeds,
        DelegateConfig {
            commit_frequency_ms,
            validator,
        },
    )
}

fn process_add(accounts: &[AccountInfo], count: u8) -> ProgramResult {
    let iter = &mut accounts.iter();
    let _payer = next_account_info(iter)?;
    let counter = next_account_info(iter)?;
    add(counter, count)
}

fn process_create_intent(
    accounts: &[AccountInfo],
    num_committees: u8,
    counter_diffs: Vec<i64>,
    is_undelegate: bool,
    compute_units: u32,
) -> ProgramResult {
    let num_committees = num_committees as usize;
    if accounts.len() != 2 * num_committees + 5 {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let iter = &mut accounts.iter();
    let destination_program = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let transfer_destination = next_account_info(iter)?;
    let system = next_account_info(iter)?;
    let escrow_authorities = next_account_infos(iter, num_committees)?;
    let committees = next_account_infos(iter, num_committees)?;

    let commit_action = FlexiInstruction::CommitActionHandler { amount: PRIZE };
    let commit_handlers = committees
        .iter()
        .zip(escrow_authorities.iter().cloned())
        .map(|(committee, escrow_authority)| CallHandler {
            args: ActionArgs {
                data: tagged(&commit_action),
                escrow_index: ACTOR_ESCROW_INDEX,
            },
            compute_units,
            escrow_authority,
            destination_program: *destination_program.key,
            accounts: vec![
                committee.into(),
                ShortAccountMeta {
                    pubkey: *transfer_destination.key,
                    is_writable: true,
                },
                system.into(),
            ],
        })
        .collect::<Vec<_>>();
    let commit_type = CommitType::WithHandler {
        commited_accounts: committees.to_vec(),
        callbacks: vec![],
        call_handlers: commit_handlers,
    };

    let magic_action = if is_undelegate {
        let undelegate_handlers = committees
            .iter()
            .zip(escrow_authorities.iter().cloned())
            .zip(counter_diffs.iter().copied())
            .map(|((committee, escrow_authority), counter_diff)| {
                let action = FlexiInstruction::UndelegateActionHandler {
                    counter_diff,
                    amount: PRIZE,
                };
                CallHandler {
                    args: ActionArgs {
                        data: tagged(&action),
                        escrow_index: ACTOR_ESCROW_INDEX,
                    },
                    compute_units,
                    escrow_authority,
                    destination_program: *destination_program.key,
                    accounts: vec![
                        committee.into(),
                        ShortAccountMeta {
                            pubkey: *transfer_destination.key,
                            is_writable: true,
                        },
                        system.into(),
                    ],
                }
            })
            .collect::<Vec<_>>();
        MagicAction::CommitAndUndelegate(CommitAndUndelegate {
            commit_type,
            undelegate_type: UndelegateType::WithHandler {
                call_handlers: undelegate_handlers,
                callbacks: vec![],
            },
        })
    } else {
        MagicAction::Commit(commit_type)
    };

    MagicInstructionBuilder {
        payer: escrow_authorities[0].clone(),
        magic_context: magic_context.clone(),
        magic_program: magic_program.clone(),
        magic_fee_vault: None,
        magic_action,
    }
    .build_and_invoke()
}

fn action_handler<'info>(
    committee: &AccountInfo<'info>,
    transfer_destination: &AccountInfo<'info>,
    system: &AccountInfo<'info>,
    destination_program: &Pubkey,
    escrow_authority: AccountInfo<'info>,
    instruction: &FlexiInstruction,
    compute_units: u32,
) -> CallHandler<'info> {
    CallHandler {
        args: ActionArgs {
            data: tagged(instruction),
            escrow_index: ACTOR_ESCROW_INDEX,
        },
        compute_units,
        escrow_authority,
        destination_program: *destination_program,
        accounts: vec![
            committee.into(),
            ShortAccountMeta {
                pubkey: *transfer_destination.key,
                is_writable: true,
            },
            system.into(),
        ],
    }
}

fn process_create_intent_bundle(
    accounts: &[AccountInfo],
    num_commit_only: u8,
    num_undelegate: u8,
    counter_diffs: Vec<i64>,
    compute_units: u32,
) -> ProgramResult {
    let num_commit_only = num_commit_only as usize;
    let num_undelegate = num_undelegate as usize;
    if accounts.len() != 5 + 2 * num_commit_only + 2 * num_undelegate {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let iter = &mut accounts.iter();
    let destination_program = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let transfer_destination = next_account_info(iter)?;
    let system = next_account_info(iter)?;
    let commit_only_escrows = next_account_infos(iter, num_commit_only)?;
    let commit_only_counters = next_account_infos(iter, num_commit_only)?;
    let undelegate_escrows = next_account_infos(iter, num_undelegate)?;
    let undelegate_counters = next_account_infos(iter, num_undelegate)?;

    let payer = if !commit_only_escrows.is_empty() {
        commit_only_escrows[0].clone()
    } else if !undelegate_escrows.is_empty() {
        undelegate_escrows[0].clone()
    } else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let mut builder = MagicIntentBundleBuilder::new(
        payer,
        magic_context.clone(),
        magic_program.clone(),
    );

    if !commit_only_counters.is_empty() {
        let handlers = commit_only_counters
            .iter()
            .zip(commit_only_escrows.iter().cloned())
            .map(|(counter, escrow_authority)| {
                action_handler(
                    counter,
                    transfer_destination,
                    system,
                    destination_program.key,
                    escrow_authority,
                    &FlexiInstruction::CommitActionHandler { amount: PRIZE },
                    compute_units,
                )
            })
            .collect::<Vec<_>>();
        builder = builder
            .commit(commit_only_counters)
            .add_post_commit_actions(handlers)
            .fold_builder();
    }

    if !undelegate_counters.is_empty() {
        let commit_handlers = undelegate_counters
            .iter()
            .zip(undelegate_escrows.iter().cloned())
            .map(|(counter, escrow_authority)| {
                action_handler(
                    counter,
                    transfer_destination,
                    system,
                    destination_program.key,
                    escrow_authority,
                    &FlexiInstruction::CommitActionHandler { amount: PRIZE },
                    compute_units,
                )
            })
            .collect::<Vec<_>>();
        let undelegate_handlers = undelegate_counters
            .iter()
            .zip(undelegate_escrows.iter().cloned())
            .zip(counter_diffs.iter().copied())
            .map(|((counter, escrow_authority), counter_diff)| {
                action_handler(
                    counter,
                    transfer_destination,
                    system,
                    destination_program.key,
                    escrow_authority,
                    &FlexiInstruction::UndelegateActionHandler {
                        counter_diff,
                        amount: PRIZE,
                    },
                    compute_units,
                )
            })
            .collect::<Vec<_>>();
        builder = builder
            .commit_and_undelegate(undelegate_counters)
            .add_post_commit_actions(commit_handlers)
            .add_post_undelegate_actions(undelegate_handlers)
            .fold_builder();
    }

    builder.build_and_invoke()
}

fn process_create_intent_bundle_commit_and_finalize(
    accounts: &[AccountInfo],
    num_commit: u8,
    num_commit_finalize: u8,
) -> ProgramResult {
    use magic_api::{
        args::{CommitTypeArgs, MagicIntentBundleArgs},
        instruction::MagicBlockInstruction,
    };

    let num_commit = num_commit as usize;
    let num_commit_finalize = num_commit_finalize as usize;
    if accounts.len() != 3 + num_commit + num_commit_finalize {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let iter = &mut accounts.iter();
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let payer = next_account_info(iter)?;
    let commit_accounts = next_account_infos(iter, num_commit)?;
    let commit_finalize_accounts =
        next_account_infos(iter, num_commit_finalize)?;

    let mut all_accounts = Vec::new();
    all_accounts.extend(commit_accounts.iter().cloned());
    all_accounts.extend(commit_finalize_accounts.iter().cloned());

    let mut dedup_keys = Vec::new();
    for account in &all_accounts {
        if !dedup_keys.iter().any(|key| key == account.key) {
            dedup_keys.push(*account.key);
        }
    }

    let account_index = |pubkey: &Pubkey| -> u8 {
        dedup_keys
            .iter()
            .position(|key| key == pubkey)
            .map(|index| (2 + index) as u8)
            .unwrap()
    };

    let args = MagicIntentBundleArgs {
        commit: Some(CommitTypeArgs::Standalone(
            commit_accounts
                .iter()
                .map(|account| account_index(account.key))
                .collect(),
        )),
        commit_and_undelegate: None,
        commit_finalize: Some(CommitTypeArgs::Standalone(
            commit_finalize_accounts
                .iter()
                .map(|account| account_index(account.key))
                .collect(),
        )),
        commit_finalize_and_undelegate: None,
        standalone_actions: vec![],
    };

    let mut metas = vec![
        AccountMeta::new(*payer.key, true),
        AccountMeta::new(*magic_context.key, false),
    ];
    metas.extend(
        dedup_keys
            .iter()
            .map(|pubkey| AccountMeta::new(*pubkey, false)),
    );

    let mut cpi_accounts =
        vec![magic_program.clone(), payer.clone(), magic_context.clone()];
    cpi_accounts.extend(dedup_keys.iter().filter_map(|pubkey| {
        all_accounts
            .iter()
            .find(|account| account.key == pubkey)
            .cloned()
    }));

    let ix = Instruction::new_with_bincode(
        *magic_program.key,
        &MagicBlockInstruction::ScheduleIntentBundle(args),
        metas,
    );
    invoke(&ix, &cpi_accounts)
}

fn process_create_transfer_intent(
    accounts: &[AccountInfo],
    amount: u64,
    fail: bool,
    compute_units: u32,
) -> ProgramResult {
    let [payer, counter, destination, system, magic_context, magic_program, magic_fee_vault] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    invoke(
        &system_instruction::transfer(payer.key, counter.key, amount),
        &[payer.clone(), counter.clone(), system.clone()],
    )?;

    let action = FlexiInstruction::TransferActionHandler { amount, fail };
    let call_handler = CallHandler {
        args: ActionArgs {
            data: tagged(&action),
            escrow_index: ACTOR_ESCROW_INDEX,
        },
        compute_units,
        escrow_authority: payer.clone(),
        destination_program: crate::ID,
        accounts: vec![
            ShortAccountMeta {
                pubkey: *destination.key,
                is_writable: true,
            },
            system.into(),
        ],
    };

    let callback = ActionCallback {
        destination_program: crate::ID,
        discriminator: TRANSFER_CALLBACK_DISCRIMINATOR.to_vec(),
        payload: amount.to_le_bytes().to_vec(),
        compute_units: 20_000,
        accounts: vec![
            ShortAccountMeta {
                pubkey: CALLBACK_SIGNER,
                is_writable: false,
            },
            ShortAccountMeta {
                pubkey: *counter.key,
                is_writable: true,
            },
            ShortAccountMeta {
                pubkey: *payer.key,
                is_writable: true,
            },
            system.into(),
        ],
    };

    MagicIntentBundleBuilder::new(
        payer.clone(),
        magic_context.clone(),
        magic_program.clone(),
    )
    .magic_fee_vault(magic_fee_vault.clone())
    .commit(std::slice::from_ref(counter))
    .add_post_commit_action(call_handler)
    .then(callback)
    .build_and_invoke()
}

fn process_commit_action_handler(
    accounts: &[AccountInfo],
    amount: u64,
) -> ProgramResult {
    let [delegated_account, destination, system, source_program, _, escrow_account] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !escrow_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if delegated_account.owner != &sdk::id() {
        return Err(ProgramError::InvalidAccountOwner);
    }
    if source_program.key != &crate::ID {
        return Err(ProgramError::IncorrectProgramId);
    }
    invoke(
        &system_instruction::transfer(
            escrow_account.key,
            destination.key,
            amount,
        ),
        &[escrow_account.clone(), destination.clone(), system.clone()],
    )
}

fn process_undelegate_action_handler(
    accounts: &[AccountInfo],
    amount: u64,
    counter_diff: i64,
) -> ProgramResult {
    let [undelegated_counter, destination, system, source_program, _, escrow_account] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !escrow_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if undelegated_counter.owner == &sdk::id() {
        return Err(ProgramError::InvalidAccountOwner);
    }
    if source_program.key != &crate::ID {
        return Err(ProgramError::IncorrectProgramId);
    }

    let mut counter = {
        let data = undelegated_counter.data.borrow();
        FlexiCounter::deserialize(&mut data.as_ref())?
    };
    counter.count = u64::try_from(counter.count as i64 + counter_diff)
        .map_err(|_| ProgramError::ArithmeticOverflow)?;
    counter.updates += 1;
    let counter_data = to_vec(&counter)?;
    undelegated_counter.data.borrow_mut()[..counter_data.len()]
        .copy_from_slice(&counter_data);

    invoke(
        &system_instruction::transfer(
            escrow_account.key,
            destination.key,
            amount,
        ),
        &[escrow_account.clone(), destination.clone(), system.clone()],
    )
}

fn process_transfer_action_handler(
    accounts: &[AccountInfo],
    amount: u64,
    fail: bool,
) -> ProgramResult {
    let [destination, system, source_program, _, escrow_account] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if source_program.key != &crate::ID {
        return Err(ProgramError::IncorrectProgramId);
    }
    if !escrow_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if fail {
        return Err(ProgramError::Custom(TRANSFER_FAIL_CODE));
    }
    invoke(
        &system_instruction::transfer(
            escrow_account.key,
            destination.key,
            amount,
        ),
        &[escrow_account.clone(), destination.clone(), system.clone()],
    )
}

pub fn process_transfer_callback(
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let [callback_signer, counter, payer, _system] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    if !callback_signer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if callback_signer.key != &CALLBACK_SIGNER {
        return Err(ProgramError::IncorrectAuthority);
    }

    let response: MagicResponse = bincode::deserialize(data)
        .map_err(|_| ProgramError::InvalidInstructionData)?;
    if !response.ok() {
        let amount = u64::from_le_bytes(
            response
                .data()
                .try_into()
                .map_err(|_| ProgramError::InvalidInstructionData)?,
        );
        **counter.try_borrow_mut_lamports()? -= amount;
        **payer.try_borrow_mut_lamports()? += amount;
    }
    Ok(())
}

pub mod build {
    use sdk::{
        consts::{MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID},
        delegate_args::{DelegateAccountMetas, DelegateAccounts},
    };

    use super::*;

    fn with_tag(
        instruction: &FlexiInstruction,
        metas: Vec<AccountMeta>,
    ) -> Instruction {
        let mut data = vec![crate::FLEXI_TAG];
        data.extend(
            to_vec(instruction).expect("instruction serialization cannot fail"),
        );
        Instruction {
            program_id: crate::id(),
            accounts: metas,
            data,
        }
    }

    pub fn init_counter(payer: Pubkey, label: &str) -> (Instruction, Pubkey) {
        let (pda, bump) = FlexiCounter::pda_and_bump(&payer);
        let metas = vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(pda, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ];
        (
            with_tag(
                &FlexiInstruction::Init {
                    label: label.to_owned(),
                    bump,
                },
                metas,
            ),
            pda,
        )
    }

    pub fn delegate_counter(
        payer: Pubkey,
        commit_frequency_ms: u32,
        validator: Option<Pubkey>,
    ) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        let delegate_accounts = DelegateAccounts::new(pda, crate::id());
        let delegate_metas = DelegateAccountMetas::from(delegate_accounts);
        let metas = vec![
            AccountMeta::new(payer, true),
            delegate_metas.delegated_account,
            delegate_metas.owner_program,
            delegate_metas.delegate_buffer,
            delegate_metas.delegation_record,
            delegate_metas.delegation_metadata,
            delegate_metas.delegation_program,
            delegate_metas.system_program,
        ];
        with_tag(
            &FlexiInstruction::Delegate {
                commit_frequency_ms,
                validator,
            },
            metas,
        )
    }

    pub fn add(payer: Pubkey, count: u8) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        let metas = vec![
            AccountMeta::new_readonly(payer, true),
            AccountMeta::new(pda, false),
        ];
        with_tag(&FlexiInstruction::Add { count }, metas)
    }

    fn intent_prefix(destination: Pubkey) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new_readonly(crate::id(), false),
            AccountMeta::new(MAGIC_CONTEXT_ID, false),
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
            AccountMeta::new_readonly(destination, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ]
    }

    pub fn create_intent(
        payers: &[Pubkey],
        destination: Pubkey,
        counter_diffs: Option<Vec<i64>>,
        compute_units: u32,
    ) -> Instruction {
        let is_undelegate = counter_diffs.is_some();
        let mut metas = intent_prefix(destination);
        metas.extend(
            payers
                .iter()
                .map(|payer| AccountMeta::new_readonly(*payer, true)),
        );
        metas.extend(payers.iter().map(|payer| {
            let (pda, _) = FlexiCounter::pda_and_bump(payer);
            AccountMeta::new(pda, false)
        }));
        with_tag(
            &FlexiInstruction::CreateIntent {
                num_committees: payers.len() as u8,
                counter_diffs: counter_diffs.unwrap_or_default(),
                is_undelegate,
                compute_units,
            },
            metas,
        )
    }

    pub fn create_intent_bundle(
        commit_only_payers: &[Pubkey],
        undelegate_payers: &[Pubkey],
        destination: Pubkey,
        counter_diffs: Vec<i64>,
        compute_units: u32,
    ) -> Instruction {
        let mut metas = intent_prefix(destination);
        metas.extend(
            commit_only_payers
                .iter()
                .map(|payer| AccountMeta::new_readonly(*payer, true)),
        );
        metas.extend(commit_only_payers.iter().map(|payer| {
            let (pda, _) = FlexiCounter::pda_and_bump(payer);
            AccountMeta::new(pda, false)
        }));
        metas.extend(
            undelegate_payers
                .iter()
                .map(|payer| AccountMeta::new_readonly(*payer, true)),
        );
        metas.extend(undelegate_payers.iter().map(|payer| {
            let (pda, _) = FlexiCounter::pda_and_bump(payer);
            AccountMeta::new(pda, false)
        }));
        with_tag(
            &FlexiInstruction::CreateIntentBundle {
                num_commit_only: commit_only_payers.len() as u8,
                num_undelegate: undelegate_payers.len() as u8,
                counter_diffs,
                compute_units,
            },
            metas,
        )
    }

    pub fn create_intent_bundle_commit_and_finalize(
        payer: Pubkey,
        commit_payers: &[Pubkey],
        commit_finalize_payers: &[Pubkey],
    ) -> Instruction {
        let mut metas = vec![
            AccountMeta::new(MAGIC_CONTEXT_ID, false),
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
            AccountMeta::new_readonly(payer, true),
        ];
        metas.extend(commit_payers.iter().map(|p| {
            let (pda, _) = FlexiCounter::pda_and_bump(p);
            AccountMeta::new(pda, false)
        }));
        metas.extend(commit_finalize_payers.iter().map(|p| {
            let (pda, _) = FlexiCounter::pda_and_bump(p);
            AccountMeta::new(pda, false)
        }));
        with_tag(
            &FlexiInstruction::CreateIntentBundleCommitAndFinalize {
                num_commit: commit_payers.len() as u8,
                num_commit_finalize: commit_finalize_payers.len() as u8,
            },
            metas,
        )
    }

    pub fn create_transfer_intent(
        payer: Pubkey,
        destination: Pubkey,
        magic_fee_vault: Pubkey,
        amount: u64,
        fail: bool,
        compute_units: u32,
    ) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        let metas = vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(pda, false),
            AccountMeta::new_readonly(destination, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new(MAGIC_CONTEXT_ID, false),
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
            AccountMeta::new(magic_fee_vault, false),
        ];
        with_tag(
            &FlexiInstruction::CreateTransferIntent {
                amount,
                fail,
                compute_units,
            },
            metas,
        )
    }
}
