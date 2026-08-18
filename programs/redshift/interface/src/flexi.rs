use borsh::{to_vec, BorshDeserialize, BorshSerialize};
use solana_program::pubkey::Pubkey;

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

pub fn tagged(instruction: &FlexiInstruction) -> Vec<u8> {
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
    AddUnsigned {
        count: u8,
    },
    AddError {
        count: u8,
    },
    ScheduleCounterTask {
        task_id: i64,
        execution_interval_millis: i64,
        iterations: i64,
        error: bool,
        signer: bool,
    },
    CancelCounterTask {
        task_id: i64,
    },
    Mul {
        multiplier: u8,
    },
    AddAndScheduleCommit {
        count: u8,
        undelegate: bool,
        has_magic_vault: bool,
    },
}

pub mod build {
    use sdk::{
        consts::{MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID},
        delegate_args::{DelegateAccountMetas, DelegateAccounts},
    };
    use solana_program::instruction::{AccountMeta, Instruction};
    use solana_sdk_ids::system_program;

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

    pub fn mul(payer: Pubkey, multiplier: u8) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        let metas = vec![
            AccountMeta::new_readonly(payer, true),
            AccountMeta::new(pda, false),
        ];
        with_tag(&FlexiInstruction::Mul { multiplier }, metas)
    }

    pub fn add_and_schedule_commit(
        payer: Pubkey,
        count: u8,
        undelegate: bool,
        magic_fee_vault: Option<Pubkey>,
    ) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        let mut metas = vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(pda, false),
            AccountMeta::new(MAGIC_CONTEXT_ID, false),
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
        ];
        if let Some(vault) = magic_fee_vault {
            metas.push(AccountMeta::new(vault, false));
        }
        with_tag(
            &FlexiInstruction::AddAndScheduleCommit {
                count,
                undelegate,
                has_magic_vault: magic_fee_vault.is_some(),
            },
            metas,
        )
    }

    pub fn add_unsigned(payer: Pubkey, count: u8) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        with_tag(
            &FlexiInstruction::AddUnsigned { count },
            vec![AccountMeta::new(pda, false)],
        )
    }

    pub fn add_error(payer: Pubkey, count: u8) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        with_tag(
            &FlexiInstruction::AddError { count },
            vec![AccountMeta::new(pda, false)],
        )
    }

    pub fn crank_signer_pda(authority: &Pubkey) -> Pubkey {
        Pubkey::find_program_address(
            &[magic_api::pda::CRANK_SEED, authority.as_ref()],
            &magic_api::CRANK_PROGRAM_ID,
        )
        .0
    }

    // The task instruction the crank runs with the per-authority crank
    // signer PDA as its only signer.
    pub fn add_unsigned_with_crank(payer: Pubkey, count: u8) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        with_tag(
            &FlexiInstruction::AddUnsigned { count },
            vec![
                AccountMeta::new(pda, false),
                AccountMeta::new_readonly(crank_signer_pda(&payer), true),
            ],
        )
    }

    pub fn schedule_counter_task(
        payer: Pubkey,
        task_id: i64,
        execution_interval_millis: i64,
        iterations: i64,
        error: bool,
        signer: bool,
    ) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        let metas = vec![
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
            AccountMeta::new(payer, true),
            AccountMeta::new(pda, false),
        ];
        with_tag(
            &FlexiInstruction::ScheduleCounterTask {
                task_id,
                execution_interval_millis,
                iterations,
                error,
                signer,
            },
            metas,
        )
    }

    pub fn cancel_counter_task(payer: Pubkey, task_id: i64) -> Instruction {
        let metas = vec![
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
            AccountMeta::new(payer, true),
        ];
        with_tag(&FlexiInstruction::CancelCounterTask { task_id }, metas)
    }

    // The raw magic-program ScheduleTask with caller-supplied task
    // instructions, for tasks the flexi Schedule variant cannot express.
    pub fn schedule_task_direct(
        authority: Pubkey,
        task_id: i64,
        execution_interval_millis: i64,
        iterations: i64,
        instructions: Vec<Instruction>,
    ) -> Instruction {
        use magic_api::{
            args::ScheduleTaskArgs, instruction::MagicBlockInstruction,
        };
        let data = bincode::serialize(&MagicBlockInstruction::ScheduleTask(
            ScheduleTaskArgs {
                task_id,
                execution_interval_millis,
                iterations,
                instructions,
            },
        ))
        .expect("schedule task serialization cannot fail");
        Instruction {
            program_id: MAGIC_PROGRAM_ID,
            accounts: vec![AccountMeta::new(authority, true)],
            data,
        }
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
