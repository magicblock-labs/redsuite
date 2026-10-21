use borsh::{to_vec, BorshDeserialize, BorshSerialize};
use pubkey::Pubkey;

pub const FLEXI_SEED: &[u8] = b"flexi_counter";
pub const ACTOR_ESCROW_INDEX: u8 = 1;
pub const TRANSFER_CALLBACK: &[u8] = &[crate::FLEXI_TAG, 255];
pub const TRANSFER_FAILURE: u32 = 0xFA11;
pub const ACTION_PRIZE: u64 = 1_000_000;
pub const ACTION_COMPUTE_UNITS: u32 = 100_000;

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy)]
pub enum IntentMode {
    Commit,
    Undelegate(i64),
    Finalize,
}
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlexiCounter {
    pub count: u64,
    pub updates: u64,
    pub label: String,
}

impl FlexiCounter {
    pub fn new(label: String) -> Self {
        Self { count: 0, updates: 0, label }
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

// Keep the existing tags when retiring instruction families.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
#[repr(u8)]
#[borsh(use_discriminant = true)]
pub enum FlexiInstruction {
    Init {
        label: String,
        bump: u8,
    } = 0,
    Delegate {
        commit_frequency_ms: u32,
        validator: Option<Pubkey>,
    } = 1,
    Add {
        count: u8,
    } = 2,
    AddUnsigned {
        count: u8,
    } = 10,
    AddError {
        count: u8,
    } = 11,
    ScheduleCounterTask {
        task_id: i64,
        execution_interval_millis: i64,
        iterations: i64,
        signer: bool,
    } = 12,
    CancelCounterTask {
        task_id: i64,
    } = 13,
    CreateActionIntent {
        counter: Pubkey,
        count: u8,
        compute_units: u32,
    } = 16,
    AddActionHandler {
        count: u8,
    } = 17,
    ScheduleIntents {
        modes: Vec<IntentMode>,
        actions: bool,
    } = 18,
    BaseAction {
        amount: u64,
        change: Option<i64>,
        fail: bool,
    } = 19,
    TransferIntent {
        amount: u64,
        fail: bool,
    } = 20,
}

pub mod build {
    use instruction::{AccountMeta, Instruction};
    use sdk::consts::{MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID};
    use sdk_ids::system_program;

    use super::*;

    fn with_tag(instruction: &FlexiInstruction, metas: Vec<AccountMeta>) -> Instruction {
        Instruction {
            program_id: crate::id(),
            accounts: metas,
            data: tagged(instruction),
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
                &FlexiInstruction::Init { label: label.to_owned(), bump },
                metas,
            ),
            pda,
        )
    }

    pub fn schedule_intents(
        payers: &[Pubkey],
        validator: Pubkey,
        destination: Pubkey,
        modes: Vec<IntentMode>,
        actions: bool,
    ) -> Instruction {
        let mut metas = vec![
            AccountMeta::new(payers[0], true),
            AccountMeta::new(MAGIC_CONTEXT_ID, false),
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
            AccountMeta::new(
                sdk::pda::magic_fee_vault_pda_from_validator(&validator),
                false,
            ),
            AccountMeta::new_readonly(destination, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ];
        for payer in payers {
            metas.extend([
                AccountMeta::new_readonly(*payer, true),
                AccountMeta::new(FlexiCounter::pda_and_bump(payer).0, false),
            ]);
        }
        with_tag(&FlexiInstruction::ScheduleIntents { modes, actions }, metas)
    }

    pub fn transfer_intent(
        payer: Pubkey,
        validator: Pubkey,
        destination: Pubkey,
        amount: u64,
        fail: bool,
    ) -> Instruction {
        let mut ix = schedule_intents(&[payer], validator, destination, vec![], false);
        ix.data = tagged(&FlexiInstruction::TransferIntent { amount, fail });
        ix
    }

    pub fn delegate_counter(
        payer: Pubkey,
        commit_frequency_ms: u32,
        validator: Option<Pubkey>,
    ) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        with_tag(
            &FlexiInstruction::Delegate { commit_frequency_ms, validator },
            crate::delegate_metas(payer, pda),
        )
    }

    pub fn add(payer: Pubkey, count: u8) -> Instruction {
        let (pda, _) = FlexiCounter::pda_and_bump(&payer);
        let metas = vec![AccountMeta::new_readonly(payer, true), AccountMeta::new(pda, false)];
        with_tag(&FlexiInstruction::Add { count }, metas)
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

    pub fn schedule_counter_task(
        payer: Pubkey,
        task_id: i64,
        execution_interval_millis: i64,
        iterations: i64,
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

    pub fn create_action_intent(
        payer: Pubkey,
        counter: Pubkey,
        count: u8,
        compute_units: u32,
    ) -> Instruction {
        let metas = vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(MAGIC_CONTEXT_ID, false),
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
        ];
        with_tag(
            &FlexiInstruction::CreateActionIntent { counter, count, compute_units },
            metas,
        )
    }
}
