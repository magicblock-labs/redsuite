//! Native protocol calls used to exercise boundaries outside the fixture ABI.

use instruction::{AccountMeta, Instruction};
use magic_api::{
    args::{AddActionCallbackArgs, MagicIntentBundleArgs, ScheduleTaskArgs},
    instruction::{CallbackInstruction, MagicBlockInstruction},
};
use pubkey::Pubkey;
pub use sdk::consts::{MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID};

use crate::SettlementMode;

pub const MAGIC: Magic = Magic;
pub struct Magic;

pub struct Task {
    pub id: i64,
    pub interval_ms: i64,
    pub iterations: i64,
}

impl Magic {
    fn encode(self, instruction: MagicBlockInstruction, accounts: Vec<AccountMeta>) -> Instruction {
        Instruction {
            program_id: MAGIC_PROGRAM_ID,
            accounts,
            data: bincode::serialize(&instruction).expect("native instruction serialization"),
        }
    }

    pub fn commit(
        self,
        payer: Pubkey,
        vault: Option<Pubkey>,
        cells: &[Pubkey],
        mode: SettlementMode,
    ) -> Instruction {
        let mut accounts =
            vec![AccountMeta::new(payer, true), AccountMeta::new(MAGIC_CONTEXT_ID, false)];
        accounts.extend(vault.map(|key| AccountMeta::new(key, false)));
        accounts.extend(cells.iter().map(|&key| AccountMeta::new(key, false)));
        let instruction = match mode {
            SettlementMode::Commit => MagicBlockInstruction::ScheduleCommit,
            SettlementMode::Undelegate => MagicBlockInstruction::ScheduleCommitAndUndelegate,
        };
        self.encode(instruction, accounts)
    }

    pub fn schedule(
        self,
        payer: Pubkey,
        cell: Pubkey,
        task: Task,
        instructions: Vec<Instruction>,
    ) -> Instruction {
        self.encode(
            MagicBlockInstruction::ScheduleTask(ScheduleTaskArgs {
                task_id: task.id,
                execution_interval_millis: task.interval_ms,
                iterations: task.iterations,
                instructions,
            }),
            vec![AccountMeta::new(payer, true), AccountMeta::new(cell, true)],
        )
    }

    pub fn cancel(self, payer: Pubkey, task_id: i64) -> Instruction {
        self.encode(
            MagicBlockInstruction::CancelTask { task_id },
            vec![AccountMeta::new(payer, true)],
        )
    }

    pub fn add_callback(
        self,
        payer: Pubkey,
        vault: Pubkey,
        args: AddActionCallbackArgs,
    ) -> Instruction {
        self.encode(
            MagicBlockInstruction::AddActionCallback(args),
            vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(MAGIC_CONTEXT_ID, false),
                AccountMeta::new(vault, false),
            ],
        )
    }

    pub fn execute_callback(self, authority: AccountMeta, instruction: Instruction) -> Instruction {
        let mut accounts = vec![
            authority,
            AccountMeta::new_readonly(magic_api::pda::CALLBACK_SIGNER, false),
            AccountMeta::new_readonly(instruction.program_id, false),
        ];
        accounts.extend(instruction.accounts.iter().cloned());
        Instruction {
            program_id: magic_api::CALLBACK_PROGRAM_ID,
            accounts,
            data: bincode::serialize(&CallbackInstruction::ExecuteCallback { instruction })
                .expect("callback instruction serialization"),
        }
    }

    pub fn intents(
        self,
        payer: Pubkey,
        vault: Pubkey,
        cells: &[Pubkey],
        args: MagicIntentBundleArgs,
    ) -> Instruction {
        let mut accounts = vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(MAGIC_CONTEXT_ID, false),
            AccountMeta::new(vault, false),
        ];
        accounts.extend(cells.iter().map(|&key| AccountMeta::new(key, false)));
        self.encode(MagicBlockInstruction::ScheduleIntentBundle(args), accounts)
    }
}
