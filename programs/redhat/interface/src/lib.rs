use borsh::{BorshDeserialize, BorshSerialize};
use pubkey::Pubkey;

pub const ID: Pubkey =
    Pubkey::from_str_const("BTczL2chGpVHw25pbmMtkFAD1t7rxoa8pVbaUjsybjiq");

pub const fn id() -> Pubkey {
    ID
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum SecurityInstruction {
    // Commits the PDAs twice: once via the owning program (legitimate) and
    // once directly via the magic program (must fail — this program does not
    // own the PDAs).
    SiblingScheduleCommitCpis(Vec<Pubkey>),
    // A no-op instruction. Used to try to confuse the CPI-parent detection.
    NonCpi,
    // Commits the PDAs directly via the magic program from a program that does
    // not own them (must fail).
    DirectScheduleCommitCpi,
}

pub mod build {
    use instruction::{AccountMeta, Instruction};
    use sdk::consts::{MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID};

    use super::*;

    fn with_pdas(
        payer: Pubkey,
        pass_schedulecommit_program: bool,
        pdas: &[Pubkey],
    ) -> Vec<AccountMeta> {
        let mut metas = vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(MAGIC_CONTEXT_ID, false),
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
        ];
        if pass_schedulecommit_program {
            metas.push(AccountMeta::new_readonly(
                redshift_interface::id(),
                false,
            ));
        }
        metas.extend(pdas.iter().map(|key| AccountMeta::new(*key, false)));
        metas
    }

    fn borsh_ix(
        instruction: &SecurityInstruction,
        metas: Vec<AccountMeta>,
    ) -> Instruction {
        Instruction {
            program_id: crate::id(),
            accounts: metas,
            data: borsh::to_vec(instruction)
                .expect("instruction serialization cannot fail"),
        }
    }

    pub fn sibling_schedule_commit_cpis(
        payer: Pubkey,
        players: &[Pubkey],
        pdas: &[Pubkey],
    ) -> Instruction {
        borsh_ix(
            &SecurityInstruction::SiblingScheduleCommitCpis(players.to_vec()),
            with_pdas(payer, true, pdas),
        )
    }

    pub fn nested_schedule_commit_cpi(
        payer: Pubkey,
        pdas: &[Pubkey],
    ) -> Instruction {
        borsh_ix(
            &SecurityInstruction::DirectScheduleCommitCpi,
            with_pdas(payer, false, pdas),
        )
    }

    pub fn non_cpi(payer: Pubkey) -> Instruction {
        borsh_ix(
            &SecurityInstruction::NonCpi,
            vec![AccountMeta::new(payer, true)],
        )
    }
}
