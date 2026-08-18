use borsh::{BorshDeserialize, BorshSerialize};
use solana_program::{declare_id, pubkey::Pubkey};

declare_id!("BTczL2chGpVHw25pbmMtkFAD1t7rxoa8pVbaUjsybjiq");

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
    DirectScheduleCommitCpi(Vec<Pubkey>),
}

pub mod build {
    use redshift_interface::schedulecommit::build::{
        magic_context_id, magic_program_id,
    };
    use solana_program::instruction::{AccountMeta, Instruction};

    use super::*;

    fn with_pdas(
        payer: Pubkey,
        pass_schedulecommit_program: bool,
        pdas: &[Pubkey],
    ) -> Vec<AccountMeta> {
        let mut metas = vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(magic_context_id(), false),
            AccountMeta::new_readonly(magic_program_id(), false),
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
        players: &[Pubkey],
        pdas: &[Pubkey],
    ) -> Instruction {
        borsh_ix(
            &SecurityInstruction::DirectScheduleCommitCpi(players.to_vec()),
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
