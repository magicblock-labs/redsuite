use pubkey::Pubkey;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum Instruction {
    CommitAccounts {
        id: u64,
    },
    CommitAndUndelegateAccounts {
        id: u64,
    },
    InitAccount {
        space: u32,
        seed: u8,
        bump: u8,
        authority: Pubkey,
    },
    Delegate {
        seed: u8,
        authority: Pubkey,
    },
    SimpleByteSet {
        id: u64,
    },
    ExpensiveHashCompute {
        id: u64,
        init: Pubkey,
        iters: u32,
    },
    MultiAccountRead {
        id: u64,
    },
    AccountDataCopy {
        id: u64,
    },
    ReadAccountsData {
        id: u64,
    },
    CloseAccount,
    HashFold {
        id: u64,
        iters: u32,
    },
}

pub mod build {
    use instruction::{AccountMeta, Instruction as SolanaInstruction};
    use sdk::consts::{MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID};
    use sdk_ids::system_program;

    use super::*;
    use crate::utils::derive_pda;

    fn with_bincode(
        data: &Instruction,
        accounts: Vec<AccountMeta>,
    ) -> SolanaInstruction {
        SolanaInstruction {
            program_id: crate::id(),
            accounts,
            data: bincode::serialize(data)
                .expect("instruction serialization is infallible"),
        }
    }

    pub fn init_account(
        payer: Pubkey,
        base: Pubkey,
        space: u32,
        seed: u8,
        authority: Pubkey,
    ) -> (SolanaInstruction, Pubkey) {
        init_account_at(crate::id(), payer, base, space, seed, authority)
    }

    pub fn init_account_at(
        program_id: Pubkey,
        payer: Pubkey,
        base: Pubkey,
        space: u32,
        seed: u8,
        authority: Pubkey,
    ) -> (SolanaInstruction, Pubkey) {
        let (pda, bump) = derive_pda(&program_id, base, space, seed, authority);
        let metas = vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(pda, false),
            AccountMeta::new_readonly(base, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ];
        let ix = Instruction::InitAccount {
            space,
            seed,
            bump,
            authority,
        };
        let mut ix = with_bincode(&ix, metas);
        ix.program_id = program_id;
        (ix, pda)
    }

    pub fn delegate(
        payer: Pubkey,
        pda: Pubkey,
        base: Pubkey,
        seed: u8,
        authority: Pubkey,
    ) -> SolanaInstruction {
        delegate_at(crate::id(), payer, pda, base, seed, authority)
    }

    pub fn delegate_at(
        program_id: Pubkey,
        payer: Pubkey,
        pda: Pubkey,
        base: Pubkey,
        seed: u8,
        authority: Pubkey,
    ) -> SolanaInstruction {
        let accounts =
            sdk::delegate_args::DelegateAccounts::new(pda, program_id);
        let m = sdk::delegate_args::DelegateAccountMetas::from(accounts);
        let metas = vec![
            AccountMeta::new(payer, true),
            m.delegated_account,
            m.owner_program,
            m.delegate_buffer,
            m.delegation_record,
            m.delegation_metadata,
            m.delegation_program,
            m.system_program,
            AccountMeta::new_readonly(base, false),
        ];
        let mut ix =
            with_bincode(&Instruction::Delegate { seed, authority }, metas);
        ix.program_id = program_id;
        ix
    }

    pub fn simple_byte_set(id: u64, accounts: &[Pubkey]) -> SolanaInstruction {
        simple_byte_set_at(crate::id(), id, accounts)
    }

    pub fn simple_byte_set_at(
        program_id: Pubkey,
        id: u64,
        accounts: &[Pubkey],
    ) -> SolanaInstruction {
        let metas = accounts
            .iter()
            .map(|&pk| AccountMeta::new(pk, false))
            .collect();
        let mut ix = with_bincode(&Instruction::SimpleByteSet { id }, metas);
        ix.program_id = program_id;
        ix
    }

    pub fn expensive_hash_compute(
        id: u64,
        init: Pubkey,
        iters: u32,
        accounts: &[Pubkey],
    ) -> SolanaInstruction {
        expensive_hash_compute_at(crate::id(), id, init, iters, accounts)
    }

    pub fn expensive_hash_compute_at(
        program_id: Pubkey,
        id: u64,
        init: Pubkey,
        iters: u32,
        accounts: &[Pubkey],
    ) -> SolanaInstruction {
        let metas = accounts
            .iter()
            .map(|&pk| AccountMeta::new(pk, false))
            .collect();
        let mut ix = with_bincode(
            &Instruction::ExpensiveHashCompute { id, init, iters },
            metas,
        );
        ix.program_id = program_id;
        ix
    }

    pub fn multi_account_read(
        id: u64,
        target: Pubkey,
        sources: &[Pubkey],
    ) -> SolanaInstruction {
        let mut metas = vec![AccountMeta::new(target, false)];
        metas.extend(
            sources
                .iter()
                .map(|&pk| AccountMeta::new_readonly(pk, false)),
        );
        with_bincode(&Instruction::MultiAccountRead { id }, metas)
    }

    pub fn account_data_copy(
        id: u64,
        sources: &[Pubkey],
        dests: &[Pubkey],
    ) -> SolanaInstruction {
        let mut metas: Vec<_> = sources
            .iter()
            .map(|&pk| AccountMeta::new_readonly(pk, false))
            .collect();
        metas.extend(dests.iter().map(|&pk| AccountMeta::new(pk, false)));
        with_bincode(&Instruction::AccountDataCopy { id }, metas)
    }

    pub fn read_accounts_data(
        id: u64,
        accounts: &[Pubkey],
    ) -> SolanaInstruction {
        let metas = accounts
            .iter()
            .map(|&pk| AccountMeta::new_readonly(pk, false))
            .collect();
        with_bincode(&Instruction::ReadAccountsData { id }, metas)
    }

    pub fn commit_accounts(
        id: u64,
        payer: Pubkey,
        accounts: &[Pubkey],
    ) -> SolanaInstruction {
        with_bincode(
            &Instruction::CommitAccounts { id },
            commit_metas(payer, accounts),
        )
    }

    pub fn commit_and_undelegate_accounts(
        id: u64,
        payer: Pubkey,
        accounts: &[Pubkey],
    ) -> SolanaInstruction {
        with_bincode(
            &Instruction::CommitAndUndelegateAccounts { id },
            commit_metas(payer, accounts),
        )
    }

    pub fn close_account(owner: Pubkey, account: Pubkey) -> SolanaInstruction {
        let metas = vec![
            AccountMeta::new(owner, true),
            AccountMeta::new(account, false),
        ];
        with_bincode(&Instruction::CloseAccount, metas)
    }

    pub fn hash_fold(
        id: u64,
        iters: u32,
        accounts: &[Pubkey],
    ) -> SolanaInstruction {
        let metas = accounts
            .iter()
            .map(|&pk| AccountMeta::new(pk, false))
            .collect();
        with_bincode(&Instruction::HashFold { id, iters }, metas)
    }

    fn commit_metas(payer: Pubkey, accounts: &[Pubkey]) -> Vec<AccountMeta> {
        let mut metas = vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(MAGIC_CONTEXT_ID, false),
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
        ];
        metas.extend(accounts.iter().map(|&pk| AccountMeta::new(pk, false)));
        metas
    }
}
