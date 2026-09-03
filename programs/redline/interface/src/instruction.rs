use serde::{Deserialize, Serialize};
use solana_program::pubkey::Pubkey;

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
    use sdk::consts::{MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID};
    use solana_program::instruction::{
        AccountMeta, Instruction as SolanaInstruction,
    };
    use solana_sdk_ids::system_program;

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

#[cfg(test)]
mod tests {
    use sdk::consts::{MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID};

    use super::*;

    #[test]
    fn discriminants_are_stable() {
        let cases: Vec<(Instruction, u32)> = vec![
            (Instruction::CommitAccounts { id: 1 }, 0),
            (Instruction::CommitAndUndelegateAccounts { id: 1 }, 1),
            (
                Instruction::InitAccount {
                    space: 128,
                    seed: 1,
                    bump: 255,
                    authority: Pubkey::new_unique(),
                },
                2,
            ),
            (
                Instruction::Delegate {
                    seed: 1,
                    authority: Pubkey::new_unique(),
                },
                3,
            ),
            (Instruction::SimpleByteSet { id: 1 }, 4),
            (
                Instruction::ExpensiveHashCompute {
                    id: 1,
                    init: Pubkey::new_unique(),
                    iters: 10,
                },
                5,
            ),
            (Instruction::MultiAccountRead { id: 1 }, 6),
            (Instruction::AccountDataCopy { id: 1 }, 7),
            (Instruction::ReadAccountsData { id: 1 }, 8),
            (Instruction::CloseAccount, 9),
            (Instruction::HashFold { id: 1, iters: 0 }, 10),
        ];
        for (ix, expected) in cases {
            let bytes = bincode::serialize(&ix).unwrap();
            let disc = u32::from_le_bytes(bytes[..4].try_into().unwrap());
            assert_eq!(disc, expected);
        }
    }

    #[test]
    fn commit_metas_layout_is_r2_safe() {
        let payer = Pubkey::new_unique();
        let committed = [Pubkey::new_unique(), Pubkey::new_unique()];
        let ix = build::commit_accounts(7, payer, &committed);

        assert_eq!(ix.accounts[0].pubkey, payer);
        assert!(ix.accounts[0].is_signer && ix.accounts[0].is_writable);
        assert_eq!(ix.accounts[1].pubkey, MAGIC_CONTEXT_ID);
        assert!(!ix.accounts[1].is_signer && ix.accounts[1].is_writable);
        assert_eq!(ix.accounts[2].pubkey, MAGIC_PROGRAM_ID);
        assert!(!ix.accounts[2].is_signer && !ix.accounts[2].is_writable);
        for (meta, pk) in ix.accounts[3..].iter().zip(committed) {
            assert_eq!(meta.pubkey, pk);
            assert!(!meta.is_signer && meta.is_writable);
        }
    }

    #[test]
    fn init_account_builder_matches_processor_order() {
        let payer = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (ix, pda) = build::init_account(payer, base, 256, 3, authority);

        assert_eq!(
            ix.accounts.iter().map(|m| m.pubkey).collect::<Vec<_>>(),
            vec![payer, pda, base, solana_sdk_ids::system_program::ID],
        );
        let decoded: Instruction = bincode::deserialize(&ix.data).unwrap();
        let Instruction::InitAccount {
            space, seed, bump, ..
        } = decoded
        else {
            panic!("wrong variant");
        };
        assert_eq!((space, seed), (256, 3));
        assert_eq!(
            crate::utils::derive_pda(&crate::ID, base, 256, 3, authority),
            (pda, bump)
        );
    }
}
