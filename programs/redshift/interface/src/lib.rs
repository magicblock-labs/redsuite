use solana_program::declare_id;

declare_id!("AijneHkXJVVWyimuwfSJdrJktARZu2WiMaZBqHsq7CS5");

pub mod flexi;

pub mod schedulecommit {
    use borsh::{BorshDeserialize, BorshSerialize};
    use solana_program::{program_error::ProgramError, pubkey::Pubkey};

    pub const FAIL_UNDELEGATION_COUNT: u64 = u64::MAX - 1;
    pub const PDA_SEED: &[u8] = b"magic_schedule_commit";
    pub const ORDER_BOOK_SEED: &[u8] = b"order_book";
    pub const ORDER_BOOK_INIT_SIZE: usize = 10 * 1024;

    pub const MAGIC_SCHEDULE_COMMIT_TAG: u32 = 1;
    pub const MAGIC_SCHEDULE_COMMIT_AND_UNDELEGATE_TAG: u32 = 2;
    pub const DLP_REQUEST_UNDELEGATION_TAG: u64 = 26;
    const ORDER_BOOK_HEADER_SIZE: usize = 8;
    const ORDER_LEVEL_SIZE: usize = 16;
    pub const SYSTEM_TRANSFER_TAG: u32 = 2;

    #[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq, Clone)]
    pub struct MainAccount {
        pub player: Pubkey,
        pub count: u64,
    }

    impl MainAccount {
        pub const SIZE: usize = 40;
    }

    #[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
    pub struct DelegateCpiArgs {
        pub valid_until: i64,
        pub commit_frequency_ms: u32,
        pub player: Pubkey,
        pub validator: Option<Pubkey>,
    }

    #[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
    pub struct ScheduleCommitCpiArgs {
        pub players: Vec<Pubkey>,
        pub modify_accounts: bool,
        pub commit_payer: bool,
        pub has_magic_vault: bool,
    }

    #[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
    pub struct ScheduleCommitCpiWithVaultArgs {
        pub players: Vec<Pubkey>,
        pub undelegate: bool,
        pub has_magic_vault: bool,
    }

    #[derive(
        BorshSerialize,
        BorshDeserialize,
        Debug,
        Clone,
        Copy,
        Default,
        PartialEq,
        Eq,
    )]
    pub struct OrderLevel {
        pub price: u64,
        pub size: u64,
    }

    #[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
    pub struct BookUpdate {
        pub bids: Vec<OrderLevel>,
        pub asks: Vec<OrderLevel>,
    }

    #[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
    pub struct DelegateOrderBookArgs {
        pub commit_frequency_ms: u32,
        pub book_manager: Pubkey,
        pub validator: Option<Pubkey>,
    }

    #[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
    pub struct ScheduleCommitWithOrderBookArgs {
        pub players: Vec<Pubkey>,
        pub with_actions: bool,
    }

    #[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
    pub enum ScheduleCommitInstruction {
        Init,
        DelegateCpi(DelegateCpiArgs),
        ScheduleCommitCpi(ScheduleCommitCpiArgs, ScheduleCommitType),
        ScheduleCommitWithVaultCpi(ScheduleCommitCpiWithVaultArgs),
        ScheduleCommitAndUndelegateCpiModAfter(Vec<Pubkey>),
        ScheduleCommitAndUndelegateCpiTwice(Vec<Pubkey>),
        IncreaseCount,
        SetCount(u64),
        InitOrderBook,
        GrowOrderBook(u64),
        DelegateOrderBook(DelegateOrderBookArgs),
        UpdateOrderBook(BookUpdate),
        ScheduleCommitWithVaultAndOrderBookCpi(ScheduleCommitWithOrderBookArgs),
        ScheduleCommitForOrderBook(ScheduleCommitType),
        RequestUndelegationCpi(Pubkey),
    }

    pub fn book_lens(data: &[u8]) -> Option<(usize, usize, usize)> {
        if data.len() < ORDER_BOOK_HEADER_SIZE {
            return None;
        }
        let capacity = (data.len() - ORDER_BOOK_HEADER_SIZE) / ORDER_LEVEL_SIZE;
        let bids = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let asks = u32::from_le_bytes(data[4..8].try_into().ok()?) as usize;
        (bids + asks <= capacity).then_some((bids, asks, capacity))
    }

    fn write_level(data: &mut [u8], index: usize, level: &OrderLevel) {
        let offset = ORDER_BOOK_HEADER_SIZE + index * ORDER_LEVEL_SIZE;
        data[offset..offset + 8].copy_from_slice(&level.price.to_le_bytes());
        data[offset + 8..offset + 16]
            .copy_from_slice(&level.size.to_le_bytes());
    }

    fn read_level(data: &[u8], index: usize) -> OrderLevel {
        let offset = ORDER_BOOK_HEADER_SIZE + index * ORDER_LEVEL_SIZE;
        OrderLevel {
            price: u64::from_le_bytes(
                data[offset..offset + 8].try_into().unwrap(),
            ),
            size: u64::from_le_bytes(
                data[offset + 8..offset + 16].try_into().unwrap(),
            ),
        }
    }

    pub fn order_book_apply(
        data: &mut [u8],
        update: &BookUpdate,
    ) -> Result<(), ProgramError> {
        let (bids_len, asks_len, capacity) =
            book_lens(data).ok_or(ProgramError::InvalidAccountData)?;
        let mut remaining = capacity - bids_len - asks_len;
        if update.bids.len() <= remaining {
            for (position, level) in update.bids.iter().enumerate() {
                write_level(data, bids_len + position, level);
            }
            let new_bids = (bids_len + update.bids.len()) as u32;
            data[0..4].copy_from_slice(&new_bids.to_le_bytes());
            remaining -= update.bids.len();
        }
        if update.asks.len() <= remaining {
            let new_asks = asks_len + update.asks.len();
            for (position, level) in update.asks.iter().rev().enumerate() {
                write_level(data, capacity - new_asks + position, level);
            }
            data[4..8].copy_from_slice(&(new_asks as u32).to_le_bytes());
        }
        Ok(())
    }

    pub fn order_book_view(
        data: &[u8],
    ) -> Option<(Vec<OrderLevel>, Vec<OrderLevel>)> {
        let (bids_len, asks_len, capacity) = book_lens(data)?;
        let bids = (0..bids_len).map(|index| read_level(data, index)).collect();
        let asks = (0..asks_len)
            .rev()
            .map(|index| read_level(data, capacity - asks_len + index))
            .collect();
        Some((bids, asks))
    }

    #[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy)]
    pub enum ScheduleCommitType {
        Commit,
        CommitAndUndelegate,
        CommitFinalize,
        CommitFinalizeAndUndelegate,
    }

    pub fn pda_and_bump(player: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[PDA_SEED, player.as_ref()], &crate::ID)
    }

    pub fn order_book_pda_and_bump(book_manager: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[ORDER_BOOK_SEED, book_manager.as_ref()],
            &crate::ID,
        )
    }

    pub mod build {
        use sdk::{
            consts::{
                DELEGATION_PROGRAM_ID, MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID,
            },
            delegate_args::{DelegateAccountMetas, DelegateAccounts},
        };
        use solana_program::instruction::{AccountMeta, Instruction};
        use solana_sdk_ids::system_program;

        use super::*;

        fn with_tag(
            instruction: &ScheduleCommitInstruction,
            metas: Vec<AccountMeta>,
        ) -> Instruction {
            let mut data = vec![crate::SCHEDULE_COMMIT_TAG];
            data.extend(
                borsh::to_vec(instruction)
                    .expect("instruction serialization cannot fail"),
            );
            Instruction {
                program_id: crate::id(),
                accounts: metas,
                data,
            }
        }

        pub fn magic_program_id() -> Pubkey {
            MAGIC_PROGRAM_ID
        }

        pub fn magic_context_id() -> Pubkey {
            MAGIC_CONTEXT_ID
        }

        // The raw magic-program ScheduleCommit (tag 1). The committees are
        // marked writable so the ER clones them. Used by the security
        // scenarios to bypass the owning program.
        pub fn direct_schedule_commit(
            payer: Pubkey,
            magic_fee_vault: Option<Pubkey>,
            committees: &[Pubkey],
        ) -> Instruction {
            let mut metas = vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(MAGIC_CONTEXT_ID, false),
            ];
            if let Some(vault) = magic_fee_vault {
                metas.push(AccountMeta::new(vault, false));
            }
            metas.extend(
                committees.iter().map(|key| AccountMeta::new(*key, false)),
            );
            Instruction {
                program_id: MAGIC_PROGRAM_ID,
                accounts: metas,
                data: 1u32.to_le_bytes().to_vec(),
            }
        }

        pub fn init_account(
            payer: Pubkey,
            player: Pubkey,
        ) -> (Instruction, Pubkey) {
            let (pda, _) = pda_and_bump(&player);
            let metas = vec![
                AccountMeta::new(payer, true),
                AccountMeta::new_readonly(player, true),
                AccountMeta::new(pda, false),
                AccountMeta::new_readonly(system_program::ID, false),
            ];
            (with_tag(&ScheduleCommitInstruction::Init, metas), pda)
        }

        pub fn delegate_cpi(
            payer: Pubkey,
            player: Pubkey,
            commit_frequency_ms: u32,
            validator: Option<Pubkey>,
        ) -> Instruction {
            let (pda, _) = pda_and_bump(&player);
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
                &ScheduleCommitInstruction::DelegateCpi(DelegateCpiArgs {
                    valid_until: 0,
                    commit_frequency_ms,
                    player,
                    validator,
                }),
                metas,
            )
        }

        fn commit_metas(
            payer: Pubkey,
            players: &[Pubkey],
            writable_committees: bool,
        ) -> Vec<AccountMeta> {
            let mut metas = vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(MAGIC_CONTEXT_ID, false),
                AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
            ];
            metas.extend(players.iter().map(|player| {
                let (pda, _) = pda_and_bump(player);
                if writable_committees {
                    AccountMeta::new(pda, false)
                } else {
                    AccountMeta::new_readonly(pda, false)
                }
            }));
            metas
        }

        pub fn schedule_commit_cpi(
            payer: Pubkey,
            players: Vec<Pubkey>,
            modify_accounts: bool,
            commit_payer: bool,
            commit_type: ScheduleCommitType,
            writable_committees: bool,
        ) -> Instruction {
            let metas = commit_metas(payer, &players, writable_committees);
            with_tag(
                &ScheduleCommitInstruction::ScheduleCommitCpi(
                    ScheduleCommitCpiArgs {
                        players,
                        modify_accounts,
                        commit_payer,
                        has_magic_vault: false,
                    },
                    commit_type,
                ),
                metas,
            )
        }

        pub fn schedule_commit_with_vault(
            payer: Pubkey,
            magic_fee_vault: Option<Pubkey>,
            players: Vec<Pubkey>,
            undelegate: bool,
        ) -> Instruction {
            let mut metas = vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(MAGIC_CONTEXT_ID, false),
                AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
            ];
            if let Some(vault) = magic_fee_vault {
                metas.push(AccountMeta::new(vault, false));
            }
            metas.extend(players.iter().map(|player| {
                let (pda, _) = pda_and_bump(player);
                AccountMeta::new(pda, false)
            }));
            with_tag(
                &ScheduleCommitInstruction::ScheduleCommitWithVaultCpi(
                    ScheduleCommitCpiWithVaultArgs {
                        has_magic_vault: magic_fee_vault.is_some(),
                        players,
                        undelegate,
                    },
                ),
                metas,
            )
        }

        pub fn schedule_commit_and_undelegate_mod_after(
            payer: Pubkey,
            players: Vec<Pubkey>,
        ) -> Instruction {
            let metas = commit_metas(payer, &players, true);
            with_tag(
                &ScheduleCommitInstruction::ScheduleCommitAndUndelegateCpiModAfter(
                    players,
                ),
                metas,
            )
        }

        pub fn schedule_commit_and_undelegate_twice(
            payer: Pubkey,
            players: Vec<Pubkey>,
        ) -> Instruction {
            let metas = commit_metas(payer, &players, true);
            with_tag(
                &ScheduleCommitInstruction::ScheduleCommitAndUndelegateCpiTwice(
                    players,
                ),
                metas,
            )
        }

        pub fn increase_count(player: Pubkey) -> Instruction {
            let (pda, _) = pda_and_bump(&player);
            with_tag(
                &ScheduleCommitInstruction::IncreaseCount,
                vec![AccountMeta::new(pda, false)],
            )
        }

        pub fn set_count(player: Pubkey, value: u64) -> Instruction {
            let (pda, _) = pda_and_bump(&player);
            with_tag(
                &ScheduleCommitInstruction::SetCount(value),
                vec![AccountMeta::new(pda, false)],
            )
        }

        pub fn init_order_book(
            payer: Pubkey,
            book_manager: Pubkey,
        ) -> (Instruction, Pubkey) {
            let (pda, _) = order_book_pda_and_bump(&book_manager);
            let metas = vec![
                AccountMeta::new(payer, true),
                AccountMeta::new_readonly(book_manager, true),
                AccountMeta::new(pda, false),
                AccountMeta::new_readonly(system_program::ID, false),
            ];
            (
                with_tag(&ScheduleCommitInstruction::InitOrderBook, metas),
                pda,
            )
        }

        pub fn grow_order_book(
            payer: Pubkey,
            book_manager: Pubkey,
            additional_space: u64,
        ) -> Instruction {
            let (pda, _) = order_book_pda_and_bump(&book_manager);
            let metas = vec![
                AccountMeta::new(payer, true),
                AccountMeta::new_readonly(book_manager, false),
                AccountMeta::new(pda, false),
                AccountMeta::new_readonly(system_program::ID, false),
            ];
            with_tag(
                &ScheduleCommitInstruction::GrowOrderBook(additional_space),
                metas,
            )
        }

        pub fn delegate_order_book(
            payer: Pubkey,
            book_manager: Pubkey,
            commit_frequency_ms: u32,
            validator: Option<Pubkey>,
        ) -> Instruction {
            let (pda, _) = order_book_pda_and_bump(&book_manager);
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
                &ScheduleCommitInstruction::DelegateOrderBook(
                    DelegateOrderBookArgs {
                        commit_frequency_ms,
                        book_manager,
                        validator,
                    },
                ),
                metas,
            )
        }

        pub fn update_order_book(
            payer: Pubkey,
            book_manager: Pubkey,
            update: BookUpdate,
        ) -> Instruction {
            let (pda, _) = order_book_pda_and_bump(&book_manager);
            let metas = vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(pda, false),
            ];
            with_tag(&ScheduleCommitInstruction::UpdateOrderBook(update), metas)
        }

        pub fn schedule_commit_for_order_book(
            payer: Pubkey,
            book_manager: Pubkey,
            commit_type: ScheduleCommitType,
        ) -> Instruction {
            let (pda, _) = order_book_pda_and_bump(&book_manager);
            let metas = vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(pda, false),
                AccountMeta::new(MAGIC_CONTEXT_ID, false),
                AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
            ];
            with_tag(
                &ScheduleCommitInstruction::ScheduleCommitForOrderBook(
                    commit_type,
                ),
                metas,
            )
        }

        pub fn schedule_commit_with_vault_and_order_book(
            payer: Pubkey,
            magic_fee_vault: Pubkey,
            book_manager: Pubkey,
            players: Vec<Pubkey>,
            with_actions: bool,
        ) -> Instruction {
            let (book, _) = order_book_pda_and_bump(&book_manager);
            let mut metas = vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(MAGIC_CONTEXT_ID, false),
                AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
                AccountMeta::new(magic_fee_vault, false),
                AccountMeta::new_readonly(book, false),
            ];
            metas.extend(players.iter().map(|player| {
                let (pda, _) = pda_and_bump(player);
                AccountMeta::new(pda, false)
            }));
            with_tag(
                &ScheduleCommitInstruction::ScheduleCommitWithVaultAndOrderBookCpi(
                    ScheduleCommitWithOrderBookArgs {
                        players,
                        with_actions,
                    },
                ),
                metas,
            )
        }

        pub fn request_undelegation(
            payer: Pubkey,
            player: Pubkey,
        ) -> Instruction {
            let (pda, _) = pda_and_bump(&player);
            let delegate_accounts = DelegateAccounts::new(pda, crate::id());
            let (request, _) = Pubkey::find_program_address(
                &[b"undelegation-request", pda.as_ref()],
                &DELEGATION_PROGRAM_ID,
            );
            let metas = vec![
                AccountMeta::new(payer, true),
                AccountMeta::new_readonly(pda, false),
                AccountMeta::new_readonly(crate::id(), false),
                AccountMeta::new(request, false),
                AccountMeta::new_readonly(
                    delegate_accounts.delegation_record,
                    false,
                ),
                AccountMeta::new(delegate_accounts.delegation_metadata, false),
                AccountMeta::new_readonly(system_program::ID, false),
                AccountMeta::new_readonly(DELEGATION_PROGRAM_ID, false),
            ];
            with_tag(
                &ScheduleCommitInstruction::RequestUndelegationCpi(player),
                metas,
            )
        }
    }
}

pub const LOG_MSG_TAG: u8 = 4;
pub const SCHEDULE_COMMIT_TAG: u8 = 5;
pub const FLEXI_TAG: u8 = 6;

pub fn log_msg_data(message: &str) -> Vec<u8> {
    let mut data = Vec::with_capacity(1 + message.len());
    data.push(LOG_MSG_TAG);
    data.extend_from_slice(message.as_bytes());
    data
}
