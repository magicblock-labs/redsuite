//! Correctness-fixture program (counter-style state the redshift scenarios assert on).

#![allow(unexpected_cfgs)]

#[cfg(feature = "schedulecommit")]
use sdk::consts::EXTERNAL_UNDELEGATE_DISCRIMINATOR;
use solana_program::{
    account_info::AccountInfo, declare_id, entrypoint::ProgramResult, msg,
    program_error::ProgramError, pubkey::Pubkey,
};

#[cfg(not(feature = "no-entrypoint"))]
solana_program::entrypoint!(process_instruction);
declare_id!("AijneHkXJVVWyimuwfSJdrJktARZu2WiMaZBqHsq7CS5");

#[cfg(feature = "schedulecommit")]
pub mod schedulecommit {
    use borsh::{BorshDeserialize, BorshSerialize};
    use sdk::{
        cpi::{
            delegate_account, undelegate_account, DelegateAccounts,
            DelegateConfig,
        },
        ephem::{
            commit_accounts, commit_and_undelegate_accounts,
            FoldableIntentBuilder, MagicIntentBundleBuilder,
        },
        utils::create_pda,
    };
    use solana_program::{
        account_info::{next_account_info, AccountInfo},
        entrypoint::ProgramResult,
        instruction::{AccountMeta, Instruction},
        msg,
        program::{invoke, invoke_signed},
        program_error::ProgramError,
        pubkey::Pubkey,
    };

    pub const FAIL_UNDELEGATION_COUNT: u64 = u64::MAX - 1;
    pub const PDA_SEED: &[u8] = b"magic_schedule_commit";

    const MAGIC_SCHEDULE_COMMIT_TAG: u32 = 1;
    const MAGIC_SCHEDULE_COMMIT_AND_UNDELEGATE_TAG: u32 = 2;

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
    }

    #[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy)]
    pub enum ScheduleCommitType {
        Commit,
        CommitAndUndelegate,
        CommitFinalize,
        CommitFinalizeAndUndelegate,
    }

    impl ScheduleCommitType {
        fn invoke_commit<'a, 'info>(
            self,
            payer: &'a AccountInfo<'info>,
            committees: Vec<&'a AccountInfo<'info>>,
            magic_context: &'a AccountInfo<'info>,
            magic_program: &'a AccountInfo<'info>,
            magic_fee_vault: Option<&'a AccountInfo<'info>>,
        ) -> ProgramResult {
            match self {
                ScheduleCommitType::Commit => invoke_via_builder(
                    payer,
                    committees,
                    magic_context,
                    magic_program,
                    magic_fee_vault,
                    false,
                ),
                ScheduleCommitType::CommitAndUndelegate => invoke_via_builder(
                    payer,
                    committees,
                    magic_context,
                    magic_program,
                    magic_fee_vault,
                    true,
                ),
                ScheduleCommitType::CommitFinalize => {
                    invoke_schedule_commit_raw(
                        payer,
                        committees,
                        magic_context,
                        magic_program,
                        magic_fee_vault,
                    )
                }
                ScheduleCommitType::CommitFinalizeAndUndelegate => {
                    commit_and_undelegate_accounts(
                        payer,
                        committees,
                        magic_context,
                        magic_program,
                        magic_fee_vault,
                    )
                }
            }
        }
    }

    fn invoke_via_builder<'a, 'info>(
        payer: &'a AccountInfo<'info>,
        committees: Vec<&'a AccountInfo<'info>>,
        magic_context: &'a AccountInfo<'info>,
        magic_program: &'a AccountInfo<'info>,
        magic_fee_vault: Option<&'a AccountInfo<'info>>,
        undelegate: bool,
    ) -> ProgramResult {
        let builder = MagicIntentBundleBuilder::new(
            payer.clone(),
            magic_context.clone(),
            magic_program.clone(),
        );
        let builder = match magic_fee_vault {
            Some(vault) => builder.magic_fee_vault(vault.clone()),
            None => builder,
        };
        let owned: Vec<_> = committees.into_iter().cloned().collect();
        if undelegate {
            builder.commit_and_undelegate(&owned).build_and_invoke()
        } else {
            builder.commit(&owned).build_and_invoke()
        }
    }

    fn invoke_schedule_commit_raw<'a, 'info>(
        payer: &'a AccountInfo<'info>,
        committees: Vec<&'a AccountInfo<'info>>,
        magic_context: &'a AccountInfo<'info>,
        magic_program: &'a AccountInfo<'info>,
        magic_fee_vault: Option<&'a AccountInfo<'info>>,
    ) -> ProgramResult {
        let mut metas = vec![
            AccountMeta::new(*payer.key, true),
            AccountMeta::new(*magic_context.key, false),
        ];
        if let Some(vault) = magic_fee_vault {
            metas.push(AccountMeta::new(*vault.key, false));
        }
        metas.extend(committees.iter().map(|committee| AccountMeta {
            pubkey: *committee.key,
            is_signer: committee.is_signer,
            is_writable: committee.is_writable,
        }));

        let instruction = Instruction::new_with_bytes(
            *magic_program.key,
            &MAGIC_SCHEDULE_COMMIT_TAG.to_le_bytes(),
            metas,
        );

        let mut infos = vec![payer.clone(), magic_context.clone()];
        if let Some(vault) = magic_fee_vault {
            infos.push(vault.clone());
        }
        infos.extend(committees.into_iter().cloned());

        invoke(&instruction, &infos)
    }

    pub fn process(
        program_id: &Pubkey,
        accounts: &[AccountInfo],
        payload: &[u8],
    ) -> ProgramResult {
        let instruction = ScheduleCommitInstruction::try_from_slice(payload)
            .map_err(|err| {
                msg!("cannot parse the schedulecommit instruction: {}", err);
                ProgramError::InvalidInstructionData
            })?;

        use ScheduleCommitInstruction::*;
        match instruction {
            Init => process_init(program_id, accounts),
            DelegateCpi(args) => process_delegate_cpi(accounts, args),
            ScheduleCommitCpi(args, commit_type) => {
                process_schedulecommit_cpi(accounts, args, commit_type)
            }
            ScheduleCommitWithVaultCpi(args) => {
                process_schedulecommit_with_vault_cpi(accounts, args)
            }
            ScheduleCommitAndUndelegateCpiModAfter(players) => {
                process_commit_undelegate_mod_after(accounts, &players)
            }
            ScheduleCommitAndUndelegateCpiTwice(players) => {
                process_commit_undelegate_twice(accounts, &players)
            }
            IncreaseCount => process_increase_count(accounts),
            SetCount(value) => process_set_count(accounts, value),
        }
    }

    fn process_init(
        program_id: &Pubkey,
        accounts: &[AccountInfo],
    ) -> ProgramResult {
        let [payer, player, pda, system_program] = accounts else {
            return Err(ProgramError::NotEnoughAccountKeys);
        };
        if !player.is_signer {
            return Err(ProgramError::MissingRequiredSignature);
        }

        let (expected_pda, bump) = pda_and_bump(player.key);
        if pda.key != &expected_pda {
            msg!(
                "the PDA {} is not correct for player {}",
                pda.key,
                player.key
            );
            return Err(ProgramError::InvalidSeeds);
        }

        let bump_slice = [bump];
        let seeds: [&[u8]; 3] = [PDA_SEED, player.key.as_ref(), &bump_slice];
        create_pda(
            pda,
            program_id,
            MainAccount::SIZE,
            &[&seeds],
            system_program,
            payer,
            true,
        )?;

        let account = MainAccount {
            player: *player.key,
            count: 0,
        };
        account.serialize(&mut &mut pda.try_borrow_mut_data()?.as_mut())?;
        Ok(())
    }

    fn process_delegate_cpi(
        accounts: &[AccountInfo],
        args: DelegateCpiArgs,
    ) -> ProgramResult {
        let [payer, pda, owner_program, buffer, delegation_record, delegation_metadata, delegation_program, system_program] =
            accounts
        else {
            return Err(ProgramError::NotEnoughAccountKeys);
        };

        let seeds: [&[u8]; 2] = [PDA_SEED, args.player.as_ref()];
        delegate_account(
            DelegateAccounts {
                payer,
                pda,
                owner_program,
                buffer,
                delegation_record,
                delegation_metadata,
                delegation_program,
                system_program,
            },
            &seeds,
            DelegateConfig {
                commit_frequency_ms: args.commit_frequency_ms,
                validator: args.validator,
            },
        )?;
        Ok(())
    }

    fn increase_committee_counts(committees: &[AccountInfo]) -> ProgramResult {
        for committee in committees {
            let mut account = {
                let data = committee.try_borrow_data()?;
                MainAccount::try_from_slice(&data)?
            };
            account.count += 1;
            account.serialize(
                &mut &mut committee.try_borrow_mut_data()?.as_mut(),
            )?;
        }
        Ok(())
    }

    fn process_schedulecommit_cpi(
        accounts: &[AccountInfo],
        args: ScheduleCommitCpiArgs,
        commit_type: ScheduleCommitType,
    ) -> ProgramResult {
        let iter = &mut accounts.iter();
        let payer = next_account_info(iter)?;
        let magic_context = next_account_info(iter)?;
        let magic_program = next_account_info(iter)?;
        let magic_fee_vault = if args.has_magic_vault {
            Some(next_account_info(iter)?)
        } else {
            None
        };
        let remaining: Vec<_> = iter.cloned().collect();

        if remaining.len() != args.players.len() {
            msg!(
                "players {} != committees {}",
                args.players.len(),
                remaining.len()
            );
            return Err(ProgramError::InvalidArgument);
        }

        if args.modify_accounts {
            increase_committee_counts(&remaining)?;
        }

        let mut committees: Vec<_> = remaining.iter().collect();
        if args.commit_payer {
            committees.push(payer);
        }

        commit_type.invoke_commit(
            payer,
            committees,
            magic_context,
            magic_program,
            magic_fee_vault,
        )
    }

    fn process_schedulecommit_with_vault_cpi(
        accounts: &[AccountInfo],
        args: ScheduleCommitCpiWithVaultArgs,
    ) -> ProgramResult {
        let iter = &mut accounts.iter();
        let payer = next_account_info(iter)?;
        let magic_context = next_account_info(iter)?;
        let magic_program = next_account_info(iter)?;
        let magic_fee_vault = if args.has_magic_vault {
            Some(next_account_info(iter)?)
        } else {
            None
        };
        let remaining: Vec<_> = iter.cloned().collect();

        if remaining.len() != args.players.len() {
            msg!(
                "players {} != committees {}",
                args.players.len(),
                remaining.len()
            );
            return Err(ProgramError::InvalidArgument);
        }

        let committees: Vec<_> = remaining.iter().collect();
        if args.undelegate {
            commit_and_undelegate_accounts(
                payer,
                committees,
                magic_context,
                magic_program,
                magic_fee_vault,
            )
        } else {
            commit_accounts(
                payer,
                committees,
                magic_context,
                magic_program,
                magic_fee_vault,
            )
        }
    }

    fn process_commit_undelegate_mod_after(
        accounts: &[AccountInfo],
        players: &[Pubkey],
    ) -> ProgramResult {
        let iter = &mut accounts.iter();
        let payer = next_account_info(iter)?;
        let magic_context = next_account_info(iter)?;
        let magic_program = next_account_info(iter)?;
        let remaining: Vec<_> = iter.cloned().collect();

        if remaining.len() != players.len() {
            return Err(ProgramError::InvalidArgument);
        }

        commit_and_undelegate_accounts(
            payer,
            remaining.iter().collect(),
            magic_context,
            magic_program,
            None,
        )?;

        increase_committee_counts(&remaining)
    }

    fn process_commit_undelegate_twice(
        accounts: &[AccountInfo],
        players: &[Pubkey],
    ) -> ProgramResult {
        let iter = &mut accounts.iter();
        let payer = next_account_info(iter)?;
        let magic_context = next_account_info(iter)?;
        let magic_program = next_account_info(iter)?;
        let remaining: Vec<_> = iter.cloned().collect();

        if remaining.len() != players.len() {
            return Err(ProgramError::InvalidArgument);
        }

        commit_and_undelegate_accounts(
            payer,
            remaining.iter().collect(),
            magic_context,
            magic_program,
            None,
        )?;

        let mut metas = vec![
            AccountMeta::new(*payer.key, true),
            AccountMeta::new(*magic_context.key, false),
        ];
        metas.extend(remaining.iter().map(|committee| AccountMeta {
            pubkey: *committee.key,
            is_signer: true,
            is_writable: committee.is_writable,
        }));
        let instruction = Instruction::new_with_bytes(
            *magic_program.key,
            &MAGIC_SCHEDULE_COMMIT_AND_UNDELEGATE_TAG.to_le_bytes(),
            metas,
        );

        let mut infos = vec![payer.clone(), magic_context.clone()];
        infos.extend(remaining.iter().cloned());

        let all_seeds: Vec<(Vec<u8>, [u8; 1])> = players
            .iter()
            .map(|player| {
                let (_, bump) = pda_and_bump(player);
                (player.as_ref().to_vec(), [bump])
            })
            .collect();
        let seed_slices: Vec<[&[u8]; 3]> = all_seeds
            .iter()
            .map(|(player_bytes, bump)| {
                [PDA_SEED, player_bytes.as_slice(), bump.as_slice()]
            })
            .collect();
        let signer_seeds: Vec<&[&[u8]]> =
            seed_slices.iter().map(|seeds| seeds.as_slice()).collect();

        invoke_signed(&instruction, &infos, &signer_seeds)
    }

    fn process_increase_count(accounts: &[AccountInfo]) -> ProgramResult {
        let iter = &mut accounts.iter();
        let account = next_account_info(iter)?;
        let mut main_account = {
            let data = account.try_borrow_data()?;
            MainAccount::try_from_slice(&data)?
        };
        main_account.count += 1;
        main_account
            .serialize(&mut &mut account.try_borrow_mut_data()?.as_mut())?;
        Ok(())
    }

    fn process_set_count(
        accounts: &[AccountInfo],
        value: u64,
    ) -> ProgramResult {
        let iter = &mut accounts.iter();
        let account = next_account_info(iter)?;
        let mut main_account = {
            let data = account.try_borrow_data()?;
            MainAccount::try_from_slice(&data)?
        };
        main_account.count = value;
        main_account
            .serialize(&mut &mut account.try_borrow_mut_data()?.as_mut())?;
        Ok(())
    }

    pub fn process_undelegate_request(
        program_id: &Pubkey,
        accounts: &[AccountInfo],
        seeds_data: &[u8],
    ) -> ProgramResult {
        let iter = &mut accounts.iter();
        let delegated_account = next_account_info(iter)?;
        let buffer = next_account_info(iter)?;
        let payer = next_account_info(iter)?;
        let system_program = next_account_info(iter)?;

        let account_seeds = <Vec<Vec<u8>>>::try_from_slice(seeds_data)
            .map_err(|err| {
                msg!("cannot parse the undelegation seeds: {}", err);
                ProgramError::InvalidArgument
            })?;

        undelegate_account(
            delegated_account,
            program_id,
            buffer,
            payer,
            system_program,
            account_seeds,
        )?;

        let data = delegated_account.try_borrow_data()?;
        if data.len() == MainAccount::SIZE {
            if let Ok(counter) = MainAccount::try_from_slice(&data) {
                if counter.count == FAIL_UNDELEGATION_COUNT {
                    return Err(ProgramError::Custom(111));
                }
            }
        }
        Ok(())
    }

    pub fn pda_and_bump(player: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[PDA_SEED, player.as_ref()], &crate::ID)
    }

    pub mod build {
        use sdk::consts::{MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID};
        use sdk::delegate_args::{DelegateAccountMetas, DelegateAccounts};
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
    }
}

const LOG_MSG_TAG: u8 = 4;
pub const SCHEDULE_COMMIT_TAG: u8 = 5;

#[cfg(feature = "upgraded")]
const LOG_MSG_SUFFIX: &str = " upgraded";
#[cfg(not(feature = "upgraded"))]
const LOG_MSG_SUFFIX: &str = "";

#[cfg_attr(not(feature = "schedulecommit"), allow(unused_variables))]
pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    #[cfg(feature = "schedulecommit")]
    if instruction_data.len() >= EXTERNAL_UNDELEGATE_DISCRIMINATOR.len() {
        let (discriminator, seeds_data) =
            instruction_data.split_at(EXTERNAL_UNDELEGATE_DISCRIMINATOR.len());
        if discriminator == EXTERNAL_UNDELEGATE_DISCRIMINATOR {
            return schedulecommit::process_undelegate_request(
                program_id, accounts, seeds_data,
            );
        }
    }

    match instruction_data.split_first() {
        Some((&LOG_MSG_TAG, message)) => {
            let text = core::str::from_utf8(message)
                .map_err(|_| ProgramError::InvalidInstructionData)?;
            msg!("LogMsg: {}{}", text, LOG_MSG_SUFFIX);
            Ok(())
        }
        #[cfg(feature = "schedulecommit")]
        Some((&SCHEDULE_COMMIT_TAG, payload)) => {
            schedulecommit::process(program_id, accounts, payload)
        }
        _ => Ok(()),
    }
}

pub fn log_msg_data(message: &str) -> Vec<u8> {
    let mut data = Vec::with_capacity(1 + message.len());
    data.push(LOG_MSG_TAG);
    data.extend_from_slice(message.as_bytes());
    data
}
