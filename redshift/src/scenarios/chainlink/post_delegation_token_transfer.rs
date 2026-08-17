use std::time::Duration;

use async_trait::async_trait;
use dlp_api::{
    args::DelegateArgs,
    instruction_builder::{delegate_with_actions, Encryptable},
    pda::{
        delegate_buffer_pda_from_delegated_account_and_owner_program,
        delegation_metadata_pda_from_delegated_account,
        delegation_record_pda_from_delegated_account,
    },
};
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, dlp, prep, system, BaseCtx, ChainCtx, ErCtx, Result,
    Scenario, ScenarioReport,
};
use signer::Signer;

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ATA_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const EATA_PROGRAM: &str = "SPLxh1LVZzEkX99H6rqYizhytLWPZVV296zyYDPagv2";

const MINT_LEN: u64 = 82;
const MINT_RENT: u64 = 2_000_000;
const AIRDROP: u64 = 2_000_000_000;
const SOURCE_BALANCE: u64 = 200;
const DESTINATION_BALANCE: u64 = 100;
const TRANSFER_AMOUNT: u64 = 100;
const PLAIN_BALANCE: u64 = 70;
const FOREIGN_BALANCE: u64 = 40;
const FAILING_AMOUNT: u64 = 1_000_000;
const TOKEN_AMOUNT_OFFSET: usize = 64;
const PROJECTION_TIMEOUT: Duration = Duration::from_secs(20);
const ACTION_TIMEOUT: Duration = Duration::from_secs(30);

pub struct PostDelegationTokenTransfer;

#[async_trait(?Send)]
impl Scenario for PostDelegationTokenTransfer {
    fn name(&self) -> &str {
        "redshift/post_delegation_token_transfer"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let fee_payer = prep::funded_payer(base, AIRDROP).await?;
        let delegated_account = Keypair::new();
        let source_authority = Keypair::new();
        let destination_authority = Keypair::new();
        let mint = Keypair::new();
        base.airdrop(&delegated_account.pubkey(), AIRDROP).await?;
        base.airdrop(&source_authority.pubkey(), AIRDROP).await?;

        let mint_key = mint.pubkey();
        let source = source_authority.pubkey();
        let destination = destination_authority.pubkey();
        let source_ata = derive_ata(&source, &mint_key);
        let destination_ata = derive_ata(&destination, &mint_key);

        base.send_with(
            &fee_payer,
            &[&mint, &source_authority],
            &[
                system::create_account(
                    &fee_payer.pubkey(),
                    &mint_key,
                    MINT_RENT,
                    MINT_LEN,
                    &token_program(),
                ),
                initialize_mint(&mint_key, &source),
                create_ata_idempotent(&fee_payer.pubkey(), &source, &mint_key),
                create_ata_idempotent(
                    &fee_payer.pubkey(),
                    &destination,
                    &mint_key,
                ),
                mint_to(&mint_key, &source_ata, &source, SOURCE_BALANCE),
                mint_to(
                    &mint_key,
                    &destination_ata,
                    &source,
                    DESTINATION_BALANCE,
                ),
            ],
        )
        .await?;

        base.send(
            &fee_payer,
            &[
                initialize_global_vault(&fee_payer.pubkey(), &mint_key),
                initialize_eata(&fee_payer.pubkey(), &source, &mint_key),
                initialize_eata(&fee_payer.pubkey(), &destination, &mint_key),
            ],
        )
        .await?;

        base.send_with(
            &fee_payer,
            &[&source_authority, &destination_authority],
            &[
                deposit_spl_tokens(&source, &mint_key, SOURCE_BALANCE),
                deposit_spl_tokens(
                    &destination,
                    &mint_key,
                    DESTINATION_BALANCE,
                ),
            ],
        )
        .await?;

        base.send(
            &fee_payer,
            &[
                delegate_eata(&fee_payer.pubkey(), &source, &mint_key, er),
                delegate_eata(&fee_payer.pubkey(), &destination, &mint_key, er),
            ],
        )
        .await?;

        check::poll(
            "the er projects the deposited eATA balances onto both ATAs",
            PROJECTION_TIMEOUT,
            || async {
                token_balance(er, &source_ata).await == Some(SOURCE_BALANCE)
                    && token_balance(er, &destination_ata).await
                        == Some(DESTINATION_BALANCE)
            },
        )
        .await?;
        check_eq!(
            token_balance(base, &source_ata).await,
            Some(0),
            "the source ATA on chain must be drained into the vault"
        )?;
        check_eq!(
            token_balance(base, &destination_ata).await,
            Some(0),
            "the destination ATA on chain must be drained into the vault"
        )?;

        let transfer_action = spl_transfer(
            &source_ata,
            &destination_ata,
            &source,
            TRANSFER_AMOUNT,
        );
        let delegate_ix = delegate_with_actions(
            fee_payer.pubkey(),
            delegated_account.pubkey(),
            None,
            DelegateArgs {
                commit_frequency_ms: u32::MAX,
                seeds: vec![],
                validator: Some(er.identity()),
            },
            vec![transfer_action.cleartext()],
        );

        base.send_with(
            &fee_payer,
            &[&delegated_account],
            &[system::assign(&delegated_account.pubkey(), &dlp::dlp_id())],
        )
        .await?;
        base.send_with(
            &fee_payer,
            &[&delegated_account, &source_authority],
            &[delegate_ix],
        )
        .await?;

        check::poll(
            "the post-delegation transfer action moves the tokens on the er",
            ACTION_TIMEOUT,
            || async {
                token_balance(er, &source_ata).await
                    == Some(SOURCE_BALANCE - TRANSFER_AMOUNT)
                    && token_balance(er, &destination_ata).await
                        == Some(DESTINATION_BALANCE + TRANSFER_AMOUNT)
            },
        )
        .await?;

        // negative: an ATA with NO eATA must clone as the plain chain
        // account, never a projection.
        let plain_owner = Keypair::new().pubkey();
        let plain_ata = derive_ata(&plain_owner, &mint_key);
        base.send_with(
            &fee_payer,
            &[&source_authority],
            &[
                create_ata_idempotent(
                    &fee_payer.pubkey(),
                    &plain_owner,
                    &mint_key,
                ),
                mint_to(&mint_key, &plain_ata, &source, PLAIN_BALANCE),
            ],
        )
        .await?;
        check::poll(
            "the er clones the eATA-less plain ATA",
            PROJECTION_TIMEOUT,
            || async { token_balance(er, &plain_ata).await.is_some() },
        )
        .await?;
        check_eq!(
            token_balance(er, &plain_ata).await,
            Some(PLAIN_BALANCE),
            "an ATA without an eATA must present its chain balance on the er"
        )?;

        // negative: an eATA delegated to a FOREIGN validator must not be
        // substituted — the er presents the drained chain ATA, not the eATA.
        let foreign_validator = Keypair::new().pubkey();
        let foreign_authority = Keypair::new();
        let foreign = foreign_authority.pubkey();
        let foreign_ata = derive_ata(&foreign, &mint_key);
        base.send_with(
            &fee_payer,
            &[&source_authority],
            &[
                create_ata_idempotent(&fee_payer.pubkey(), &foreign, &mint_key),
                mint_to(&mint_key, &foreign_ata, &source, FOREIGN_BALANCE),
                initialize_eata(&fee_payer.pubkey(), &foreign, &mint_key),
            ],
        )
        .await?;
        base.send_with(
            &fee_payer,
            &[&foreign_authority],
            &[deposit_spl_tokens(&foreign, &mint_key, FOREIGN_BALANCE)],
        )
        .await?;
        base.send(
            &fee_payer,
            &[delegate_eata_to(
                &fee_payer.pubkey(),
                &foreign,
                &mint_key,
                &foreign_validator,
            )],
        )
        .await?;
        check_eq!(
            token_balance(base, &foreign_ata).await,
            Some(0),
            "the foreign user's chain ATA must be drained into the vault"
        )?;
        check::poll(
            "the er clones the foreign-delegated ATA",
            PROJECTION_TIMEOUT,
            || async { token_balance(er, &foreign_ata).await.is_some() },
        )
        .await?;
        check_eq!(
            token_balance(er, &foreign_ata).await,
            Some(0),
            "an eATA delegated to a foreign validator must not substitute — \
             the er must present the drained chain ATA"
        )?;

        // failing post-delegation action — an action that cannot execute
        // must route the freshly delegated account into scheduled
        // undelegation (ownership returns to system on base).
        let failing_account = Keypair::new();
        base.airdrop(&failing_account.pubkey(), AIRDROP).await?;
        let failing_action = spl_transfer(
            &source_ata,
            &destination_ata,
            &source,
            FAILING_AMOUNT,
        );
        let failing_delegate = delegate_with_actions(
            fee_payer.pubkey(),
            failing_account.pubkey(),
            None,
            DelegateArgs {
                commit_frequency_ms: u32::MAX,
                seeds: vec![],
                validator: Some(er.identity()),
            },
            vec![failing_action.cleartext()],
        );
        base.send_with(
            &fee_payer,
            &[&failing_account],
            &[system::assign(&failing_account.pubkey(), &dlp::dlp_id())],
        )
        .await?;
        base.send_with(
            &fee_payer,
            &[&failing_account, &source_authority],
            &[failing_delegate],
        )
        .await?;
        check::poll(
            "the failing action returns base ownership to the system program",
            ACTION_TIMEOUT,
            || async {
                matches!(
                    base.account(&failing_account.pubkey()).await,
                    Ok(Some(account)) if account.owner == system::system_id()
                )
            },
        )
        .await?;
        let failing_on_base = base
            .account(&failing_account.pubkey())
            .await?
            .ok_or("the failing-action account vanished on base")?;
        check_eq!(
            failing_on_base.owner,
            system::system_id(),
            "a failing post-delegation action must undelegate the account \
             back to its system owner"
        )?;
        check_eq!(
            token_balance(er, &source_ata).await,
            Some(SOURCE_BALANCE - TRANSFER_AMOUNT),
            "the failing action must not move tokens"
        )?;

        Ok(ScenarioReport::ok(self.name())
            .setting("mint decimals", 0u64)
            .setting("transfer amount", TRANSFER_AMOUNT)
            .setting(
                "source after",
                token_balance(er, &source_ata).await.unwrap_or_default(),
            )
            .setting(
                "destination after",
                token_balance(er, &destination_ata)
                    .await
                    .unwrap_or_default(),
            )
            .setting("plain ata on er", PLAIN_BALANCE)
            .setting("foreign-delegated ata on er", 0u64))
    }
}

fn token_program() -> Pubkey {
    TOKEN_PROGRAM.parse().expect("token program id")
}

fn ata_program() -> Pubkey {
    ATA_PROGRAM.parse().expect("ata program id")
}

fn eata_program() -> Pubkey {
    EATA_PROGRAM.parse().expect("eata program id")
}

fn derive_ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token_program().as_ref(), mint.as_ref()],
        &ata_program(),
    )
    .0
}

fn derive_eata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), mint.as_ref()],
        &eata_program(),
    )
    .0
}

fn derive_global_vault(mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[mint.as_ref()], &eata_program()).0
}

fn initialize_mint(mint: &Pubkey, authority: &Pubkey) -> Instruction {
    let mut data = vec![20u8, 0u8];
    data.extend_from_slice(authority.as_ref());
    data.push(0);
    Instruction {
        program_id: token_program(),
        accounts: vec![AccountMeta::new(*mint, false)],
        data,
    }
}

fn mint_to(
    mint: &Pubkey,
    destination: &Pubkey,
    authority: &Pubkey,
    amount: u64,
) -> Instruction {
    let mut data = vec![7u8];
    data.extend_from_slice(&amount.to_le_bytes());
    Instruction {
        program_id: token_program(),
        accounts: vec![
            AccountMeta::new(*mint, false),
            AccountMeta::new(*destination, false),
            AccountMeta::new_readonly(*authority, true),
        ],
        data,
    }
}

fn spl_transfer(
    source: &Pubkey,
    destination: &Pubkey,
    authority: &Pubkey,
    amount: u64,
) -> Instruction {
    let mut data = vec![3u8];
    data.extend_from_slice(&amount.to_le_bytes());
    Instruction {
        program_id: token_program(),
        accounts: vec![
            AccountMeta::new(*source, false),
            AccountMeta::new(*destination, false),
            AccountMeta::new_readonly(*authority, true),
        ],
        data,
    }
}

fn create_ata_idempotent(
    funder: &Pubkey,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Instruction {
    Instruction {
        program_id: ata_program(),
        accounts: vec![
            AccountMeta::new(*funder, true),
            AccountMeta::new(derive_ata(owner, mint), false),
            AccountMeta::new_readonly(*owner, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new_readonly(system::system_id(), false),
            AccountMeta::new_readonly(token_program(), false),
        ],
        data: vec![1],
    }
}

fn initialize_global_vault(payer: &Pubkey, mint: &Pubkey) -> Instruction {
    let vault = derive_global_vault(mint);
    Instruction {
        program_id: eata_program(),
        accounts: vec![
            AccountMeta::new(vault, false),
            AccountMeta::new(*payer, true),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new(derive_eata(&vault, mint), false),
            AccountMeta::new(derive_ata(&vault, mint), false),
            AccountMeta::new_readonly(token_program(), false),
            AccountMeta::new_readonly(ata_program(), false),
            AccountMeta::new_readonly(system::system_id(), false),
        ],
        data: vec![1],
    }
}

fn initialize_eata(
    payer: &Pubkey,
    user: &Pubkey,
    mint: &Pubkey,
) -> Instruction {
    Instruction {
        program_id: eata_program(),
        accounts: vec![
            AccountMeta::new(derive_eata(user, mint), false),
            AccountMeta::new(*payer, true),
            AccountMeta::new_readonly(*user, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new_readonly(system::system_id(), false),
        ],
        data: vec![0],
    }
}

fn deposit_spl_tokens(
    user: &Pubkey,
    mint: &Pubkey,
    amount: u64,
) -> Instruction {
    let vault = derive_global_vault(mint);
    let mut data = vec![2u8];
    data.extend_from_slice(&amount.to_le_bytes());
    Instruction {
        program_id: eata_program(),
        accounts: vec![
            AccountMeta::new(derive_eata(user, mint), false),
            AccountMeta::new_readonly(vault, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new(derive_ata(user, mint), false),
            AccountMeta::new(derive_ata(&vault, mint), false),
            AccountMeta::new_readonly(*user, true),
            AccountMeta::new_readonly(token_program(), false),
        ],
        data,
    }
}

fn delegate_eata(
    payer: &Pubkey,
    user: &Pubkey,
    mint: &Pubkey,
    er: &ErCtx,
) -> Instruction {
    delegate_eata_to(payer, user, mint, &er.identity())
}

fn delegate_eata_to(
    payer: &Pubkey,
    user: &Pubkey,
    mint: &Pubkey,
    validator: &Pubkey,
) -> Instruction {
    let eata = derive_eata(user, mint);
    let mut data = vec![4u8];
    data.extend_from_slice(validator.as_ref());
    Instruction {
        program_id: eata_program(),
        accounts: vec![
            AccountMeta::new(*payer, true),
            AccountMeta::new(eata, false),
            AccountMeta::new_readonly(eata_program(), false),
            AccountMeta::new(
                delegate_buffer_pda_from_delegated_account_and_owner_program(
                    &eata,
                    &eata_program(),
                ),
                false,
            ),
            AccountMeta::new(
                delegation_record_pda_from_delegated_account(&eata),
                false,
            ),
            AccountMeta::new(
                delegation_metadata_pda_from_delegated_account(&eata),
                false,
            ),
            AccountMeta::new_readonly(dlp::dlp_id(), false),
            AccountMeta::new_readonly(system::system_id(), false),
        ],
        data,
    }
}

async fn token_balance(ctx: &impl ChainCtx, account: &Pubkey) -> Option<u64> {
    let account = ctx.account(account).await.ok()??;
    let bytes = account
        .data
        .get(TOKEN_AMOUNT_OFFSET..TOKEN_AMOUNT_OFFSET + 8)?;
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}
