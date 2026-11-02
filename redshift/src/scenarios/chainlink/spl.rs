use instruction::{AccountMeta, Instruction};
use pubkey::Pubkey;
use redsuite_core::{system, ChainCtx, Result};
use sdk::{
    consts::{
        ASSOCIATED_TOKEN_PROGRAM_ID, ESPL_TOKEN_PROGRAM_ID, MAGIC_PROGRAM_ID, TOKEN_PROGRAM_ID,
    },
    spl::builders::{
        DelegateEphemeralAtaBuilder, DepositSplTokensBuilder, InitializeEphemeralAtaBuilder,
        InitializeGlobalVaultBuilder,
    },
};

pub(crate) const MINT_LEN: u64 = 82;
pub(crate) const MINT_RENT: u64 = 2_000_000;
pub(crate) const TOKEN_ACCOUNT_LEN: usize = 165;
pub(crate) const MINT_OFFSET: usize = 0;
pub(crate) const OWNER_OFFSET: usize = 32;
const TOKEN_AMOUNT_OFFSET: usize = 64;

pub(crate) fn token_program() -> Pubkey {
    TOKEN_PROGRAM_ID
}

pub(crate) fn ata_program() -> Pubkey {
    ASSOCIATED_TOKEN_PROGRAM_ID
}

pub(crate) fn eata_program() -> Pubkey {
    ESPL_TOKEN_PROGRAM_ID
}

pub fn derive_eata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[owner.as_ref(), mint.as_ref()], &eata_program()).0
}

pub fn derive_global_vault(mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[mint.as_ref()], &eata_program()).0
}

pub fn derive_ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token_program().as_ref(), mint.as_ref()],
        &ata_program(),
    )
    .0
}

/// Builds the eSPL instruction that exposes a temporary ER-local ATA overlay.
pub(crate) fn ensure_magic_ata(payer: Pubkey, owner: Pubkey, mint: Pubkey) -> Instruction {
    Instruction {
        program_id: eata_program(),
        accounts: vec![
            AccountMeta::new_readonly(payer, true),
            AccountMeta::new_readonly(owner, false),
            AccountMeta::new(derive_ata(&owner, &mint), false),
            AccountMeta::new_readonly(mint, false),
            AccountMeta::new_readonly(token_program(), false),
            AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false),
        ],
        data: vec![36],
    }
}

/// Closes the Magic ATA overlay through the native protocol, not the physical SPL token account.
pub(crate) fn close_magic_ata(owner: Pubkey, mint: Pubkey) -> Instruction {
    Instruction {
        program_id: MAGIC_PROGRAM_ID,
        accounts: vec![
            AccountMeta::new_readonly(owner, true),
            AccountMeta::new(derive_ata(&owner, &mint), false),
        ],
        data: 26u32.to_le_bytes().to_vec(),
    }
}

/// Recognizes the overlay's rent-sysvar delegate marker in its SPL-compatible account layout.
pub(crate) fn is_magic_ata(data: &[u8]) -> bool {
    data.get(129..133) == Some(1u32.to_le_bytes().as_slice())
        && data.get(133..165) == Some(sdk_ids::sysvar::rent::ID.as_ref())
}

pub fn allocate_mint(payer: &Pubkey, mint: &Pubkey) -> Instruction {
    system::create_account(payer, mint, MINT_RENT, MINT_LEN, &token_program())
}

pub fn initialize_mint(mint: &Pubkey, authority: &Pubkey) -> Instruction {
    let mut data = vec![20u8, 0u8];
    data.extend_from_slice(authority.as_ref());
    data.push(0);
    Instruction {
        program_id: token_program(),
        accounts: vec![AccountMeta::new(*mint, false)],
        data,
    }
}

pub fn create_ata_idempotent(funder: &Pubkey, owner: &Pubkey, mint: &Pubkey) -> Instruction {
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

pub fn mint_to(
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

pub(crate) async fn token_balance(ctx: &impl ChainCtx, account: &Pubkey) -> Result<Option<u64>> {
    Ok(ctx.account(account).await?.as_ref().and_then(account_balance))
}

pub fn account_balance(account: &account::Account) -> Option<u64> {
    let bytes = account.data.get(TOKEN_AMOUNT_OFFSET..)?.first_chunk()?;
    Some(u64::from_le_bytes(*bytes))
}

pub(crate) fn transfer(
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

pub(crate) fn approve(
    source: &Pubkey,
    delegate: &Pubkey,
    owner: &Pubkey,
    amount: u64,
) -> Instruction {
    let mut data = vec![4u8];
    data.extend_from_slice(&amount.to_le_bytes());
    Instruction {
        program_id: token_program(),
        accounts: vec![
            AccountMeta::new(*source, false),
            AccountMeta::new_readonly(*delegate, false),
            AccountMeta::new_readonly(*owner, true),
        ],
        data,
    }
}

pub(crate) fn initialize_global_vault(payer: &Pubkey, mint: &Pubkey) -> Instruction {
    InitializeGlobalVaultBuilder { payer: *payer, mint: *mint }.instruction()
}

pub(crate) fn initialize_eata(payer: &Pubkey, user: &Pubkey, mint: &Pubkey) -> Instruction {
    InitializeEphemeralAtaBuilder {
        payer: *payer,
        user: *user,
        mint: *mint,
    }
    .instruction()
}

pub(crate) fn deposit_spl_tokens(user: &Pubkey, mint: &Pubkey, amount: u64) -> Instruction {
    DepositSplTokensBuilder {
        authority: *user,
        user: *user,
        mint: *mint,
        amount,
    }
    .instruction()
}

pub(crate) fn withdraw_spl_tokens(user: &Pubkey, mint: &Pubkey, amount: u64) -> Instruction {
    let vault = derive_global_vault(mint);
    let mut data = vec![3u8];
    data.extend_from_slice(&amount.to_le_bytes());
    Instruction {
        program_id: eata_program(),
        accounts: vec![
            AccountMeta::new_readonly(*user, true),
            AccountMeta::new(derive_eata(user, mint), false),
            AccountMeta::new_readonly(vault, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new(derive_ata(&vault, mint), false),
            AccountMeta::new(derive_ata(user, mint), false),
            AccountMeta::new_readonly(token_program(), false),
        ],
        data,
    }
}

pub(crate) fn delegate_eata(
    payer: &Pubkey,
    user: &Pubkey,
    mint: &Pubkey,
    validator: &Pubkey,
) -> Instruction {
    DelegateEphemeralAtaBuilder {
        payer: *payer,
        user: *user,
        mint: *mint,
        validator: Some(*validator),
    }
    .instruction()
}
