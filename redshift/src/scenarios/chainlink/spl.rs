use instruction::{AccountMeta, Instruction};
use pubkey::Pubkey;
use redsuite_core::{dlp, system, ChainCtx, Result};

pub(crate) const MINT_LEN: u64 = 82;
pub(crate) const MINT_RENT: u64 = 2_000_000;
pub(crate) const TOKEN_ACCOUNT_LEN: usize = 165;
pub(crate) const MINT_OFFSET: usize = 0;
pub(crate) const OWNER_OFFSET: usize = 32;
const TOKEN_AMOUNT_OFFSET: usize = 64;

pub(crate) fn token_program() -> Pubkey {
    sdk::consts::TOKEN_PROGRAM_ID
}

pub(crate) fn ata_program() -> Pubkey {
    sdk::consts::ASSOCIATED_TOKEN_PROGRAM_ID
}

pub(crate) fn eata_program() -> Pubkey {
    sdk::consts::ESPL_TOKEN_PROGRAM_ID
}

pub(crate) fn derive_eata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), mint.as_ref()],
        &eata_program(),
    )
    .0
}

pub(crate) fn derive_global_vault(mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[mint.as_ref()], &eata_program()).0
}

pub(crate) fn derive_ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token_program().as_ref(), mint.as_ref()],
        &ata_program(),
    )
    .0
}

pub(crate) fn initialize_mint(
    mint: &Pubkey,
    authority: &Pubkey,
) -> Instruction {
    let mut data = vec![20u8, 0u8];
    data.extend_from_slice(authority.as_ref());
    data.push(0);
    Instruction {
        program_id: token_program(),
        accounts: vec![AccountMeta::new(*mint, false)],
        data,
    }
}

pub(crate) fn create_ata_idempotent(
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

pub(crate) fn mint_to(
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

pub(crate) async fn token_balance(
    ctx: &impl ChainCtx,
    account: &Pubkey,
) -> Result<Option<u64>> {
    let Some(account) = ctx.account(account).await? else {
        return Ok(None);
    };
    let Some(bytes) = account
        .data
        .get(TOKEN_AMOUNT_OFFSET..TOKEN_AMOUNT_OFFSET + 8)
    else {
        return Ok(None);
    };
    Ok(Some(u64::from_le_bytes(bytes.try_into()?)))
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

pub(crate) fn initialize_global_vault(
    payer: &Pubkey,
    mint: &Pubkey,
) -> Instruction {
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

pub(crate) fn initialize_eata(
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

pub(crate) fn deposit_spl_tokens(
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

pub(crate) fn delegate_eata(
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
                dlp::delegate_buffer_pda(&eata, &eata_program()),
                false,
            ),
            AccountMeta::new(dlp::delegation_record_pda(&eata), false),
            AccountMeta::new(dlp::delegation_metadata_pda(&eata), false),
            AccountMeta::new_readonly(dlp::dlp_id(), false),
            AccountMeta::new_readonly(system::system_id(), false),
        ],
        data,
    }
}
