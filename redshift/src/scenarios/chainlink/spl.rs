use instruction::{AccountMeta, Instruction};
use pubkey::Pubkey;
use redsuite_core::{system, ChainCtx, Result};

pub(super) const MINT_LEN: u64 = 82;
pub(super) const MINT_RENT: u64 = 2_000_000;
const TOKEN_AMOUNT_OFFSET: usize = 64;

pub(super) fn token_program() -> Pubkey {
    sdk::consts::TOKEN_PROGRAM_ID
}

pub(super) fn ata_program() -> Pubkey {
    sdk::consts::ASSOCIATED_TOKEN_PROGRAM_ID
}

pub(super) fn derive_ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token_program().as_ref(), mint.as_ref()],
        &ata_program(),
    )
    .0
}

pub(super) fn initialize_mint(
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

pub(super) fn create_ata_idempotent(
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

pub(super) fn mint_to(
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

pub(super) async fn token_balance(
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
