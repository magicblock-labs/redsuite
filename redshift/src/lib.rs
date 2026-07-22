//! CORRECTNESS family — observed state vs expected model; shared helpers for its tests live here.

pub mod scenarios;

use keypair::Keypair;
use pubkey::Pubkey;
pub use redline_program as program;
use redsuite_core::{ChainCtx, Result};
use signer::Signer;

pub const ACCOUNT_SPACE: u32 = 128;
pub const PAYER_LAMPORTS: u64 = 2_000_000_000;

pub fn written_id(data: &[u8]) -> Option<u64> {
    use program::layout::{ID_OFFSET, ID_SIZE};
    let bytes = data.get(ID_OFFSET..ID_OFFSET + ID_SIZE)?;
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}

pub async fn init_account(
    base: &impl ChainCtx,
    payer: &Keypair,
    seed: u8,
    authority: Pubkey,
) -> Result<Pubkey> {
    let (init, pda) = program::instruction::build::init_account(
        payer.pubkey(),
        payer.pubkey(),
        ACCOUNT_SPACE,
        seed,
        authority,
    );
    base.send(payer, &[init]).await?;
    Ok(pda)
}

pub async fn init_delegated_account(
    base: &impl ChainCtx,
    payer: &Keypair,
    seed: u8,
    authority: Pubkey,
) -> Result<Pubkey> {
    init_delegated_account_at(base, program::id(), payer, seed, authority).await
}

pub async fn init_delegated_account_at(
    base: &impl ChainCtx,
    program_id: Pubkey,
    payer: &Keypair,
    seed: u8,
    authority: Pubkey,
) -> Result<Pubkey> {
    let (init, pda) = program::instruction::build::init_account_at(
        program_id,
        payer.pubkey(),
        payer.pubkey(),
        ACCOUNT_SPACE,
        seed,
        authority,
    );
    let delegate = program::instruction::build::delegate_at(
        program_id,
        payer.pubkey(),
        pda,
        payer.pubkey(),
        seed,
        authority,
    );
    base.send(payer, &[init, delegate]).await?;
    Ok(pda)
}
