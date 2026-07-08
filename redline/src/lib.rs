//! PERFORMANCE family — load scenarios; shared helpers for its tests live here.

use keypair::Keypair;
use pubkey::Pubkey;
pub use redline_program as program;
use redsuite_core::{ChainCtx, Result};
use signer::Signer;

pub const ACCOUNT_SPACE: u32 = 256;

pub fn account_update_id(data: &[u8]) -> Option<u64> {
    use program::layout::{ID_OFFSET, ID_SIZE};
    let bytes = data.get(ID_OFFSET..ID_OFFSET + ID_SIZE)?;
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}

pub async fn init_delegated_accounts(
    base: &impl ChainCtx,
    payer: &Keypair,
    count: u8,
    space: u32,
    authority: Pubkey,
) -> Result<Vec<Pubkey>> {
    let mut pdas = Vec::with_capacity(count as usize);
    for seed in 0..count {
        let (init, pda) = program::instruction::build::init_account(
            payer.pubkey(),
            payer.pubkey(),
            space,
            seed,
            authority,
        );
        let delegate = program::instruction::build::delegate(
            payer.pubkey(),
            pda,
            payer.pubkey(),
            seed,
            authority,
        );
        base.send(payer, &[init, delegate]).await?;
        pdas.push(pda);
    }
    Ok(pdas)
}
