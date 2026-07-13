//! PERFORMANCE family — load scenarios; shared helpers for its tests live here.

pub mod scenarios;
use keypair::Keypair;
use pubkey::Pubkey;
pub use redline_program as program;
use redsuite_core::{ChainCtx, DynError, Result};
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

// init+delegate pairs packed per tx, then multiple such txs fit in a slot
const PREP_PAIRS_PER_TX: usize = 3;
// init-only txs pack denser (no dlp account metas)
const PREP_INITS_PER_TX: usize = 6;

pub async fn init_accounts_batched(
    base: &impl ChainCtx,
    payers: &[Keypair],
    count: usize,
    space: u32,
    authority: Pubkey,
) -> Result<Vec<Pubkey>> {
    if payers.is_empty() {
        return Err("at least one prep payer is required".into());
    }
    let per_payer = count.div_ceil(payers.len());
    if per_payer > u8::MAX as usize + 1 {
        return Err(format!(
            "{count} accounts over {} payers exceeds the u8 seed namespace",
            payers.len()
        )
        .into());
    }

    let batches =
        futures_util::future::join_all(payers.iter().enumerate().map(
            |(payer_index, payer)| async move {
                let first_index = payer_index * per_payer;
                let last_index = ((payer_index + 1) * per_payer).min(count);
                let mut pdas =
                    Vec::with_capacity(last_index.saturating_sub(first_index));
                let mut pending = Vec::new();
                for account_index in first_index..last_index {
                    let seed = (account_index - first_index) as u8;
                    let (init, pda) = program::instruction::build::init_account(
                        payer.pubkey(),
                        payer.pubkey(),
                        space,
                        seed,
                        authority,
                    );
                    pdas.push(pda);
                    pending.push(init);
                    if pending.len() >= PREP_INITS_PER_TX {
                        base.send(payer, &pending).await?;
                        pending.clear();
                    }
                }
                if !pending.is_empty() {
                    base.send(payer, &pending).await?;
                }
                Ok::<Vec<Pubkey>, DynError>(pdas)
            },
        ))
        .await;

    let mut pdas = Vec::with_capacity(count);
    for batch in batches {
        pdas.extend(batch?);
    }
    Ok(pdas)
}

pub async fn init_delegated_accounts_batched(
    base: &impl ChainCtx,
    payers: &[Keypair],
    count: usize,
    space: u32,
    authority: Pubkey,
) -> Result<Vec<Pubkey>> {
    if payers.is_empty() {
        return Err("at least one prep payer is required".into());
    }
    let per_payer = count.div_ceil(payers.len());
    if per_payer > u8::MAX as usize + 1 {
        return Err(format!(
            "{count} accounts over {} payers exceeds the u8 seed namespace",
            payers.len()
        )
        .into());
    }

    let batches =
        futures_util::future::join_all(payers.iter().enumerate().map(
            |(payer_index, payer)| async move {
                let first_index = payer_index * per_payer;
                let last_index = ((payer_index + 1) * per_payer).min(count);
                let mut pdas =
                    Vec::with_capacity(last_index.saturating_sub(first_index));
                let mut pending = Vec::new();
                for account_index in first_index..last_index {
                    let seed = (account_index - first_index) as u8;
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
                    pdas.push(pda);
                    pending.push(init);
                    pending.push(delegate);
                    if pending.len() >= PREP_PAIRS_PER_TX * 2 {
                        base.send(payer, &pending).await?;
                        pending.clear();
                    }
                }
                if !pending.is_empty() {
                    base.send(payer, &pending).await?;
                }
                Ok::<Vec<Pubkey>, DynError>(pdas)
            },
        ))
        .await;

    let mut pdas = Vec::with_capacity(count);
    for batch in batches {
        pdas.extend(batch?);
    }
    Ok(pdas)
}
