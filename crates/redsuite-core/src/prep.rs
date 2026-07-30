use keypair::Keypair;
use pubkey::Pubkey;
use signer::Signer;

use crate::{context::ChainCtx, dlp, system, Result};

const ZERO_DATA_RENT_EXEMPT_LAMPORTS: u64 = 890_880;

pub async fn funded_payer(
    ctx: &impl ChainCtx,
    lamports: u64,
) -> Result<Keypair> {
    let payer = Keypair::new();
    ctx.airdrop(&payer.pubkey(), lamports).await?;
    Ok(payer)
}

pub async fn funded_payers(
    ctx: &impl ChainCtx,
    count: usize,
    lamports: u64,
) -> Result<Vec<Keypair>> {
    let mut payers = Vec::with_capacity(count);
    for _ in 0..count {
        payers.push(funded_payer(ctx, lamports).await?);
    }
    Ok(payers)
}

pub struct EscrowedPayer {
    pub payer: Keypair,
    pub escrow: Pubkey,
    pub delegation_record: Pubkey,
    pub escrow_lamports: u64,
}

pub async fn escrowed_payer(
    ctx: &impl ChainCtx,
    validator: Pubkey,
    lamports: u64,
) -> Result<EscrowedPayer> {
    let payer = funded_payer(ctx, lamports).await?;
    let payer_pubkey = payer.pubkey();
    let top_up_lamports = lamports / 2;
    let escrow_setup = [
        dlp::top_up_ephemeral_balance(&payer_pubkey, top_up_lamports, 0),
        dlp::delegate_ephemeral_balance(&payer_pubkey, &validator, 0),
    ];
    ctx.send(&payer, &escrow_setup).await?;

    let escrow = dlp::ephemeral_balance_pda(&payer_pubkey, 0);
    let escrow_lamports = top_up_lamports + ZERO_DATA_RENT_EXEMPT_LAMPORTS;
    let on_chain = ctx
        .account(&escrow)
        .await?
        .ok_or("escrow account missing after top-up + delegate")?;
    if on_chain.owner != dlp::dlp_id() {
        return Err(format!(
            "escrow {escrow} owned by {}, expected the delegation program",
            on_chain.owner
        )
        .into());
    }
    if on_chain.lamports != escrow_lamports {
        return Err(format!(
            "escrow {escrow} holds {} lamports, expected {escrow_lamports}",
            on_chain.lamports
        )
        .into());
    }
    Ok(EscrowedPayer {
        payer,
        escrow,
        delegation_record: dlp::delegation_record_pda(&escrow),
        escrow_lamports,
    })
}

pub async fn escrowed_payers(
    ctx: &impl ChainCtx,
    count: usize,
    validator: Pubkey,
    lamports: u64,
) -> Result<Vec<EscrowedPayer>> {
    let mut payers = Vec::with_capacity(count);
    for _ in 0..count {
        payers.push(escrowed_payer(ctx, validator, lamports).await?);
    }
    Ok(payers)
}

pub async fn delegated_payer(
    ctx: &impl ChainCtx,
    funder: &Keypair,
    validator: Pubkey,
    lamports: u64,
) -> Result<Keypair> {
    let delegatee = Keypair::new();
    let delegatee_pubkey = delegatee.pubkey();
    ctx.airdrop(&delegatee_pubkey, lamports).await?;
    let delegate_setup = [
        system::assign(&delegatee_pubkey, &dlp::dlp_id()),
        dlp::delegate_account(&funder.pubkey(), &delegatee_pubkey, &validator),
    ];
    ctx.send_with(funder, &[&delegatee], &delegate_setup)
        .await?;
    let on_chain = ctx
        .account(&delegatee_pubkey)
        .await?
        .ok_or("delegated payer account missing after delegation")?;
    if on_chain.owner != dlp::dlp_id() {
        return Err(format!(
            "delegated payer {delegatee_pubkey} owned by {}, expected the \
             delegation program",
            on_chain.owner
        )
        .into());
    }
    Ok(delegatee)
}
