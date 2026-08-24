use std::time::Duration;

use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::schedulecommit::{build, MainAccount};
use signer::Signer;

use crate::{
    check,
    context::{BaseCtx, ChainCtx, ErCtx},
    dlp, system, Result,
};

const ZERO_DATA_RENT_EXEMPT_LAMPORTS: u64 = 890_880;
pub const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;
const COMMITTEE_CLONE_TIMEOUT: Duration = Duration::from_secs(15);

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
    ctx.submit_and_confirm(&payer, &escrow_setup).await?;

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

pub struct Committee {
    pub player: Keypair,
    pub pda: Pubkey,
}

pub async fn init_committees(
    base: &BaseCtx,
    payer: &Keypair,
    validator: Pubkey,
    count: usize,
) -> Result<Vec<Committee>> {
    let mut committees = Vec::with_capacity(count);
    for _ in 0..count {
        let player = Keypair::new();
        let (init, pda) = build::init_account(payer.pubkey(), player.pubkey());
        let delegate = build::delegate_cpi(
            payer.pubkey(),
            player.pubkey(),
            COMMIT_FREQUENCY_MS,
            Some(validator),
        );
        base.submit_and_confirm_with(payer, &[&player], &[init, delegate])
            .await?;
        let on_base = base.account(&pda).await?.ok_or(
            "the committee pda is not on base after init and delegate",
        )?;
        crate::check_eq!(
            on_base.owner,
            dlp::dlp_id(),
            "dlp must own a delegated committee on base"
        )?;
        committees.push(Committee { player, pda });
    }
    Ok(committees)
}

pub async fn await_committee_clones(
    er: &ErCtx,
    committees: &[Committee],
) -> Result<()> {
    for committee in committees {
        check::poll(
            &format!("the ER clones committee {}", committee.pda),
            COMMITTEE_CLONE_TIMEOUT,
            || async {
                matches!(
                    er.account(&committee.pda).await,
                    Ok(Some(clone)) if clone.data.len() == MainAccount::SIZE
                )
            },
        )
        .await?;
    }
    Ok(())
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
    ctx.submit_and_confirm_with(funder, &[&delegatee], &delegate_setup)
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
