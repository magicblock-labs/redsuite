use std::{rc::Rc, time::Duration};

use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::flexi::build as flexi;
use redshift_interface::schedulecommit::{build, MainAccount};
use signer::Signer;

use crate::{
    check,
    context::{BaseCtx, ChainCtx, ErCtx},
    dlp, system, DynError, ErClient, Result, TxSender,
};

const ZERO_DATA_RENT_EXEMPT_LAMPORTS: u64 = 890_880;
pub const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;
const COMMITTEE_CLONE_TIMEOUT: Duration = Duration::from_secs(15);

const PREP_CONCURRENCY: usize = 32;

pub async fn funded_payer(
    ctx: &impl ChainCtx,
    lamports: u64,
) -> Result<Keypair> {
    let payer = Keypair::new();
    ctx.airdrop(&payer.pubkey(), lamports)
        .await
        .map_err(|error| {
            format!("funding payer {}: {error}", payer.pubkey())
        })?;
    Ok(payer)
}

pub async fn funded_payers(
    ctx: &impl ChainCtx,
    count: usize,
    lamports: u64,
) -> Result<Vec<Keypair>> {
    bounded(count, "payer", |_| funded_payer(ctx, lamports)).await
}

pub(crate) async fn bounded<T, F, Fut>(
    count: usize,
    what: &str,
    make: F,
) -> Result<Vec<T>>
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    use futures_util::{StreamExt, TryFutureExt, TryStreamExt};
    futures_util::stream::iter(0..count)
        .map(|index| {
            make(index).map_err(move |error| {
                DynError::from(format!(
                    "preparing {what} {index} of {count}: {error}"
                ))
            })
        })
        .buffered(PREP_CONCURRENCY)
        .try_collect()
        .await
}

pub fn payer_from_bytes(bytes: &[u8]) -> Keypair {
    Keypair::try_from(bytes).expect("payer bytes round-trip")
}

// Each worker owns its client/cache and the same round-robin payer partition.
pub fn worker_senders(
    rpc_url: &str,
    payers: &[[u8; 64]],
    worker: usize,
    threads: usize,
) -> Vec<TxSender> {
    let client = ErClient::new(rpc_url);
    payers
        .iter()
        .enumerate()
        .filter(|(index, _)| index % threads == worker)
        .map(|(_, bytes)| client.sender(Rc::new(payer_from_bytes(bytes))))
        .collect()
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
    let pdas: Vec<Pubkey> =
        committees.iter().map(|committee| committee.pda).collect();
    await_clones(er, &pdas, MainAccount::SIZE, COMMITTEE_CLONE_TIMEOUT).await
}

pub async fn await_clones(
    er: &ErCtx,
    pdas: &[Pubkey],
    space: usize,
    timeout: Duration,
) -> Result<()> {
    for pda in pdas {
        check::poll(
            &format!("the ER clones the delegated pda {pda}"),
            timeout,
            || async {
                matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == space)
            },
        )
        .await?;
    }
    Ok(())
}

pub async fn await_cloned_payers(
    er: &ErCtx,
    payers: &[Pubkey],
    timeout: Duration,
) -> Result<()> {
    bounded(payers.len(), "cloned payer", |index| {
        let payer = payers[index];
        async move {
            check::poll(
                &format!("the ER clones payer {payer}"),
                timeout,
                || async {
                    matches!(er.account(&payer).await, Ok(Some(acc)) if acc.lamports > 0)
                },
            )
            .await?;
            Ok(())
        }
    })
    .await?;
    Ok(())
}

pub async fn await_program_clone(
    er: &ErCtx,
    program: &Pubkey,
    timeout: Duration,
) -> Result<()> {
    check::poll(
        &format!("the er clones the program {program} as executable"),
        timeout,
        || async {
            matches!(er.account(program).await, Ok(Some(clone)) if clone.executable)
        },
    ).await?;
    Ok(())
}

pub fn flexi_counter(
    owner: Pubkey,
    label: &str,
    validator: Pubkey,
    commit_frequency_ms: u32,
) -> (Pubkey, [Instruction; 2]) {
    let (init, counter) = flexi::init_counter(owner, label);
    (
        counter,
        [
            init,
            flexi::delegate_counter(
                owner,
                commit_frequency_ms,
                Some(validator),
            ),
        ],
    )
}

pub async fn delegated_payer(
    ctx: &impl ChainCtx,
    funder: &Keypair,
    validator: Pubkey,
    lamports: u64,
) -> Result<Keypair> {
    let delegatee = funded_payer(ctx, lamports).await?;
    delegate_payer(ctx, funder, &delegatee, validator).await?;
    Ok(delegatee)
}

pub async fn delegate_payer(
    ctx: &impl ChainCtx,
    funder: &Keypair,
    delegatee: &Keypair,
    validator: Pubkey,
) -> Result<()> {
    let delegatee_pubkey = delegatee.pubkey();
    let delegate_setup = [
        system::assign(&delegatee_pubkey, &dlp::dlp_id()),
        dlp::delegate_account(&funder.pubkey(), &delegatee_pubkey, &validator),
    ];
    ctx.submit_and_confirm_with(funder, &[delegatee], &delegate_setup)
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
    Ok(())
}
