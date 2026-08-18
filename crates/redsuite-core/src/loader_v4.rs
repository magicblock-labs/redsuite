use std::time::Duration;

use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use signer::Signer;
use solana_loader_v4_interface::instruction as v4;
use transaction::Transaction;

use crate::{context::BaseCtx, system, ChainCtx, Result};

const CHUNK_SIZE: usize = 800;
const DEPLOY_LAMPORTS: u64 = 10 * 1_000_000_000;
const LENGTH_HEADROOM: u32 = 1024;
const CONFIRM_ATTEMPTS: u32 = 100;
const CONFIRM_POLL: Duration = Duration::from_millis(200);

// Deploy passes repeat byte-identical transactions: the Deploy instruction,
// and every Write chunk the upgrade did not change. Under the BaseCtx cached
// blockhash (20 s TTL) such a repeat gets the signature of its first landing
// and dedups into a silent no-op — the upgrade "succeeds" while the program
// stays Retracted. A fresh blockhash per send keeps every pass distinct.
async fn send_fresh(
    base: &BaseCtx,
    payer: &Keypair,
    cosigners: &[&Keypair],
    ixs: &[Instruction],
) -> Result<()> {
    let hash = base.api().get_latest_blockhash().await?;
    let mut signers = vec![payer];
    signers.extend_from_slice(cosigners);
    let tx = Transaction::new_signed_with_payer(
        ixs,
        Some(&payer.pubkey()),
        &signers,
        hash,
    );
    let sig = base.api().send_transaction(&tx).await?;
    for _ in 0..CONFIRM_ATTEMPTS {
        if base.api().signature_confirmed(&sig).await? {
            return Ok(());
        }
        tokio::time::sleep(CONFIRM_POLL).await;
    }
    Err(format!(
        "loader-v4 transaction {sig} not confirmed within \
         {CONFIRM_ATTEMPTS}x{CONFIRM_POLL:?}"
    )
    .into())
}

pub fn loader_v4_id() -> Pubkey {
    sdk_ids::loader_v4::ID
}

pub async fn deploy_program(
    base: &BaseCtx,
    authority: &Keypair,
    program: &Keypair,
    bytes: &[u8],
) -> Result<()> {
    let program_id = program.pubkey();
    let authority_id = authority.pubkey();

    if base.account(&program_id).await?.is_none() {
        let create = system::create_account(
            &authority_id,
            &program_id,
            DEPLOY_LAMPORTS,
            0,
            &loader_v4_id(),
        );
        send_fresh(base, authority, &[program], &[create]).await?;
    } else {
        send_fresh(
            base,
            authority,
            &[],
            &[v4::retract(&program_id, &authority_id)],
        )
        .await?;
    }

    let balance = base
        .account(&program_id)
        .await?
        .map(|account| account.lamports)
        .unwrap_or(0);
    if balance < DEPLOY_LAMPORTS {
        send_fresh(
            base,
            authority,
            &[],
            &[system::transfer(
                &authority_id,
                &program_id,
                DEPLOY_LAMPORTS - balance,
            )],
        )
        .await?;
    }

    let new_size = bytes.len() as u32 + LENGTH_HEADROOM;
    send_fresh(
        base,
        authority,
        &[],
        &[v4::set_program_length(
            &program_id,
            &authority_id,
            new_size,
            &authority_id,
        )],
    )
    .await?;

    for (index, chunk) in bytes.chunks(CHUNK_SIZE).enumerate() {
        let offset = (index * CHUNK_SIZE) as u32;
        send_fresh(
            base,
            authority,
            &[],
            &[v4::write(
                &program_id,
                &authority_id,
                offset,
                chunk.to_vec(),
            )],
        )
        .await?;
    }

    send_fresh(
        base,
        authority,
        &[],
        &[v4::deploy(&program_id, &authority_id)],
    )
    .await?;
    Ok(())
}
