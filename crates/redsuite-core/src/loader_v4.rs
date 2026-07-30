use keypair::Keypair;
use pubkey::Pubkey;
use signer::Signer;
use solana_loader_v4_interface::instruction as v4;

use crate::{context::BaseCtx, system, ChainCtx, Result};

const CHUNK_SIZE: usize = 800;
const DEPLOY_LAMPORTS: u64 = 10 * 1_000_000_000;
const LENGTH_HEADROOM: u32 = 1024;

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
        base.send_with(authority, &[program], &[create]).await?;
    } else {
        base.send(authority, &[v4::retract(&program_id, &authority_id)])
            .await?;
    }

    let balance = base
        .account(&program_id)
        .await?
        .map(|account| account.lamports)
        .unwrap_or(0);
    if balance < DEPLOY_LAMPORTS {
        base.send(
            authority,
            &[system::transfer(
                &authority_id,
                &program_id,
                DEPLOY_LAMPORTS - balance,
            )],
        )
        .await?;
    }

    let new_size = bytes.len() as u32 + LENGTH_HEADROOM;
    base.send(
        authority,
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
        base.send(
            authority,
            &[v4::write(
                &program_id,
                &authority_id,
                offset,
                chunk.to_vec(),
            )],
        )
        .await?;
    }

    base.send(authority, &[v4::deploy(&program_id, &authority_id)])
        .await?;
    Ok(())
}
