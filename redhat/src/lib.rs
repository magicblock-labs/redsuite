pub mod scenarios;

use std::time::Duration;

use keypair::Keypair;
use pubkey::Pubkey;
use redshift_program::schedulecommit::{build, MainAccount};
use redsuite_core::{assert::poll_until, BaseCtx, ChainCtx, ErCtx, Result};
use signer::Signer;

pub const PAYER_LAMPORTS: u64 = 2_000_000_000;
pub const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;
pub const DELEGATION_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh");

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Committee {
    pub player: Keypair,
    pub pda: Pubkey,
}

pub async fn init_delegated_committees(
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
        base.send_with(payer, &[&player], &[init, delegate]).await?;
        let on_base = base.account(&pda).await?.ok_or(
            "the committee pda is not on base after init and delegate",
        )?;
        assert_eq!(
            on_base.owner, DELEGATION_PROGRAM_ID,
            "dlp must own a delegated committee on base"
        );
        committees.push(Committee { player, pda });
    }
    Ok(committees)
}

pub async fn await_committee_clones(er: &ErCtx, committees: &[Committee]) {
    for committee in committees {
        poll_until(CLONE_TIMEOUT, || async {
            matches!(
                er.account(&committee.pda).await,
                Ok(Some(clone)) if clone.data.len() == MainAccount::SIZE
            )
        })
        .await;
    }
}
