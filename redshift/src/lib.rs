//! CORRECTNESS family — observed state vs expected model; shared helpers for its tests live here.

pub mod scenarios;

use std::time::Duration;

use keypair::Keypair;
use pubkey::Pubkey;
pub use redline_program as program;
use redshift_program::schedulecommit::{build, MainAccount};
use redsuite_core::{
    assert::poll_until, receipt, BaseCtx, ChainCtx, ErCtx, Result,
};
use signer::Signer;

pub const ACCOUNT_SPACE: u32 = 128;
pub const PAYER_LAMPORTS: u64 = 2_000_000_000;
pub const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;

const COMMITTEE_CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);

pub struct Committee {
    pub player: Keypair,
    pub pda: Pubkey,
}

pub async fn init_schedulecommit_committees(
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
            on_base.owner,
            program::DELEGATION_PROGRAM_ID,
            "dlp must own a delegated committee on base"
        );
        committees.push(Committee { player, pda });
    }
    Ok(committees)
}

pub async fn await_committee_clones(er: &ErCtx, committees: &[Committee]) {
    for committee in committees {
        poll_until(COMMITTEE_CLONE_TIMEOUT, || async {
            matches!(
                er.account(&committee.pda).await,
                Ok(Some(clone)) if clone.data.len() == MainAccount::SIZE
            )
        })
        .await;
    }
}

pub async fn assert_commit_receipt(
    base: &BaseCtx,
    er: &ErCtx,
    signature: &signature::Signature,
    expected: &[Pubkey],
    expect_undelegation: bool,
) -> Result<receipt::CommitReceipt> {
    let commit_receipt =
        receipt::fetch_commit_receipt(er.api(), signature, RECEIPT_TIMEOUT)
            .await?;
    if let Some(message) = &commit_receipt.error_message {
        return Err(format!("the commit intent failed: {message}").into());
    }
    let mut included = commit_receipt.included.clone();
    included.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    assert_eq!(
        included, expected,
        "the receipt must list exactly the committed accounts"
    );
    assert!(
        commit_receipt.excluded.is_empty(),
        "the receipt must exclude no accounts"
    );
    assert_eq!(
        commit_receipt.requested_undelegation, expect_undelegation,
        "the receipt undelegation flag must match the commit type"
    );
    receipt::confirm_base_signatures(
        base.api(),
        &commit_receipt,
        BASE_CONFIRM_TIMEOUT,
    )
    .await?;
    Ok(commit_receipt)
}

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
