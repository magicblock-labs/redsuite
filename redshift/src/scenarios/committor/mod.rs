pub mod claim_fees;
pub mod commit_and_undelegate;
pub mod commit_blackout;
pub mod commit_exactly_once;
pub mod commit_roundtrip;
pub mod commit_settlement_order;
pub mod delegation_session_isolation;
pub mod table_mania;
pub mod undelegation_recovery;

use std::time::{Duration, Instant};

use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{check, check_eq, BaseCtx, ChainCtx, ErCtx, Result};
use signature::Signature;
use signer::Signer;

use crate::program::instruction::build;

const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(30);

async fn write_and_commit(
    er: &ErCtx,
    payer: &Keypair,
    commit_id: u64,
    write: u64,
    account: &Pubkey,
) -> Result<(Vec<u8>, Signature)> {
    er.submit_and_confirm(payer, &[build::simple_byte_set(write, &[*account])])
        .await?;
    let snapshot = er
        .account(account)
        .await?
        .ok_or("er copy vanished after the write")?
        .data;
    check_eq!(
        crate::written_id(&snapshot),
        Some(write),
        "the er write must land before the commit"
    )?;
    let commit_signature = er
        .submit_and_confirm(
            payer,
            &[build::commit_accounts(
                commit_id,
                payer.pubkey(),
                &[*account],
            )],
        )
        .await?;
    Ok((snapshot, commit_signature))
}

async fn prove_landed(
    base: &BaseCtx,
    base_signature: &Signature,
    account: &Pubkey,
    snapshot: &[u8],
) -> Result<()> {
    let base_tx = base
        .api()
        .await_transaction(base_signature, BASE_CONFIRM_TIMEOUT)
        .await?;
    check!(
        base_tx.err.is_none(),
        "the intercepted base commit {base_signature} must succeed on base, \
         got {:?}",
        base_tx.err
    )?;
    check::poll(
        "the base copy carries the er snapshot before the fault is applied",
        BASE_STATE_TIMEOUT,
        || async {
            matches!(base.account(account).await, Ok(Some(acc)) if acc.data == snapshot)
        },
    )
    .await?;
    Ok(())
}

async fn hold_nonces(
    base: &BaseCtx,
    accounts: &[Pubkey],
    expected: &[u64],
    window: Duration,
    phase: &str,
) -> Result<()> {
    let deadline = Instant::now() + window;
    loop {
        for (account, expected) in accounts.iter().zip(expected) {
            let nonce = crate::last_commit_id(base, account).await?;
            check_eq!(
                nonce,
                *expected,
                "{phase}: the nonce of {account} must remain unchanged throughout the window"
            )?;
        }
        if Instant::now() >= deadline {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// Duplicate rejection is not settlement evidence for exactly-once, ordering,
// or projected-token checks. Confirmation and target checks stay at the caller.
pub(super) fn require_settled(
    receipt: &redsuite_core::receipt::CommitReceipt,
    phase: &str,
) -> Result<()> {
    check!(
        receipt.succeeded(),
        "{phase}: settlement failed: {receipt:?}"
    )?;
    check!(
        !receipt.base_signatures.is_empty(),
        "{phase}: a settled receipt names its base transactions"
    )?;
    Ok(())
}
