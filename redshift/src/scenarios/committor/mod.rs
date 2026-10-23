use anyhow::{ensure, Context};
pub mod commit_and_undelegate;
pub mod commit_blackout;
pub mod commit_exactly_once;
pub mod commit_settlement_order;
pub mod delegation_session_isolation;
mod scheduled_intents;
pub mod undelegation_recovery;

use std::time::{Duration, Instant};

use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check,
    programs::{CellHeader, SettlementMode, REDLINE},
    BaseCtx, ChainCtx, ErCtx, Result,
};
use signature::Signature;
use tokio::time;

pub(super) const PROXIED_GROUP: &str = "this scenario needs the base-chain proxies of its ER group";
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(30);

async fn write_and_commit(
    er: &ErCtx,
    payer: &Keypair,
    commit_id: u64,
    write: u64,
    account: &Pubkey,
) -> Result<(Vec<u8>, Signature)> {
    let snapshot = write_snapshot(er, payer, write, account).await?;
    let commit_signature = er
        .cells(REDLINE, payer)
        .settle(commit_id, &[*account], SettlementMode::Commit)
        .await?;
    Ok((snapshot, commit_signature))
}

async fn write_snapshot(
    er: &ErCtx,
    payer: &Keypair,
    value: u64,
    account: &Pubkey,
) -> Result<Vec<u8>> {
    er.cells(REDLINE, payer).write(value, &[*account]).await?;
    let snapshot = er.account(account).await?.context("ER cell missing after write")?.data;
    ensure!(CellHeader::written_id(&snapshot) == Some(value));
    Ok(snapshot)
}

async fn prove_landed(
    base: &BaseCtx,
    base_signature: &Signature,
    account: &Pubkey,
    snapshot: &[u8],
) -> Result<()> {
    let base_tx = base.api().await_transaction(base_signature, BASE_CONFIRM_TIMEOUT).await?;
    ensure!(
        base_tx.err.is_none(),
        "the intercepted base commit {base_signature} must succeed on base, \
         got {:?}",
        base_tx.err
    );
    check::poll("base snapshot before fault", BASE_STATE_TIMEOUT, || async {
        matches!(base.account(account).await, Ok(Some(acc)) if acc.data == snapshot)
    })
    .await?;
    Ok(())
}

async fn hold_nonces(
    base: &BaseCtx,
    accounts: &[Pubkey],
    expected: &[u64],
    window: Duration,
) -> Result<()> {
    let deadline = Instant::now() + window;
    loop {
        for (account, expected) in accounts.iter().zip(expected) {
            let nonce = crate::last_commit_id(base, account).await?;
            ensure!(nonce == *expected);
        }
        if Instant::now() >= deadline {
            return Ok(());
        }
        time::sleep(Duration::from_millis(250)).await;
    }
}
