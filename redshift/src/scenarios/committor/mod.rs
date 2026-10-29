use anyhow::{ensure, Context};
use std::slice;

pub mod commit_snapshot_ordering;
pub mod dlp_v2_buffered_settlement;
pub mod dlp_v2_lifecycle;
pub mod intent_execution_and_accounting;
pub mod settlement_lifecycle;
pub mod stale_delegation_session_isolation;
pub mod undelegation_recovery;
mod v2;

use std::time::{Duration, Instant};

use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    programs::{CellHeader, SettlementMode, REDLINE},
    BaseCtx, ChainCtx, ErCtx, Result,
};
use signature::Signature;
use tokio::time;

pub(super) const PROXIED_GROUP: &str = "this scenario needs the base-chain proxies of its ER group";

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
    let base_tx = base.api().await_transaction(base_signature).await?;
    let meta = transaction_meta(&base_tx)?;
    ensure!(meta.err == None);
    base.api()
        .await_accounts(
            slice::from_ref(account),
            "base snapshot before fault",
            |accounts| {
                let account = accounts[0].as_ref().context("base account missing")?;
                ensure!(account.data == snapshot);
                Ok(())
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

pub mod commit_recovery;
