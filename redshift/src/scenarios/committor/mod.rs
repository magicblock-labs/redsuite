use anyhow::{ensure, Context};
use std::{
    slice,
    time::{Duration, Instant},
};

pub mod commit_recovery;
pub mod commit_snapshot_ordering;
pub mod intent_execution_and_accounting;
pub mod settlement_lifecycle;
pub mod stale_delegation_session_isolation;
pub mod undelegation_recovery;

use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::{transaction_meta, SendError},
    programs::{CellHeader, SettlementMode, REDLINE},
    rpc_types::response::TransactionError,
    BaseCtx, ChainCtx, ErCtx, Result,
};
use signature::Signature;
use solana_instruction_error::InstructionError;
use tokio::time;

/// Accepts write-lockout enforcement at admission or execution, not an infrastructure failure.
fn check_lockout(error: &SendError) -> Result<()> {
    let locked = match error {
        SendError::Rejected(error) => {
            error.error.code.code() == -32003
                && [
                    "InvalidWritableAccount",
                    "ExternalAccountDataModified",
                    "ProgramFailedToComplete",
                    "Immutable",
                ]
                .iter()
                .any(|code| error.error.message.contains(code))
        }
        SendError::Execution(error) => matches!(
            error.err,
            TransactionError::InvalidWritableAccount
                | TransactionError::InstructionError(
                    _,
                    InstructionError::ExternalAccountDataModified
                        | InstructionError::ProgramFailedToComplete
                        | InstructionError::Immutable
                )
        ),
    };
    ensure!(locked, "expected write lockout, got {error}");
    Ok(())
}

/// Captures the exact ER image before scheduling its commit, keeping later writes
/// out of the oracle.
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

/// Confirms the write marker before returning its full account-data snapshot.
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

/// Establishes successful base execution and the expected image before a response is discarded.
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

/// Watches a negative progress boundary: held or recovered work must not advance these nonces.
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
