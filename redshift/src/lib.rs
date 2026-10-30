use anyhow::Context;
pub mod scenarios;

use account::Account;
use pubkey::Pubkey;
pub use redline_interface as program;
use redline_interface::DELEGATION_PROGRAM_ID;
use redsuite_core::{dlp, programs::CellHeader, BaseCtx, ChainCtx, Result};

pub const ACCOUNT_SPACE: u32 = 128;
pub const PAYER_LAMPORTS: u64 = 2_000_000_000;

pub fn account_id(account: Option<Account>) -> Option<u64> {
    account.and_then(|account| CellHeader::written_id(&account.data))
}

pub const ACCOUNT_OWNERS: [Pubkey; 2] = [program::ID, DELEGATION_PROGRAM_ID];

pub async fn last_commit_id(base: &BaseCtx, account: &Pubkey) -> Result<u64> {
    let metadata = base
        .account(&dlp::delegation_metadata_pda(account))
        .await?
        .context("the delegation metadata is missing on base")?;
    dlp::last_commit_id(&metadata.data)
}
