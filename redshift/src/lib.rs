pub mod scenarios;

use account::Account;
use pubkey::Pubkey;
pub use redline_interface as program;
use redline_interface::DELEGATION_PROGRAM_ID;
pub use redsuite_core::phase;
use redsuite_core::{check::CheckError, dlp, programs::CellHeader, BaseCtx, ChainCtx, Result};

pub const ACCOUNT_SPACE: u32 = 128;
pub(crate) const LARGE_ACCOUNT_SPACE: u32 = 10 * 1024;
pub const PAYER_LAMPORTS: u64 = 2_000_000_000;

pub const LOCKOUT_REJECTIONS: [&str; 4] = [
    "InvalidWritableAccount",
    "ExternalAccountDataModified",
    "ProgramFailedToComplete",
    "Immutable",
];

pub fn rejection_code(
    what: &str,
    accepted: &[&'static str],
    error_text: &str,
) -> Result<&'static str> {
    accepted.iter().copied().find(|code| error_text.contains(code)).ok_or_else(|| {
        CheckError::new(format!("{what} is rejected with an upstream lockout code"))
            .expected(accepted.join(", "))
            .actual(error_text)
            .into()
    })
}

pub fn account_id(account: Option<Account>) -> Option<u64> {
    account.and_then(|account| CellHeader::written_id(&account.data))
}

pub const ACCOUNT_OWNERS: [Pubkey; 2] = [program::ID, DELEGATION_PROGRAM_ID];

pub async fn last_commit_id(base: &BaseCtx, account: &Pubkey) -> Result<u64> {
    let metadata = base
        .account(&dlp::delegation_metadata_pda(account))
        .await?
        .ok_or("the delegation metadata is missing on base")?;
    dlp::last_commit_id(&metadata.data)
}
