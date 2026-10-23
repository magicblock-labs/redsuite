use anyhow::ensure;
pub mod account_info_semantics;
pub mod activation_single_shot;
pub mod aml_gate;
pub mod cache_lifecycle;
pub mod clone_on_access;
pub mod clone_without_alt_writes;
pub mod loader_matrix;
pub mod multi_program_clone;
pub mod program_allowlist;
pub mod program_upgrade;
pub mod projected_token_lifecycle;
pub(crate) mod spl;
pub mod undelegation_grpc_redundancy;

use pubkey::Pubkey;
use redsuite_core::{programs::CellHeader, ChainCtx, ErCtx, Result};

use crate::ACCOUNT_OWNERS;

const ER_WRITE: u64 = 700;
const BASE_REWRITE: u64 = 900;
const BASE_REFRESH: u64 = 901;
const FETCHES: [&str; 2] = ["getAccountInfo", "getMultipleAccounts"];

async fn await_local_value(er: &ErCtx, what: &str, keys: &[Pubkey], expected: u64) -> Result<()> {
    let mut watch = er.api().watch_accounts(keys).await?;
    watch.seed(er.local_accounts(&ACCOUNT_OWNERS, keys).await?)?;
    watch
        .until(what, |accounts| {
            for account in accounts {
                let actual = account.as_ref().and_then(|a| CellHeader::written_id(&a.data));
                ensure!(actual == Some(expected));
            }
            Result::Ok(())
        })
        .await
}
