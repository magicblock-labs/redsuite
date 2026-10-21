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

use std::time::Duration;

use pubkey::Pubkey;
use redsuite_core::{check, check_eq, ErCtx, Result};

use crate::ACCOUNT_OWNERS;

const ER_WRITE: u64 = 700;
const BASE_REWRITE: u64 = 900;
const BASE_REFRESH: u64 = 901;
const FETCHES: [&str; 2] = ["getAccountInfo", "getMultipleAccounts"];

async fn await_local_value(
    er: &ErCtx,
    what: &str,
    keys: &[Pubkey],
    expected: u64,
    timeout: Duration,
) -> Result<()> {
    check::poll_for(what, timeout, || async {
        for (key, account) in keys.iter().zip(er.local_accounts(&ACCOUNT_OWNERS, keys).await?) {
            check_eq!(
                crate::account_id(account),
                Some(expected),
                "local value of {key}"
            )?;
        }
        Result::Ok(())
    })
    .await
    .map_err(Into::into)
}
