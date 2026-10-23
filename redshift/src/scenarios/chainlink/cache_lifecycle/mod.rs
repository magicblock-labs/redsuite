mod pressure;
mod transport;

use crate::ACCOUNT_OWNERS;
use anyhow::ensure;
use pubkey::Pubkey;
use redsuite_core::{
    netfault::BaseProxies, programs::CellHeader, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use std::rc::Rc;

pub enum Mode {
    Churn,
    UndelegationReconnectGap,
}

/// Checks stale-fetch ordering or reconnect recovery while preserving pending/delegated residents.
pub struct CacheLifecycle {
    base: Rc<BaseCtx>,
    mode: Mode,
    http: Rc<BaseProxies>,
    ws: Rc<BaseProxies>,
    er: ErCtx,
}

impl Scenario<(), Mode> for CacheLifecycle {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, mode: Mode) -> Result<Self> {
        let http = er.http_proxy()?;
        let ws = er.ws_proxy()?;
        Ok(Self { base, er, mode, http, ws })
    }
    async fn run(self) -> Result<()> {
        if matches!(self.mode, Mode::UndelegationReconnectGap) {
            self.transport_baseline().await?;
        }
        self.pressure().await
    }
}

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
