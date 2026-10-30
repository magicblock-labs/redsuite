mod pressure;
mod transport;

use crate::ACCOUNT_OWNERS;
use anyhow::{ensure, Context};
use pubkey::Pubkey;
use redsuite_core::{
    netfault::Faults, programs::CellHeader, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use std::{rc::Rc, time::Duration};

pub const CACHE_CAPACITY: usize = 256;
const FETCH_BATCH: usize = 32;
const EVICTIONS: &str = "engine_keeper_account_cache_evictions";

pub async fn pressure_preserves(
    base: &BaseCtx,
    er: &ErCtx,
    mirror: Pubkey,
    pool: &[Pubkey],
    protected: &[Pubkey],
) -> Result<()> {
    let before = er.local_images(protected).await?;
    ensure!(
        before.iter().all(Option::is_some),
        "protected cache image missing"
    );
    let base_before = base.accounts(protected).await?;
    let expected = er.account(&mirror).await?.context("cache mirror missing")?;
    ensure!(base.account(&mirror).await? == Some(expected.clone()));
    ensure!(er.local_images(&[mirror]).await? == vec![Some(expected.clone())]);
    let evictions = cache_evictions(er).await?;
    churn_cache(er, pool, evictions).await?;
    tokio::time::timeout(
        Duration::from_secs(20),
        er.api().await_query(
            "resident mirror evicted under bounded cache pressure",
            async || {
                ensure!(er.local_images(&[mirror]).await? == vec![None]);
                Ok(())
            },
        ),
    )
    .await
    .context("cache mirror eviction not witnessed within 20s")??;
    ensure!(er.local_images(protected).await? == before);
    ensure!(base.accounts(protected).await? == base_before);
    ensure!(er.account(&mirror).await? == Some(expected.clone()));
    ensure!(er.local_images(&[mirror]).await? == vec![Some(expected.clone())]);
    ensure!(base.account(&mirror).await? == Some(expected));
    ensure!(er.local_images(protected).await? == before);
    ensure!(base.accounts(protected).await? == base_before);
    Ok(())
}

async fn cache_evictions(er: &ErCtx) -> Result<f64> {
    er.scrape_metrics().await?.get(EVICTIONS).context("eviction metric missing")
}

async fn churn_cache(er: &ErCtx, pool: &[Pubkey], previous: f64) -> Result<f64> {
    for chunk in pool.chunks(FETCH_BATCH) {
        er.accounts(chunk).await?;
    }
    let current = cache_evictions(er).await?;
    ensure!(current > previous);
    Ok(current)
}

pub enum Mode {
    Churn,
    UndelegationReconnectGap,
}

/// Checks stale-fetch ordering or reconnect recovery while preserving pending/delegated residents.
pub struct CacheLifecycle {
    base: Rc<BaseCtx>,
    mode: Mode,
    faults: Rc<Faults>,
    er: ErCtx,
}

impl Scenario<(), Mode> for CacheLifecycle {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, mode: Mode) -> Result<Self> {
        let faults = er.faults()?;
        Ok(Self { base, er, mode, faults })
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
