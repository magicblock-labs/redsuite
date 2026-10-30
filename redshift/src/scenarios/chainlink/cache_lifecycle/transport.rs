use std::time::Duration;

use anyhow::{bail, ensure, Context};
use redsuite_core::{
    netfault::{Selector, Transport},
    programs::{CellId, SettlementMode, REDLINE},
    ChainCtx, Result,
};
use signer::Signer;
use tokio::time::Instant;

use super::{await_local_value, CacheLifecycle, BASE_REFRESH, BASE_REWRITE, ER_WRITE, FETCHES};
use crate::{program, ACCOUNT_OWNERS, ACCOUNT_SPACE, PAYER_LAMPORTS};

const TIMEOUT: Duration = Duration::from_secs(30);
const CLONED_COUNT: usize = 1;
const PENDING_COUNT: usize = 2;

impl CacheLifecycle {
    pub(super) async fn transport_baseline(&self) -> Result<()> {
        let er = &self.er;
        let evictions_before = er
            .scrape_metrics()
            .await?
            .get("engine_keeper_account_cache_evictions")
            .context("missing cache eviction counter")?;
        let validator = er.identity();
        let funder = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let owner = funder.pubkey();
        let payer = self.base.delegated_payer(&funder, validator, PAYER_LAMPORTS).await?;

        let keys = self
            .base
            .cells(REDLINE, &funder)
            .create_many(ACCOUNT_SPACE, validator, CLONED_COUNT + PENDING_COUNT)
            .await?;
        let (cloned, pending) = keys.split_at(CLONED_COUNT);
        let delegates: Vec<_> = (CLONED_COUNT..keys.len())
            .map(|index| {
                REDLINE.delegate(
                    owner,
                    CellId::new(owner, index as u64),
                    Some(validator),
                    u32::MAX,
                )
            })
            .collect();
        self.base.submit_and_confirm(&funder, &delegates).await?;
        er.accounts(&keys).await?;
        er.cells(REDLINE, &payer).write(ER_WRITE, pending).await?;
        ensure!(er
            .local_accounts(&ACCOUNT_OWNERS, pending)
            .await?
            .into_iter()
            .all(|account| crate::account_id(account) == Some(ER_WRITE)));

        let settlement = self
            .faults
            .stall(Selector::method("sendTransaction").http().request().account(&pending[0]));
        er.submit_and_confirm(
            &funder,
            &[REDLINE
                .batch()
                .nonce(1)
                .settle(owner, pending, SettlementMode::Undelegate)
                .build()],
        )
        .await?;
        settlement.wait_hit(TIMEOUT).await?;

        let ws_blackout = self.faults.stall(Selector::default().ws().response().notification());
        for key in &keys {
            self.faults.stall(Selector::methods(&FETCHES).http().response().account(key));
        }
        self.faults.disconnect(Transport::Ws);
        settlement.remove();

        self.base
            .api()
            .await_accounts(pending, "base completes the undelegation", |accounts| {
                let settled = accounts.iter().all(
                    |account| matches!(account, Some(account) if account.owner == program::id()),
                );
                ensure!(settled);
                Result::Ok(())
            })
            .await?;
        self.base.cells(REDLINE, &funder).write(BASE_REWRITE, pending).await?;
        self.base.cells(REDLINE, &funder).write(BASE_REFRESH, cloned).await?;

        let refresh_started = Instant::now();
        await_local_value(
            er,
            "the er observes a base write to a cloned account while websocket \
             is cut",
            cloned,
            BASE_REFRESH,
        )
        .await?;
        let refresh = refresh_started.elapsed();
        if let Err(error) = await_local_value(
            er,
            "undelegation discovered without WebSocket",
            pending,
            BASE_REWRITE,
        )
        .await
        {
            let served: Vec<_> =
                er.accounts(pending).await?.into_iter().map(crate::account_id).collect();
            bail!(
                "{error:#}; the cloned account did refresh over grpc after \
                 {refresh:.1?}, and a direct client read returns {served:?}"
            );
        }

        ensure!(ws_blackout.hits() > 0);
        self.faults.restore();

        ensure!(
            er.scrape_metrics().await?.get("engine_keeper_account_cache_evictions")
                == Some(evictions_before),
            "transport baseline evicted accounts"
        );
        self.faults.reset().await?;
        Ok(())
    }
}
