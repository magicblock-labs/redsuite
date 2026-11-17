use std::time::Duration;

use anyhow::{ensure, Context};
use redsuite_core::{
    netfault::{Selector, Transport},
    programs::{CellId, SettlementMode, REDLINE},
    wait, ChainCtx, Result,
};
use signer::Signer;

use super::{
    await_fetched_value, await_local_value, CacheLifecycle, BASE_REFRESH, BASE_REWRITE, ER_WRITE,
    FETCHES,
};
use crate::{program, ACCOUNT_OWNERS, ACCOUNT_SPACE, PAYER_LAMPORTS};

const TIMEOUT: Duration = Duration::from_secs(30);
const CLONED_COUNT: usize = 1;
const PENDING_COUNT: usize = 2;

impl CacheLifecycle {
    /// Tests gRPC mirror updates and undelegation discovery without WebSocket delivery.
    /// Subscription failure evicts released images; requests reacquire them after recovery.
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

        wait::retry(
            "gRPC mirror coverage before WebSocket loss",
            TIMEOUT,
            || async {
                let metrics = er.scrape_metrics().await?;
                ensure!(
                    metrics.get("engine_chainsync_coverage{coverage=\"both\"}").unwrap_or_default()
                        >= cloned.len() as f64
                );
                ensure!(
                    metrics
                        .get("engine_chainsync_coverage{coverage=\"ws_only\"}")
                        .unwrap_or_default()
                        == 0.0
                );
                Ok(())
            },
        )
        .await?;
        let ws_blackout = self.faults.stall(Selector::default().ws().response().notification());
        for key in cloned {
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

        await_local_value(
            er,
            "the er observes a base write to a cloned account while websocket \
             is cut",
            cloned,
            BASE_REFRESH,
        )
        .await?;
        self.await_released(pending).await?;

        ensure!(ws_blackout.hits() > 0);
        self.faults.restore();

        await_fetched_value(
            er,
            "requests reacquire released accounts after recovery",
            pending,
            BASE_REWRITE,
        )
        .await?;

        ensure!(
            er.scrape_metrics().await?.get("engine_keeper_account_cache_evictions")
                == Some(evictions_before),
            "transport baseline evicted accounts"
        );
        self.faults.reset().await?;
        Ok(())
    }
}
