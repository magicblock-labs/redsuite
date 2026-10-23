use std::{rc::Rc, time::Duration};

use anyhow::{anyhow, ensure, Context};
use redsuite_core::{
    netfault::{BaseProxies, Selector},
    programs::{CellId, SettlementMode, REDLINE},
    topology::{self, ErOptions, PrivateEr},
    BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use signer::Signer;
use tokio::time::Instant;

use super::{await_local_value, BASE_REFRESH, BASE_REWRITE, ER_WRITE, FETCHES};
use crate::{program, ACCOUNT_OWNERS, ACCOUNT_SPACE, PAYER_LAMPORTS};

const LABEL: &str = "grpc-redundancy";
const TIMEOUT: Duration = Duration::from_secs(30);
const CLONED_COUNT: usize = 1;
const PENDING_COUNT: usize = 2;

pub struct GrpcRedundancy {
    shared: Rc<BaseCtx>,
    http: BaseProxies,
    ws: BaseProxies,
    private: PrivateEr,
}

impl PrivateErScenario for GrpcRedundancy {
    async fn setup(shared: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let (grpc_url, _) = shared.grpc().context("base L1 Yellowstone gRPC endpoint missing")?;
        let http = BaseProxies::spawn(&shared).await?;
        let ws = BaseProxies::spawn(&shared).await?;
        let mut endpoints = http.endpoints();
        endpoints.ws_url = ws.endpoints().ws_url;
        endpoints.grpc_url = Some(grpc_url.clone());

        let private = topology::private_er(
            &shared,
            ErOptions {
                label: LABEL.into(),
                base_endpoints: Some(endpoints),

                ..ErOptions::default()
            },
        )
        .await?;
        Ok(Self { shared, http, ws, private })
    }

    async fn run(self) -> Result<()> {
        let er = self.private.ctx();
        let validator = er.identity();
        let funder = self.shared.funded_payer(PAYER_LAMPORTS).await?;
        let owner = funder.pubkey();
        let payer = self.shared.delegated_payer(&funder, validator, PAYER_LAMPORTS).await?;

        let keys = self
            .shared
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
        self.shared.submit_and_confirm(&funder, &delegates).await?;
        er.accounts(&keys).await?;
        er.cells(REDLINE, &payer).write(ER_WRITE, pending).await?;
        ensure!(er
            .local_accounts(&ACCOUNT_OWNERS, pending)
            .await?
            .into_iter()
            .all(|account| crate::account_id(account) == Some(ER_WRITE)));

        let settlement = self
            .http
            .stall(Selector::method("sendTransaction").request().account(&pending[0]));
        er.submit_and_confirm(
            &funder,
            &[REDLINE
                .batch()
                .nonce(1)
                .settle(owner, pending, SettlementMode::Undelegate)
                .build()],
        )
        .await?;
        self.http.await_held(Selector::default(), TIMEOUT).await?;

        let mut blackout = vec![self.ws.stall(Selector::default().response().notification())];
        for key in &keys {
            blackout.push(self.http.stall(Selector::methods(&FETCHES).response().account(key)));
        }
        self.ws.close_connections();
        settlement.remove();

        self.shared
            .api()
            .await_accounts(pending, "base completes the undelegation", |accounts| {
                let settled = accounts.iter().all(
                    |account| matches!(account, Some(account) if account.owner == program::id()),
                );
                ensure!(settled);
                Result::Ok(())
            })
            .await?;
        self.shared.cells(REDLINE, &funder).write(BASE_REWRITE, pending).await?;
        self.shared.cells(REDLINE, &funder).write(BASE_REFRESH, cloned).await?;

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
            return Err(anyhow!(
                "{error:#}; the cloned account did refresh over grpc after \
                 {refresh:.1?}, and a direct client read returns {served:?}"
            ));
        }

        let ws_held = self.ws.held_count(&Selector::default());
        ensure!(ws_held > 0);
        for rule in blackout {
            rule.remove();
        }
        self.ws.restore();
        self.http.restore();

        self.private.finish().await?;
        self.http.finish()?;
        self.ws.finish()?;
        Ok(())
    }
}
