use std::{rc::Rc, time::Duration};

use redsuite_core::{
    check,
    netfault::{Action, BaseProxies, Selector},
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
const RECOVERY: Duration = Duration::from_secs(60);
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
        let (grpc_url, _) = shared.grpc().ok_or("base L1 Yellowstone gRPC endpoint missing")?;
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
                request_timeout: Some(TIMEOUT),
                ..ErOptions::default()
            },
        )
        .await?;
        Ok(Self { shared, http, ws, private })
    }

    async fn run(self) -> Result<()> {
        let Self { shared, http, ws, private } = self;
        let er = private.ctx();
        let validator = er.identity();
        let funder = shared.funded_payer(PAYER_LAMPORTS).await?;
        let owner = funder.pubkey();
        let payer = shared.delegated_payer(&funder, validator, PAYER_LAMPORTS).await?;

        let keys = shared
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
        shared.submit_and_confirm(&funder, &delegates).await?;
        er.accounts(&keys).await?;
        er.cells(REDLINE, &payer).write(ER_WRITE, pending).await?;
        check!(
            er.local_accounts(&ACCOUNT_OWNERS, pending)
                .await?
                .into_iter()
                .all(|account| crate::account_id(account) == Some(ER_WRITE)),
            "the er serves its own pre-undelegation writes"
        )?;

        let settlement =
            http.stall(Selector::method("sendTransaction").request().account(&pending[0]));
        er.submit_and_confirm(
            &funder,
            &[REDLINE
                .batch()
                .nonce(1)
                .settle(owner, pending, SettlementMode::Undelegate)
                .build()],
        )
        .await?;
        check::poll("undelegation submission held", TIMEOUT, || async {
            http.events().iter().any(|event| event.action == Action::Held)
        })
        .await?;

        let mut blackout = vec![ws.stall(Selector::default().response().notification())];
        for key in &keys {
            blackout.push(http.stall(Selector::methods(&FETCHES).response().account(key)));
        }
        ws.close_connections();
        settlement.remove();

        check::poll_for("base completes the undelegation", TIMEOUT, || async {
            let settled =
                shared.accounts(pending).await?.into_iter().all(
                    |account| matches!(account, Some(account) if account.owner == program::id()),
                );
            check!(
                settled,
                "the accounts are still owned by the delegation program"
            )?;
            Result::Ok(())
        })
        .await?;
        shared.cells(REDLINE, &funder).write(BASE_REWRITE, pending).await?;
        shared.cells(REDLINE, &funder).write(BASE_REFRESH, cloned).await?;

        let refresh_started = Instant::now();
        await_local_value(
            er,
            "the er observes a base write to a cloned account while websocket \
             is cut",
            cloned,
            BASE_REFRESH,
            RECOVERY,
        )
        .await?;
        let refresh = refresh_started.elapsed();
        if let Err(error) = await_local_value(
            er,
            "the er observes the completed undelegation while websocket is cut",
            pending,
            BASE_REWRITE,
            RECOVERY,
        )
        .await
        {
            let served: Vec<_> =
                er.accounts(pending).await?.into_iter().map(crate::account_id).collect();
            return Err(format!(
                "{error}; the cloned account did refresh over grpc after \
                 {refresh:.1?}, and a direct client read returns {served:?}"
            )
            .into());
        }

        let ws_held = ws.events().iter().filter(|event| event.action == Action::Held).count();
        check!(
            ws_held > 0,
            "the websocket proxy held nothing, so the blackout this run \
             applied does not show that grpc carried the updates"
        )?;
        for rule in blackout {
            rule.remove();
        }
        ws.restore();
        http.restore();

        private.finish().await?;
        http.finish()?;
        ws.finish()?;
        Ok(())
    }
}
