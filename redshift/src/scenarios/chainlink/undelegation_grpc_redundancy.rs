use std::time::Duration;

use async_trait::async_trait;
use redsuite_core::{
    check,
    netfault::{Action, BaseProxies, Selector},
    prep,
    topology::{self, ErOptions},
    BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use signer::Signer;
use tokio::time::Instant;

use crate::program::{self, instruction::build};

use super::{
    await_local_value, init_accounts, write, BASE_REFRESH, BASE_REWRITE, ER_WRITE, FETCHES,
};

const LABEL: &str = "grpc-redundancy";
const TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY: Duration = Duration::from_secs(60);
const CLONED_COUNT: usize = 1;
const PENDING_COUNT: usize = 2;

pub struct GrpcRedundancy;

#[async_trait(?Send)]
impl PrivateErScenario for GrpcRedundancy {
    fn name(&self) -> &str {
        "redshift/undelegation_grpc_redundancy"
    }

    async fn run(&self, shared: &BaseCtx) -> Result<()> {
        let (grpc_url, _) = shared.grpc().ok_or("base L1 Yellowstone gRPC endpoint missing")?;
        let http = BaseProxies::spawn(shared).await?;
        let ws = BaseProxies::spawn(shared).await?;
        let mut endpoints = http.endpoints();
        endpoints.ws_url = ws.endpoints().ws_url;
        endpoints.grpc_url = Some(grpc_url.to_owned());

        let private = topology::private_er(
            shared,
            ErOptions {
                label: LABEL.into(),
                base_endpoints: Some(endpoints),
                request_timeout: Some(TIMEOUT),
                ..ErOptions::default()
            },
        )
        .await?;
        let er = private.ctx();
        let validator = er.identity();
        let funder = prep::funded_payer(shared, crate::PAYER_LAMPORTS).await?;
        let owner = funder.pubkey();
        let payer =
            prep::delegated_payer(shared, &funder, validator, crate::PAYER_LAMPORTS).await?;

        let keys = init_accounts(shared, &funder, validator, CLONED_COUNT + PENDING_COUNT).await?;
        let (cloned, pending) = keys.split_at(CLONED_COUNT);
        let delegates: Vec<_> = pending
            .iter()
            .zip(CLONED_COUNT as u8..)
            .map(|(&key, seed)| build::delegate(owner, key, owner, seed, validator))
            .collect();
        shared.submit_and_confirm(&funder, &delegates).await?;
        er.accounts(&keys).await?;
        write(er, &payer, ER_WRITE, pending).await?;
        check!(
            crate::local_accounts(er, pending)
                .await?
                .into_iter()
                .all(|account| crate::account_id(account) == Some(ER_WRITE)),
            "the er serves its own pre-undelegation writes"
        )?;

        let settlement =
            http.stall(Selector::method("sendTransaction").request().account(&pending[0]));
        er.submit_and_confirm(
            &funder,
            &[build::commit_and_undelegate_accounts(1, owner, pending)],
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
        write(shared, &funder, BASE_REWRITE, pending).await?;
        write(shared, &funder, BASE_REFRESH, cloned).await?;

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
