use std::time::Duration;

use async_trait::async_trait;
use futures_util::future::try_join_all;
use pubkey::Pubkey;
use redsuite_core::{
    check,
    netfault::{Action, BaseProxies, Selector},
    prep,
    topology::{self, ErOptions},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signer::Signer;

use crate::program::{self, instruction::build};

const LABEL: &str = "grpc-redundancy";
const TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY: Duration = Duration::from_secs(60);
const WRITTEN_ON_ER: u64 = 700;
const SETTLED_ON_BASE: u64 = 900;
const REFRESHED_ON_BASE: u64 = 901;
const FETCHES: [&str; 2] = ["getAccountInfo", "getMultipleAccounts"];

pub struct GrpcRedundancy;

async fn pushed_to(
    er: &ErCtx,
    what: &str,
    keys: &[Pubkey],
    expected: u64,
) -> Result<Duration> {
    let started = tokio::time::Instant::now();
    check::poll_for(what, RECOVERY, || async {
        match crate::local_accounts(er, keys).await {
            Ok(accounts) => {
                let seen: Vec<_> =
                    accounts.into_iter().map(crate::account_id).collect();
                if seen.iter().all(|value| *value == Some(expected)) {
                    Ok(started.elapsed())
                } else {
                    Err(format!("{seen:?}"))
                }
            }
            Err(error) => Err(error.to_string()),
        }
    })
    .await
    .map_err(Into::into)
}

#[async_trait(?Send)]
impl PrivateErScenario for GrpcRedundancy {
    fn name(&self) -> &str {
        "redshift/undelegation_grpc_redundancy"
    }

    async fn run(&self, shared: &BaseCtx) -> Result<()> {
        let (grpc_url, _) = shared.grpc().ok_or(
            "the base L1 is serving no Yellowstone gRPC feed, so this \
             scenario cannot compare transports; run `redsuite stack down` \
             and retry on a host where the plugin resolves",
        )?;
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
        let payer = prep::delegated_payer(
            shared,
            &funder,
            validator,
            crate::PAYER_LAMPORTS,
        )
        .await?;

        let keys =
            try_join_all((0..3).map(|seed| {
                crate::init_account(shared, &funder, seed, validator)
            }))
            .await?;
        let (cloned, pending) = keys.split_at(1);
        let delegates: Vec<_> = pending
            .iter()
            .zip(1..)
            .map(|(&key, seed)| {
                build::delegate(owner, key, owner, seed, validator)
            })
            .collect();
        shared.submit_and_confirm(&funder, &delegates).await?;
        er.accounts(&keys).await?;
        er.submit_and_confirm(
            &payer,
            &[build::simple_byte_set(WRITTEN_ON_ER, pending)],
        )
        .await?;
        check!(
            crate::local_accounts(er, pending).await?.into_iter().all(
                |account| crate::account_id(account) == Some(WRITTEN_ON_ER)
            ),
            "the er serves its own pre-undelegation writes"
        )?;

        let settlement = http.stall(
            Selector::method("sendTransaction")
                .request()
                .account(&pending[0]),
        );
        er.submit_and_confirm(
            &funder,
            &[build::commit_and_undelegate_accounts(1, owner, pending)],
        )
        .await?;
        check::poll("undelegation submission held", TIMEOUT, || async {
            http.events()
                .iter()
                .any(|event| event.action == Action::Held)
        })
        .await?;

        let mut blackout =
            vec![ws.stall(Selector::default().response().notification())];
        for key in &keys {
            blackout.push(
                http.stall(Selector::methods(&FETCHES).response().account(key)),
            );
        }
        ws.close_connections();
        settlement.remove();

        check::poll_for("base completes the undelegation", TIMEOUT, || async {
            let settled = shared.accounts(pending).await?.into_iter().all(
                |account| {
                    matches!(account, Some(account) if account.owner == program::id())
                },
            );
            check!(settled, "the accounts are still owned by the delegation program")?;
            Result::Ok(())
        })
        .await?;
        shared
            .submit_and_confirm(
                &funder,
                &[build::simple_byte_set(SETTLED_ON_BASE, pending)],
            )
            .await?;
        shared
            .submit_and_confirm(
                &funder,
                &[build::simple_byte_set(REFRESHED_ON_BASE, cloned)],
            )
            .await?;

        let refresh = pushed_to(
            er,
            "the er observes a base write to a cloned account while websocket \
             is cut",
            cloned,
            REFRESHED_ON_BASE,
        )
        .await?;
        if let Err(error) = pushed_to(
            er,
            "the er observes the completed undelegation while websocket is cut",
            pending,
            SETTLED_ON_BASE,
        )
        .await
        {
            let served: Vec<_> = er
                .accounts(pending)
                .await?
                .into_iter()
                .map(crate::account_id)
                .collect();
            return Err(format!(
                "{error}; the cloned account did refresh over grpc after \
                 {refresh:.1?}, and a direct client read returns {served:?}"
            )
            .into());
        }

        let ws_held = ws
            .events()
            .iter()
            .filter(|event| event.action == Action::Held)
            .count();
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
