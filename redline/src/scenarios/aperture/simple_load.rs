use std::{rc::Rc, sync::Arc, time::Duration};

use async_trait::async_trait;
use redsuite_core::redline::Accounts;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, prep,
    runner::{execute_and_sync, Pacing, RunConfig},
    transport::{
        clock::SendClock,
        ws::{AccountUpdates, SignatureConfirmations},
    },
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};

use crate::program::{layout, DELEGATION_PROGRAM_ID};

const ACCOUNTS: u8 = 4;
const ITERATIONS: u64 = 40;
const RATE: u32 = 20;
const CONCURRENCY: usize = 1;
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);
const PAYER_LAMPORTS: u64 = 2_000_000_000;

pub struct SimpleLoad;

#[async_trait(?Send)]
impl Scenario<ScenarioReport> for SimpleLoad {
    fn name(&self) -> &str {
        "redline/simple_load"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let payer = prep::funded_payer(base, PAYER_LAMPORTS).await?;
        let pdas = Accounts::new(crate::ACCOUNT_SPACE, er.identity())
            .init_delegated(base, &payer, ACCOUNTS)
            .await?;

        for pda in &pdas {
            let on_base = base.account(pda).await?.ok_or("pda not on base")?;
            check_eq!(
                on_base.owner,
                DELEGATION_PROGRAM_ID,
                "delegated pda must be dlp-owned on base"
            )?;
        }
        prep::await_clones(
            er,
            &pdas,
            crate::ACCOUNT_SPACE as usize,
            Duration::from_secs(15),
        )
        .await?;

        let clock = Arc::new(SendClock::new(1, ITERATIONS as usize));
        let updates = Rc::new(
            AccountUpdates::connect(
                er.ws_url(),
                crate::account_update_id,
                clock.clone(),
            )
            .await?,
        );
        for pda in &pdas {
            updates.account_subscribe(pda).await?;
        }
        updates
            .await_subscribed(pdas.len(), Duration::from_secs(5))
            .await?;
        let sigs = Rc::new(
            SignatureConfirmations::connect(er.ws_url(), clock.clone()).await?,
        );
        let sender = er.sender(Rc::new(payer));

        let request = |id: u64| {
            let target = pdas[((id - 1) % pdas.len() as u64) as usize];
            let ix = crate::program::instruction::build::simple_byte_set(
                id,
                &[target],
            );
            updates.track(id, target);
            let (sender, sigs, clock) =
                (sender.clone(), sigs.clone(), clock.clone());
            async move {
                let tx = sender.prepare(&[ix]).await?;
                sigs.subscribe(id, &tx.signatures[0]).await?;
                Ok(clock.stamped(id, sender.deliver(tx)))
            }
        };
        let sync = |id: u64| {
            let sigs = sigs.clone();
            async move {
                tokio::time::timeout(CONFIRM_TIMEOUT, sigs.await_id(id))
                    .await
                    .map_err(|_| {
                        format!("confirmation for id {id} not within {CONFIRM_TIMEOUT:?}")
                    })?
            }
        };
        let outcome = execute_and_sync(
            RunConfig {
                iterations: ITERATIONS,
                rate: Pacing::PerSecond(RATE),
                concurrency: CONCURRENCY,
            },
            request,
            sync,
        )
        .await?;
        check_eq!(
            outcome.failed,
            0,
            "deliveries or confirmations failed: {:?}",
            outcome.first_error
        )?;

        updates
            .await_observed(ITERATIONS as usize, Duration::from_secs(15))
            .await?;
        let update_lag = updates.finalize().lag;
        let confirmations = sigs.finalize();
        check_eq!(
            confirmations.failed,
            0,
            "transactions failed on-chain: {:?}",
            confirmations.first_failure
        )?;

        for (i, pda) in pdas.iter().enumerate() {
            let last_id = ITERATIONS - ACCOUNTS as u64 + 1 + i as u64;
            let on_er = er.account(pda).await?.ok_or("pda not on er")?;
            check_eq!(
                crate::account_update_id(&on_er.data),
                Some(last_id),
                "er copy must hold the last id written to pda {i}"
            )?;

            let on_base = base.account(pda).await?.ok_or("pda gone on base")?;
            check!(
                on_base.data[layout::DATA_OFFSET..].iter().all(|&b| b == 0),
                "base copy must stay untouched until an explicit commit"
            )?;
        }

        Ok(ScenarioReport::ok(self.name())
            .setting("offered tps", RATE)
            .setting("concurrency", CONCURRENCY)
            .observe(
                "send+confirm us",
                Unit::Micros,
                outcome.sync.unwrap_or_default(),
            )
            .observe("delivery us", Unit::Micros, outcome.delivery)
            .client_side(&outcome)
            .metric("achieved tps", Unit::Tps, outcome.achieved_rps())
            .observe("account-update lag us", Unit::Micros, update_lag))
    }
}
