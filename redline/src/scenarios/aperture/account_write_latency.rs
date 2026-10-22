use std::{rc::Rc, sync::Arc, time::Duration};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    programs::{CellHeader, REDLINE},
    redline::Accounts,
    report::Unit,
    runner::{execute_and_sync, Pacing, RunConfig},
    transport::{
        clock::SendClock,
        ws::{AccountUpdates, SignatureConfirmations},
    },
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use tokio::time;

use crate::{
    program::{layout, DELEGATION_PROGRAM_ID},
    ACCOUNT_SPACE,
};

const ACCOUNTS: u8 = 4;
const ITERATIONS: u64 = 40;
const RATE: u32 = 20;
const CONCURRENCY: usize = 1;
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);
const PAYER_LAMPORTS: u64 = 2_000_000_000;

pub struct AccountWriteLatency {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
    pdas: Vec<Pubkey>,
}

impl Scenario<ScenarioReport> for AccountWriteLatency {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let pdas = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_delegated(base.as_ref(), &payer, ACCOUNTS)
            .await?;

        for pda in &pdas {
            let on_base = base.account(pda).await?.context("pda not on base")?;
            ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
        }
        er.await_clones(&pdas, ACCOUNT_SPACE as usize, Duration::from_secs(15)).await?;

        Ok(Self { base, er, payer, pdas })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let Self { base, er, payer, pdas } = self;
        let clock = Arc::new(SendClock::new(1, ITERATIONS as usize));
        let updates = Rc::new(
            AccountUpdates::connect(er.ws_url(), CellHeader::written_id, clock.clone()).await?,
        );
        for pda in &pdas {
            updates.account_subscribe(pda).await?;
        }
        updates.await_subscribed(pdas.len(), Duration::from_secs(5)).await?;
        let sigs = Rc::new(SignatureConfirmations::connect(er.ws_url(), clock.clone()).await?);
        let sender = er.sender(Rc::new(payer));

        let request = |id: u64| {
            let target = pdas[((id - 1) % pdas.len() as u64) as usize];
            let ix = REDLINE.write(id, &[target]);
            updates.track(id, target);
            let (sender, sigs, clock) = (sender.clone(), sigs.clone(), clock.clone());
            async move {
                let tx = sender.prepare(&[ix]).await?;
                sigs.subscribe(id, &tx.signatures[0]).await?;
                Ok(clock.stamped(id, sender.deliver(tx)))
            }
        };
        let sync = |id: u64| {
            let sigs = sigs.clone();
            async move {
                time::timeout(CONFIRM_TIMEOUT, sigs.await_id(id)).await.with_context(|| {
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
        ensure!(
            outcome.failed == 0,
            "failures={:?}; first error={:?}",
            outcome.failed,
            outcome.first_error
        );

        updates.await_observed(ITERATIONS as usize, Duration::from_secs(15)).await?;
        let update_lag = updates.finalize().lag;
        let confirmations = sigs.finalize();
        ensure!(
            confirmations.failed == 0,
            "failures={:?}; first error={:?}",
            confirmations.failed,
            confirmations.first_failure
        );

        for (i, pda) in pdas.iter().enumerate() {
            let last_id = ITERATIONS - ACCOUNTS as u64 + 1 + i as u64;
            let on_er = er.account(pda).await?.context("pda not on er")?;
            ensure!(CellHeader::written_id(&on_er.data) == Some(last_id));

            let on_base = base.account(pda).await?.context("pda gone on base")?;
            ensure!(
                on_base.data[layout::DATA_OFFSET..].iter().all(|&b| b == 0),
                "base copy must stay untouched until an explicit commit"
            );
        }

        Ok(ScenarioReport::ok()
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
