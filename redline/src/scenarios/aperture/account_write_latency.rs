use std::{rc::Rc, sync::Arc};

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

use crate::{
    program::{layout, DELEGATION_PROGRAM_ID},
    ACCOUNT_SPACE,
};

const ACCOUNTS: u8 = 4;
const ITERATIONS: u64 = 40;
const RATE: u32 = 20;
const CONCURRENCY: usize = 1;
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
        er.await_clones(&pdas, ACCOUNT_SPACE as usize).await?;

        Ok(Self { base, er, payer, pdas })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let clock = Arc::new(SendClock::new(1, ITERATIONS as usize));
        let updates = Rc::new(
            AccountUpdates::connect(self.er.ws_url(), CellHeader::written_id, clock.clone())
                .await?,
        );
        for pda in &self.pdas {
            updates.account_subscribe(pda).await?;
        }
        updates.await_subscribed(self.pdas.len()).await?;
        let sigs = Rc::new(SignatureConfirmations::connect(self.er.ws_url(), clock.clone()).await?);
        let sender = self.er.sender(Rc::new(self.payer));

        let request = |id: u64| {
            let target = self.pdas[((id - 1) % self.pdas.len() as u64) as usize];
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
            async move { sigs.await_id(id).await }
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
        ensure!(outcome.failed == 0);

        updates.await_observed(ITERATIONS as usize).await?;
        let update_lag = updates.finalize().lag;
        let confirmations = sigs.finalize();
        ensure!(confirmations.failed == 0);

        for (i, pda) in self.pdas.iter().enumerate() {
            let last_id = ITERATIONS - ACCOUNTS as u64 + 1 + i as u64;
            let on_er = self.er.account(pda).await?.context("pda not on er")?;
            ensure!(CellHeader::written_id(&on_er.data) == Some(last_id));

            let on_base = self.base.account(pda).await?.context("pda gone on base")?;
            ensure!(on_base.data[layout::DATA_OFFSET..].iter().all(|&b| b == 0));
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
