use crate::RunControl;
use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    config::Profile,
    programs::{CellHeader, REDLINE},
    redline::Accounts,
    report::Unit,
    runner::{execute, Pacing, RunConfig},
    transport::{clock::SendClock, ws::AccountUpdates},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use std::{rc::Rc, sync::Arc, time::Instant};

/// Measures notification fanout while requiring each subscriber to settle both write targets.
pub struct WebsocketFanout {
    base: Rc<BaseCtx>,
    er: ErCtx,
    keys: Vec<Pubkey>,
    payers: Vec<Rc<Keypair>>,
    connections: usize,
    rate: u32,
}
impl Scenario<(ScenarioReport, Result<()>)> for WebsocketFanout {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let (count, connections, rate) = match base.config().profile {
            Profile::Lite => (16, 8, 200),
            Profile::Full => (64, 64, 1000),
        };
        let payers = base.funded_payers(count, 2_000_000_000).await?;
        let keys = Accounts::new(crate::ACCOUNT_SPACE, er.identity())
            .init_batched(&*base, &payers, count, true)
            .await?;
        er.await_clones(&keys, crate::ACCOUNT_SPACE as usize).await?;
        er.submit_and_confirm(&payers[0], &[REDLINE.copy_three(&keys, 0).0]).await?;
        Ok(Self {
            base,
            er,
            keys,
            payers: payers.into_iter().map(Rc::new).collect(),
            connections,
            rate,
        })
    }
    async fn run(self) -> Result<(ScenarioReport, Result<()>)> {
        let options = RunControl::from_config(&self.base.config())?;
        let mut remaining = options.duration;
        let mut cursor = 0;
        let mut received = 0;
        let mut superseded = 0;
        let mut peak = 0;
        let mut worst_p95 = 0;
        let mut report = ScenarioReport::ok()
            .setting("subscribers", self.connections)
            .setting("offered tps", self.rate);
        let clock = Arc::new(SendClock::new(1, 1024));
        let mut readers = Vec::with_capacity(self.connections);
        for _ in 0..self.connections {
            let reader = Rc::new(
                AccountUpdates::connect(self.er.ws_url(), CellHeader::written_id, clock.clone())
                    .await?,
            );
            for key in &self.keys {
                reader.account_subscribe(key).await?;
            }
            reader.await_subscribed(self.keys.len()).await?;
            readers.push(reader);
        }
        let result: Result<()> = async {
            while !remaining.is_zero() && !*options.stop.borrow() {
                clock.reset(cursor + 1);
                let deadline = Instant::now()
                    .checked_add(remaining)
                    .context("duration exceeds monotonic clock range")?;
                let outcome = execute(
                    RunConfig {
                        until: Some((deadline, options.stop.clone())),
                        iterations: 1024,
                        rate: Pacing::PerSecond(self.rate),
                        concurrency: 64,
                    },
                    |iteration| {
                        let id = cursor + iteration;
                        let sender =
                            self.er.sender(self.payers[id as usize % self.payers.len()].clone());
                        let (ix, targets) = REDLINE.copy_three(&self.keys, id);
                        for reader in &readers {
                            for target in targets {
                                reader.track(id, target);
                            }
                        }
                        let clock = clock.clone();
                        async move {
                            let staged = sender.staged([ix]).await?;
                            Ok(clock.stamped(id, staged))
                        }
                    },
                )
                .await?;
                ensure!(outcome.failed == 0);
                if outcome.delivered == 0 {
                    break;
                }
                let mut window_p95 = 0;
                for reader in &readers {
                    reader.await_settled().await?;
                    let observed = reader.take_outcome();
                    ensure!(
                        observed.observed + observed.superseded == 2 * outcome.delivered as usize
                    );
                    received += observed.observed;
                    superseded += observed.superseded;
                    peak = peak.max(observed.lag.max);
                    window_p95 = window_p95.max(observed.lag.quantile95);
                    tracing::debug!(
                        received = observed.observed,
                        superseded = observed.superseded,
                        p95_us = observed.lag.quantile95,
                        max_us = observed.lag.max,
                        "fanout subscriber window"
                    );
                }
                for key in &self.keys {
                    let last = (cursor + 1..=cursor + outcome.delivered)
                        .rev()
                        .find(|id| REDLINE.copy_three(&self.keys, *id).1.contains(key));
                    if let Some(last) = last {
                        let account =
                            self.er.account(key).await?.context("fanout account missing")?;
                        ensure!(CellHeader::written_id(&account.data) == Some(last));
                    }
                }
                remaining = remaining.saturating_sub(outcome.admission_wall);
                cursor += outcome.delivered;
                worst_p95 = worst_p95.max(window_p95);
                tracing::info!(
                    delivered = outcome.delivered,
                    worst_subscriber_p95_us = window_p95,
                    peak_lag_us = peak,
                    "fanout window"
                );
                report = ScenarioReport::ok()
                    .setting("subscribers", self.connections)
                    .setting("offered tps", self.rate)
                    .client_side(&outcome);
            }
            ensure!(
                cursor > 0 || *options.stop.borrow(),
                "no measured fanout work completed"
            );
            Ok(())
        }
        .await;
        for reader in &readers {
            reader.finalize();
        }
        report = report
            .metric("delivered transactions", Unit::Count, cursor as f64)
            .metric("notifications observed", Unit::Count, received as f64)
            .metric("writes superseded", Unit::Count, superseded as f64)
            .metric("peak notification lag us", Unit::Micros, peak as f64)
            .metric(
                "worst subscriber-window p95 us",
                Unit::Micros,
                worst_p95 as f64,
            )
            .setting("completion", options.completion(&result));
        Ok((report, result))
    }
}
