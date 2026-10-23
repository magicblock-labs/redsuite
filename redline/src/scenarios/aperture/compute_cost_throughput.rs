use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use anyhow::{ensure, Context};
use pubkey::Pubkey;
use redsuite_core::{
    profile::ProfileValues,
    programs::{CellHeader, HashState, REDLINE},
    redline::{causal, Accounts},
    report::Unit,
    runner::{execute, Pacing, RunConfig, RunOutcome},
    transport::{
        clock::SendClock,
        ws::{AccountUpdates, UpdateOutcome},
    },
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport, TxSender,
};
use signature::Signature;
use tracing::{debug, instrument};

use crate::{metrics, program::layout, ACCOUNT_SPACE};

const PAYER_LAMPORTS: u64 = 2_000_000_000;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(300);
const TX_COUNT: &str = metrics::ENGINE_TRANSACTIONS;
const LIGHT_ITERS: u32 = 1;
const HEAVY_ITERS: u32 = 20;
const CU_CONTRAST_FLOOR: f64 = 10.0;

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: u8,
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    accounts: 16,
    warmup: 100,
    iterations: 1,
    rate: 200,
    concurrency: 64,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    accounts: 64,
    warmup: 2_000,
    iterations: 20_000,
    rate: 1_000,
    concurrency: 256,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

struct Cell {
    label: &'static str,
    iters: u32,
    outcome: RunOutcome,
    updates: UpdateOutcome,
    drain: Duration,
    probe_cus: f64,
    validator_avg_us: Option<f64>,
    validator_txs: Option<f64>,
}

impl Cell {
    fn executed_tps(&self, iterations: u64) -> f64 {
        iterations as f64 / (self.outcome.wall + self.drain).as_secs_f64()
    }
}

pub struct ComputeCostThroughput {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    senders: Vec<TxSender>,
    pdas: Vec<Pubkey>,
}

impl Scenario<ScenarioReport> for ComputeCostThroughput {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let payers = base.funded_payers(profile.payers, PAYER_LAMPORTS).await?;
        let pdas = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_delegated(base.as_ref(), &payers[0], profile.accounts)
            .await?;
        er.await_clones(&pdas, ACCOUNT_SPACE as usize).await?;
        let senders: Vec<TxSender> =
            payers.into_iter().map(|payer| er.sender(Rc::new(payer))).collect();

        Ok(Self { base, er, profile, senders, pdas })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let mut offset = 0u64;
        let mut cells: Vec<Cell> = Vec::new();
        for (label, iters) in [("light", LIGHT_ITERS), ("heavy", HEAVY_ITERS)] {
            let cell = self.run_cell(label, iters, offset).await?;
            offset += self.profile.warmup + self.profile.iterations;
            debug!(
                label,
                executed_tps = cell.executed_tps(self.profile.iterations),
                "compute cell finished",
            );

            self.report_cell(&cell)?;
            cells.push(cell);
        }

        self.check_final_state(offset).await?;
        self.summarize(&cells)
    }
}

impl ComputeCostThroughput {
    fn report_cell(&self, cell: &Cell) -> Result<()> {
        let cell_report = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("sha256 iters", cell.iters)
            .setting("shape", "width-1 sha256 hash-chain compute")
            .setting("payers", self.profile.payers)
            .setting("accounts", self.profile.accounts)
            .setting("measured iters", self.profile.iterations)
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .observe("delivery us", Unit::Micros, cell.outcome.delivery)
            .client_side(&cell.outcome)
            .observe("account-update lag us", Unit::Micros, cell.updates.lag)
            .metric("achieved tps", Unit::Tps, cell.outcome.achieved_rps())
            .metric(
                "executed tps",
                Unit::Tps,
                cell.executed_tps(self.profile.iterations),
            )
            .metric("drain s", Unit::Seconds, cell.drain.as_secs_f64())
            .metric("superseded", Unit::Count, cell.updates.superseded as f64)
            .metric("probe consumed cus", Unit::Count, cell.probe_cus)
            .metric_if(
                "validator tx processing avg us",
                Unit::Micros,
                cell.validator_avg_us,
            )
            .metric_if("validator txs in window", Unit::Count, cell.validator_txs);
        self.base.report_cell(cell.label, &cell_report)
    }

    #[instrument(skip_all, err)]
    async fn check_final_state(&self, offset: u64) -> Result<()> {
        let heavy_start = offset - self.profile.iterations + 1;
        let heavy_end = offset;
        let expected_hash = HashState::chain(causal::HASH_INIT.to_bytes(), HEAVY_ITERS);
        let pool = self.pdas.len() as u64;
        let end_index = (heavy_end - 1) % pool;
        for (index, pda) in self.pdas.iter().enumerate() {
            let index = index as u64;
            let last_id = if end_index >= index {
                heavy_end - (end_index - index)
            } else {
                heavy_end - (end_index + pool - index)
            };
            if last_id < heavy_start {
                continue;
            }
            let on_er = self.er.account(pda).await?.context("pda not on er")?;
            ensure!(CellHeader::written_id(&on_er.data) == Some(last_id));
            let hash_bytes =
                &on_er.data[layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE];
            ensure!(hash_bytes == expected_hash);
        }

        Ok(())
    }

    fn summarize(&self, cells: &[Cell]) -> Result<ScenarioReport> {
        let light = &cells[0];
        let heavy = &cells[1];
        let cu_ratio = heavy.probe_cus / light.probe_cus;
        ensure!(cu_ratio >= CU_CONTRAST_FLOOR);
        let validator_avg_ratio = match (light.validator_avg_us, heavy.validator_avg_us) {
            (Some(light_avg), Some(heavy_avg)) if light_avg > 0.0 => Some(heavy_avg / light_avg),
            _ => None,
        };

        let mut summary = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("shape", "width-1 sha256 hash-chain compute")
            .setting("light iters", LIGHT_ITERS)
            .setting("heavy iters", HEAVY_ITERS)
            .setting("payers", self.profile.payers)
            .setting("accounts", self.profile.accounts)
            .setting("warmup iters per cell", self.profile.warmup)
            .setting("measured iters per cell", self.profile.iterations)
            .setting("offered tps", self.profile.rate)
            .setting("concurrency", self.profile.concurrency);
        for cell in cells {
            summary = summary
                .with_flags(&cell.outcome)
                .metric(
                    format!("{} delivery p50 us", cell.label),
                    Unit::Micros,
                    cell.outcome.delivery.median as f64,
                )
                .metric(
                    format!("{} delivery p95 us", cell.label),
                    Unit::Micros,
                    cell.outcome.delivery.quantile95 as f64,
                )
                .metric(
                    format!("{} account-update lag p50 us", cell.label),
                    Unit::Micros,
                    cell.updates.lag.median as f64,
                )
                .metric(
                    format!("{} achieved tps", cell.label),
                    Unit::Tps,
                    cell.outcome.achieved_rps(),
                )
                .metric(
                    format!("{} executed tps", cell.label),
                    Unit::Tps,
                    cell.executed_tps(self.profile.iterations),
                )
                .metric(
                    format!("{} drain s", cell.label),
                    Unit::Seconds,
                    cell.drain.as_secs_f64(),
                )
                .metric(
                    format!("{} probe consumed cus", cell.label),
                    Unit::Count,
                    cell.probe_cus,
                )
                .metric_if(
                    format!("{} validator tx processing avg us", cell.label),
                    Unit::Micros,
                    cell.validator_avg_us,
                );
        }
        summary = summary.metric("heavy/light probe cu ratio", Unit::Ratio, cu_ratio);
        if let Some(ratio) = validator_avg_ratio {
            summary = summary.metric("heavy/light validator avg ratio", Unit::Ratio, ratio);
        }
        Ok(summary)
    }

    async fn run_cell(&self, label: &'static str, iters: u32, offset: u64) -> Result<Cell> {
        let warmup_request = |iteration: u64| {
            let global_id = offset + iteration;
            let pda_index = ((global_id - 1) % self.pdas.len() as u64) as usize;
            let ix = REDLINE.hash(
                global_id,
                causal::HASH_INIT.to_bytes(),
                iters,
                &[self.pdas[pda_index]],
            );
            let sender = self.senders[(global_id as usize) % self.senders.len()].clone();
            sender.staged([ix])
        };
        let count_before_warmup = self.er.scrape_metrics().await?.get(TX_COUNT);
        let warm = execute(
            RunConfig {
                iterations: self.profile.warmup,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            warmup_request,
        )
        .await?;
        ensure!(warm.failed == 0);
        if let Some(seen) = count_before_warmup {
            crate::await_executed(&self.er, seen + self.profile.warmup as f64, DRAIN_TIMEOUT)
                .await?;
        }

        let clock = Arc::new(SendClock::new(
            offset + self.profile.warmup + 1,
            self.profile.iterations as usize,
        ));
        let updates = Rc::new(
            AccountUpdates::connect(self.er.ws_url(), CellHeader::written_id, clock.clone())
                .await?,
        );
        for pda in &self.pdas {
            updates.account_subscribe(pda).await?;
        }
        updates.await_subscribed(self.pdas.len()).await?;

        let offset = offset + self.profile.warmup;
        let probe: Rc<RefCell<Option<Signature>>> = Rc::new(RefCell::new(None));
        let request = |iteration: u64| {
            let global_id = offset + iteration;
            let pda_index = ((global_id - 1) % self.pdas.len() as u64) as usize;
            let ix = REDLINE.hash(
                global_id,
                causal::HASH_INIT.to_bytes(),
                iters,
                &[self.pdas[pda_index]],
            );
            updates.track(global_id, self.pdas[pda_index]);
            let sender = self.senders[(global_id as usize) % self.senders.len()].clone();
            let probe = probe.clone();
            let clock = clock.clone();
            async move {
                let tx = sender.prepare(&[ix]).await?;
                if probe.borrow().is_none() {
                    *probe.borrow_mut() = Some(tx.signatures[0]);
                }
                Ok(clock.stamped(global_id, sender.deliver(tx)))
            }
        };
        let before = self.er.scrape_metrics().await?;
        let outcome = execute(
            RunConfig {
                iterations: self.profile.iterations,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            request,
        )
        .await?;
        ensure!(outcome.failed == 0);
        let drain = match before.get(TX_COUNT) {
            Some(seen) => {
                crate::await_executed(
                    &self.er,
                    seen + self.profile.iterations as f64,
                    DRAIN_TIMEOUT,
                )
                .await?
            }
            None => Duration::ZERO,
        };

        let probe_sig = (*probe.borrow()).context("no probe signature captured")?;
        let probe_cus = crate::probe_cus(&self.er, &probe_sig, label, iters).await?;

        updates.await_settled().await?;
        let after = self.er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);
        if let Some(failed) = delta.counter_all(metrics::FAILED_TRANSACTIONS) {
            ensure!(failed == 0.0);
        }
        let update_outcome = updates.finalize();
        ensure!(
            (update_outcome.observed + update_outcome.superseded)
                == (self.profile.iterations as usize)
        );

        Ok(Cell {
            label,
            iters,
            outcome,
            updates: update_outcome,
            drain,
            probe_cus,
            validator_avg_us: delta
                .histogram_avg("mbv_transaction_processing_time")
                .map(|seconds| seconds * 1e6),
            validator_txs: delta.counter(TX_COUNT),
        })
    }
}
