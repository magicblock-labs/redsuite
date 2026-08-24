use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, prep,
    profile::{self, ProfileValues},
    report,
    runner::{execute, RunConfig, RunOutcome},
    transport::ws::{AccountUpdates, UpdateOutcome},
    BaseCtx, ChainCtx, CheckError, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport, TxSender,
};
use signature::Signature;

use crate::program::{instruction::build, layout, utils::hash_chain};

const PAYER_LAMPORTS: u64 = 2_000_000_000;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(300);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(20);
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const TX_COUNT: &str = "mbv_transaction_count";
const HASH_INIT: Pubkey = Pubkey::new_from_array([7u8; 32]);
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

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
    soak: None,
    deep: None,
};

fn consumed_cus(logs: &[String]) -> Option<f64> {
    logs.iter().find_map(|line| {
        let (_, rest) = line.split_once(" consumed ")?;
        let (cus, tail) = rest.split_once(" of ")?;
        if !tail.contains("compute units") {
            return None;
        }
        cus.parse().ok()
    })
}

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

async fn drain_processed(er: &ErCtx, target: f64) -> Result<()> {
    check::poll(
        &format!("the validator transaction count reaches {target}"),
        DRAIN_TIMEOUT,
        || async {
            matches!(
                er.scrape_metrics().await.ok().and_then(|metrics| metrics.get(TX_COUNT)),
                Some(count) if count >= target
            )
        },
    )
    .await?;
    Ok(())
}

async fn run_cell(
    er: &ErCtx,
    senders: &[TxSender],
    pdas: &[Pubkey],
    profile: &Profile,
    label: &'static str,
    iters: u32,
    offset: u64,
) -> Result<Cell> {
    let warmup_request = |iteration: u64| {
        let global_id = offset + iteration;
        let pda_index = ((global_id - 1) % pdas.len() as u64) as usize;
        let ix = build::expensive_hash_compute(
            global_id,
            HASH_INIT,
            iters,
            &[pdas[pda_index]],
        );
        let sender = senders[(global_id as usize) % senders.len()].clone();
        async move { sender.submit(&[ix]).await.map(|_| ()) }
    };
    let count_before_warmup = er.scrape_metrics().await?.get(TX_COUNT);
    let warm = execute(
        RunConfig {
            iterations: profile.warmup,
            rate: profile.rate,
            concurrency: profile.concurrency,
        },
        warmup_request,
    )
    .await;
    check_eq!(
        warm.failed,
        0,
        "{label} warmup deliveries failed: {:?}",
        warm.first_error
    )?;
    if let Some(seen) = count_before_warmup {
        drain_processed(er, seen + profile.warmup as f64).await?;
    }

    let updates = Rc::new(
        AccountUpdates::connect(er.ws_url(), crate::account_update_id).await?,
    );
    for pda in pdas {
        updates.account_subscribe(pda).await?;
    }
    updates
        .await_subscribed(pdas.len(), Duration::from_secs(5))
        .await?;

    let offset = offset + profile.warmup;
    let probe: Rc<RefCell<Option<Signature>>> = Rc::new(RefCell::new(None));
    let request = |iteration: u64| {
        let global_id = offset + iteration;
        let pda_index = ((global_id - 1) % pdas.len() as u64) as usize;
        let ix = build::expensive_hash_compute(
            global_id,
            HASH_INIT,
            iters,
            &[pdas[pda_index]],
        );
        updates.track(global_id, pdas[pda_index]);
        let sender = senders[(global_id as usize) % senders.len()].clone();
        let probe = probe.clone();
        async move {
            let sig = sender.submit(&[ix]).await?;
            if probe.borrow().is_none() {
                *probe.borrow_mut() = Some(sig);
            }
            Ok(())
        }
    };
    let before = er.scrape_metrics().await?;
    let outcome = execute(
        RunConfig {
            iterations: profile.iterations,
            rate: profile.rate,
            concurrency: profile.concurrency,
        },
        request,
    )
    .await;
    check_eq!(
        outcome.failed,
        0,
        "{label} measured deliveries failed: {:?}",
        outcome.first_error
    )?;
    let drain_started = Instant::now();
    if let Some(seen) = before.get(TX_COUNT) {
        drain_processed(er, seen + profile.iterations as f64).await?;
    }
    let drain = drain_started.elapsed();

    let probe_sig = probe
        .borrow()
        .as_ref()
        .cloned()
        .ok_or("no probe signature captured")?;
    let probe_tx = er
        .api()
        .await_transaction(&probe_sig, PROBE_TIMEOUT)
        .await?;
    check!(
        probe_tx.err.is_none(),
        "{label} probe tx failed on-chain (sha256 iters {iters} over the \
         compute budget?): {:?}\nlogs: {:#?}",
        probe_tx.err,
        probe_tx.logs
    )?;
    let probe_cus = consumed_cus(&probe_tx.logs).ok_or_else(|| {
        CheckError::new(format!(
            "{label} probe logs carry no `consumed .. compute units` line"
        ))
        .actual(format!("{:#?}", probe_tx.logs))
    })?;

    updates.await_settled(SETTLE_TIMEOUT).await?;
    let after = er.scrape_metrics().await?;
    let delta = MetricsDelta::new(before, after);
    if let Some(failed) = delta.counter("mbv_failed_transactions_count") {
        check_eq!(
            failed,
            0.0,
            "{label}: transactions failed on the validator"
        )?;
    }
    let update_outcome = updates.finalize();
    check_eq!(
        update_outcome.observed + update_outcome.superseded,
        profile.iterations as usize,
        "{label}: every tracked write must be observed or superseded — \
         missing updates mean transactions executed without writing"
    )?;

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

pub struct HighCu;

#[async_trait(?Send)]
impl Scenario for HighCu {
    fn name(&self) -> &str {
        "redline/high_cu"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);
        let payers =
            prep::funded_payers(base, profile.payers, PAYER_LAMPORTS).await?;
        let pdas = crate::init_delegated_accounts(
            base,
            &payers[0],
            profile.accounts,
            crate::ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        for pda in &pdas {
            check::poll(
                &format!("the ER clones the delegated pda {pda}"),
                Duration::from_secs(15),
                || async {
                    matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                },
            )
            .await?;
        }
        let senders: Vec<TxSender> = payers
            .into_iter()
            .map(|payer| er.sender(Rc::new(payer)))
            .collect();

        let mut offset = 0u64;
        let mut cells: Vec<Cell> = Vec::new();
        for (label, iters) in [("light", LIGHT_ITERS), ("heavy", HEAVY_ITERS)] {
            let cell =
                run_cell(er, &senders, &pdas, profile, label, iters, offset)
                    .await?;
            offset += profile.warmup + profile.iterations;
            eprintln!(
                "[redsuite] {}: {label} (sha256 iters {iters}): delivery p50 {} us / p95 {} us, {:.0} tps delivered, {:.0} tps executed, lag p50 {} us, probe {:.0} cus, validator avg {} us",
                self.name(),
                cell.outcome.delivery.median,
                cell.outcome.delivery.quantile95,
                cell.outcome.achieved_rps(),
                cell.executed_tps(profile.iterations),
                cell.updates.lag.median,
                cell.probe_cus,
                cell.validator_avg_us
                    .map(|avg| format!("{avg:.1}"))
                    .unwrap_or_else(|| "n/a".into()),
            );

            let cell_report =
                ScenarioReport::ok(&format!("{}/{label}", self.name()))
                    .setting("profile", profile.name)
                    .setting("sha256 iters", cell.iters)
                    .setting("shape", "width-1 sha256 hash-chain compute")
                    .setting("payers", profile.payers)
                    .setting("accounts", profile.accounts)
                    .setting("measured iters", profile.iterations)
                    .setting("offered tps", profile.rate)
                    .setting("concurrency", profile.concurrency)
                    .observe("delivery us", Unit::Micros, cell.outcome.delivery)
                    .observe(
                        "account-update lag us",
                        Unit::Micros,
                        cell.updates.lag,
                    )
                    .metric(
                        "achieved tps",
                        Unit::Tps,
                        cell.outcome.achieved_rps(),
                    )
                    .metric(
                        "executed tps",
                        Unit::Tps,
                        cell.executed_tps(profile.iterations),
                    )
                    .metric("drain s", Unit::Seconds, cell.drain.as_secs_f64())
                    .metric(
                        "superseded",
                        Unit::Count,
                        cell.updates.superseded as f64,
                    )
                    .metric("probe consumed cus", Unit::Count, cell.probe_cus)
                    .metric_if(
                        "validator tx processing avg us",
                        Unit::Micros,
                        cell.validator_avg_us,
                    )
                    .metric_if(
                        "validator txs in window",
                        Unit::Count,
                        cell.validator_txs,
                    );
            match report::persist_cell(self.name(), &cell_report) {
                Ok(path) => {
                    eprintln!("[redsuite]   cell report: {}", path.display())
                }
                Err(err) => eprintln!(
                    "[redsuite]   warning: cell report not persisted: {err}"
                ),
            }
            cells.push(cell);
        }

        let heavy_start = offset - profile.iterations + 1;
        let heavy_end = offset;
        let expected_hash = hash_chain(HASH_INIT.to_bytes(), HEAVY_ITERS);
        let pool = pdas.len() as u64;
        let end_index = (heavy_end - 1) % pool;
        for (index, pda) in pdas.iter().enumerate() {
            let index = index as u64;
            let last_id = if end_index >= index {
                heavy_end - (end_index - index)
            } else {
                heavy_end - (end_index + pool - index)
            };
            if last_id < heavy_start {
                continue;
            }
            let on_er = er.account(pda).await?.ok_or("pda not on er")?;
            let id_bytes = &on_er.data
                [layout::ID_OFFSET..layout::ID_OFFSET + layout::ID_SIZE];
            check_eq!(
                id_bytes,
                last_id.to_le_bytes(),
                "er copy must hold the last id written to pda {index}"
            )?;
            let hash_bytes = &on_er.data
                [layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE];
            check_eq!(
                hash_bytes,
                expected_hash,
                "pda {index} must hold the full {HEAVY_ITERS}-iteration hash \
                 chain"
            )?;
        }

        let light = &cells[0];
        let heavy = &cells[1];
        let cu_ratio = heavy.probe_cus / light.probe_cus;
        check!(
            cu_ratio >= CU_CONTRAST_FLOOR,
            "heavy cell consumed {:.0} cus per tx vs light {:.0} — not the \
             >= {CU_CONTRAST_FLOOR}x compute contrast this scenario is about",
            heavy.probe_cus,
            light.probe_cus,
        )?;
        let validator_avg_ratio =
            match (light.validator_avg_us, heavy.validator_avg_us) {
                (Some(light_avg), Some(heavy_avg)) if light_avg > 0.0 => {
                    Some(heavy_avg / light_avg)
                }
                _ => None,
            };

        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("shape", "width-1 sha256 hash-chain compute")
            .setting("light iters", LIGHT_ITERS)
            .setting("heavy iters", HEAVY_ITERS)
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("warmup iters per cell", profile.warmup)
            .setting("measured iters per cell", profile.iterations)
            .setting("offered tps", profile.rate)
            .setting("concurrency", profile.concurrency);
        for cell in &cells {
            summary = summary
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
                    cell.executed_tps(profile.iterations),
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
        summary =
            summary.metric("heavy/light probe cu ratio", Unit::Ratio, cu_ratio);
        if let Some(ratio) = validator_avg_ratio {
            summary = summary.metric(
                "heavy/light validator avg ratio",
                Unit::Ratio,
                ratio,
            );
        }
        Ok(summary)
    }
}
