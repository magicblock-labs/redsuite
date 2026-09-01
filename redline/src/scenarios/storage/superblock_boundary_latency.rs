use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use json::JsonValueTrait;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, host, prep,
    profile::{self, ProfileValues},
    runner::{execute, RunConfig},
    topology,
    transport::wsraw::RawWs,
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
    TxSender,
};

use super::storage_prodsize_sustain::shape;

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const LEDGER_FILL_TIMEOUT: Duration = Duration::from_secs(30);
const NOTIFICATION_POLL: Duration = Duration::from_millis(200);
const HEALTH_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const BLOCKTIME_MS: u64 = 50;

const LEDGER_TRANSACTIONS: &str = "engine_ledger_transactions";
const SUPERBLOCKS: &str = "engine_ledger_superblocks";
const SNAPSHOT_SIZE: &str = "engine_keeper_snapshot_size";
const BLOCKED_TRANSACTIONS: &str = "engine_processor_blocked_transactions";
const BUSY_EXECUTORS: &str = "engine_processor_busy_executors";
const PENDING_TRANSACTIONS: &str = "engine_ledger_pending_transactions";
const ORDERING_DEPENDENCIES: &str = "engine_processor_ordering_dependencies";
const KEEPER_OPERATION: &str = "engine_keeper_operation_duration_micros";
const ACCOUNTSDB_OPERATION: &str =
    "engine_accountsdb_operation_duration_micros";

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: usize,
    fill: u64,
    superblock_slots: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
    min_boundaries: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    accounts: 16,
    fill: 2_000,
    superblock_slots: 16,
    iterations: 6_000,
    rate: 300,
    concurrency: 64,
    min_boundaries: 8,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 16,
    accounts: 64,
    fill: 20_000,
    superblock_slots: 32,
    iterations: 55_000,
    rate: 1_000,
    concurrency: 256,
    min_boundaries: 30,
};

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
    soak: None,
    deep: None,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Normal,
    Boundary,
}

struct Interval {
    kind: Kind,
    micros: u32,
}

#[derive(Default)]
struct Timings {
    p50: u32,
    p95: u32,
    p99: u32,
    max: u32,
    count: usize,
    mean: f64,
}

fn timings(mut samples: Vec<u32>) -> Timings {
    if samples.is_empty() {
        return Timings::default();
    }
    samples.sort_unstable();
    let quantile = |q: f64| -> u32 {
        let last = samples.len() - 1;
        let rank = (q * last as f64).round() as usize;
        samples[rank.min(last)]
    };
    let sum: u64 = samples.iter().map(|value| *value as u64).sum();
    Timings {
        p50: quantile(0.50),
        p95: quantile(0.95),
        p99: quantile(0.99),
        max: *samples.last().expect("samples is non-empty"),
        count: samples.len(),
        mean: sum as f64 / samples.len() as f64,
    }
}

fn operation_mean_us(
    delta: &MetricsDelta,
    metric: &str,
    op: &str,
) -> Option<f64> {
    let count = delta.counter(&format!("{metric}_count{{op=\"{op}\"}}"))?;
    if count <= 0.0 {
        return None;
    }
    let sum = delta.counter(&format!("{metric}_sum{{op=\"{op}\"}}"))?;
    Some(sum / count)
}

fn operation_count(
    delta: &MetricsDelta,
    metric: &str,
    op: &str,
) -> Option<f64> {
    delta.counter(&format!("{metric}_count{{op=\"{op}\"}}"))
}

#[derive(Default)]
struct Health {
    blocked_peak: f64,
    busy_peak: f64,
    pending_peak: f64,
    samples: usize,
}

async fn observe_slots(
    ws_url: String,
    superblock_slots: u64,
    stop: Rc<Cell<bool>>,
) -> Result<Vec<Interval>> {
    let mut socket = RawWs::connect(&ws_url).await?;
    socket.slot_subscribe().await?;
    let mut intervals = Vec::new();
    let mut previous: Option<(u64, Instant)> = None;
    while !stop.get() {
        let notification = socket.next_notification(NOTIFICATION_POLL).await?;
        let Some((_, _, payload)) = notification else {
            continue;
        };
        let arrived = Instant::now();
        let Some(slot) = payload.get("slot").as_u64() else {
            continue;
        };
        if let Some((previous_slot, previous_arrival)) = previous {
            if slot == previous_slot + 1 {
                let kind = if previous_slot % superblock_slots == 0 {
                    Kind::Boundary
                } else {
                    Kind::Normal
                };
                intervals.push(Interval {
                    kind,
                    micros: arrived.duration_since(previous_arrival).as_micros()
                        as u32,
                });
            }
        }
        previous = Some((slot, arrived));
    }
    Ok(intervals)
}

async fn observe_health(er: &ErCtx, stop: Rc<Cell<bool>>) -> Health {
    let mut health = Health::default();
    while !stop.get() {
        if let Ok(metrics) = er.scrape_metrics().await {
            let peak = |name: &str, current: f64| {
                metrics.value_sum(name).unwrap_or_default().max(current)
            };
            health.blocked_peak =
                peak(BLOCKED_TRANSACTIONS, health.blocked_peak);
            health.busy_peak = peak(BUSY_EXECUTORS, health.busy_peak);
            health.pending_peak =
                peak(PENDING_TRANSACTIONS, health.pending_peak);
            health.samples += 1;
        }
        tokio::time::sleep(HEALTH_SAMPLE_INTERVAL).await;
    }
    health
}

pub struct SuperblockBoundaryLatency;

#[async_trait(?Send)]
impl Scenario for SuperblockBoundaryLatency {
    fn name(&self) -> &str {
        "redline/superblock_boundary_latency"
    }

    async fn run(&self, base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);

        let prep_payers =
            prep::funded_payers(base, profile.payers, PREP_PAYER_LAMPORTS)
                .await?;
        let private = topology::private_er(
            base,
            topology::ErOptions {
                label: "superblock-boundary".to_owned(),
                env: vec![
                    (
                        "MBV_ENGINE__BLOCKSTORE__BLOCKTIME".to_owned(),
                        format!("{BLOCKTIME_MS}ms"),
                    ),
                    (
                        "MBV_ENGINE__BLOCKSTORE__SUPERBLOCK".to_owned(),
                        profile.superblock_slots.to_string(),
                    ),
                ],
                request_timeout: None,
            },
        )
        .await?;
        let cell_er = private.ctx();

        let pool = crate::init_delegated_accounts_batched(
            base,
            &prep_payers,
            profile.accounts,
            crate::ACCOUNT_SPACE,
            cell_er.identity(),
        )
        .await?;
        for pda in &pool {
            check::poll(
                &format!("the ER clones the delegated pda {pda}"),
                CLONE_TIMEOUT,
                || async {
                    matches!(cell_er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                },
            )
            .await?;
        }

        let payers: Vec<Rc<keypair::Keypair>> =
            prep_payers.into_iter().map(Rc::new).collect();
        let senders: Vec<TxSender> = payers
            .iter()
            .map(|payer| cell_er.sender(payer.clone()))
            .collect();

        let ledger_txs_at_boot = cell_er
            .scrape_metrics()
            .await?
            .get(LEDGER_TRANSACTIONS)
            .unwrap_or(0.0);
        let fill = execute(
            RunConfig {
                iterations: profile.fill,
                rate: profile.rate,
                concurrency: profile.concurrency,
            },
            |id| {
                let sender = senders[(id as usize) % senders.len()].clone();
                let ix = shape(&pool, id);
                async move { sender.submit(&[ix]).await.map(|_| ()) }
            },
        )
        .await;
        check_eq!(
            fill.failed,
            0,
            "fill deliveries failed: {:?}",
            fill.first_error
        )?;
        check::poll(
            &format!(
                "the ledger records the {} fill transactions",
                profile.fill
            ),
            LEDGER_FILL_TIMEOUT,
            || async {
                matches!(
                    cell_er.scrape_metrics().await,
                    Ok(metrics) if metrics
                        .get(LEDGER_TRANSACTIONS)
                        .unwrap_or(0.0)
                        - ledger_txs_at_boot
                        >= profile.fill as f64
                )
            },
        )
        .await?;

        let storage_before = host::dir_size_bytes(private.storage_dir())?;
        let before = cell_er.scrape_metrics().await?;
        let stop = Rc::new(Cell::new(false));
        let observer = observe_slots(
            cell_er.ws_url().to_owned(),
            profile.superblock_slots,
            stop.clone(),
        );
        let health = observe_health(cell_er, stop.clone());
        let load = async {
            let outcome = execute(
                RunConfig {
                    iterations: profile.iterations,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                |id| {
                    let sender = senders[(id as usize) % senders.len()].clone();
                    let ix = shape(&pool, profile.fill + id);
                    async move { sender.submit(&[ix]).await.map(|_| ()) }
                },
            )
            .await;
            stop.set(true);
            outcome
        };
        let (outcome, intervals, health) = tokio::join!(load, observer, health);
        let intervals = intervals?;
        let after = cell_er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);
        let storage_after = host::dir_size_bytes(private.storage_dir())?;

        check_eq!(
            outcome.failed,
            0,
            "measured deliveries failed: {:?}",
            outcome.first_error
        )?;

        let boundary = timings(
            intervals
                .iter()
                .filter(|interval| interval.kind == Kind::Boundary)
                .map(|interval| interval.micros)
                .collect(),
        );
        let normal = timings(
            intervals
                .iter()
                .filter(|interval| interval.kind == Kind::Normal)
                .map(|interval| interval.micros)
                .collect(),
        );
        check!(
            boundary.count >= profile.min_boundaries,
            "observed {} superblock boundaries, fewer than the {} this \
             profile must capture under one unchanged load",
            boundary.count,
            profile.min_boundaries
        )?;
        check!(
            normal.count > 0,
            "observed no normal block intervals to compare boundaries against"
        )?;

        let sealed = delta.counter(SUPERBLOCKS).unwrap_or_default();
        let finalize_us =
            operation_mean_us(&delta, KEEPER_OPERATION, "finalize_superblock");
        let finalize_count =
            operation_count(&delta, KEEPER_OPERATION, "finalize_superblock");
        let snapshot_us =
            operation_mean_us(&delta, ACCOUNTSDB_OPERATION, "snapshot");
        let checksum_us =
            operation_mean_us(&delta, ACCOUNTSDB_OPERATION, "checksum");
        let snapshot_bytes = delta.gauge(SNAPSHOT_SIZE).unwrap_or_default();
        let storage_growth = storage_after.saturating_sub(storage_before);
        let stall_ratio = if normal.p50 > 0 {
            boundary.p50 as f64 / normal.p50 as f64
        } else {
            0.0
        };

        eprintln!(
            "[redsuite] {}: superblock every {} slots at {BLOCKTIME_MS} ms — \
             {} boundaries / {} normal blocks; boundary p50 {} us / p95 {} us \
             / p99 {} us / max {} us, normal p50 {} us / p95 {} us / p99 {} us \
             / max {} us ({:.2}x), finalize {} us over {} seals, snapshot \
             {:.1} MB, tx p50 {} us / max {} us",
            self.name(),
            profile.superblock_slots,
            boundary.count,
            normal.count,
            boundary.p50,
            boundary.p95,
            boundary.p99,
            boundary.max,
            normal.p50,
            normal.p95,
            normal.p99,
            normal.max,
            stall_ratio,
            finalize_us
                .map(|us| format!("{us:.0}"))
                .unwrap_or_else(|| "n/a".to_owned()),
            finalize_count.unwrap_or_default(),
            snapshot_bytes / 1e6,
            outcome.delivery.median,
            outcome.delivery.max,
        );

        Ok(ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("shape", "account data copy, 1 source into 2 dests")
            .setting("blocktime ms", BLOCKTIME_MS)
            .setting("superblock slots", profile.superblock_slots)
            .setting("fill txs", profile.fill)
            .setting("measured txs", profile.iterations)
            .setting("offered tps", profile.rate)
            .setting("accounts", profile.accounts)
            .setting("payers", profile.payers)
            .setting("concurrency", profile.concurrency)
            .observe("delivery us", Unit::Micros, outcome.delivery)
            .metric("boundary count", Unit::Count, boundary.count as f64)
            .metric("boundary p50 us", Unit::Micros, boundary.p50 as f64)
            .metric("boundary p95 us", Unit::Micros, boundary.p95 as f64)
            .metric("boundary p99 us", Unit::Micros, boundary.p99 as f64)
            .metric("boundary max us", Unit::Micros, boundary.max as f64)
            .metric("boundary mean us", Unit::Micros, boundary.mean)
            .metric("normal count", Unit::Count, normal.count as f64)
            .metric("normal p50 us", Unit::Micros, normal.p50 as f64)
            .metric("normal p95 us", Unit::Micros, normal.p95 as f64)
            .metric("normal p99 us", Unit::Micros, normal.p99 as f64)
            .metric("normal max us", Unit::Micros, normal.max as f64)
            .metric("normal mean us", Unit::Micros, normal.mean)
            .metric("boundary p50 over normal p50", Unit::Count, stall_ratio)
            .metric_if("finalize superblock mean us", Unit::Micros, finalize_us)
            .metric_if("finalize superblock count", Unit::Count, finalize_count)
            .metric_if("snapshot mean us", Unit::Micros, snapshot_us)
            .metric_if("checksum mean us", Unit::Micros, checksum_us)
            .metric("snapshot size bytes", Unit::Count, snapshot_bytes)
            .metric("superblocks sealed", Unit::Count, sealed)
            .metric("storage growth bytes", Unit::Count, storage_growth as f64)
            .metric("achieved tps", Unit::Tps, outcome.achieved_rps())
            .metric("delivered txs", Unit::Count, outcome.delivered as f64)
            .metric("tx gap max us", Unit::Micros, outcome.delivery.max as f64)
            .metric(
                "tx gap p95 us",
                Unit::Micros,
                outcome.delivery.quantile95 as f64,
            )
            .metric("blocked peak", Unit::Count, health.blocked_peak)
            .metric("busy peak", Unit::Count, health.busy_peak)
            .metric("pending peak", Unit::Count, health.pending_peak)
            .metric("health samples", Unit::Count, health.samples as f64)
            .metric_if(
                "ordering dependencies",
                Unit::Count,
                delta.counter(ORDERING_DEPENDENCIES),
            )
            .metric_if(
                "failed transactions",
                Unit::Count,
                delta.counter_all(crate::metrics::FAILED_TRANSACTIONS),
            ))
    }
}
