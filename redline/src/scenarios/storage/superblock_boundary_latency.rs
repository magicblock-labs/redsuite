use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::ensure;
use json::JsonValueTrait;
use pubkey::Pubkey;
use redsuite_core::{
    host,
    profile::ProfileValues,
    programs::REDLINE,
    redline::Accounts,
    report::Unit,
    runner::{execute, Pacing, RunConfig, RunOutcome},
    sampler::{Sampled, Sampler, Trailing},
    topology::{self, ErOptions, PrivateEr},
    transport::wsraw::RawWs,
    BaseCtx, MetricsDelta, PrivateErScenario, Result, ScenarioReport, TxSender,
};
use tracing::{debug, instrument};
use url::Url;

use crate::{metrics, ACCOUNT_SPACE};

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const LEDGER_FILL_TIMEOUT: Duration = Duration::from_secs(30);
const NOTIFICATION_POLL: Duration = Duration::from_millis(200);
const HEALTH_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const BLOCKTIME_MS: u64 = 50;
const BLOCKTIME: Duration = Duration::from_millis(BLOCKTIME_MS);

const LEDGER_TRANSACTIONS: &str = "engine_ledger_transactions";
const SUPERBLOCKS: &str = "engine_ledger_superblocks";
const SNAPSHOT_SIZE: &str = "engine_keeper_snapshot_size";
const BLOCKED_TRANSACTIONS: &str = "engine_processor_blocked_transactions";
const BUSY_EXECUTORS: &str = "engine_processor_busy_executors";
const PENDING_TRANSACTIONS: &str = "engine_ledger_pending_transactions";
const ORDERING_DEPENDENCIES: &str = "engine_processor_ordering_dependencies";
const FINALIZE_SUPERBLOCK: &str =
    r#"engine_keeper_operation_duration_micros{op="finalize_superblock"}"#;
const FINALIZE_SUPERBLOCK_COUNT: &str =
    r#"engine_keeper_operation_duration_micros_count{op="finalize_superblock"}"#;
const SNAPSHOT: &str = r#"engine_accountsdb_operation_duration_micros{op="snapshot"}"#;
const CHECKSUM: &str = r#"engine_accountsdb_operation_duration_micros{op="checksum"}"#;

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

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

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

#[derive(Default)]
struct Health {
    blocked_peak: f64,
    busy_peak: f64,
    pending_peak: f64,
}

async fn observe_slots(
    ws_url: Url,
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
                    micros: arrived.duration_since(previous_arrival).as_micros() as u32,
                });
            }
        }
        previous = Some((slot, arrived));
    }
    Ok(intervals)
}

pub struct SuperblockBoundaryLatency {
    profile: &'static Profile,
    pool: Vec<Pubkey>,
    senders: Vec<TxSender>,
    private: PrivateEr,
}

impl PrivateErScenario<ScenarioReport> for SuperblockBoundaryLatency {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);

        let prep_payers = base.funded_payers(profile.payers, PREP_PAYER_LAMPORTS).await?;
        let private = topology::private_er(
            &base,
            ErOptions {
                label: "superblock-boundary".to_owned(),
                settings: Box::new(move |params| {
                    params.engine.blockstore.blocktime = BLOCKTIME;
                    params.engine.blockstore.superblock = profile.superblock_slots;
                }),
                ..Default::default()
            },
        )
        .await?;
        let cell_er = private.ctx();

        let pool = Accounts::new(ACCOUNT_SPACE, cell_er.identity())
            .init_batched(base.as_ref(), &prep_payers, profile.accounts, true)
            .await?;
        cell_er.await_clones(&pool, ACCOUNT_SPACE as usize).await?;

        let senders: Vec<TxSender> =
            prep_payers.into_iter().map(|payer| cell_er.sender(Rc::new(payer))).collect();

        Ok(Self { profile, pool, senders, private })
    }

    async fn run(self) -> Result<ScenarioReport> {
        self.fill().await?;
        let measurement = self.measure().await?;
        ensure!(measurement.outcome.failed == 0);
        ensure!(measurement.boundary.count >= self.profile.min_boundaries);
        ensure!(measurement.normal.count > 0);

        Ok(self.report(&measurement))
    }
}

struct Measurement {
    outcome: RunOutcome,
    boundary: Timings,
    normal: Timings,
    health: Sampled<Health>,
    delta: MetricsDelta,
    storage_growth: u64,
}

impl SuperblockBoundaryLatency {
    #[instrument(skip_all, err)]
    async fn fill(&self) -> Result<()> {
        let ledger_txs_at_boot = self
            .private
            .ctx()
            .scrape_metrics()
            .await?
            .get(LEDGER_TRANSACTIONS)
            .unwrap_or(0.0);
        let fill = execute(
            RunConfig {
                iterations: self.profile.fill,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            |id| {
                let sender = self.senders[(id as usize) % self.senders.len()].clone();
                let (ix, _) = REDLINE.copy_three(&self.pool, id);
                sender.staged([ix])
            },
        )
        .await?;
        ensure!(fill.failed == 0);
        crate::await_executed(
            self.private.ctx(),
            ledger_txs_at_boot + self.profile.fill as f64,
            LEDGER_FILL_TIMEOUT,
        )
        .await?;

        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn measure(&self) -> Result<Measurement> {
        let storage_before = host::dir_size_bytes(self.private.storage_dir())?;
        let before = self.private.ctx().scrape_metrics().await?;
        let stop = Rc::new(Cell::new(false));
        let observer = observe_slots(
            self.private.ctx().ws_url().clone(),
            self.profile.superblock_slots,
            stop.clone(),
        );
        let health = Sampler::spawn(
            self.private.ctx().metrics().clone(),
            HEALTH_SAMPLE_INTERVAL,
            Trailing::Skip,
            |metrics, health: &mut Health| {
                let peak = |name: &str, current: f64| {
                    metrics.value_sum(name).unwrap_or_default().max(current)
                };
                health.blocked_peak = peak(BLOCKED_TRANSACTIONS, health.blocked_peak);
                health.busy_peak = peak(BUSY_EXECUTORS, health.busy_peak);
                health.pending_peak = peak(PENDING_TRANSACTIONS, health.pending_peak);
                Some(())
            },
        );
        let load = async {
            let outcome: Result<_> = execute(
                RunConfig {
                    iterations: self.profile.iterations,
                    rate: Pacing::PerSecond(self.profile.rate),
                    concurrency: self.profile.concurrency,
                },
                |id| {
                    let sender = self.senders[(id as usize) % self.senders.len()].clone();
                    let (ix, _) = REDLINE.copy_three(&self.pool, self.profile.fill + id);
                    sender.staged([ix])
                },
            )
            .await;
            stop.set(true);
            (outcome, health.finish().await)
        };
        let ((outcome, health), intervals) = tokio::join!(load, observer);
        let outcome = outcome?;
        let intervals = intervals?;
        let health = health?;
        let after = self.private.ctx().scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);
        let storage_after = host::dir_size_bytes(self.private.storage_dir())?;
        let storage_growth = storage_after.saturating_sub(storage_before);
        let mut boundary = Vec::new();
        let mut normal = Vec::new();
        for interval in intervals {
            match interval.kind {
                Kind::Boundary => boundary.push(interval.micros),
                Kind::Normal => normal.push(interval.micros),
            }
        }

        Ok(Measurement {
            outcome,
            boundary: timings(boundary),
            normal: timings(normal),
            health,
            delta,
            storage_growth,
        })
    }

    fn report(&self, measurement: &Measurement) -> ScenarioReport {
        let boundary = &measurement.boundary;
        let normal = &measurement.normal;
        let sealed = measurement.delta.counter(SUPERBLOCKS).unwrap_or_default();
        let finalize_us = measurement.delta.histogram_avg(FINALIZE_SUPERBLOCK);
        let finalize_count = measurement.delta.counter(FINALIZE_SUPERBLOCK_COUNT);
        let snapshot_us = measurement.delta.histogram_avg(SNAPSHOT);
        let checksum_us = measurement.delta.histogram_avg(CHECKSUM);
        let snapshot_bytes = measurement.delta.gauge(SNAPSHOT_SIZE).unwrap_or_default();
        let stall_ratio =
            if normal.p50 > 0 { boundary.p50 as f64 / normal.p50 as f64 } else { 0.0 };

        debug!(
            boundary.count,
            stall_ratio, "superblock boundary workload finished",
        );

        ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("shape", "account data copy, 1 source into 2 dests")
            .setting("blocktime ms", BLOCKTIME_MS)
            .setting("superblock slots", self.profile.superblock_slots)
            .setting("fill txs", self.profile.fill)
            .setting("measured txs", self.profile.iterations)
            .setting("offered tps", self.profile.rate)
            .setting("accounts", self.profile.accounts)
            .setting("payers", self.profile.payers)
            .setting("concurrency", self.profile.concurrency)
            .observe("delivery us", Unit::Micros, measurement.outcome.delivery)
            .client_side(&measurement.outcome)
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
            .metric(
                "storage growth bytes",
                Unit::Count,
                measurement.storage_growth as f64,
            )
            .metric(
                "achieved tps",
                Unit::Tps,
                measurement.outcome.achieved_rps(),
            )
            .metric(
                "delivered txs",
                Unit::Count,
                measurement.outcome.delivered as f64,
            )
            .metric(
                "tx gap max us",
                Unit::Micros,
                measurement.outcome.delivery.max as f64,
            )
            .metric(
                "tx gap p95 us",
                Unit::Micros,
                measurement.outcome.delivery.quantile95 as f64,
            )
            .metric(
                "blocked peak",
                Unit::Count,
                measurement.health.value.blocked_peak,
            )
            .metric("busy peak", Unit::Count, measurement.health.value.busy_peak)
            .metric(
                "pending peak",
                Unit::Count,
                measurement.health.value.pending_peak,
            )
            .metric(
                "health samples",
                Unit::Count,
                measurement.health.observations as f64,
            )
            .metric_if(
                "ordering dependencies",
                Unit::Count,
                measurement.delta.counter(ORDERING_DEPENDENCIES),
            )
            .metric_if(
                "failed transactions",
                Unit::Count,
                measurement.delta.counter_all(metrics::FAILED_TRANSACTIONS),
            )
    }
}
