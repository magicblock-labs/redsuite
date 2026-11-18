mod observations;
mod worker;

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufWriter, Write},
    path::Path,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use redsuite_core::{
    config::{LoopMode, ProfileValues},
    prep,
    redline::Accounts,
    report::{self, Unit},
    runner::spawn_workers,
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use tokio::{
    sync::{mpsc, watch},
    time::{self, MissedTickBehavior},
};

use self::observations::{Clock, Count, Sample};
use crate::RunControl;

const WRITABLE: usize = 16;
const SEED: u64 = 20_261_112;
const ACCOUNT_SPACE: u32 = 128;
const PAYER_LAMPORTS: u64 = 4_000_000_000;

#[derive(Clone, Copy)]
pub(super) struct Profile {
    pub(super) workers: usize,
    pub(super) accounts: usize,
    pub(super) payers: usize,
    pub(super) sockets: usize,
    pub(super) rate_per_worker: u32,
    pub(super) concurrency: usize,
    pub(super) http_concurrency: usize,
    warmup: Duration,
}

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: Profile {
        workers: 1,
        accounts: 32,
        payers: 16,
        sockets: 4,
        rate_per_worker: 1_000,
        concurrency: 64,
        http_concurrency: 32,
        warmup: Duration::from_secs(5),
    },
    full: Profile {
        workers: 4,
        accounts: 512,
        payers: 256,
        sockets: 16,
        rate_per_worker: 24_000,
        concurrency: 512,
        http_concurrency: 128,
        warmup: Duration::from_secs(60),
    },
};

pub struct FixedPool {
    base: Rc<BaseCtx>,
    er: ErCtx,
    run: RunControl,
    config: worker::Config,
}

impl Scenario<(ScenarioReport, Result<()>)> for FixedPool {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        ensure!(
            base.config().loop_mode == LoopMode::Open,
            "fixed_pool requires --loop open"
        );
        let run = RunControl::from_config(&base.config())?;
        let profile = *PROFILES.select(base.config().profile);
        let payers = base.funded_payers(profile.payers, PAYER_LAMPORTS).await?;
        let accounts = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base.as_ref(), &payers, profile.accounts, true)
            .await?;
        er.await_clones(&accounts, ACCOUNT_SPACE as usize).await?;
        let config = worker::Config {
            profile,
            rpc: er.api().url().clone(),
            ws: er.ws_url().clone(),
            accounts: accounts.into(),
            payers: prep::payer_bytes(&payers),
            stop: run.stop.clone(),
        };
        Ok(Self { base, er, run, config })
    }

    async fn run(self) -> Result<(ScenarioReport, Result<()>)> {
        let profile = self.config.profile;
        let (ready, mut workers_ready) = mpsc::channel(profile.workers);
        let (start, clock) = watch::channel(None);
        let workers = spawn_workers(profile.workers, move |worker| {
            worker::run(
                worker.index,
                self.config.clone(),
                ready.clone(),
                clock.clone(),
            )
        });
        for _ in 0..profile.workers {
            if workers_ready.recv().await.is_none() {
                drop(start);
                workers.join().await?;
                anyhow::bail!("benchmark workers failed to initialize");
            }
        }
        let clock = Clock::new(profile.warmup, self.run.duration);
        start.send_replace(Some(clock));
        let directory = report::campaign_dir();
        fs::create_dir_all(&directory)?;
        let api_path = directory.join("fixed_pool-api.jsonl");
        let validator_path = directory.join("fixed_pool-validator.jsonl");
        let mut validator = BufWriter::new(File::create(&validator_path)?);
        let mut joined = std::pin::pin!(workers.join());
        let mut samples = time::interval(Duration::from_secs(1));
        samples.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut metrics_enabled = true;
        let mut stop = self.run.stop.clone();
        let mut interrupted_at = None;
        let outcomes = loop {
            tokio::select! {
                result = &mut joined => break result?,
                _ = stop.wait_for(|stop| *stop), if interrupted_at.is_none() => {
                    interrupted_at = Some(Instant::now());
                }
                _ = samples.tick(), if metrics_enabled => {
                    let now = Instant::now();
                    let row = scrape(&self.er, clock, now).await;
                    if let Err(error) = write_line(&mut validator, &row) {
                        tracing::warn!(%error, "could not persist benchmark validator sample");
                        metrics_enabled = false;
                    }
                }
            }
        };
        validator.flush()?;

        let mut by_second = Vec::<Sample>::new();
        let mut worker_failures = Vec::new();
        for outcome in outcomes {
            if let Err(error) = outcome.result {
                worker_failures.push(format!("{error:#}"));
            }
            if by_second.len() < outcome.samples.len() {
                by_second.resize_with(outcome.samples.len(), Sample::default);
            }
            for (total, sample) in by_second.iter_mut().zip(&outcome.samples) {
                total.merge(sample);
            }
        }
        let measured_seconds = interrupted_at
            .unwrap_or_else(Instant::now)
            .min(clock.end)
            .saturating_duration_since(clock.begin)
            .as_secs_f64();
        let total = write_api_samples(&api_path, clock, measured_seconds, by_second)?;

        let result = check_accounting(
            &total,
            &worker_failures,
            metrics_enabled,
            *self.run.stop.borrow(),
        );
        let mut report = ScenarioReport::ok()
            .setting("profile", self.base.config().profile.name())
            .setting("completion", self.run.completion(&result))
            .setting("seed", SEED)
            .setting("workers", profile.workers)
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("writable accounts per transaction", WRITABLE)
            .setting("request mix", "80% writes / 20% getAccountInfo")
            .setting(
                "WebSockets per worker per subscription type",
                profile.sockets,
            )
            .setting(
                "offered requests per second",
                u64::from(profile.rate_per_worker) * profile.workers as u64,
            )
            .setting("API time series", api_path.display())
            .setting("validator time series", validator_path.display())
            .metric("measured seconds", Unit::Seconds, measured_seconds);
        for (name, value) in total.counters() {
            report = report.metric(name, Unit::Count, value as f64);
        }
        for (name, _, mean, max) in total.timings() {
            report = report.metric(format!("{name} mean us"), Unit::Micros, mean).metric(
                format!("{name} max us"),
                Unit::Micros,
                max,
            );
        }
        if measured_seconds > 0.0 {
            for (name, kind) in [
                ("signature-confirmed TPS", Count::Signature),
                ("status-confirmed TPS", Count::Status),
            ] {
                report =
                    report.metric(name, Unit::Tps, total.count(kind) as f64 / measured_seconds);
            }
        }
        Ok((report, result))
    }
}

fn write_api_samples(
    path: &Path,
    clock: Clock,
    measured_seconds: f64,
    mut samples: Vec<Sample>,
) -> Result<Sample> {
    let mut api = BufWriter::new(File::create(path)?);
    let windows = (measured_seconds.ceil() as usize).max(samples.len()).max(1);
    samples.resize_with(windows, Sample::default);
    let mut total = Sample::default();
    for (second, sample) in samples.iter().enumerate() {
        let counters: BTreeMap<_, _> = sample.counters().collect();
        let latencies: BTreeMap<_, _> = sample
            .timings()
            .map(|(name, count, mean, max)| {
                (
                    name,
                    json::json!({ "count": count, "mean_us": mean, "max_us": max }),
                )
            })
            .collect();
        write_line(
            &mut api,
            &json::json!({
                "second": second, "unix_seconds": clock.unix + second as f64,
                "phase": if second as f64 >= measured_seconds { "drain" } else { "measure" },
                "counters": counters, "latencies": latencies,
            }),
        )?;
        total.merge(sample);
    }
    api.flush()?;
    Ok(total)
}

fn check_accounting(
    total: &Sample,
    worker_failures: &[String],
    metrics_saved: bool,
    interrupted: bool,
) -> Result<()> {
    ensure!(
        worker_failures.is_empty(),
        "benchmark workers failed: {}",
        worker_failures.join("; ")
    );
    ensure!(metrics_saved, "benchmark validator samples were not saved");
    let count = |counter| total.count(counter);
    let submitted = count(Count::Submitted);
    ensure!(
        interrupted || submitted > 0,
        "benchmark submitted no transactions"
    );
    ensure!(
        submitted == count(Count::Accepted) + count(Count::SendError),
        "HTTP requests did not reconcile"
    );
    ensure!(
        submitted
            == count(Count::Signature)
                + count(Count::SignatureError)
                + count(Count::SignatureExpired),
        "signature notifications did not reconcile"
    );
    ensure!(
        submitted == count(Count::Status) + count(Count::StatusError) + count(Count::StatusExpired),
        "status observations did not reconcile"
    );
    ensure!(
        count(Count::AccountExpected) == count(Count::Account) + count(Count::AccountExpired),
        "account notifications did not reconcile"
    );
    Ok(())
}

async fn scrape(er: &ErCtx, clock: Clock, now: Instant) -> json::Value {
    let elapsed = if now < clock.begin {
        -clock.begin.duration_since(now).as_secs_f64()
    } else {
        now.duration_since(clock.begin).as_secs_f64()
    };
    let metrics = time::timeout(Duration::from_millis(500), er.scrape_metrics()).await;
    let values: Option<BTreeMap<_, _>> = metrics.ok().and_then(Result::ok).map(|metrics| {
        metrics
            .0
            .into_iter()
            .filter(|(name, _)| {
                name.starts_with("engine_keeper_subscription_slow_consumer_disconnects")
                    || name.starts_with("engine_ledger_operation_duration_micros_count")
                    || name.starts_with("engine_ledger_operation_duration_micros_sum")
                    || name.starts_with("engine_keeper_operation_duration_micros_count")
                    || name.starts_with("engine_keeper_operation_duration_micros_sum")
                    || name.starts_with("engine_processor_blocked_transactions")
                    || matches!(
                        name.as_str(),
                        "engine_ledger_transactions"
                            | "engine_ledger_blocks"
                            | "engine_ledger_superblocks"
                            | "engine_ledger_pending_transactions"
                    )
            })
            .collect()
    });
    json::json!({ "elapsed_seconds": elapsed, "unix_seconds": clock.unix + elapsed, "values": values })
}

fn write_line(out: &mut impl Write, value: &json::Value) -> Result<()> {
    let line = json::to_string(value).context("serializing benchmark JSON")?;
    writeln!(out, "{line}").context("writing benchmark JSON line")
}
