mod workers;
pub use workers::{
    execute_threaded, spawn_workers, split_iterations, ThreadRunConfig, Worker, WorkerBudgets,
    Workers,
};

use std::{
    any::Any,
    future::{pending, Future, Ready},
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure, Context};
use futures_util::{stream::FuturesUnordered, StreamExt};
use tokio::{sync::watch, task, task::coop::cooperative, time};

pub use crate::transport::rate::{split_budget, Pacing};
use crate::{
    stats::{ObservationsStats, StreamingStats},
    transport::rate::{AdmissionStats, Pacer},
    Result,
};

const SUSTAINED_FRACTION: f64 = 0.9;

pub const SEND_ORIGIN: &str = "client, immediately before sendTransaction";
pub const REQUEST_ORIGIN: &str = "client, immediately before the request";

pub trait Delivery: 'static {
    const ORIGIN: &'static str = REQUEST_ORIGIN;
    type Fut: Future<Output = Result<()>> + 'static;
    fn deliver(self, sent: Instant) -> Self::Fut;
}

pub struct SendTransaction<D>(pub D);

impl<D: Delivery> Delivery for SendTransaction<D> {
    const ORIGIN: &'static str = SEND_ORIGIN;
    type Fut = D::Fut;

    fn deliver(self, sent: Instant) -> D::Fut {
        self.0.deliver(sent)
    }
}

impl<F, Fut> Delivery for F
where
    F: FnOnce(Instant) -> Fut + 'static,
    Fut: Future<Output = Result<()>> + 'static,
{
    type Fut = Fut;

    fn deliver(self, sent: Instant) -> Fut {
        self(sent)
    }
}

pub struct RunConfig {
    pub until: Option<(Instant, watch::Receiver<bool>)>,
    pub iterations: u64,
    pub rate: Pacing,
    pub concurrency: usize,
}

struct Job {
    scheduling: u32,
    delivery: Option<u32>,
    sync: Option<u32>,
    outcome: Result<()>,
}

impl Job {
    async fn run<D, Request, Sync>(admitted: Instant, request: Request, sync: Option<Sync>) -> Self
    where
        D: Delivery,
        Request: Future<Output = Result<D>>,
        Sync: Future<Output = Result<()>>,
    {
        let mut job = Self {
            scheduling: admitted.elapsed().as_micros() as u32,
            delivery: None,
            sync: None,
            outcome: Ok(()),
        };
        job.outcome = async {
            let send = request.await?;
            let started = Instant::now();
            send.deliver(started).await?;
            job.delivery = Some(started.elapsed().as_micros() as u32);
            if let Some(sync) = sync {
                sync.await.context("sync")?;
                job.sync = Some(started.elapsed().as_micros() as u32);
            }
            Ok(())
        }
        .await;
        job
    }
}

#[derive(Debug, Default)]
pub struct RunOutcome<S = ObservationsStats> {
    pub delivered: u64,
    /// Unsuccessful requests; panics abort the run instead of producing a tally.
    /// Together with `delivered`, this gives the admitted count.
    pub failed: u64,
    pub first_error: Option<String>,
    pub delivery: S,
    /// Admission to first poll, excluding pacing and capacity waits.
    pub scheduling: S,
    pub origin: &'static str,
    // closed loop only: send-start → all confirmations for the id
    pub sync: Option<S>,
    pub offered: Pacing,
    pub rps: ObservationsStats,
    /// Driver time through completion of all admitted requests, including drain.
    pub wall: Duration,
    /// Driver time before admission stops, excluding in-flight request drain.
    pub admission_wall: Duration,
}

impl RunOutcome {
    pub fn achieved_rps(&self) -> f64 {
        self.delivered as f64 / self.wall.as_secs_f64()
    }

    pub fn flags(&self) -> Vec<(&'static str, String)> {
        let mut flags = Vec::new();
        let achieved = self.achieved_rps();
        let (rate, interval) = match self.offered {
            Pacing::PerSecond(offered) => (f64::from(offered), "pacing"),
            Pacing::Unlimited => (achieved, "achieved inter-arrival"),
        };
        if rate > 0.0 {
            let interval_micros = (1e6 / rate) as i64;
            if i64::from(self.scheduling.quantile95) > interval_micros {
                flags.push((
                    "client saturated",
                    format!(
                        "scheduling delay p95 {} us exceeds the \
                         {interval_micros} us {interval} interval",
                        self.scheduling.quantile95
                    ),
                ));
            }
        }
        if let Pacing::PerSecond(offered) = self.offered {
            if achieved < f64::from(offered) * SUSTAINED_FRACTION {
                flags.push((
                    "offered rate not sustained",
                    format!("achieved {achieved:.0} of {offered} offered tps"),
                ));
            }
        }
        flags
    }
}

pub type RawRunOutcome = RunOutcome<StreamingStats>;

impl RawRunOutcome {
    pub fn merge(&mut self, other: RawRunOutcome) {
        self.delivered += other.delivered;
        self.failed += other.failed;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
        self.delivery.merge(other.delivery);
        self.scheduling.merge(other.scheduling);
        if self.origin.is_empty() {
            self.origin = other.origin;
        }
        self.sync = match (self.sync.take(), other.sync) {
            (Some(mut own_sync), Some(other_sync)) => {
                own_sync.merge(other_sync);
                Some(own_sync)
            }
            (own_sync, other_sync) => own_sync.or(other_sync),
        };
        self.offered = self.offered.combine(other.offered);
        self.rps = self.rps.add_rates(other.rps);
        self.wall = self.wall.max(other.wall);
        self.admission_wall = self.admission_wall.max(other.admission_wall);
    }

    pub fn finalize(self) -> RunOutcome {
        RunOutcome {
            delivered: self.delivered,
            failed: self.failed,
            first_error: self.first_error,
            delivery: self.delivery.finalize(false),
            scheduling: self.scheduling.finalize(false),
            origin: self.origin,
            sync: self.sync.map(|sync| sync.finalize(false)),
            offered: self.offered,
            rps: self.rps,
            wall: self.wall,
            admission_wall: self.admission_wall,
        }
    }

    fn record(&mut self, job: Job) {
        self.scheduling.push(job.scheduling);
        if let Some(micros) = job.delivery {
            self.delivery.push(micros);
        }
        if let (Some(stats), Some(micros)) = (&mut self.sync, job.sync) {
            stats.push(micros);
        }
        match job.outcome {
            Ok(()) => self.delivered += 1,
            Err(error) => {
                self.failed += 1;
                self.first_error.get_or_insert_with(|| format!("{error:#}"));
            }
        }
    }
}

pub fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

enum Completion {
    Iterations(u64, Option<(Instant, watch::Receiver<bool>)>),
    Until {
        deadline: Option<Instant>,
        stop: watch::Receiver<bool>,
    },
}

impl Completion {
    fn done(&self, admitted: u64) -> bool {
        match self {
            Self::Iterations(total, until) => {
                admitted >= *total
                    || until.as_ref().is_some_and(|(deadline, stop)| {
                        Instant::now() >= *deadline || *stop.borrow() || stop.has_changed().is_err()
                    })
            }
            Self::Until { deadline, stop } => {
                deadline.is_some_and(|deadline| Instant::now() >= deadline)
                    || *stop.borrow()
                    || stop.has_changed().is_err()
            }
        }
    }

    async fn stopped(&self) {
        match self {
            Completion::Iterations(_, None) => pending().await,
            Completion::Iterations(_, Some((deadline, stop))) => {
                let mut stop = stop.clone();
                tokio::select! { _ = stop.wait_for(|stop| *stop) => {}, _ = time::sleep_until((*deadline).into()) => {} }
            }
            Completion::Until { deadline, stop } => {
                let mut stop = stop.clone();
                tokio::select! {
                    _ = stop.wait_for(|stop| *stop) => {}
                    _ = async {
                        match deadline {
                            Some(deadline) => time::sleep_until((*deadline).into()).await,
                            None => pending().await,
                        }
                    } => {}
                }
            }
        }
    }
}

type NoSync = fn(u64) -> Ready<Result<()>>;
const NO_SYNC: Option<NoSync> = None;

async fn execute_inner<Request, RequestFut, D, Sync, SyncFut>(
    completion: Completion,
    pacing: Pacing,
    concurrency: usize,
    mut request: Request,
    mut sync: Option<Sync>,
) -> Result<RawRunOutcome>
where
    Request: FnMut(u64) -> RequestFut,
    RequestFut: Future<Output = Result<D>> + 'static,
    D: Delivery,
    Sync: FnMut(u64) -> SyncFut,
    SyncFut: Future<Output = Result<()>> + 'static,
{
    ensure!(
        concurrency != 0,
        "an admission budget needs a concurrency above zero"
    );
    let mut pacer = Pacer::new(pacing)?;
    let mut stats = AdmissionStats::new();
    let mut tally = RawRunOutcome {
        sync: sync.is_some().then(StreamingStats::new),
        origin: D::ORIGIN,
        ..RawRunOutcome::default()
    };
    let started = Instant::now();

    let mut jobs = FuturesUnordered::new();
    let mut admitted = 0u64;
    let mut window = StreamingStats::new();
    let mut interval = time::interval(Duration::from_secs(5));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    interval.tick().await;
    let mut last_completion = Instant::now();
    let mut stalled = false;
    let stopped = completion.stopped();
    tokio::pin!(stopped);
    'admit: while !completion.done(admitted) {
        let tick = pacer.wait();
        tokio::pin!(tick);
        loop {
            tokio::select! {
                biased;
                _ = &mut stopped => break 'admit,
                Some(job) = cooperative(jobs.next()), if !jobs.is_empty() => {
                    let job: Job = job;
                    if let Some(delivery) = job.delivery { window.push(delivery); }
                    last_completion = Instant::now();
                    tally.record(job);
                },
                _ = interval.tick() => {
                    let stats = std::mem::take(&mut window).finalize(false);
                    tracing::info!(admitted, delivered = tally.delivered, failed = tally.failed, outstanding = jobs.len(), count = stats.count, p50_us = stats.median, p95_us = stats.quantile95, max_us = stats.max, "load interval");
                    if !jobs.is_empty() && last_completion.elapsed() >= crate::transport::NETWORK_TIMEOUT { stalled = true; break 'admit; }
                },
                _ = &mut tick, if jobs.len() < concurrency => break,
            }
        }
        if completion.done(admitted) {
            break;
        }
        stats.record();
        let admitted_at = Instant::now();
        admitted += 1;
        let request_fut = request(admitted);
        let sync_fut = sync.as_mut().map(|sync| sync(admitted));
        if jobs.is_empty() {
            last_completion = Instant::now();
        }
        jobs.push(Job::run(admitted_at, request_fut, sync_fut));
        // Admission can be immediately ready without touching a Tokio resource.
        task::consume_budget().await;
    }
    let admission_wall = started.elapsed();
    let rps = stats.finish();
    let drain_deadline = Instant::now() + crate::transport::NETWORK_TIMEOUT;
    while !jobs.is_empty() {
        tokio::select! {
            Some(job) = cooperative(jobs.next()) => tally.record(job),
            _ = time::sleep_until(drain_deadline.into()) => return Err(anyhow!("admitted workload did not drain within {:?}", crate::transport::NETWORK_TIMEOUT)),
        }
    }
    ensure!(
        !stalled,
        "workload stopped completing requests while work was outstanding"
    );

    Ok(RawRunOutcome {
        offered: pacing,
        rps,
        wall: started.elapsed(),
        admission_wall,
        ..tally
    })
}

pub async fn execute<F, Fut, D>(cfg: RunConfig, request: F) -> Result<RunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<D>> + 'static,
    D: Delivery,
{
    Ok(execute_raw(cfg, request).await?.finalize())
}

pub async fn execute_raw<F, Fut, D>(cfg: RunConfig, request: F) -> Result<RawRunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<D>> + 'static,
    D: Delivery,
{
    execute_inner(
        Completion::Iterations(cfg.iterations, cfg.until),
        cfg.rate,
        cfg.concurrency,
        request,
        NO_SYNC,
    )
    .await
}

/// Stop admission on notification or channel closure, then drain admitted work.
pub async fn execute_until_raw<F, Fut, D>(
    pacing: Pacing,
    concurrency: usize,
    stop: watch::Receiver<bool>,
    request: F,
) -> Result<RawRunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<D>> + 'static,
    D: Delivery,
{
    execute_inner(
        Completion::Until { deadline: None, stop },
        pacing,
        concurrency,
        request,
        NO_SYNC,
    )
    .await
}

/// Stop admission at the deadline or stop notification, then drain admitted work.
/// Closing the stop channel also stops admission.
///
/// Await this future after signaling stop: dropping it aborts in-flight jobs.
pub async fn execute_for_raw<F, Fut, D>(
    pacing: Pacing,
    concurrency: usize,
    deadline: Instant,
    stop: watch::Receiver<bool>,
    request: F,
) -> Result<RawRunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<D>> + 'static,
    D: Delivery,
{
    execute_inner(
        Completion::Until { deadline: Some(deadline), stop },
        pacing,
        concurrency,
        request,
        NO_SYNC,
    )
    .await
}

pub async fn execute_and_sync<F, Fut, D, S, SFut>(
    cfg: RunConfig,
    request: F,
    sync: S,
) -> Result<RunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<D>> + 'static,
    D: Delivery,
    S: FnMut(u64) -> SFut,
    SFut: Future<Output = Result<()>> + 'static,
{
    Ok(execute_inner(
        Completion::Iterations(cfg.iterations, cfg.until),
        cfg.rate,
        cfg.concurrency,
        request,
        Some(sync),
    )
    .await?
    .finalize())
}

pub fn merge_outcomes(outcomes: impl IntoIterator<Item = RawRunOutcome>) -> RunOutcome {
    outcomes
        .into_iter()
        .reduce(|mut merged, outcome| {
            merged.merge(outcome);
            merged
        })
        .map(RawRunOutcome::finalize)
        .unwrap_or_default()
}
