use std::{
    any::Any,
    cell::Cell,
    future::Future,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use tokio::{
    sync::{watch, OwnedSemaphorePermit},
    task::JoinSet,
};
use tracing::{Instrument, Span};

pub use crate::transport::rate::{split_budget, Pacing};
use crate::{
    stats::{ObservationsStats, StreamingStats},
    transport::rate::Throttle,
    Result,
};

const STOP_POLL: Duration = Duration::from_millis(20);
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
    pub iterations: u64,
    pub rate: Pacing,
    pub concurrency: usize,
}

pub struct ThreadRunConfig {
    pub threads: usize,
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
    async fn run<D: Delivery>(
        admitted: Instant,
        request: impl Future<Output = Result<D>>,
        sync: Option<impl Future<Output = Result<()>>>,
    ) -> Self {
        let mut job = Self {
            scheduling: admitted.elapsed().as_micros() as u32,
            delivery: None,
            sync: None,
            outcome: Ok(()),
        };
        job.outcome = job.deliver(request, sync).await;
        job
    }

    async fn deliver<D: Delivery>(
        &mut self,
        request: impl Future<Output = Result<D>>,
        sync: Option<impl Future<Output = Result<()>>>,
    ) -> Result<()> {
        let send = request.await?;
        let started = Instant::now();
        send.deliver(started).await?;
        self.delivery = Some(started.elapsed().as_micros() as u32);
        if let Some(sync) = sync {
            sync.await.map_err(|error| format!("sync: {error}"))?;
            self.sync = Some(started.elapsed().as_micros() as u32);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct RunOutcome<S = ObservationsStats> {
    pub delivered: u64,
    /// Unsuccessful requests; task panics abort the run instead of producing a tally.
    /// Together with `delivered`, this gives the admitted count.
    pub failed: u64,
    pub first_error: Option<String>,
    pub delivery: S,
    pub scheduling: S,
    pub origin: &'static str,
    // closed loop only: send-start → all confirmations for the id
    pub sync: Option<S>,
    pub offered: Pacing,
    pub rps: ObservationsStats,
    /// Driver time through completion of all admitted requests, including drain.
    pub wall: std::time::Duration,
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
                self.first_error.get_or_insert_with(|| error.to_string());
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
    Iterations(u64),
    Stop(Rc<Cell<bool>>),
    Until {
        deadline: Instant,
        stop: watch::Receiver<bool>,
    },
}

impl Completion {
    async fn admit(
        &mut self,
        admitted: u64,
        throttle: &mut Throttle,
    ) -> Option<OwnedSemaphorePermit> {
        match self {
            Completion::Iterations(total) if admitted >= *total => None,
            Completion::Stop(stop) if stop.get() => None,
            Completion::Until { deadline, stop } => {
                if Instant::now() >= *deadline || *stop.borrow() {
                    return None;
                }
                let permit = tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until((*deadline).into()) => None,
                    _ = stop.wait_for(|stop| *stop) => None,
                    permit = throttle.acquire() => Some(permit),
                };
                permit.filter(|_| Instant::now() < *deadline && !*stop.borrow())
            }
            _ => Some(throttle.acquire().await),
        }
    }
}

type NoSync = fn(u64) -> std::future::Ready<Result<()>>;
const NO_SYNC: Option<NoSync> = None;

async fn execute_inner<Request, RequestFut, D, Sync, SyncFut>(
    mut completion: Completion,
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
    let mut throttle = Throttle::new(pacing, concurrency)?;
    let mut tally = RawRunOutcome {
        sync: sync.is_some().then(StreamingStats::new),
        origin: D::ORIGIN,
        ..RawRunOutcome::default()
    };
    let started = Instant::now();

    let mut jobs = JoinSet::new();
    let mut admitted = 0u64;
    while let Some(permit) = completion.admit(admitted, &mut throttle).await {
        throttle.record();
        let admitted_at = Instant::now();
        admitted += 1;
        let request_fut = request(admitted);
        let sync_fut = sync.as_mut().map(|sync| sync(admitted));
        jobs.spawn_local(async move {
            let job = Job::run(admitted_at, request_fut, sync_fut).await;
            drop(permit);
            job
        });
        while let Some(joined) = jobs.try_join_next() {
            tally.record(joined?);
        }
    }
    let admission_wall = started.elapsed();
    let rps = throttle.finish();
    while let Some(joined) = jobs.join_next().await {
        tally.record(joined?);
    }

    Ok(RawRunOutcome {
        offered: pacing,
        rps,
        wall: started.elapsed(),
        admission_wall,
        ..tally
    })
}

pub async fn execute<F, Fut, D>(
    cfg: RunConfig,
    request: F,
) -> Result<RunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<D>> + 'static,
    D: Delivery,
{
    Ok(execute_raw(cfg, request).await?.finalize())
}

pub async fn execute_raw<F, Fut, D>(
    cfg: RunConfig,
    request: F,
) -> Result<RawRunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<D>> + 'static,
    D: Delivery,
{
    execute_inner(
        Completion::Iterations(cfg.iterations),
        cfg.rate,
        cfg.concurrency,
        request,
        NO_SYNC,
    )
    .await
}

/// Generate open-loop load until `stop` is observed between admissions, then drain.
/// Unlike [`execute_for_raw`], a stop does not interrupt a pending throttle wait.
pub async fn execute_until_raw<F, Fut, D>(
    pacing: Pacing,
    concurrency: usize,
    stop: Rc<Cell<bool>>,
    request: F,
) -> Result<RawRunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<D>> + 'static,
    D: Delivery,
{
    execute_inner(
        Completion::Stop(stop),
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
        Completion::Until { deadline, stop },
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
        Completion::Iterations(cfg.iterations),
        cfg.rate,
        cfg.concurrency,
        request,
        Some(sync),
    )
    .await?
    .finalize())
}

pub struct Worker {
    pub index: usize,
    pub threads: usize,
    stop: Arc<AtomicBool>,
}

impl Worker {
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }

    pub fn stop_cell(&self) -> Rc<Cell<bool>> {
        let cell = Rc::new(Cell::new(false));
        let stop = self.stop.clone();
        let bridge = cell.clone();
        tokio::task::spawn_local(async move {
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(STOP_POLL).await;
            }
            bridge.set(true);
        });
        cell
    }
}

pub struct Workers<T> {
    stop: Arc<AtomicBool>,
    handles: Vec<JoinHandle<Result<T>>>,
}

impl<T> Drop for Workers<T> {
    fn drop(&mut self) {
        self.stop();
    }
}

pub fn spawn_workers<T, Factory, Fut>(
    threads: usize,
    factory: Factory,
) -> Workers<T>
where
    T: Send + 'static,
    Factory: Fn(Worker) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<T>>,
{
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(threads);
    for index in 0..threads {
        let factory = factory.clone();
        let span = Span::current();
        let worker = Worker {
            index,
            threads,
            stop: stop.clone(),
        };
        handles.push(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("driver runtime build is infallible");
            let local = tokio::task::LocalSet::new();
            // Run future construction in the span too: the factory can log.
            runtime.block_on(local.run_until(
                async move { factory(worker).await }.instrument(span),
            ))
        }));
    }
    Workers { stop, handles }
}

impl<T> Workers<T> {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl<T: Send + 'static> Workers<T> {
    pub fn join(mut self) -> Result<Vec<T>> {
        self.stop();
        let handles = std::mem::take(&mut self.handles);
        let mut outcomes = Vec::with_capacity(handles.len());
        let mut first_panic = None;
        let mut first_error = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(outcome)) => outcomes.push(outcome),
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                }
                Err(payload) => {
                    first_panic.get_or_insert_with(|| panic_message(payload));
                }
            }
        }
        if let Some(panic) = first_panic {
            return Err(
                format!("driver worker thread panicked: {panic}").into()
            );
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(outcomes)
    }

    pub async fn join_async(self) -> Result<Vec<T>> {
        self.stop();
        tokio::task::spawn_blocking(move || self.join())
            .await
            .map_err(|error| format!("driver worker join failed: {error}"))?
    }
}

pub fn split_iterations(iterations: u64, workers: usize) -> Vec<(u64, u64)> {
    let mut first_id = 0u64;
    split_budget(iterations, workers)
        .into_iter()
        .take_while(|count| *count > 0)
        .map(|count| {
            let span = (first_id, count);
            first_id += count;
            span
        })
        .collect()
}

pub struct WorkerBudgets {
    pub iterations: Vec<(u64, u64)>,
    pub rate: Vec<Pacing>,
    pub concurrency: Vec<usize>,
}

impl WorkerBudgets {
    pub fn partition(
        threads: usize,
        iterations: u64,
        rate: Pacing,
        concurrency: usize,
    ) -> Result<Self> {
        rate.interval()?;
        if concurrency == 0 {
            return Err(
                "an admission budget needs a concurrency above zero".into()
            );
        }
        let mut workers = threads.max(1).min(concurrency);
        if let Pacing::PerSecond(rate) = rate {
            workers = workers.min(rate as usize);
        }
        let iterations = split_iterations(iterations, workers);
        let workers = iterations.len();
        Ok(Self {
            iterations,
            rate: rate.partition(workers)?,
            concurrency: split_budget(concurrency as u64, workers)
                .into_iter()
                .map(|share| share as usize)
                .collect(),
        })
    }

    pub fn workers(&self) -> usize {
        self.iterations.len()
    }
}

pub fn execute_threaded<Factory, Request, Fut, D>(
    config: ThreadRunConfig,
    factory: Factory,
) -> Result<RunOutcome>
where
    Factory: Fn(usize) -> Request + Clone + Send + 'static,
    Request: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<D>> + 'static,
    D: Delivery,
{
    let budgets = WorkerBudgets::partition(
        config.threads,
        config.iterations,
        config.rate,
        config.concurrency,
    )?;
    let outcomes = spawn_workers(budgets.workers(), move |worker| {
        let (first_id, iterations) = budgets.iterations[worker.index];
        let rate = budgets.rate[worker.index];
        let concurrency = budgets.concurrency[worker.index];
        let mut request = factory(worker.index);
        async move {
            execute_raw(
                RunConfig {
                    iterations,
                    rate,
                    concurrency,
                },
                |iteration| request(first_id + iteration),
            )
            .await
        }
    })
    .join()?;
    Ok(merge_outcomes(outcomes))
}

pub fn merge_outcomes(
    outcomes: impl IntoIterator<Item = RawRunOutcome>,
) -> RunOutcome {
    outcomes
        .into_iter()
        .reduce(|mut merged, outcome| {
            merged.merge(outcome);
            merged
        })
        .map(RawRunOutcome::finalize)
        .unwrap_or_default()
}
