use std::{
    any::Any,
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use tokio::task::{JoinError, JoinSet};

pub use crate::transport::rate::{split_budget, Pacing};
use crate::{
    stats::{ObservationsStats, StreamingStats},
    transport::rate::Throttle,
    DynError, Result,
};

const STOP_POLL: Duration = Duration::from_millis(20);

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

#[derive(Debug)]
pub enum JobOutcome {
    Delivered {
        delivery_micros: u32,
        sync_micros: Option<u32>,
    },
    DeliveryFailed(DynError),
    SyncFailed {
        delivery_micros: u32,
        error: DynError,
    },
    Cancelled,
    Panicked(JoinError),
}

#[derive(Debug, Default)]
pub struct RunOutcome<S = ObservationsStats> {
    pub delivered: u64,
    // every admitted iteration that did not end Delivered; panicked and
    // cancelled below break this count down, so delivered + failed == admitted
    pub failed: u64,
    pub panicked: u64,
    pub cancelled: u64,
    pub first_error: Option<String>,
    pub delivery: S,
    // closed loop only: send-start → all confirmations for the id
    pub sync: Option<S>,
    pub offered: Pacing,
    pub rps: ObservationsStats,
    pub wall: std::time::Duration,
}

impl RunOutcome {
    pub fn achieved_rps(&self) -> f64 {
        self.delivered as f64 / self.wall.as_secs_f64()
    }
}

pub type RawRunOutcome = RunOutcome<StreamingStats>;

impl RawRunOutcome {
    pub fn merge(&mut self, other: RawRunOutcome) {
        self.delivered += other.delivered;
        self.failed += other.failed;
        self.panicked += other.panicked;
        self.cancelled += other.cancelled;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
        self.delivery.merge(other.delivery);
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
    }

    pub fn finalize(self) -> RunOutcome {
        RunOutcome {
            delivered: self.delivered,
            failed: self.failed,
            panicked: self.panicked,
            cancelled: self.cancelled,
            first_error: self.first_error,
            delivery: self.delivery.finalize(false),
            sync: self.sync.map(|sync| sync.finalize(false)),
            offered: self.offered,
            rps: self.rps,
            wall: self.wall,
        }
    }

    fn record(&mut self, outcome: JobOutcome) {
        match outcome {
            JobOutcome::Delivered {
                delivery_micros,
                sync_micros,
            } => {
                self.delivery.push(delivery_micros);
                if let (Some(sync_stats), Some(sync_micros)) =
                    (self.sync.as_mut(), sync_micros)
                {
                    sync_stats.push(sync_micros);
                }
                self.delivered += 1;
            }
            JobOutcome::DeliveryFailed(error) => {
                self.failed += 1;
                self.first_error.get_or_insert_with(|| error.to_string());
            }
            JobOutcome::SyncFailed {
                delivery_micros,
                error,
            } => {
                self.delivery.push(delivery_micros);
                self.failed += 1;
                self.first_error
                    .get_or_insert_with(|| format!("sync: {error}"));
            }
            JobOutcome::Cancelled => {
                self.failed += 1;
                self.cancelled += 1;
                self.first_error.get_or_insert_with(|| {
                    "request task was cancelled".to_string()
                });
            }
            JobOutcome::Panicked(join_error) => {
                self.failed += 1;
                self.panicked += 1;
                let message = format!(
                    "panic: {}",
                    panic_message(join_error.into_panic())
                );
                self.first_error.get_or_insert(message);
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
}

impl Completion {
    fn admits(&self, admitted: u64) -> bool {
        match self {
            Completion::Iterations(total) => admitted < *total,
            Completion::Stop(stop) => !stop.get(),
        }
    }
}

type NoSync = fn(u64) -> std::future::Ready<Result<()>>;
const NO_SYNC: Option<NoSync> = None;

async fn execute_inner<Request, RequestFut, Sync, SyncFut>(
    completion: Completion,
    pacing: Pacing,
    concurrency: usize,
    mut request: Request,
    mut sync: Option<Sync>,
) -> Result<RawRunOutcome>
where
    Request: FnMut(u64) -> RequestFut,
    RequestFut: Future<Output = Result<()>> + 'static,
    Sync: FnMut(u64) -> SyncFut,
    SyncFut: Future<Output = Result<()>> + 'static,
{
    let mut throttle = Throttle::new(pacing, concurrency)?;
    let tally = Rc::new(RefCell::new(RawRunOutcome {
        sync: sync.is_some().then(StreamingStats::new),
        ..RawRunOutcome::default()
    }));
    let started = Instant::now();

    let mut jobs = JoinSet::new();
    let mut admitted = 0u64;
    while completion.admits(admitted) {
        let permit = throttle.admit().await;
        admitted += 1;
        let request_fut = request(admitted);
        let sync_fut = sync.as_mut().map(|sync| sync(admitted));
        let job_tally = tally.clone();
        jobs.spawn_local(async move {
            let sent = Instant::now();
            let outcome = match request_fut.await {
                Ok(()) => {
                    let delivery_micros = sent.elapsed().as_micros() as u32;
                    match sync_fut {
                        None => JobOutcome::Delivered {
                            delivery_micros,
                            sync_micros: None,
                        },
                        Some(sync_fut) => match sync_fut.await {
                            Ok(()) => JobOutcome::Delivered {
                                delivery_micros,
                                sync_micros: Some(
                                    sent.elapsed().as_micros() as u32
                                ),
                            },
                            Err(error) => JobOutcome::SyncFailed {
                                delivery_micros,
                                error,
                            },
                        },
                    }
                }
                Err(error) => JobOutcome::DeliveryFailed(error),
            };
            job_tally.borrow_mut().record(outcome);
            drop(permit);
        });
        while let Some(joined) = jobs.try_join_next() {
            record_join(&tally, joined);
        }
    }
    let rps = throttle.finish();
    while let Some(joined) = jobs.join_next().await {
        record_join(&tally, joined);
    }

    let tally = Rc::try_unwrap(tally)
        .unwrap_or_else(|_| panic!("execute jobs still hold the tally"))
        .into_inner();
    Ok(RawRunOutcome {
        offered: pacing,
        rps,
        wall: started.elapsed(),
        ..tally
    })
}

fn record_join(
    tally: &Rc<RefCell<RawRunOutcome>>,
    joined: std::result::Result<(), JoinError>,
) {
    if let Err(join_error) = joined {
        let outcome = if join_error.is_panic() {
            JobOutcome::Panicked(join_error)
        } else {
            JobOutcome::Cancelled
        };
        tally.borrow_mut().record(outcome);
    }
}

pub async fn execute<F, Fut>(cfg: RunConfig, request: F) -> Result<RunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
{
    Ok(execute_raw(cfg, request).await?.finalize())
}

pub async fn execute_raw<F, Fut>(
    cfg: RunConfig,
    request: F,
) -> Result<RawRunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
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

// Same open-loop pacing as `execute`, but runs until `stop` is set instead of a
// fixed iteration count — for load that must span an externally-timed event.
pub async fn execute_until_raw<F, Fut>(
    pacing: Pacing,
    concurrency: usize,
    stop: Rc<Cell<bool>>,
    request: F,
) -> Result<RawRunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
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

pub async fn execute_and_sync<F, Fut, S, SFut>(
    cfg: RunConfig,
    request: F,
    sync: S,
) -> Result<RunOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
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
    let handles = (0..threads)
        .map(|index| {
            let factory = factory.clone();
            let worker = Worker {
                index,
                threads,
                stop: stop.clone(),
            };
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("driver runtime build is infallible");
                let local = tokio::task::LocalSet::new();
                runtime.block_on(
                    local.run_until(async move { factory(worker).await }),
                )
            })
        })
        .collect();
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

pub fn execute_threaded<Factory, Request, Fut>(
    config: ThreadRunConfig,
    factory: Factory,
) -> Result<RunOutcome>
where
    Factory: Fn(usize) -> Request + Clone + Send + 'static,
    Request: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
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
