use std::{
    any::Any,
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    time::Instant,
};

use tokio::task::{JoinError, JoinSet};

use crate::{
    stats::{ObservationsStats, StreamingStats},
    transport::rate::RateManager,
    DynError, Result,
};

pub struct RunConfig {
    pub iterations: u64,
    pub rate: u32,
    pub concurrency: usize,
}

pub struct ThreadRunConfig {
    pub threads: usize,
    pub iterations: u64,
    pub rate: u32,
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
pub struct RunOutcome {
    pub delivered: u64,
    // every admitted iteration that did not end Delivered; panicked and
    // cancelled below break this count down, so delivered + failed == admitted
    pub failed: u64,
    pub panicked: u64,
    pub cancelled: u64,
    pub first_error: Option<String>,
    pub delivery: ObservationsStats,
    // closed loop only: send-start → all confirmations for the id
    pub sync: Option<ObservationsStats>,
    pub rps: ObservationsStats,
    pub wall: std::time::Duration,
}

impl RunOutcome {
    pub fn achieved_rps(&self) -> f64 {
        self.delivered as f64 / self.wall.as_secs_f64()
    }
}

#[derive(Debug)]
pub struct RawRunOutcome {
    pub delivered: u64,
    pub failed: u64,
    pub panicked: u64,
    pub cancelled: u64,
    pub first_error: Option<String>,
    pub delivery: StreamingStats,
    pub sync: Option<StreamingStats>,
    pub rps: ObservationsStats,
    pub wall: std::time::Duration,
}

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
            rps: self.rps,
            wall: self.wall,
        }
    }
}

struct Tally {
    delivered: u64,
    failed: u64,
    panicked: u64,
    cancelled: u64,
    first_error: Option<String>,
    delivery: StreamingStats,
    sync: Option<StreamingStats>,
}

impl Tally {
    fn new(closed_loop: bool) -> Self {
        Self {
            delivered: 0,
            failed: 0,
            panicked: 0,
            cancelled: 0,
            first_error: None,
            delivery: StreamingStats::new(),
            sync: closed_loop.then(StreamingStats::new),
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
    rate: u32,
    concurrency: usize,
    mut request: Request,
    mut sync: Option<Sync>,
) -> RawRunOutcome
where
    Request: FnMut(u64) -> RequestFut,
    RequestFut: Future<Output = Result<()>> + 'static,
    Sync: FnMut(u64) -> SyncFut,
    SyncFut: Future<Output = Result<()>> + 'static,
{
    let mut rate_manager = RateManager::new(concurrency, rate);
    let tally = Rc::new(RefCell::new(Tally::new(sync.is_some())));
    let started = Instant::now();

    let mut jobs = JoinSet::new();
    let mut admitted = 0u64;
    while completion.admits(admitted) {
        let permit = rate_manager.tick().await;
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
    let rps = rate_manager.stats();
    while let Some(joined) = jobs.join_next().await {
        record_join(&tally, joined);
    }

    let tally = Rc::try_unwrap(tally)
        .unwrap_or_else(|_| panic!("execute jobs still hold the tally"))
        .into_inner();
    RawRunOutcome {
        delivered: tally.delivered,
        failed: tally.failed,
        panicked: tally.panicked,
        cancelled: tally.cancelled,
        first_error: tally.first_error,
        delivery: tally.delivery,
        sync: tally.sync,
        rps,
        wall: started.elapsed(),
    }
}

fn record_join(
    tally: &Rc<RefCell<Tally>>,
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

pub async fn execute<F, Fut>(cfg: RunConfig, request: F) -> RunOutcome
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
{
    execute_raw(cfg, request).await.finalize()
}

pub async fn execute_raw<F, Fut>(cfg: RunConfig, request: F) -> RawRunOutcome
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
pub async fn execute_until<F, Fut>(
    rate: u32,
    concurrency: usize,
    stop: Rc<Cell<bool>>,
    request: F,
) -> RunOutcome
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
{
    execute_until_raw(rate, concurrency, stop, request)
        .await
        .finalize()
}

pub async fn execute_until_raw<F, Fut>(
    rate: u32,
    concurrency: usize,
    stop: Rc<Cell<bool>>,
    request: F,
) -> RawRunOutcome
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
{
    execute_inner(Completion::Stop(stop), rate, concurrency, request, NO_SYNC)
        .await
}

pub async fn execute_and_sync<F, Fut, S, SFut>(
    cfg: RunConfig,
    request: F,
    sync: S,
) -> RunOutcome
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
    S: FnMut(u64) -> SFut,
    SFut: Future<Output = Result<()>> + 'static,
{
    execute_inner(
        Completion::Iterations(cfg.iterations),
        cfg.rate,
        cfg.concurrency,
        request,
        Some(sync),
    )
    .await
    .finalize()
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
    let threads = config.threads.max(1);
    let base_iterations = config.iterations / threads as u64;
    let remainder = config.iterations % threads as u64;
    let (outcome_sender, outcome_receiver) = std::sync::mpsc::channel();

    let mut first_id = 0u64;
    let mut handles = Vec::with_capacity(threads);
    for thread_index in 0..threads {
        let iterations =
            base_iterations + u64::from((thread_index as u64) < remainder);
        if iterations == 0 {
            continue;
        }
        let thread_first_id = first_id;
        first_id += iterations;
        let rate = (config.rate / threads as u32).max(1);
        let concurrency = (config.concurrency / threads).max(1);
        let factory = factory.clone();
        let outcome_sender = outcome_sender.clone();
        handles.push(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("driver runtime build is infallible");
            let local = tokio::task::LocalSet::new();
            let outcome = runtime.block_on(local.run_until(async move {
                let mut request = factory(thread_index);
                execute_raw(
                    RunConfig {
                        iterations,
                        rate,
                        concurrency,
                    },
                    |iteration| request(thread_first_id + iteration),
                )
                .await
            }));
            let _ = outcome_sender.send(outcome);
        }));
    }
    drop(outcome_sender);

    let workers = handles.len();
    let mut outcomes = Vec::with_capacity(workers);
    while let Ok(outcome) = outcome_receiver.recv() {
        outcomes.push(outcome);
    }
    let mut first_worker_panic = None;
    for handle in handles {
        if let Err(payload) = handle.join() {
            first_worker_panic.get_or_insert_with(|| panic_message(payload));
        }
    }
    if let Some(panic) = first_worker_panic {
        return Err(format!("driver worker thread panicked: {panic}").into());
    }
    if outcomes.len() != workers {
        return Err("a driver worker thread stopped without an outcome".into());
    }
    Ok(merge_outcomes(outcomes))
}

fn merge_outcomes(outcomes: Vec<RawRunOutcome>) -> RunOutcome {
    outcomes
        .into_iter()
        .reduce(|mut merged, outcome| {
            merged.merge(outcome);
            merged
        })
        .map(RawRunOutcome::finalize)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(iterations: u64) -> RunConfig {
        RunConfig {
            iterations,
            rate: 10_000,
            concurrency: 4,
        }
    }

    #[test]
    fn panic_payloads_keep_their_message() {
        let caught = std::panic::catch_unwind(|| panic!("boom {}", 7));
        assert_eq!(panic_message(caught.unwrap_err()), "boom 7");
        let caught = std::panic::catch_unwind(|| panic!("plain"));
        assert_eq!(panic_message(caught.unwrap_err()), "plain");
    }

    #[tokio::test]
    async fn panicked_iterations_land_in_the_tally() {
        let outcome = tokio::task::LocalSet::new()
            .run_until(execute(config(5), |id| async move {
                assert_ne!(id, 3, "iteration three explodes");
                Ok(())
            }))
            .await;
        assert_eq!(outcome.delivered, 4);
        assert_eq!(outcome.failed, 1);
        assert_eq!(outcome.panicked, 1);
        assert_eq!(outcome.cancelled, 0);
        assert!(outcome.first_error.unwrap().contains("explodes"));
    }

    #[tokio::test]
    async fn failed_deliveries_keep_the_first_error() {
        let outcome = tokio::task::LocalSet::new()
            .run_until(execute(config(4), |id| async move {
                if id == 2 {
                    Err("delivery refused".into())
                } else {
                    Ok(())
                }
            }))
            .await;
        assert_eq!(outcome.delivered, 3);
        assert_eq!(outcome.failed, 1);
        assert_eq!(outcome.panicked, 0);
        assert_eq!(outcome.first_error.as_deref(), Some("delivery refused"));
    }

    #[tokio::test]
    async fn sync_failures_do_not_count_as_delivered() {
        let outcome = tokio::task::LocalSet::new()
            .run_until(execute_and_sync(
                config(3),
                |_| async { Ok(()) },
                |id| async move {
                    if id == 1 {
                        Err("confirmation lost".into())
                    } else {
                        Ok(())
                    }
                },
            ))
            .await;
        assert_eq!(outcome.delivered, 2);
        assert_eq!(outcome.failed, 1);
        assert_eq!(outcome.delivered + outcome.failed, 3);
        assert_eq!(
            outcome.first_error.as_deref(),
            Some("sync: confirmation lost")
        );
        assert!(outcome.sync.is_some());
    }

    #[tokio::test]
    async fn execute_until_accounts_every_admitted_iteration() {
        let stop = Rc::new(Cell::new(false));
        let admitted = Rc::new(Cell::new(0u64));
        let outcome = tokio::task::LocalSet::new()
            .run_until(execute_until(10_000, 4, stop.clone(), {
                let stop = stop.clone();
                let admitted = admitted.clone();
                move |id| {
                    admitted.set(admitted.get() + 1);
                    if id >= 3 {
                        stop.set(true);
                    }
                    let explode = id == 2;
                    async move {
                        assert!(!explode, "iteration two explodes");
                        Ok(())
                    }
                }
            }))
            .await;
        assert_eq!(outcome.delivered + outcome.failed, admitted.get());
        assert_eq!(outcome.panicked, 1);
    }

    #[test]
    fn execute_threaded_counts_request_panics_in_the_outcome() {
        let outcome = execute_threaded(
            ThreadRunConfig {
                threads: 2,
                iterations: 6,
                rate: 10_000,
                concurrency: 4,
            },
            |_thread_index| {
                |id: u64| async move {
                    assert_ne!(id, 4, "iteration four explodes");
                    Ok(())
                }
            },
        )
        .expect("workers stay alive");
        assert_eq!(outcome.delivered, 5);
        assert_eq!(outcome.failed, 1);
        assert_eq!(outcome.panicked, 1);
        assert!(outcome.first_error.unwrap().contains("explodes"));
    }

    #[test]
    fn execute_threaded_reports_a_worker_panic() {
        let error = execute_threaded(
            ThreadRunConfig {
                threads: 2,
                iterations: 4,
                rate: 10_000,
                concurrency: 4,
            },
            |thread_index| {
                assert_ne!(thread_index, 1, "worker one dies at setup");
                |_id: u64| std::future::ready(Ok(()))
            },
        )
        .expect_err("the dead worker surfaces");
        assert!(error.to_string().contains("worker thread panicked"));
    }
}
