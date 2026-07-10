use std::{cell::RefCell, future::Future, rc::Rc, time::Instant};

use crate::{
    stats::{ObservationsStats, StreamingStats},
    transport::rate::RateManager,
    Result,
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
pub struct RunOutcome {
    pub delivered: u64,
    // iterations that errored at either stage (delivery or sync)
    pub failed: u64,
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

#[derive(Default)]
struct Tally {
    delivered: u64,
    failed: u64,
    first_error: Option<String>,
    delivery: StreamingStats,
    sync: StreamingStats,
}

pub async fn drive<F, Fut>(cfg: RunConfig, mut request: F) -> RunOutcome
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
{
    let mut rate = RateManager::new(cfg.concurrency, cfg.rate);
    let tally = Rc::new(RefCell::new(Tally::default()));
    let started = Instant::now();

    let mut handles = Vec::with_capacity(cfg.iterations as usize);
    for id in 1..=cfg.iterations {
        let permit = rate.tick().await;
        let fut = request(id);
        let tally = tally.clone();
        handles.push(tokio::task::spawn_local(async move {
            let sent = Instant::now();
            let result = fut.await;
            let mut tally = tally.borrow_mut();
            match result {
                Ok(()) => {
                    tally.delivery.push(sent.elapsed().as_micros() as u32);
                    tally.delivered += 1;
                }
                Err(e) => {
                    tally.failed += 1;
                    tally.first_error.get_or_insert(e.to_string());
                }
            }
            drop(permit);
        }));
    }
    let rps = rate.stats();
    for handle in handles {
        let _ = handle.await;
    }

    let tally = Rc::try_unwrap(tally)
        .unwrap_or_else(|_| panic!("drive tasks still hold the tally"))
        .into_inner();
    RunOutcome {
        delivered: tally.delivered,
        failed: tally.failed,
        first_error: tally.first_error,
        delivery: tally.delivery.finalize(false),
        sync: None,
        rps,
        wall: started.elapsed(),
    }
}

pub fn drive_threads<Factory, Request, Fut>(
    config: ThreadRunConfig,
    factory: Factory,
) -> RunOutcome
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
                drive(
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

    let mut outcomes = Vec::new();
    while let Ok(outcome) = outcome_receiver.recv() {
        outcomes.push(outcome);
    }
    for handle in handles {
        let _ = handle.join();
    }
    merge_outcomes(outcomes)
}

fn merge_outcomes(outcomes: Vec<RunOutcome>) -> RunOutcome {
    let delivered = outcomes.iter().map(|outcome| outcome.delivered).sum();
    let failed = outcomes.iter().map(|outcome| outcome.failed).sum();
    let first_error = outcomes
        .iter()
        .find_map(|outcome| outcome.first_error.clone());
    let wall = outcomes
        .iter()
        .map(|outcome| outcome.wall)
        .max()
        .unwrap_or_default();
    let delivery = ObservationsStats::merge(
        outcomes.iter().map(|outcome| outcome.delivery).collect(),
        true,
    );
    let rps = ObservationsStats::merge(
        outcomes.iter().map(|outcome| outcome.rps).collect(),
        false,
    );
    RunOutcome {
        delivered,
        failed,
        first_error,
        delivery,
        sync: None,
        rps,
        wall,
    }
}

pub async fn drive_closed<F, Fut, S, SFut>(
    cfg: RunConfig,
    mut request: F,
    mut sync: S,
) -> RunOutcome
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
    S: FnMut(u64) -> SFut,
    SFut: Future<Output = Result<()>> + 'static,
{
    let mut rate = RateManager::new(cfg.concurrency, cfg.rate);
    let tally = Rc::new(RefCell::new(Tally::default()));
    let started = Instant::now();

    let mut handles = Vec::with_capacity(cfg.iterations as usize);
    for id in 1..=cfg.iterations {
        let permit = rate.tick().await;
        let fut = request(id);
        let sync_fut = sync(id);
        let tally = tally.clone();
        handles.push(tokio::task::spawn_local(async move {
            let sent = Instant::now();
            let delivered = match fut.await {
                Ok(()) => {
                    let mut tally = tally.borrow_mut();
                    tally.delivery.push(sent.elapsed().as_micros() as u32);
                    tally.delivered += 1;
                    true
                }
                Err(e) => {
                    let mut tally = tally.borrow_mut();
                    tally.failed += 1;
                    tally.first_error.get_or_insert(e.to_string());
                    false
                }
            };
            if delivered {
                match sync_fut.await {
                    Ok(()) => tally
                        .borrow_mut()
                        .sync
                        .push(sent.elapsed().as_micros() as u32),
                    Err(e) => {
                        let mut tally = tally.borrow_mut();
                        tally.failed += 1;
                        tally.first_error.get_or_insert(format!("sync: {e}"));
                    }
                }
            }
            drop(permit);
        }));
    }
    let rps = rate.stats();
    for handle in handles {
        let _ = handle.await;
    }

    let tally = Rc::try_unwrap(tally)
        .unwrap_or_else(|_| panic!("drive tasks still hold the tally"))
        .into_inner();
    RunOutcome {
        delivered: tally.delivered,
        failed: tally.failed,
        first_error: tally.first_error,
        delivery: tally.delivery.finalize(false),
        sync: Some(tally.sync.finalize(false)),
        rps,
        wall: started.elapsed(),
    }
}
