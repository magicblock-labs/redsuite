use super::{
    execute_raw, merge_outcomes, panic_message, split_budget, Delivery, Pacing, RunConfig,
    RunOutcome,
};
use crate::Result;
use anyhow::{anyhow, ensure, Context};
use std::{
    future::Future,
    mem,
    thread::{self, JoinHandle},
    time::Instant,
};
use tokio::{
    runtime::Builder,
    sync::watch,
    task::{self, LocalSet},
    time,
};
use tracing::{Instrument, Span};

pub struct ThreadRunConfig {
    pub until: Option<(Instant, watch::Receiver<bool>)>,
    pub threads: usize,
    pub iterations: u64,
    pub rate: Pacing,
    pub concurrency: usize,
}

pub struct Worker {
    pub index: usize,
    pub threads: usize,
    stop: watch::Sender<bool>,
}

impl Worker {
    pub fn stop_signal(&self) -> watch::Receiver<bool> {
        self.stop.subscribe()
    }
}

pub struct Workers<T> {
    stop: watch::Sender<bool>,
    handles: Handles<T>,
}

enum Handles<T> {
    Local(task::JoinHandle<Result<T>>),
    Threads(Vec<JoinHandle<Result<T>>>),
}

impl<T> Drop for Workers<T> {
    fn drop(&mut self) {
        self.stop();
        if let Handles::Local(handle) = &self.handles {
            handle.abort();
        }
    }
}

pub fn spawn_workers<T, Factory, Fut>(threads: usize, factory: Factory) -> Workers<T>
where
    T: Send + 'static,
    Factory: Fn(Worker) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<T>> + 'static,
{
    let (stop, _) = watch::channel(false);
    if threads == 1 {
        let worker = Worker {
            index: 0,
            threads,
            stop: stop.clone(),
        };
        return Workers {
            stop,
            handles: Handles::Local(task::spawn_local(
                async move { factory(worker).await }.in_current_span(),
            )),
        };
    }
    let mut handles = Vec::with_capacity(threads);
    for index in 0..threads {
        let factory = factory.clone();
        let span = Span::current();
        let worker = Worker {
            index,
            threads,
            stop: stop.clone(),
        };
        handles.push(thread::spawn(move || {
            let _entered = span.enter();
            let runtime = Builder::new_current_thread().enable_all().build()?;
            let local = LocalSet::new();
            // The factory may need the runtime or spawn local tasks immediately.
            runtime.block_on(local.run_until(async move { factory(worker).await }))
        }));
    }
    Workers {
        stop,
        handles: Handles::Threads(handles),
    }
}

impl<T> Workers<T> {
    pub fn stop(&self) {
        self.stop.send_replace(true);
    }

    pub fn stop_at(
        &self,
        deadline: Instant,
        mut signal: watch::Receiver<bool>,
    ) -> task::JoinHandle<()> {
        let stop = self.stop.clone();
        task::spawn_local(async move {
            tokio::select! { _ = time::sleep_until(deadline.into()) => {}, _ = signal.wait_for(|stop| *stop) => {} }
            stop.send_replace(true);
        })
    }
}

impl<T> Workers<T>
where
    T: Send + 'static,
{
    pub async fn join(mut self) -> Result<Vec<T>> {
        self.stop();
        match &mut self.handles {
            Handles::Local(handle) => Ok(vec![handle.await.context("driver worker failed")??]),
            Handles::Threads(handles) => {
                let handles = mem::take(handles);
                task::spawn_blocking(move || {
                    // Join every worker before returning an error, so none outlive cleanup.
                    handles
                        .into_iter()
                        .map(|handle| {
                            handle.join().unwrap_or_else(|panic| {
                                Err(anyhow!("driver worker panicked: {}", panic_message(panic)))
                            })
                        })
                        .collect::<Vec<_>>()
                        .into_iter()
                        .collect()
                })
                .await
                .context("driver worker join failed")?
            }
        }
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
        ensure!(
            concurrency != 0,
            "an admission budget needs a concurrency above zero"
        );
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

pub async fn execute_threaded<Factory, Request, Fut, D>(
    config: ThreadRunConfig,
    factory: Factory,
) -> Result<RunOutcome>
where
    Factory: Fn(usize) -> Request + Clone + Send + 'static,
    Request: FnMut(u64) -> Fut + 'static,
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
        let until = config.until.clone();
        async move {
            execute_raw(
                RunConfig {
                    until,
                    iterations,
                    rate,
                    concurrency,
                },
                |iteration| request(first_id + iteration),
            )
            .await
        }
    })
    .join()
    .await?;
    Ok(merge_outcomes(outcomes))
}
