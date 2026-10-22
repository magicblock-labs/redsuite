use std::{thread, thread::JoinHandle, time::Duration};

use anyhow::{ensure, Context};
use derive_more::Display;
use futures_util::future::{AbortHandle, AbortRegistration, Abortable};
use tokio::{
    runtime::Builder,
    sync::{oneshot, watch},
    time,
};
use tracing::{info, warn, Span};

use crate::{
    api::{Metrics, MetricsCollector},
    Result,
};

#[derive(Default, Display)]
#[display("{observations}/{scrapes} observations, {failures} failed scrapes")]
pub struct Sampled<T> {
    pub value: T,
    pub scrapes: usize,
    pub failures: usize,
    pub observations: usize,
}

#[derive(PartialEq, Eq)]
pub enum Trailing {
    Sample,
    Skip,
}

pub struct Sampler<T> {
    stop: watch::Sender<bool>,
    abort: AbortHandle,
    result: oneshot::Receiver<Result<Sampled<T>>>,
    handle: Option<JoinHandle<()>>,
}

struct Sampling<T, Observe> {
    collector: MetricsCollector,
    interval: Duration,
    trailing: Trailing,
    observe: Observe,
    halted: watch::Receiver<bool>,
    samples: Sampled<T>,
    consecutive_failures: usize,
}

impl<T, Observe> Sampling<T, Observe>
where
    Observe: FnMut(&Metrics, &mut T) -> Option<()>,
{
    fn run(self, registration: AbortRegistration) -> Result<Sampled<T>> {
        let runtime = Builder::new_current_thread().enable_all().build()?;
        Ok(runtime.block_on(Abortable::new(self.collect(), registration))?)
    }

    async fn collect(mut self) -> Sampled<T> {
        while self.trailing == Trailing::Sample || !*self.halted.borrow() {
            self.scrape().await;
            if *self.halted.borrow() {
                break;
            }
            tokio::select! {
                _ = time::sleep(self.interval) => {}
                _ = self.halted.changed(), if self.trailing == Trailing::Skip => {}
            }
        }
        if self.samples.failures != 0 {
            warn!(
                scrapes = self.samples.scrapes,
                failures = self.samples.failures,
                consecutive_failures = self.consecutive_failures,
                "metrics sampling finished with failures",
            );
        }
        self.samples
    }

    async fn scrape(&mut self) {
        self.samples.scrapes += 1;
        match self.collector.scrape().await {
            Ok(metrics) => {
                if self.consecutive_failures != 0 {
                    info!(
                        consecutive_failures = self.consecutive_failures,
                        "metrics scraping recovered"
                    );
                    self.consecutive_failures = 0;
                }
                self.samples.observations +=
                    usize::from((self.observe)(&metrics, &mut self.samples.value).is_some());
            }
            Err(error) => {
                self.samples.failures += 1;
                self.consecutive_failures += 1;
                if self.consecutive_failures == 1 {
                    warn!(error = %format_args!("{error:#}"), "metrics scrape failed; repeats suppressed until recovery");
                }
            }
        }
    }
}

impl<T> Sampler<T>
where
    T: Default + Send + 'static,
{
    pub fn spawn<Observe>(
        collector: MetricsCollector,
        interval: Duration,
        trailing: Trailing,
        observe: Observe,
    ) -> Self
    where
        Observe: FnMut(&Metrics, &mut T) -> Option<()> + Send + 'static,
    {
        let (stop, halted) = watch::channel(false);
        let (abort, registration) = AbortHandle::new_pair();
        let (done, result) = oneshot::channel();
        let sampling = Sampling {
            collector,
            interval,
            trailing,
            observe,
            halted,
            samples: Sampled::<T>::default(),
            consecutive_failures: 0,
        };
        let span = Span::current();
        let handle = thread::spawn(move || {
            let _entered = span.enter();
            let _ = done.send(sampling.run(registration));
        });
        Self {
            stop,
            abort,
            result,
            handle: Some(handle),
        }
    }

    pub async fn finish(mut self) -> Result<Sampled<T>> {
        let _ = self.stop.send(true);
        (&mut self.result).await.context("metrics sampler worker failed")?
    }

    pub async fn finish_complete(self, what: &str) -> Result<Sampled<T>> {
        let sampled = self.finish().await?;
        ensure!(
            sampled.observations > 0 && sampled.observations == sampled.scrapes,
            "incomplete {what} sampling: {sampled}"
        );
        Ok(sampled)
    }
}

impl<T> Drop for Sampler<T> {
    fn drop(&mut self) {
        self.abort.abort();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Default)]
pub struct MeanMax {
    sum: f64,
    pub max: f64,
    pub count: usize,
}

impl MeanMax {
    pub fn push(&mut self, value: f64) {
        self.sum += value;
        self.max = self.max.max(value);
        self.count += 1;
    }

    pub fn mean(&self) -> f64 {
        self.sum / self.count.max(1) as f64
    }
}
