use std::{thread, thread::JoinHandle, time::Duration};

use derive_more::Display;
use futures_util::future::{AbortHandle, Abortable};
use tokio::{
    runtime::Builder,
    sync::{oneshot, watch},
    time,
};
use tracing::{info, warn, Span};

use crate::{
    api::{Metrics, MetricsCollector},
    check, Result,
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

impl<T> Sampler<T>
where
    T: Default + Send + 'static,
{
    pub fn spawn<Observe>(
        collector: MetricsCollector,
        interval: Duration,
        trailing: Trailing,
        mut observe: Observe,
    ) -> Self
    where
        Observe: FnMut(&Metrics, &mut T) -> Option<()> + Send + 'static,
    {
        let (stop, mut halted) = watch::channel(false);
        let (abort, registration) = AbortHandle::new_pair();
        let (done, result) = oneshot::channel();
        let span = Span::current();
        let handle = thread::spawn(move || {
            let _entered = span.enter();
            let sampling = async move {
                let mut samples = Sampled::<T>::default();
                let mut consecutive_failures = 0usize;
                while trailing == Trailing::Sample || !*halted.borrow() {
                    samples.scrapes += 1;
                    match collector.scrape().await {
                        Ok(metrics) => {
                            if consecutive_failures != 0 {
                                info!(consecutive_failures, "metrics scraping recovered",);
                                consecutive_failures = 0;
                            }
                            samples.observations +=
                                usize::from(observe(&metrics, &mut samples.value).is_some())
                        }
                        Err(error) => {
                            samples.failures += 1;
                            consecutive_failures += 1;
                            if consecutive_failures == 1 {
                                warn!(
                                    %error,
                                    "metrics scrape failed; repeats suppressed until recovery",
                                );
                            }
                        }
                    }
                    if *halted.borrow() {
                        break;
                    }
                    tokio::select! {
                        _ = time::sleep(interval) => {}
                        _ = halted.changed(), if trailing == Trailing::Skip => {}
                    }
                }
                if samples.failures != 0 {
                    warn!(
                        samples.scrapes,
                        samples.failures,
                        consecutive_failures,
                        "metrics sampling finished with failures",
                    );
                }
                samples
            };
            let output =
                Builder::new_current_thread().enable_all().build().map_err(Into::into).and_then(
                    |runtime| {
                        runtime.block_on(Abortable::new(sampling, registration)).map_err(Into::into)
                    },
                );
            let _ = done.send(output);
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
        (&mut self.result).await.map_err(|_| "metrics sampler worker failed")?
    }

    pub async fn finish_complete(self, what: &str) -> Result<Sampled<T>> {
        let sampled = self.finish().await?;
        check!(
            sampled.observations > 0 && sampled.observations == sampled.scrapes,
            "incomplete {what} sampling: {sampled}"
        )?;
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
