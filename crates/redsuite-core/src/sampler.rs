use std::{thread::JoinHandle, time::Duration};

use futures_util::future::{AbortHandle, Abortable};
use tokio::sync::{oneshot, watch};

use crate::{
    api::{Metrics, MetricsCollector},
    check, Result,
};

#[derive(Default)]
pub struct Sampled<T> {
    pub value: T,
    pub scrapes: usize,
    pub failures: usize,
    pub observations: usize,
}

impl<T> std::fmt::Display for Sampled<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{} observations, {} failed scrapes",
            self.observations, self.scrapes, self.failures
        )
    }
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

impl<T: Default + Send + 'static> Sampler<T> {
    pub fn spawn(
        collector: MetricsCollector,
        interval: Duration,
        trailing: Trailing,
        mut observe: impl FnMut(&Metrics, &mut T) -> Option<()> + Send + 'static,
    ) -> Self {
        let (stop, mut halted) = watch::channel(false);
        let (abort, registration) = AbortHandle::new_pair();
        let (done, result) = oneshot::channel();
        let handle = std::thread::spawn(move || {
            let sampling = async move {
                let mut samples = Sampled::<T>::default();
                while trailing == Trailing::Sample || !*halted.borrow() {
                    samples.scrapes += 1;
                    match collector.scrape().await {
                        Ok(metrics) => {
                            samples.observations += usize::from(
                                observe(&metrics, &mut samples.value).is_some(),
                            )
                        }
                        Err(error) => {
                            samples.failures += 1;
                            eprintln!("[redsuite] metrics scrape: {error}");
                        }
                    }
                    if *halted.borrow() {
                        break;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(interval) => {}
                        _ = halted.changed(), if trailing == Trailing::Skip => {}
                    }
                }
                samples
            };
            let output = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(Into::into)
                .and_then(|runtime| {
                    runtime
                        .block_on(Abortable::new(sampling, registration))
                        .map_err(Into::into)
                });
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
        (&mut self.result)
            .await
            .map_err(|_| "metrics sampler worker failed")?
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
