use std::time::Duration;

use anyhow::Context;
use derive_more::Display;
use tokio::{
    sync::watch,
    task::{self, JoinHandle},
    time,
};
use tracing::{info, warn, Instrument};

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
    handle: JoinHandle<Sampled<T>>,
}

impl<T> Sampler<T>
where
    T: Default + 'static,
{
    pub fn spawn<Observe>(
        collector: MetricsCollector,
        interval: Duration,
        trailing: Trailing,
        mut observe: Observe,
    ) -> Self
    where
        Observe: FnMut(&Metrics, &mut T) -> Option<()> + 'static,
    {
        let (stop, mut halted) = watch::channel(false);
        let mut samples = Sampled::<T>::default();
        let collect = async move {
            let mut consecutive_failures = 0usize;
            while trailing == Trailing::Sample || !*halted.borrow() {
                samples.scrapes += 1;
                match collector.scrape().await {
                    Ok(metrics) => {
                        if consecutive_failures != 0 {
                            info!(consecutive_failures, "metrics scraping recovered");
                            consecutive_failures = 0;
                        }
                        samples.observations +=
                            usize::from(observe(&metrics, &mut samples.value).is_some());
                    }
                    Err(error) => {
                        samples.failures += 1;
                        consecutive_failures += 1;
                        if consecutive_failures == 1 {
                            warn!(error = %format_args!("{error:#}"), "metrics scrape failed; repeats suppressed until recovery");
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
                    scrapes = samples.scrapes,
                    failures = samples.failures,
                    consecutive_failures,
                    "metrics sampling finished with failures",
                );
            }
            samples
        };
        Self {
            stop,
            handle: task::spawn_local(collect.in_current_span()),
        }
    }

    pub async fn finish(mut self) -> Result<Sampled<T>> {
        let _ = self.stop.send(true);
        (&mut self.handle).await.context("metrics sampler failed")
    }

    pub async fn finish_complete(self, what: &str) -> Result<Sampled<T>> {
        let sampled = self.finish().await?;
        crate::check!(
            sampled.observations > 0 && sampled.observations == sampled.scrapes,
            "incomplete {what} sampling: {sampled}"
        );
        Ok(sampled)
    }
}

impl<T> Drop for Sampler<T> {
    fn drop(&mut self) {
        self.handle.abort();
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
