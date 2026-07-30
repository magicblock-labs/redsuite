use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::stats::{ObservationsStats, StreamingStats};

const ONESEC: Duration = Duration::from_secs(1);
const ONEMS: Duration = Duration::from_millis(1);

pub struct RateManager {
    count: u32,
    rate: u32,
    epoch: Instant,
    permits: Arc<Semaphore>,
    stats: StreamingStats,
}

impl RateManager {
    pub fn new(concurrency: usize, rate: u32) -> Self {
        Self {
            rate,
            permits: Arc::new(Semaphore::new(concurrency)),
            count: 0,
            epoch: Instant::now(),
            stats: StreamingStats::new(),
        }
    }

    pub async fn tick(&mut self) -> OwnedSemaphorePermit {
        let elapsed = self.epoch.elapsed();
        self.count += 1;
        if elapsed >= ONESEC {
            self.stats.push(self.count);
            self.reset();
        }
        let remaining = self.rate.saturating_sub(self.count).max(1) as u64;
        let mut sleep_dur = Duration::from_millis(
            1000u64.saturating_sub(elapsed.as_millis() as u64) / remaining,
        );
        if sleep_dur >= ONEMS {
            tokio::time::sleep(sleep_dur).await;
        } else if self.count >= self.rate {
            sleep_dur = Duration::from_millis(
                1000u64.saturating_sub(elapsed.as_millis() as u64),
            );
            tokio::time::sleep(sleep_dur).await;
        }
        self.permits.clone().acquire_owned().await.unwrap()
    }

    pub fn stats(self) -> ObservationsStats {
        self.stats.finalize(true)
    }

    #[inline]
    pub fn reset(&mut self) {
        self.epoch = Instant::now();
        self.count = 0;
    }
}
