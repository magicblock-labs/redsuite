use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    stats::{ObservationsStats, StreamingStats},
    Result,
};

const ONESEC: Duration = Duration::from_secs(1);
const TIMER_RESOLUTION: Duration = Duration::from_millis(1);
const RESYNC_SLACK: Duration = Duration::from_millis(5);
const TAIL_BUCKET_MIN: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Pacing {
    #[default]
    Unlimited,
    PerSecond(u32),
}

impl Pacing {
    pub fn interval(self) -> Result<Option<Duration>> {
        match self {
            Pacing::Unlimited => Ok(None),
            Pacing::PerSecond(0) => {
                Err("a paced run needs an offered rate above zero".into())
            }
            Pacing::PerSecond(rate) => Ok(Some(ONESEC / rate)),
        }
    }

    pub fn partition(self, workers: usize) -> Result<Vec<Pacing>> {
        match self {
            Pacing::Unlimited => Ok(vec![Pacing::Unlimited; workers]),
            Pacing::PerSecond(rate) => {
                self.interval()?;
                if (rate as usize) < workers {
                    return Err(format!(
                        "an offered rate of {rate}/s cannot be split across \
                         {workers} workers"
                    )
                    .into());
                }
                Ok(split_budget(u64::from(rate), workers)
                    .into_iter()
                    .map(|share| Pacing::PerSecond(share as u32))
                    .collect())
            }
        }
    }

    pub fn combine(self, other: Pacing) -> Pacing {
        match (self, other) {
            (Pacing::PerSecond(own), Pacing::PerSecond(other)) => {
                Pacing::PerSecond(own.saturating_add(other))
            }
            _ => Pacing::Unlimited,
        }
    }
}

impl fmt::Display for Pacing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Pacing::Unlimited => f.write_str("unlimited"),
            Pacing::PerSecond(rate) => write!(f, "{rate}/s"),
        }
    }
}

pub fn split_budget(total: u64, parts: usize) -> Vec<u64> {
    let base = total / (parts.max(1) as u64);
    let remainder = total % (parts.max(1) as u64);
    (0..parts as u64)
        .map(|index| base + u64::from(index < remainder))
        .collect()
}

pub struct Pacer {
    interval: Option<Duration>,
    next: tokio::time::Instant,
}

impl Pacer {
    pub fn new(pacing: Pacing) -> Result<Self> {
        Ok(Self {
            interval: pacing.interval()?,
            next: tokio::time::Instant::now(),
        })
    }

    pub async fn wait(&mut self) {
        let Some(interval) = self.interval else {
            return;
        };
        let now = tokio::time::Instant::now();
        if self.next > now {
            if self.next - now >= TIMER_RESOLUTION {
                tokio::time::sleep_until(self.next).await;
            }
        } else if now - self.next > RESYNC_SLACK {
            self.next = now;
        }
        self.next += interval;
    }
}

pub struct Admission {
    permits: Arc<Semaphore>,
}

impl Admission {
    pub fn new(concurrency: usize) -> Result<Self> {
        if concurrency == 0 {
            return Err(
                "an admission budget needs a concurrency above zero".into()
            );
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(concurrency)),
        })
    }

    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        self.permits
            .clone()
            .acquire_owned()
            .await
            .expect("the admission semaphore is never closed")
    }
}

pub struct AdmissionStats {
    origin: Instant,
    bucket: u64,
    count: u32,
    rps: StreamingStats,
}

impl Default for AdmissionStats {
    fn default() -> Self {
        Self::new()
    }
}

impl AdmissionStats {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
            bucket: 0,
            count: 0,
            rps: StreamingStats::new(),
        }
    }

    pub fn record(&mut self) {
        let now = Instant::now();
        let bucket = now.duration_since(self.origin).as_secs();
        while self.bucket < bucket {
            self.rps.push(self.count);
            self.count = 0;
            self.bucket += 1;
        }
        self.count += 1;
    }

    pub fn finish(mut self) -> ObservationsStats {
        let tail = self.origin.elapsed() - Duration::from_secs(self.bucket);
        if self.count > 0 && (self.bucket == 0 || tail >= TAIL_BUCKET_MIN) {
            self.rps
                .push((f64::from(self.count) / tail.as_secs_f64()) as u32);
        }
        self.rps.finalize(true)
    }
}

pub struct Throttle {
    pacer: Pacer,
    admission: Admission,
    stats: AdmissionStats,
}

impl Throttle {
    pub fn new(pacing: Pacing, concurrency: usize) -> Result<Self> {
        Ok(Self {
            pacer: Pacer::new(pacing)?,
            admission: Admission::new(concurrency)?,
            stats: AdmissionStats::new(),
        })
    }

    pub async fn admit(&mut self) -> OwnedSemaphorePermit {
        self.pacer.wait().await;
        let permit = self.admission.acquire().await;
        self.stats.record();
        permit
    }

    pub fn finish(self) -> ObservationsStats {
        self.stats.finish()
    }
}
