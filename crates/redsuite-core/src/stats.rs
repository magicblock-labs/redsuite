//! Statistical aggregation for benchmark observations: Welford's algorithm
//! for mean/variance, reservoir sampling for percentiles — O(1) memory over
//! millions of latency measurements.

use std::collections::HashMap;

use json::{Deserialize, Serialize};
use rand::{rngs::ThreadRng, thread_rng, Rng};

#[derive(Debug)]
pub struct StreamingStats {
    count: usize,
    mean: f64,
    m2: f64, // sum of squared deviations, for variance
    min: u32,
    max: u32,
    reservoir: Vec<u32>,
    reservoir_size: usize,
    rng: ThreadRng,
}

impl StreamingStats {
    const DEFAULT_RESERVOIR_SIZE: usize = 10_000;

    pub fn new() -> Self {
        Self::with_reservoir_size(Self::DEFAULT_RESERVOIR_SIZE)
    }

    pub fn with_reservoir_size(reservoir_size: usize) -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            min: u32::MAX,
            max: 0,
            reservoir: Vec::with_capacity(reservoir_size),
            reservoir_size,
            rng: thread_rng(),
        }
    }

    pub fn push(&mut self, value: u32) {
        self.count += 1;
        let delta = value as f64 - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (value as f64 - self.mean);

        self.min = self.min.min(value);
        self.max = self.max.max(value);

        if self.reservoir.len() < self.reservoir_size {
            self.reservoir.push(value);
        } else {
            let j = self.rng.gen_range(0..self.count);
            if j < self.reservoir_size {
                self.reservoir[j] = value;
            }
        }
    }

    pub fn finalize(mut self, invertedq: bool) -> ObservationsStats {
        if self.count == 0 {
            return ObservationsStats::default();
        }

        self.reservoir.sort_unstable();

        let avg = self.mean as i32;
        let median = if !self.reservoir.is_empty() {
            self.reservoir[self.reservoir.len() / 2] as i32
        } else {
            avg
        };

        let q95_count = (self.reservoir.len() as f64 * 0.95).ceil() as usize;
        let p95_idx = if invertedq {
            self.reservoir.len().saturating_sub(q95_count + 1)
        } else {
            q95_count.saturating_sub(1).min(self.reservoir.len() - 1)
        };
        let quantile95 = if !self.reservoir.is_empty() {
            self.reservoir[p95_idx] as i32
        } else {
            avg
        };

        let variance = if self.count > 1 {
            self.m2 / self.count as f64
        } else {
            0.0
        };
        let stddev = variance.sqrt() as u32;

        ObservationsStats {
            count: self.count,
            median,
            min: self.min,
            max: self.max,
            avg,
            quantile95,
            stddev,
        }
    }
}

impl Default for StreamingStats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct BenchStatistics {
    /// The configuration used for the benchmark.
    pub configuration: json::Value,
    /// Statistics per RPC-based benchmark mode.
    pub request_stats: HashMap<String, ObservationsStats>,
    /// Latency for receiving signature confirmations.
    pub signature_confirmation_latency: ObservationsStats,
    /// Latency for receiving account updates.
    pub account_update_latency: ObservationsStats,
    /// Throughput statistics for the entire benchmark run.
    pub rps: ObservationsStats,
}

#[derive(Deserialize, Serialize, Clone, Copy, Default)]
#[serde(rename_all = "kebab-case")]
pub struct ObservationsStats {
    pub count: usize,
    pub median: i32,
    pub min: u32,
    pub max: u32,
    pub avg: i32,
    pub quantile95: i32,
    pub stddev: u32,
}

impl BenchStatistics {
    /// Consolidate the reports of parallel bench instances into one.
    pub fn merge(mut stats: Vec<Self>) -> Self {
        if stats.is_empty() {
            return Self::default();
        }
        let configuration =
            std::mem::take(&mut stats.first_mut().unwrap().configuration);
        let mut request_stats = HashMap::new();
        let mut rps = Vec::new();
        let mut account_update_stats = Vec::new();
        let mut signature_confirmation_stats = Vec::new();

        for s in stats {
            for (key, value) in s.request_stats {
                request_stats
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(value);
            }
            account_update_stats.push(s.account_update_latency);
            signature_confirmation_stats.push(s.signature_confirmation_latency);
            rps.push(s.rps);
        }

        let request_stats = request_stats
            .into_iter()
            .map(|(key, value)| (key, ObservationsStats::merge(value, true)))
            .collect();

        Self {
            configuration,
            account_update_latency: ObservationsStats::merge(
                account_update_stats,
                true,
            ),
            signature_confirmation_latency: ObservationsStats::merge(
                signature_confirmation_stats,
                true,
            ),
            request_stats,
            rps: ObservationsStats::merge(rps, false),
        }
    }
}

impl ObservationsStats {
    /// With `average` the values are averaged across instances (min-of-mins,
    /// max-of-maxes); without it they are summed (used for RPS totals).
    pub fn merge(stats: Vec<ObservationsStats>, average: bool) -> Self {
        let total_count = if average { stats.len() } else { 1 };
        if total_count == 0 {
            return Self::default();
        }
        let sum = stats.iter().fold(
            (0usize, 0i32, u32::MAX, 0u32, 0i32, 0i32, 0u32),
            |acc, stat| {
                (
                    acc.0 + stat.count,
                    acc.1 + stat.median,
                    if average {
                        acc.2.min(stat.min)
                    } else {
                        acc.2 + stat.min
                    },
                    if average {
                        acc.3.max(stat.max)
                    } else {
                        acc.3 + stat.max
                    },
                    acc.4 + stat.avg,
                    acc.5 + stat.quantile95,
                    acc.6 + stat.stddev,
                )
            },
        );

        ObservationsStats {
            count: sum.0,
            median: sum.1 / total_count as i32,
            min: sum.2,
            max: sum.3,
            avg: sum.4 / total_count as i32,
            quantile95: sum.5 / total_count as i32,
            stddev: sum.6 / total_count as u32,
        }
    }
}
