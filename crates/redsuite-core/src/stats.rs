//! Statistical aggregation for benchmark observations.
//!
//! Uses streaming algorithms (Welford's for mean/variance, reservoir sampling
//! for percentiles) to efficiently process millions of latency measurements
//! with minimal memory overhead.
//!
//! Ported verbatim from redline `core/src/stats.rs`
//! (magicblock-labs/redline @ 14c642f).

use json::{Deserialize, Serialize};
use rand::{rngs::ThreadRng, thread_rng, Rng};
use std::collections::HashMap;

/// # Streaming Statistics
///
/// Collects observations using Welford's algorithm for mean/variance and reservoir
/// sampling for percentiles. Memory-efficient for millions of observations.
#[derive(Debug)]
pub struct StreamingStats {
    count: usize,
    mean: f64,
    m2: f64, // Sum of squared deviations for variance calculation
    min: u32,
    max: u32,
    reservoir: Vec<u32>, // Reservoir sampling for percentile estimation
    reservoir_size: usize,
    rng: ThreadRng,
}

impl StreamingStats {
    const DEFAULT_RESERVOIR_SIZE: usize = 10_000;

    /// Creates a new `StreamingStats` with default reservoir size (10K samples).
    pub fn new() -> Self {
        Self::with_reservoir_size(Self::DEFAULT_RESERVOIR_SIZE)
    }

    /// Creates a new `StreamingStats` with specified reservoir size.
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

    /// Adds a new observation using Welford's online algorithm.
    pub fn push(&mut self, value: u32) {
        // Welford's online algorithm for mean and variance
        self.count += 1;
        let delta = value as f64 - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (value as f64 - self.mean);

        // Track min/max
        self.min = self.min.min(value);
        self.max = self.max.max(value);

        // Reservoir sampling for percentiles
        if self.reservoir.len() < self.reservoir_size {
            self.reservoir.push(value);
        } else {
            let j = self.rng.gen_range(0..self.count);
            if j < self.reservoir_size {
                self.reservoir[j] = value;
            }
        }
    }

    /// Finalizes the statistics and returns `ObservationsStats`.
    pub fn finalize(mut self, invertedq: bool) -> ObservationsStats {
        if self.count == 0 {
            return ObservationsStats::default();
        }

        // Sort reservoir for percentile calculation
        self.reservoir.sort_unstable();

        let avg = self.mean as i32;
        let median = if !self.reservoir.is_empty() {
            self.reservoir[self.reservoir.len() / 2] as i32
        } else {
            avg
        };

        // Calculate 95th percentile from reservoir
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

        // Calculate standard deviation from variance
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

/// # Benchmark Statistics
///
/// A unified structure for storing all benchmark statistics, with a clear distinction
/// between transaction and RPC request metrics.
#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct BenchStatistics {
    /// The configuration used for the benchmark.
    pub configuration: json::Value,
    /// A map of statistics for each RPC-based benchmark mode.
    pub request_stats: HashMap<String, ObservationsStats>,
    /// Latency for receiving signature confirmations.
    pub signature_confirmation_latency: ObservationsStats,
    /// Latency for receiving account updates.
    pub account_update_latency: ObservationsStats,
    /// Throughput statistics for the entire benchmark run.
    pub rps: ObservationsStats,
}

/// # Observation Statistics
///
/// A detailed breakdown of a set of observations, including count, median, min, max, average, 95th percentile, and standard deviation.
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
    /// # Merge Statistics
    ///
    /// Merges a vector of `BenchStatistics` into a single, consolidated report.
    pub fn merge(mut stats: Vec<Self>) -> Self {
        if stats.is_empty() {
            return Self::default();
        }
        let configuration = std::mem::take(&mut stats.first_mut().unwrap().configuration);
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
            account_update_latency: ObservationsStats::merge(account_update_stats, true),
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
    /// # Merge Observation Statistics
    ///
    /// Merges a vector of `ObservationsStats` into a single, consolidated report.
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
