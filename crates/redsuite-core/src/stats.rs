use std::collections::HashMap;

use json::{Deserialize, Serialize};
use rand::{rngs::StdRng, Rng, SeedableRng};

#[derive(Debug)]
pub struct StreamingStats {
    count: usize,
    mean: f64,
    m2: f64, // sum of squared deviations, for variance
    min: u32,
    max: u32,
    reservoir: Vec<u32>,
    reservoir_size: usize,
    rng: StdRng,
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
            rng: StdRng::from_entropy(),
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

    pub fn merge(&mut self, other: StreamingStats) {
        if other.count == 0 {
            return;
        }
        let own_weight = self.count as f64;
        let other_weight = other.count as f64;
        let combined_weight = own_weight + other_weight;
        let delta = other.mean - self.mean;
        self.mean += delta * other_weight / combined_weight;
        self.m2 += other.m2
            + delta * delta * own_weight * other_weight / combined_weight;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.merge_reservoir(other.reservoir, other.count);
        self.count += other.count;
    }

    fn merge_reservoir(
        &mut self,
        mut other_reservoir: Vec<u32>,
        other_count: usize,
    ) {
        if self.reservoir.len() + other_reservoir.len() <= self.reservoir_size {
            self.reservoir.append(&mut other_reservoir);
            return;
        }
        let mut own_reservoir = std::mem::take(&mut self.reservoir);
        let own_value_weight = self.count as f64 / own_reservoir.len() as f64;
        let other_value_weight =
            other_count as f64 / other_reservoir.len() as f64;
        let mut merged = Vec::with_capacity(self.reservoir_size);
        while merged.len() < self.reservoir_size
            && !(own_reservoir.is_empty() && other_reservoir.is_empty())
        {
            let source = if own_reservoir.is_empty() {
                &mut other_reservoir
            } else if other_reservoir.is_empty() {
                &mut own_reservoir
            } else {
                let own_remaining =
                    own_reservoir.len() as f64 * own_value_weight;
                let other_remaining =
                    other_reservoir.len() as f64 * other_value_weight;
                let draw =
                    self.rng.gen_range(0.0..own_remaining + other_remaining);
                if draw < own_remaining {
                    &mut own_reservoir
                } else {
                    &mut other_reservoir
                }
            };
            let picked = self.rng.gen_range(0..source.len());
            merged.push(source.swap_remove(picked));
        }
        self.reservoir = merged;
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
    pub configuration: json::Value,
    pub request_stats: HashMap<String, ObservationsStats>,
    pub signature_confirmation_latency: ObservationsStats,
    pub account_update_latency: ObservationsStats,
    pub rps: ObservationsStats,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq)]
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

impl ObservationsStats {
    pub fn add_rates(self, other: Self) -> Self {
        Self {
            count: self.count + other.count,
            median: self.median + other.median,
            min: self.min + other.min,
            max: self.max + other.max,
            avg: self.avg + other.avg,
            quantile95: self.quantile95 + other.quantile95,
            stddev: self.stddev + other.stddev,
        }
    }
}
