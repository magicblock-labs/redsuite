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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merged_partitions_match_a_single_accumulator() {
        let mut single = StreamingStats::new();
        let mut first = StreamingStats::new();
        let mut second = StreamingStats::new();
        for value in [1, 1, 1, 1] {
            single.push(value);
            first.push(value);
        }
        single.push(100);
        second.push(100);

        first.merge(second);
        let merged = first.finalize(false);
        let expected = single.finalize(false);

        assert_eq!(merged.count, expected.count);
        assert_eq!(merged.avg, expected.avg);
        assert_eq!(merged.avg, 20);
        assert_eq!(merged.median, expected.median);
        assert_eq!(merged.quantile95, expected.quantile95);
        assert_eq!(merged.stddev, expected.stddev);
        assert_eq!(merged.min, expected.min);
        assert_eq!(merged.max, expected.max);
    }

    #[test]
    fn reservoir_merge_downsamples_to_capacity() {
        let mut heavy = StreamingStats::with_reservoir_size(8);
        let mut light = StreamingStats::with_reservoir_size(8);
        for _ in 0..16 {
            heavy.push(0);
        }
        for _ in 0..4 {
            light.push(1_000);
        }
        heavy.merge(light);
        assert_eq!(heavy.count, 20);
        assert_eq!(heavy.reservoir.len(), 8);
        let merged = heavy.finalize(false);
        assert_eq!(merged.min, 0);
        assert_eq!(merged.max, 1_000);
        assert_eq!(merged.avg, 200);
    }

    #[test]
    fn merge_into_empty_adopts_the_other_partition() {
        let mut empty = StreamingStats::new();
        let mut filled = StreamingStats::new();
        for value in [5, 10, 15] {
            filled.push(value);
        }
        empty.merge(filled);
        let merged = empty.finalize(false);
        assert_eq!(merged.count, 3);
        assert_eq!(merged.min, 5);
        assert_eq!(merged.max, 15);
        assert_eq!(merged.avg, 10);
        assert_eq!(merged.median, 10);
    }

    #[test]
    fn rate_addition_sums_per_thread_stats() {
        let first = ObservationsStats {
            count: 3,
            median: 10,
            min: 1,
            max: 20,
            avg: 11,
            quantile95: 19,
            stddev: 2,
        };
        let second = ObservationsStats {
            count: 2,
            median: 30,
            min: 2,
            max: 40,
            avg: 29,
            quantile95: 39,
            stddev: 3,
        };
        let summed = first.add_rates(second);
        assert_eq!(summed.count, 5);
        assert_eq!(summed.median, 40);
        assert_eq!(summed.min, 3);
        assert_eq!(summed.max, 60);
        assert_eq!(summed.avg, 40);
        assert_eq!(summed.quantile95, 58);
        assert_eq!(summed.stddev, 5);
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

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default)]
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
