use std::{cell::Cell, rc::Rc, time::Duration};

use crate::{api, Result};

// backlog must actually form before OVERLOAD is on the table
const OVERLOAD_BACKLOG_FLOOR: f64 = 5.0;
// drain within this fraction of arrival still counts as keeping up
const DRAIN_KEEPUP_FRACTION: f64 = 0.9;
const OVERLOAD_STREAK: usize = 2;

pub struct MonitorSpec {
    pub arrival_counter: String,
    pub drain_counter: String,
    pub backlog_gauge: String,
    pub busy_gauge: Option<String>,
    pub window: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct SteadyStateSample {
    pub elapsed_secs: f64,
    pub arrivals_total: f64,
    pub drained_total: f64,
    pub backlog: f64,
    pub busy: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SteadyStateVerdict {
    Pass,
    Overload,
    Invalid,
}

impl std::fmt::Display for SteadyStateVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            SteadyStateVerdict::Pass => "PASS",
            SteadyStateVerdict::Overload => "OVERLOAD",
            SteadyStateVerdict::Invalid => "INVALID",
        };
        write!(f, "{label}")
    }
}

#[derive(Debug)]
pub struct SteadyStateOutcome {
    pub verdict: SteadyStateVerdict,
    pub arrival_rate: f64,
    pub drain_rate: f64,
    pub backlog_peak: f64,
    pub backlog_end: f64,
    pub outstanding_peak: f64,
    pub busy_peak: f64,
    pub samples: Vec<SteadyStateSample>,
}

pub struct SteadyStateMonitor {
    stop: Rc<Cell<bool>>,
    task: tokio::task::JoinHandle<Vec<SteadyStateSample>>,
}

impl SteadyStateMonitor {
    // Requires a LocalSet (run_scenario provides one). Scrape failures skip
    // the sample rather than killing the monitor.
    pub fn start(metrics_url: String, spec: MonitorSpec) -> Self {
        let stop = Rc::new(Cell::new(false));
        let stop_flag = stop.clone();
        let task = tokio::task::spawn_local(async move {
            let started = tokio::time::Instant::now();
            let mut samples = Vec::new();
            loop {
                if let Ok(metrics) = api::scrape_metrics(&metrics_url).await {
                    samples.push(SteadyStateSample {
                        elapsed_secs: started.elapsed().as_secs_f64(),
                        arrivals_total: metrics
                            .value_sum(&spec.arrival_counter)
                            .unwrap_or(0.0),
                        drained_total: metrics
                            .value_sum(&spec.drain_counter)
                            .unwrap_or(0.0),
                        backlog: metrics
                            .value_sum(&spec.backlog_gauge)
                            .unwrap_or(0.0),
                        busy: spec
                            .busy_gauge
                            .as_deref()
                            .and_then(|gauge| metrics.value_sum(gauge))
                            .unwrap_or(0.0),
                    });
                }
                // a final fresh sample is taken after stop is requested
                if stop_flag.get() {
                    break;
                }
                tokio::time::sleep(spec.window).await;
            }
            samples
        });
        Self { stop, task }
    }

    pub async fn finish(self) -> Result<SteadyStateOutcome> {
        self.stop.set(true);
        let samples = self
            .task
            .await
            .map_err(|join_error| format!("monitor task: {join_error}"))?;
        Ok(judge(samples))
    }
}

fn judge(samples: Vec<SteadyStateSample>) -> SteadyStateOutcome {
    let backlog_peak = samples
        .iter()
        .map(|sample| sample.backlog)
        .fold(0.0, f64::max);
    let busy_peak =
        samples.iter().map(|sample| sample.busy).fold(0.0, f64::max);
    let backlog_end =
        samples.last().map(|sample| sample.backlog).unwrap_or(0.0);

    let (Some(first), Some(last)) = (samples.first(), samples.last()) else {
        return SteadyStateOutcome {
            verdict: SteadyStateVerdict::Invalid,
            arrival_rate: 0.0,
            drain_rate: 0.0,
            backlog_peak,
            backlog_end,
            outstanding_peak: 0.0,
            busy_peak,
            samples,
        };
    };
    let outstanding_peak = samples
        .iter()
        .map(|sample| {
            (sample.arrivals_total - first.arrivals_total)
                - (sample.drained_total - first.drained_total)
        })
        .fold(0.0, f64::max);
    let span_secs = last.elapsed_secs - first.elapsed_secs;
    let arrivals = last.arrivals_total - first.arrivals_total;
    let drained = last.drained_total - first.drained_total;
    let (arrival_rate, drain_rate) = if span_secs > 0.0 {
        (arrivals / span_secs, drained / span_secs)
    } else {
        (0.0, 0.0)
    };

    let mut lagging_streak = 0usize;
    let mut overloaded = false;
    for pair in samples.windows(2) {
        let window_arrivals = pair[1].arrivals_total - pair[0].arrivals_total;
        let window_drained = pair[1].drained_total - pair[0].drained_total;
        let outstanding_end = (pair[1].arrivals_total - first.arrivals_total)
            - (pair[1].drained_total - first.drained_total);
        let queue_deep =
            outstanding_end.max(pair[1].backlog) >= OVERLOAD_BACKLOG_FLOOR;
        let drain_lagging = window_arrivals > 0.0
            && window_drained < window_arrivals * DRAIN_KEEPUP_FRACTION;
        if queue_deep && drain_lagging {
            lagging_streak += 1;
            if lagging_streak >= OVERLOAD_STREAK {
                overloaded = true;
            }
        } else {
            lagging_streak = 0;
        }
    }
    let verdict = if samples.len() < 2 || arrivals <= 0.0 {
        // measured nothing — must never read as a pass (cross-cutting #5)
        SteadyStateVerdict::Invalid
    } else if overloaded {
        SteadyStateVerdict::Overload
    } else {
        SteadyStateVerdict::Pass
    };

    SteadyStateOutcome {
        verdict,
        arrival_rate,
        drain_rate,
        backlog_peak,
        backlog_end,
        outstanding_peak,
        busy_peak,
        samples,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(
        elapsed_secs: f64,
        arrivals_total: f64,
        drained_total: f64,
        backlog: f64,
    ) -> SteadyStateSample {
        SteadyStateSample {
            elapsed_secs,
            arrivals_total,
            drained_total,
            backlog,
            busy: 0.0,
        }
    }

    #[test]
    fn keeping_up_is_a_pass() {
        let outcome = judge(vec![
            sample(0.0, 100.0, 100.0, 0.0),
            sample(10.0, 150.0, 148.0, 2.0),
        ]);
        assert_eq!(outcome.verdict, SteadyStateVerdict::Pass);
        assert!((outcome.arrival_rate - 5.0).abs() < 1e-9);
    }

    #[test]
    fn growing_backlog_with_lagging_drain_is_overload() {
        let outcome = judge(vec![
            sample(0.0, 100.0, 100.0, 0.0),
            sample(10.0, 200.0, 110.0, 60.0),
            sample(20.0, 300.0, 120.0, 150.0),
        ]);
        assert_eq!(outcome.verdict, SteadyStateVerdict::Overload);
        assert_eq!(outcome.backlog_peak, 150.0);
        assert!(outcome.drain_rate < outcome.arrival_rate);
    }

    #[test]
    fn measuring_nothing_is_invalid_not_a_pass() {
        let outcome = judge(vec![
            sample(0.0, 100.0, 100.0, 0.0),
            sample(10.0, 100.0, 100.0, 0.0),
        ]);
        assert_eq!(outcome.verdict, SteadyStateVerdict::Invalid);
        assert_eq!(judge(Vec::new()).verdict, SteadyStateVerdict::Invalid);
    }

    #[test]
    fn stuck_backlog_gauge_cannot_mask_overload() {
        let outcome = judge(vec![
            sample(0.0, 100.0, 100.0, 0.0),
            sample(10.0, 200.0, 110.0, 0.0),
            sample(20.0, 300.0, 120.0, 0.0),
        ]);
        assert_eq!(outcome.verdict, SteadyStateVerdict::Overload);
        assert_eq!(outcome.backlog_peak, 0.0);
        assert_eq!(outcome.outstanding_peak, 180.0);
    }

    #[test]
    fn full_eventual_drain_cannot_mask_overload() {
        let outcome = judge(vec![
            sample(0.0, 0.0, 0.0, 0.0),
            sample(10.0, 100.0, 5.0, 0.0),
            sample(20.0, 150.0, 10.0, 0.0),
            sample(30.0, 150.0, 80.0, 0.0),
            sample(40.0, 150.0, 150.0, 0.0),
        ]);
        assert_eq!(outcome.verdict, SteadyStateVerdict::Overload);
        assert_eq!(outcome.outstanding_peak, 140.0);
    }

    #[test]
    fn single_lagging_window_is_noise_not_overload() {
        let outcome = judge(vec![
            sample(0.0, 0.0, 0.0, 0.0),
            sample(10.0, 100.0, 90.0, 0.0),
            sample(20.0, 200.0, 200.0, 0.0),
            sample(30.0, 300.0, 300.0, 0.0),
        ]);
        assert_eq!(outcome.verdict, SteadyStateVerdict::Pass);
    }
}
