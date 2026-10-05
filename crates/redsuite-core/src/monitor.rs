use std::time::Duration;

use crate::{
    api::MetricsCollector,
    sampler::{Sampler, Trailing},
};

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

#[derive(Debug, Clone, Copy, Default)]
pub struct SteadyStateSample {
    pub elapsed_secs: f64,
    pub arrivals_total: f64,
    pub drained_total: f64,
    pub backlog: f64,
    pub busy: Option<f64>,
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
    pub busy_peak: Option<f64>,
    pub samples: Vec<SteadyStateSample>,
}

pub fn start(
    collector: MetricsCollector,
    spec: MonitorSpec,
) -> Sampler<Vec<SteadyStateSample>> {
    let started = std::time::Instant::now();
    Sampler::spawn(
        collector,
        spec.window,
        Trailing::Sample,
        move |metrics, samples: &mut Vec<SteadyStateSample>| {
            samples.push(SteadyStateSample {
                elapsed_secs: started.elapsed().as_secs_f64(),
                arrivals_total: metrics.value_sum(&spec.arrival_counter)?,
                drained_total: metrics.value_sum(&spec.drain_counter)?,
                backlog: metrics.value_sum(&spec.backlog_gauge)?,
                busy: spec
                    .busy_gauge
                    .as_deref()
                    .and_then(|gauge| metrics.value_sum(gauge)),
            });
            Some(())
        },
    )
}

pub fn judge(samples: Vec<SteadyStateSample>) -> SteadyStateOutcome {
    let backlog_peak = samples
        .iter()
        .map(|sample| sample.backlog)
        .fold(0.0, f64::max);
    let first = samples.first().copied().unwrap_or_default();
    let last = samples.last().copied().unwrap_or_default();
    let outstanding = |sample: &SteadyStateSample| {
        (sample.arrivals_total - first.arrivals_total)
            - (sample.drained_total - first.drained_total)
    };
    let outstanding_peak = samples.iter().map(outstanding).fold(0.0, f64::max);
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
        let queue_deep = outstanding(&pair[1]).max(pair[1].backlog)
            >= OVERLOAD_BACKLOG_FLOOR;
        let drain_lagging = window_arrivals > 0.0
            && window_drained < window_arrivals * DRAIN_KEEPUP_FRACTION;
        lagging_streak = if queue_deep && drain_lagging {
            lagging_streak + 1
        } else {
            0
        };
        overloaded |= lagging_streak >= OVERLOAD_STREAK;
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
        backlog_end: last.backlog,
        outstanding_peak,
        busy_peak: samples
            .iter()
            .filter_map(|sample| sample.busy)
            .reduce(f64::max),
        samples,
    }
}
