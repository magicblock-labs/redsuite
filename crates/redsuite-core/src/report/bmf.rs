use std::{collections::BTreeMap, fs};

use json::Serialize;
use tracing::warn;

use super::{store, MeasureValue, ScenarioRun, Unit, LATENCY_ORIGIN};
use crate::{
    runner::{REQUEST_ORIGIN, SEND_ORIGIN},
    Result,
};

fn run_gate(run: &ScenarioRun) -> Option<String> {
    if run
        .config
        .iter()
        .any(|(key, value)| key == "completion" && value == "interrupted")
    {
        return Some("load interrupted".to_owned());
    }
    if !run.passed {
        return Some(match run.failures.first() {
            Some(failure) => {
                format!("run failed ({}: {})", failure.phase, failure.message)
            }
            None => "run failed".to_owned(),
        });
    }
    run.failures.first().map(|failure| {
        format!("run has a {} failure: {}", failure.phase, failure.message)
    })
}

fn origin_suffix(run: &ScenarioRun) -> String {
    run.config
        .iter()
        .find(|(key, _)| key == LATENCY_ORIGIN)
        .map(|(_, origins)| {
            let tags: Vec<&str> = origins
                .split("; ")
                .map(|origin| match origin {
                    SEND_ORIGIN => "send",
                    REQUEST_ORIGIN => "request",
                    other => other,
                })
                .collect();
            format!("@{}", slug(&tags.join("+")))
        })
        .unwrap_or_default()
}

fn withheld(run: &ScenarioRun) -> Option<String> {
    run.config
        .iter()
        .find(|(key, _)| super::LOAD_FLAGS.contains(&key.as_str()))
        .map(|(flag, reason)| format!("{flag}: {reason}"))
}

// Bencher Metric Format: {"benchmark": {"measure": {"value", "lower_value",
// "upper_value"}}}. Latency measures are nanoseconds; ours are µs → ×1000.
#[derive(Serialize)]
struct MeasureVal {
    value: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    lower_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upper_value: Option<f64>,
}

fn slug(label: &str) -> String {
    label
        .trim_end_matches(" us")
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch == ' ' { '-' } else { ch })
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
        .collect()
}

fn export_run(
    doc: &mut BTreeMap<String, BTreeMap<String, MeasureVal>>,
    run: &ScenarioRun,
) {
    let withheld = withheld(run);
    if let Some(reason) = &withheld {
        warn!(%run.scenario, reason, "BMF latency withheld");
    }
    let origin = origin_suffix(run);
    for measurement in &run.measurements {
        let label = &measurement.label;
        let mut benchmark = format!("{}/{}", run.scenario, slug(label));
        let client_latency = measurement.unit == Unit::Micros
            && !label.contains("validator")
            && !label.contains("scheduling delay")
            && !["boundary", "normal", "finalize", "snapshot", "checksum"]
                .contains(&label.split_whitespace().next().unwrap_or(""));
        if client_latency {
            benchmark.push_str(&origin);
        }
        match (measurement.unit, &measurement.value) {
            (Unit::Micros, _) if client_latency && withheld.is_some() => {}
            (Unit::Micros, MeasureValue::Distribution(stats)) => {
                doc.entry(benchmark).or_default().insert(
                    "latency".to_owned(),
                    MeasureVal {
                        value: stats.median as f64 * 1e3,
                        lower_value: Some(stats.min as f64 * 1e3),
                        upper_value: Some(stats.max as f64 * 1e3),
                    },
                );
            }
            (Unit::Micros, MeasureValue::Scalar(value)) => {
                doc.entry(benchmark).or_default().insert(
                    "latency".to_owned(),
                    MeasureVal {
                        value: value * 1e3,
                        lower_value: None,
                        upper_value: None,
                    },
                );
            }
            (Unit::Tps | Unit::Rps, MeasureValue::Scalar(value)) => {
                doc.entry(benchmark).or_default().insert(
                    "throughput".to_owned(),
                    MeasureVal {
                        value: *value,
                        lower_value: None,
                        upper_value: None,
                    },
                );
            }
            _ => {}
        }
    }
}

fn bmf_document(
    campaign: &store::Campaign,
) -> BTreeMap<String, BTreeMap<String, MeasureVal>> {
    let mut doc: BTreeMap<String, BTreeMap<String, MeasureVal>> =
        BTreeMap::new();
    for scenario in &campaign.scenarios {
        if let Some(reason) = run_gate(&scenario.run) {
            warn!(%scenario.run.scenario, reason, "BMF run excluded");
            continue;
        }
        export_run(&mut doc, &scenario.run);
        for cell in &scenario.cells {
            export_run(&mut doc, cell);
        }
    }
    for parent_slug in &campaign.orphan_cells {
        warn!(
            parent_slug,
            "BMF cells excluded; parent run did not conclude"
        );
    }
    doc
}

pub fn bmf(out: Option<&str>) -> Result<()> {
    let campaigns = store::load()?;
    let Some(latest) = campaigns.last() else {
        return Err(
            "no campaign reports to export — run a scenario first".into()
        );
    };
    let doc = bmf_document(latest);
    if doc.is_empty() {
        return Err("the latest campaign has no exportable measurements".into());
    }
    let body = json::to_string_pretty(&doc)?;
    match out {
        Some(path) => {
            fs::write(path, &body)?;
            println!("wrote {path}");
        }
        None => println!("{body}"),
    }
    Ok(())
}
