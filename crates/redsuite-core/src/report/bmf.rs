use std::{collections::BTreeMap, fs};

use json::Serialize;

use super::{store, MeasureValue, ScenarioRun, Unit};
use crate::Result;

fn run_gate(run: &ScenarioRun) -> Option<String> {
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
    for measurement in &run.measurements {
        let benchmark =
            format!("{}/{}", run.scenario, slug(&measurement.label));
        match (measurement.unit, &measurement.value) {
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
            eprintln!(
                "[redsuite] bmf: excluded {} — {reason}",
                scenario.run.scenario
            );
            continue;
        }
        export_run(&mut doc, &scenario.run);
        for cell in &scenario.cells {
            export_run(&mut doc, cell);
        }
    }
    for parent_slug in &campaign.orphan_cells {
        eprintln!(
            "[redsuite] bmf: excluded {parent_slug} cells — the parent run did not \
             conclude"
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
