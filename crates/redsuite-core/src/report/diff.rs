use std::{collections::BTreeMap, fs};

use json::Serialize;

use super::{
    store::{self, ReportStore},
    CampaignMeta, Direction, MeasureValue, Measurement, ScenarioRun, Unit,
};
use crate::Result;

struct RunView<'a> {
    stamp: String,
    file: String,
    meta: Option<&'a CampaignMeta>,
    run: &'a ScenarioRun,
    gate: Option<String>,
}

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

fn timelines(report_store: &ReportStore) -> BTreeMap<String, Vec<RunView<'_>>> {
    let mut map: BTreeMap<String, Vec<RunView>> = BTreeMap::new();
    for legacy in &report_store.legacy {
        map.entry(legacy.run.scenario.clone())
            .or_default()
            .push(RunView {
                stamp: legacy.meta.started_at.clone(),
                file: legacy.file.clone(),
                meta: Some(&legacy.meta),
                run: &legacy.run,
                gate: run_gate(&legacy.run),
            });
    }
    for campaign in &report_store.campaigns {
        let stamp = campaign.stamp().to_owned();
        for scenario in &campaign.scenarios {
            map.entry(scenario.run.scenario.clone()).or_default().push(
                RunView {
                    stamp: stamp.clone(),
                    file: format!("{}/{}", campaign.dir_name, scenario.file),
                    meta: campaign.meta.as_ref(),
                    run: &scenario.run,
                    gate: run_gate(&scenario.run),
                },
            );
            let parent_gate = run_gate(&scenario.run).map(|reason| {
                format!(
                    "parent {} did not pass ({reason})",
                    scenario.run.scenario
                )
            });
            for cell in &scenario.cells {
                map.entry(cell.scenario.clone()).or_default().push(RunView {
                    stamp: stamp.clone(),
                    file: format!("{}/{}", campaign.dir_name, scenario.file),
                    meta: campaign.meta.as_ref(),
                    run: cell,
                    gate: parent_gate.clone().or_else(|| run_gate(cell)),
                });
            }
        }
        for orphans in &campaign.orphan_cells {
            for cell in &orphans.cells {
                map.entry(cell.scenario.clone()).or_default().push(RunView {
                    stamp: stamp.clone(),
                    file: format!(
                        "{}/{}.cells.jsonl",
                        campaign.dir_name, orphans.parent_slug
                    ),
                    meta: campaign.meta.as_ref(),
                    run: cell,
                    gate: Some(
                        "the parent run is absent — the scenario did not \
                         conclude"
                            .to_owned(),
                    ),
                });
            }
        }
    }
    for views in map.values_mut() {
        views.sort_by(|left, right| left.stamp.cmp(&right.stamp));
    }
    map
}

fn profile_of(run: &ScenarioRun) -> &str {
    run.config
        .iter()
        .find(|(key, _)| key == "profile")
        .map(|(_, value)| value.as_str())
        .unwrap_or("-")
}

pub fn list() -> Result<()> {
    let report_store = store::load()?;
    if report_store.campaigns.is_empty() && report_store.legacy.is_empty() {
        println!(
            "no reports in {} — run a scenario first",
            super::reports_dir().display()
        );
        return Ok(());
    }
    for campaign in &report_store.campaigns {
        match &campaign.meta {
            Some(meta) => println!(
                "campaign {} run={} er=\"{}\" [{}]",
                meta.started_at, meta.run, meta.er_version, meta.er_fingerprint
            ),
            None => println!(
                "campaign {} (campaign.json is missing)",
                campaign.dir_name
            ),
        }
        for scenario in &campaign.scenarios {
            let failure_note = match scenario.run.failures.len() {
                0 => String::new(),
                count => format!(" failures={count}"),
            };
            println!(
                "  {}  passed={}{failure_note} profile={}",
                scenario.run.scenario,
                scenario.run.passed,
                profile_of(&scenario.run),
            );
            if scenario.run.scenario.starts_with("suite/") {
                print_host(&scenario.run);
                for measurement in &scenario.run.measurements {
                    if let Some(value) = measurement.scalar() {
                        println!("    {}: {value:.3}", measurement.label);
                    }
                }
            }
            for cell in &scenario.cells {
                println!("    cell {}  passed={}", cell.scenario, cell.passed);
            }
        }
        for orphans in &campaign.orphan_cells {
            println!(
                "  {}: cells without a concluded parent run",
                orphans.parent_slug
            );
            for cell in &orphans.cells {
                println!("    cell {}  passed={}", cell.scenario, cell.passed);
            }
        }
    }
    if !report_store.legacy.is_empty() {
        println!("legacy reports (schema 0):");
        for legacy in &report_store.legacy {
            println!(
                "  {}  passed={} profile={} er=\"{}\" [{}]",
                legacy.file,
                legacy.run.passed,
                profile_of(&legacy.run),
                legacy.meta.er_version,
                legacy.meta.er_fingerprint,
            );
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum Verdict {
    Flat,
    Regression,
    Improvement,
    Mixed,
    Info,
}

const FLAT_FOLD_CHANGE: f64 = 2.0;

fn verdict(direction: Direction, old: f64, new: f64) -> Verdict {
    if matches!(direction, Direction::Info) || old == 0.0 {
        return Verdict::Info;
    }
    let fold_change = if new > old {
        new / old
    } else {
        old / new.max(f64::MIN_POSITIVE)
    };
    if fold_change < FLAT_FOLD_CHANGE {
        return Verdict::Flat;
    }
    let worse = match direction {
        Direction::LowerIsBetter => new > old,
        Direction::HigherIsBetter => new < old,
        Direction::Info => unreachable!(),
    };
    if worse {
        Verdict::Regression
    } else {
        Verdict::Improvement
    }
}

fn verdict_tag(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::Flat => "~ flat",
        Verdict::Regression => "▲ worse",
        Verdict::Improvement => "▼ better",
        Verdict::Mixed => "~ mixed",
        Verdict::Info => "",
    }
}

fn combined_verdict(median: Verdict, quantile95: Verdict) -> Verdict {
    match (median, quantile95) {
        (median, quantile95) if median == quantile95 => median,
        (Verdict::Info, other) | (other, Verdict::Info) => other,
        _ => Verdict::Mixed,
    }
}

fn pct(old: f64, new: f64) -> String {
    if old == 0.0 {
        return String::new();
    }
    format!("{:+.1}%", (new - old) / old * 100.0)
}

fn pick_baseline<'view, 'store>(
    earlier: &'view [RunView<'store>],
    config: &[(String, String)],
) -> Option<&'view RunView<'store>> {
    earlier.iter().rev().find(|candidate| {
        candidate.gate.is_none() && candidate.run.config == *config
    })
}

fn baseline_gap_reason(
    earlier: &[RunView],
    config: &[(String, String)],
) -> String {
    let Some(nearest) = earlier.last() else {
        return "no earlier run".to_owned();
    };
    match &nearest.gate {
        Some(reason) => {
            format!("nearest earlier run {}: {reason}", nearest.file)
        }
        None => format!(
            "nearest earlier run {} used a different config ({})",
            nearest.file,
            config_gap(&nearest.run.config, config),
        ),
    }
}

fn config_gap(old: &[(String, String)], new: &[(String, String)]) -> String {
    for (key, new_value) in new {
        match old.iter().find(|(old_key, _)| old_key == key) {
            None => return format!("{key} is new"),
            Some((_, old_value)) if old_value != new_value => {
                return format!("{key}: {old_value} → {new_value}")
            }
            Some(_) => {}
        }
    }
    for (key, _) in old {
        if !new.iter().any(|(new_key, _)| new_key == key) {
            return format!("{key} is gone");
        }
    }
    "keys reordered".to_owned()
}

fn print_host(run: &ScenarioRun) {
    if let Some((_, host)) = run.config.iter().find(|(key, _)| key == "host") {
        println!("  profile={} host={host}", profile_of(run));
    }
}

fn print_run_context(baseline: &RunView, latest: &RunView) {
    print_host(latest.run);
    println!("  prev: {}", baseline.file);
    println!("  last: {}", latest.file);
    match (baseline.meta, latest.meta) {
        (Some(prev_meta), Some(last_meta))
            if prev_meta.er_fingerprint == last_meta.er_fingerprint =>
        {
            println!(
                "  validator: same build ({}) — differences are noise or harness changes",
                last_meta.er_version
            );
        }
        (Some(prev_meta), Some(last_meta)) => {
            println!("  validator: DIFFERENT builds");
            println!(
                "    prev: \"{}\" [{}] {}",
                prev_meta.er_version,
                prev_meta.er_fingerprint,
                prev_meta.er_bin,
            );
            println!(
                "    last: \"{}\" [{}] {}",
                last_meta.er_version,
                last_meta.er_fingerprint,
                last_meta.er_bin,
            );
        }
        _ => println!("  validator: build provenance unknown"),
    }
}

fn measurement_rows(
    baseline: &ScenarioRun,
    latest: &ScenarioRun,
) -> Vec<(Verdict, String)> {
    let previous: BTreeMap<&str, &Measurement> = baseline
        .measurements
        .iter()
        .map(|measurement| (measurement.label.as_str(), measurement))
        .collect();
    let mut rows = Vec::new();
    for measurement in &latest.measurements {
        let Some(old) = previous.get(measurement.label.as_str()) else {
            continue;
        };
        let label = &measurement.label;
        if old.unit != measurement.unit {
            rows.push((
                Verdict::Info,
                format!(
                    "  {label:<34} unit changed ({:?} → {:?}) — not compared",
                    old.unit, measurement.unit
                ),
            ));
            continue;
        }
        match (&old.value, &measurement.value) {
            (
                MeasureValue::Distribution(old_stats),
                MeasureValue::Distribution(new_stats),
            ) => {
                let median_verdict = verdict(
                    measurement.direction,
                    old_stats.median as f64,
                    new_stats.median as f64,
                );
                let quantile95_verdict = verdict(
                    measurement.direction,
                    old_stats.quantile95 as f64,
                    new_stats.quantile95 as f64,
                );
                let row_verdict =
                    combined_verdict(median_verdict, quantile95_verdict);
                rows.push((
                    row_verdict,
                    format!(
                        "  {label:<34} median {} → {} ({})  p95 {} → {} ({})  {}",
                        old_stats.median,
                        new_stats.median,
                        pct(old_stats.median as f64, new_stats.median as f64),
                        old_stats.quantile95,
                        new_stats.quantile95,
                        pct(
                            old_stats.quantile95 as f64,
                            new_stats.quantile95 as f64
                        ),
                        verdict_tag(row_verdict),
                    ),
                ));
            }
            (
                MeasureValue::Scalar(old_value),
                MeasureValue::Scalar(new_value),
            ) => {
                let row_verdict =
                    verdict(measurement.direction, *old_value, *new_value);
                rows.push((
                    row_verdict,
                    format!(
                        "  {label:<34} {old_value:.1} → {new_value:.1} ({})  {}",
                        pct(*old_value, *new_value),
                        verdict_tag(row_verdict),
                    ),
                ));
            }
            _ => rows.push((
                Verdict::Info,
                format!("  {label:<34} value shape changed — not compared"),
            )),
        }
    }
    rows
}

pub fn compare(filter: Option<&str>, strict: bool, brief: bool) -> Result<()> {
    let report_store = store::load()?;
    let map = timelines(&report_store);
    let mut regressions = 0usize;
    let mut compared = 0usize;

    for (scenario, runs) in &map {
        if filter.is_some_and(|wanted| !scenario.contains(wanted)) {
            continue;
        }
        let has_cell_children = map
            .keys()
            .any(|other| other.starts_with(&format!("{scenario}/")));
        if brief && has_cell_children {
            continue;
        }
        if runs.len() < 2 {
            continue;
        }
        let latest = runs.last().unwrap();
        if let Some(reason) = &latest.gate {
            println!("{scenario}");
            println!("  not compared: latest {}: {reason}", latest.file);
            println!();
            continue;
        }
        let earlier = &runs[..runs.len() - 1];
        let Some(baseline) = pick_baseline(earlier, &latest.run.config) else {
            println!("{scenario}");
            println!(
                "  not compared: no comparable baseline — {}",
                baseline_gap_reason(earlier, &latest.run.config)
            );
            println!();
            continue;
        };
        compared += 1;

        let rows = measurement_rows(baseline.run, latest.run);
        regressions += rows
            .iter()
            .filter(|(row_verdict, _)| {
                matches!(row_verdict, Verdict::Regression)
            })
            .count();

        if brief {
            let changed: Vec<&String> = rows
                .iter()
                .filter(|(row_verdict, _)| {
                    matches!(
                        row_verdict,
                        Verdict::Regression | Verdict::Improvement
                    )
                })
                .map(|(_, line)| line)
                .collect();
            if !changed.is_empty() {
                println!("{scenario}");
                for line in &changed {
                    println!("{line}");
                }
                let hidden = rows.len() - changed.len();
                if hidden > 0 {
                    println!(
                        "  ({hidden} flat/mixed/info metric(s) not shown)"
                    );
                }
                println!();
            }
        } else {
            println!("{scenario}");
            print_run_context(baseline, latest);
            for (_, line) in &rows {
                println!("{line}");
            }
            println!();
        }
    }

    if compared == 0 {
        println!(
            "nothing compared — need at least two comparable runs of a \
             scenario"
        );
    } else if regressions > 0 {
        println!("{regressions} metric(s) worse than base");
        if strict {
            return Err("metrics worse than base (--strict)".into());
        }
    } else {
        println!("nothing worse than base");
    }
    Ok(())
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
    for orphans in &campaign.orphan_cells {
        eprintln!(
            "[redsuite] bmf: excluded {} cells — the parent run did not \
             conclude",
            orphans.parent_slug
        );
    }
    doc
}

pub fn bmf(out: Option<&str>) -> Result<()> {
    let report_store = store::load()?;
    let Some(latest) = report_store.campaigns.last() else {
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
