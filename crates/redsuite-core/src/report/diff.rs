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

fn print_run_context(baseline: &RunView, latest: &RunView) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        report::{PersistedFailure, ScenarioReport, SCHEMA_VERSION},
        stats::ObservationsStats,
    };

    fn doc_of(report: ScenarioReport) -> ScenarioRun {
        ScenarioRun {
            schema: SCHEMA_VERSION,
            run: "test-run".into(),
            scenario: report.scenario,
            passed: report.passed,
            config: report.config,
            measurements: report.measurements,
            failures: Vec::new(),
        }
    }

    fn sample_stats() -> ObservationsStats {
        ObservationsStats {
            count: 400,
            median: 8480,
            min: 8030,
            max: 10485,
            avg: 8531,
            quantile95: 9022,
            stddev: 274,
        }
    }

    #[test]
    fn verdict_thresholds() {
        let lower = Direction::LowerIsBetter;
        let higher = Direction::HigherIsBetter;
        assert_eq!(verdict(lower, 100.0, 150.0), Verdict::Flat);
        assert_eq!(verdict(lower, 100.0, 199.0), Verdict::Flat);
        assert_eq!(verdict(lower, 100.0, 51.0), Verdict::Flat);
        assert_eq!(verdict(lower, 100.0, 250.0), Verdict::Regression);
        assert_eq!(verdict(lower, 100.0, 45.0), Verdict::Improvement);
        assert_eq!(verdict(higher, 100.0, 45.0), Verdict::Regression);
        assert_eq!(verdict(higher, 100.0, 250.0), Verdict::Improvement);
        assert_eq!(verdict(lower, 100.0, 0.0), Verdict::Improvement);
        assert_eq!(verdict(Direction::Info, 1.0, 5.0), Verdict::Info);
        assert_eq!(verdict(lower, 0.0, 5.0), Verdict::Info);
    }

    #[test]
    fn quantile_agreement_gates_the_verdict() {
        assert_eq!(
            combined_verdict(Verdict::Regression, Verdict::Regression),
            Verdict::Regression
        );
        assert_eq!(
            combined_verdict(Verdict::Improvement, Verdict::Improvement),
            Verdict::Improvement
        );
        assert_eq!(
            combined_verdict(Verdict::Regression, Verdict::Flat),
            Verdict::Mixed
        );
        assert_eq!(
            combined_verdict(Verdict::Regression, Verdict::Improvement),
            Verdict::Mixed
        );
        assert_eq!(
            combined_verdict(Verdict::Improvement, Verdict::Flat),
            Verdict::Mixed
        );
        assert_eq!(
            combined_verdict(Verdict::Flat, Verdict::Flat),
            Verdict::Flat
        );
        assert_eq!(
            combined_verdict(Verdict::Info, Verdict::Regression),
            Verdict::Regression
        );
        assert_eq!(
            combined_verdict(Verdict::Info, Verdict::Info),
            Verdict::Info
        );
    }

    #[test]
    fn failed_runs_and_their_cells_are_gated_with_a_reason() {
        let mut failed = doc_of(ScenarioReport::failed("redline/x"));
        failed.failures.push(PersistedFailure {
            phase: "scenario".into(),
            kind: "check".into(),
            message: "the clone matches base".into(),
            expected: None,
            actual: None,
            context: Vec::new(),
        });
        let reason = run_gate(&failed).unwrap();
        assert!(reason.contains("run failed"));
        assert!(reason.contains("the clone matches base"));

        let passed_with_teardown_failure = ScenarioRun {
            failures: vec![PersistedFailure {
                phase: "teardown".into(),
                kind: "infrastructure".into(),
                message: "private ER `x` is still running".into(),
                expected: None,
                actual: None,
                context: Vec::new(),
            }],
            ..doc_of(ScenarioReport::ok("redline/y"))
        };
        let reason = run_gate(&passed_with_teardown_failure).unwrap();
        assert!(reason.contains("teardown"));

        assert!(run_gate(&doc_of(ScenarioReport::ok("redline/z"))).is_none());
    }

    #[test]
    fn the_baseline_skips_failed_and_reconfigured_runs() {
        let latest_config = vec![("rate".to_owned(), "200".to_owned())];
        let old_config = vec![("rate".to_owned(), "100".to_owned())];

        let comparable =
            doc_of(ScenarioReport::ok("redline/x").setting("rate", 200));
        let reconfigured =
            doc_of(ScenarioReport::ok("redline/x").setting("rate", 100));
        let failed = doc_of(ScenarioReport::failed("redline/x"));

        let view = |run: &'static ScenarioRun, stamp: &str| RunView {
            stamp: stamp.to_owned(),
            file: format!("{stamp}-file"),
            meta: None,
            run,
            gate: run_gate(run),
        };
        let comparable_ref: &'static ScenarioRun =
            Box::leak(Box::new(comparable));
        let reconfigured_ref: &'static ScenarioRun =
            Box::leak(Box::new(reconfigured));
        let failed_ref: &'static ScenarioRun = Box::leak(Box::new(failed));

        let earlier = vec![
            view(comparable_ref, "a"),
            view(reconfigured_ref, "b"),
            view(failed_ref, "c"),
        ];
        let baseline = pick_baseline(&earlier, &latest_config).unwrap();
        assert_eq!(baseline.stamp, "a");

        assert!(pick_baseline(&earlier[1..], &latest_config).is_none());
        assert!(baseline_gap_reason(&earlier[1..], &latest_config)
            .contains("run failed"));
        let reconfigured_reason =
            baseline_gap_reason(&earlier[1..2], &latest_config);
        assert!(reconfigured_reason.contains("different config"));
        assert!(reconfigured_reason.contains("rate: 100 → 200"));

        assert!(pick_baseline(&earlier, &old_config).is_some());
    }

    #[test]
    fn rows_compare_typed_values_and_flag_unit_changes() {
        let baseline = doc_of(
            ScenarioReport::ok("redline/x")
                .observe("delivery us", Unit::Micros, sample_stats())
                .metric("achieved tps", Unit::Tps, 100.0)
                .metric("evictions in window", Unit::Count, 4.0)
                .metric("reshaped", Unit::Count, 1.0),
        );
        let latest = doc_of(
            ScenarioReport::ok("redline/x")
                .observe(
                    "delivery us",
                    Unit::Micros,
                    ObservationsStats {
                        median: 20_000,
                        quantile95: 25_000,
                        ..sample_stats()
                    },
                )
                .metric("achieved tps", Unit::Tps, 30.0)
                .metric("evictions in window", Unit::Ratio, 4.0)
                .observe("reshaped", Unit::Count, sample_stats()),
        );
        let rows = measurement_rows(&baseline, &latest);
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].0, Verdict::Regression);
        assert_eq!(rows[1].0, Verdict::Regression);
        assert!(rows[2].1.contains("unit changed"));
        assert!(rows[3].1.contains("value shape changed"));
    }

    #[test]
    fn bmf_mapping_keeps_the_v0_benchmark_names() {
        let run = doc_of(
            ScenarioReport::ok("redline/rpc_warm_ingress")
                .observe("delivery us", Unit::Micros, sample_stats())
                .metric("achieved tps", Unit::Tps, 198.8)
                .metric("superseded", Unit::Count, 0.0)
                .metric("validator tx processing avg us", Unit::Micros, 7815.3),
        );
        let campaign =
            store::Campaign {
                dir_name: "run-a".into(),
                meta: None,
                scenarios: vec![store::StoredScenario {
                    file: "redline-rpc_warm_ingress.json".into(),
                    run,
                    cells: vec![doc_of(
                        ScenarioReport::ok("redline/rpc_warm_ingress/cold")
                            .metric("achieved rps", Unit::Rps, 55.0),
                    )],
                }],
                orphan_cells: Vec::new(),
            };
        let doc = bmf_document(&campaign);

        let delivery = &doc["redline/rpc_warm_ingress/delivery"]["latency"];
        assert_eq!(delivery.value, 8_480_000.0);
        assert_eq!(delivery.lower_value, Some(8_030_000.0));
        let tps = &doc["redline/rpc_warm_ingress/achieved-tps"]["throughput"];
        assert_eq!(tps.value, 198.8);
        assert!(
            doc["redline/rpc_warm_ingress/validator-tx-processing-avg"]
                ["latency"]
                .value
                > 7_800_000.0
        );
        let cell_rps =
            &doc["redline/rpc_warm_ingress/cold/achieved-rps"]["throughput"];
        assert_eq!(cell_rps.value, 55.0);
        assert!(!doc.contains_key("redline/rpc_warm_ingress/superseded"));
    }

    #[test]
    fn bmf_excludes_failed_runs_and_their_cells() {
        let campaign = store::Campaign {
            dir_name: "run-a".into(),
            meta: None,
            scenarios: vec![store::StoredScenario {
                file: "redline-x.json".into(),
                run: doc_of(ScenarioReport::failed("redline/x")),
                cells: vec![doc_of(
                    ScenarioReport::ok("redline/x/cell").metric(
                        "achieved tps",
                        Unit::Tps,
                        10.0,
                    ),
                )],
            }],
            orphan_cells: vec![store::OrphanCells {
                parent_slug: "redline-y".into(),
                cells: vec![doc_of(
                    ScenarioReport::ok("redline/y/cell").metric(
                        "achieved tps",
                        Unit::Tps,
                        10.0,
                    ),
                )],
            }],
        };
        assert!(bmf_document(&campaign).is_empty());
    }
}
