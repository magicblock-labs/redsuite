//! Read back persisted scenario reports: list, diff two runs, export BMF.

use std::{collections::BTreeMap, fs, path::PathBuf};

use json::{Deserialize, Serialize};

use crate::{topology::workspace_root, Result};

#[derive(Deserialize)]
struct Persisted {
    meta: Meta,
    report: Report,
}

#[derive(Deserialize)]
struct Meta {
    // redundant with the filename prefix, kept for schema fidelity
    #[allow(dead_code)]
    recorded_at: String,
    er_bin: String,
    er_version: String,
    er_fingerprint: String,
}

#[derive(Deserialize)]
struct Report {
    scenario: String,
    passed: bool,
    config: Vec<(String, String)>,
    observations: Vec<(String, Stats)>,
    metrics: Vec<(String, f64)>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)]
struct Stats {
    count: usize,
    median: i32,
    min: u32,
    max: u32,
    avg: i32,
    quantile95: i32,
    stddev: u32,
}

fn reports_dir() -> PathBuf {
    workspace_root().join("target/redsuite-reports")
}

// Chronological per scenario — the timestamp prefix makes name order time order.
fn load_all() -> Result<BTreeMap<String, Vec<(String, Persisted)>>> {
    let dir = reports_dir();
    let mut by_scenario: BTreeMap<String, Vec<(String, Persisted)>> =
        BTreeMap::new();
    let Ok(entries) = fs::read_dir(&dir) else {
        return Ok(by_scenario);
    };
    let mut files: Vec<PathBuf> = entries
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    for path in files {
        let doc: Persisted = json::from_str(&fs::read_to_string(&path)?)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        by_scenario
            .entry(doc.report.scenario.clone())
            .or_default()
            .push((name, doc));
    }
    Ok(by_scenario)
}

pub fn list() -> Result<()> {
    let all = load_all()?;
    if all.is_empty() {
        println!(
            "no reports in {} — run a scenario first",
            reports_dir().display()
        );
        return Ok(());
    }
    for (scenario, runs) in &all {
        println!("{scenario}");
        for (file, doc) in runs {
            let profile = doc
                .report
                .config
                .iter()
                .find(|(key, _)| key == "profile")
                .map(|(_, value)| value.as_str())
                .unwrap_or("-");
            println!(
                "  {file}  passed={} profile={profile} er=\"{}\" [{}]",
                doc.report.passed, doc.meta.er_version, doc.meta.er_fingerprint,
            );
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
enum Direction {
    Lower,  // latency-like: lower is better
    Higher, // throughput-like: higher is better
    Info,   // counts, gauges
}

fn direction(label: &str) -> Direction {
    let lowered = label.to_ascii_lowercase();
    if lowered.contains("rps") || lowered.contains("tps") {
        Direction::Higher
    } else if lowered.ends_with(" us")
        || lowered.contains("lag")
        || lowered.contains("latency")
    {
        Direction::Lower
    } else {
        Direction::Info
    }
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

fn verdict(dir: &Direction, old: f64, new: f64) -> Verdict {
    if matches!(dir, Direction::Info) || old == 0.0 {
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
    let worse = match dir {
        Direction::Lower => new > old,
        Direction::Higher => new < old,
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

fn print_run_context(
    prev_file: &str,
    prev: &Persisted,
    last_file: &str,
    last: &Persisted,
) {
    println!("  prev: {prev_file}");
    println!("  last: {last_file}");
    if prev.meta.er_fingerprint == last.meta.er_fingerprint {
        println!(
            "  validator: same build ({}) — differences are noise or harness changes",
            last.meta.er_version
        );
    } else {
        println!("  validator: DIFFERENT builds");
        println!(
            "    prev: \"{}\" [{}] {}",
            prev.meta.er_version, prev.meta.er_fingerprint, prev.meta.er_bin,
        );
        println!(
            "    last: \"{}\" [{}] {}",
            last.meta.er_version, last.meta.er_fingerprint, last.meta.er_bin,
        );
    }
}

pub fn compare(filter: Option<&str>, strict: bool, brief: bool) -> Result<()> {
    let all = load_all()?;
    let mut regressions = 0usize;
    let mut compared = 0usize;

    for (scenario, runs) in &all {
        if filter.is_some_and(|wanted| !scenario.contains(wanted)) {
            continue;
        }
        let has_cell_children = all
            .keys()
            .any(|other| other.starts_with(&format!("{scenario}/")));
        if brief && has_cell_children {
            continue;
        }
        if runs.len() < 2 {
            continue;
        }
        let (prev_file, prev) = &runs[runs.len() - 2];
        let (last_file, last) = &runs[runs.len() - 1];
        compared += 1;

        if prev.report.config != last.report.config {
            continue;
        }

        let mut rows: Vec<(Verdict, String)> = Vec::new();

        let prev_obs: BTreeMap<_, _> = prev
            .report
            .observations
            .iter()
            .map(|(label, stats)| (label.clone(), *stats))
            .collect();
        for (label, new_stats) in &last.report.observations {
            let Some(old_stats) = prev_obs.get(label) else {
                continue;
            };
            let dir = direction(label);
            let median_verdict =
                verdict(&dir, old_stats.median as f64, new_stats.median as f64);
            let quantile95_verdict = verdict(
                &dir,
                old_stats.quantile95 as f64,
                new_stats.quantile95 as f64,
            );
            let row_verdict =
                combined_verdict(median_verdict, quantile95_verdict);
            if row_verdict == Verdict::Regression {
                regressions += 1;
            }
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

        let prev_metrics: BTreeMap<_, _> = prev
            .report
            .metrics
            .iter()
            .map(|(label, stats)| (label.clone(), *stats))
            .collect();
        for (label, new_value) in &last.report.metrics {
            let Some(old_value) = prev_metrics.get(label) else {
                continue;
            };
            let dir = direction(label);
            let row_verdict = verdict(&dir, *old_value, *new_value);
            if row_verdict == Verdict::Regression {
                regressions += 1;
            }
            rows.push((
                row_verdict,
                format!(
                    "  {label:<34} {old_value:.1} → {new_value:.1} ({})  {}",
                    pct(*old_value, *new_value),
                    verdict_tag(row_verdict),
                ),
            ));
        }

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
            print_run_context(prev_file, prev, last_file, last);
            for (_, line) in &rows {
                println!("{line}");
            }
            println!();
        }
    }

    if compared == 0 {
        println!("nothing compared — need at least two runs of a scenario");
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

fn bmf_document(
    latest: &BTreeMap<String, &Report>,
) -> BTreeMap<String, BTreeMap<String, MeasureVal>> {
    let mut doc: BTreeMap<String, BTreeMap<String, MeasureVal>> =
        BTreeMap::new();
    for (scenario, report) in latest {
        for (label, stats) in &report.observations {
            if !label.ends_with(" us") {
                continue;
            }
            doc.entry(format!("{scenario}/{}", slug(label)))
                .or_default()
                .insert(
                    "latency".to_owned(),
                    MeasureVal {
                        value: stats.median as f64 * 1e3,
                        lower_value: Some(stats.min as f64 * 1e3),
                        upper_value: Some(stats.max as f64 * 1e3),
                    },
                );
        }
        for (label, value) in &report.metrics {
            let lowered = label.to_ascii_lowercase();
            if lowered.contains("tps") || lowered.contains("rps") {
                doc.entry(format!("{scenario}/{}", slug(label)))
                    .or_default()
                    .insert(
                        "throughput".to_owned(),
                        MeasureVal {
                            value: *value,
                            lower_value: None,
                            upper_value: None,
                        },
                    );
            } else if label.ends_with(" us") {
                doc.entry(format!("{scenario}/{}", slug(label)))
                    .or_default()
                    .insert(
                        "latency".to_owned(),
                        MeasureVal {
                            value: value * 1e3,
                            lower_value: None,
                            upper_value: None,
                        },
                    );
            }
        }
    }
    doc
}

pub fn bmf(out: Option<&str>) -> Result<()> {
    let all = load_all()?;
    let latest: BTreeMap<String, &Report> = all
        .iter()
        .filter_map(|(scenario, runs)| {
            runs.last().map(|(_, doc)| (scenario.clone(), &doc.report))
        })
        .collect();
    if latest.is_empty() {
        return Err("no reports to export — run a scenario first".into());
    }
    let body = json::to_string_pretty(&bmf_document(&latest))?;
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

    #[test]
    fn direction_heuristic_matches_report_labels() {
        assert_eq!(direction("delivery us"), Direction::Lower);
        assert_eq!(direction("account-update lag us"), Direction::Lower);
        assert_eq!(
            direction("validator tx processing avg us"),
            Direction::Lower
        );
        assert_eq!(direction("achieved tps"), Direction::Higher);
        assert_eq!(direction("achieved rps"), Direction::Higher);
        assert_eq!(direction("superseded"), Direction::Info);
        assert_eq!(direction("monitored accounts (gauge)"), Direction::Info);
        assert_eq!(direction("measured wall s"), Direction::Info);
    }

    #[test]
    fn verdict_thresholds() {
        let lower = Direction::Lower;
        let higher = Direction::Higher;
        assert_eq!(verdict(&lower, 100.0, 150.0), Verdict::Flat);
        assert_eq!(verdict(&lower, 100.0, 199.0), Verdict::Flat);
        assert_eq!(verdict(&lower, 100.0, 51.0), Verdict::Flat);
        assert_eq!(verdict(&lower, 100.0, 250.0), Verdict::Regression);
        assert_eq!(verdict(&lower, 100.0, 45.0), Verdict::Improvement);
        assert_eq!(verdict(&higher, 100.0, 45.0), Verdict::Regression);
        assert_eq!(verdict(&higher, 100.0, 250.0), Verdict::Improvement);
        assert_eq!(verdict(&lower, 100.0, 0.0), Verdict::Improvement);
        assert_eq!(verdict(&Direction::Info, 1.0, 5.0), Verdict::Info);
        assert_eq!(verdict(&lower, 0.0, 5.0), Verdict::Info);
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
    fn bmf_mapping() {
        let stats = Stats {
            count: 400,
            median: 8480,
            min: 8030,
            max: 10485,
            avg: 8531,
            quantile95: 9022,
            stddev: 274,
        };
        let report = Report {
            scenario: "redline/rpc_warm_ingress".into(),
            passed: true,
            config: vec![],
            observations: vec![("delivery us".into(), stats)],
            metrics: vec![
                ("achieved tps".into(), 198.8),
                ("superseded".into(), 0.0),
                ("validator tx processing avg us".into(), 7815.3),
            ],
        };
        let mut latest = BTreeMap::new();
        latest.insert(report.scenario.clone(), &report);
        let doc = bmf_document(&latest);

        let delivery = &doc["redline/rpc_warm_ingress/delivery"]["latency"];
        assert_eq!(delivery.value, 8_480_000.0); // µs → ns
        assert_eq!(delivery.lower_value, Some(8_030_000.0));
        let tps = &doc["redline/rpc_warm_ingress/achieved-tps"]["throughput"];
        assert_eq!(tps.value, 198.8);
        assert!(
            doc["redline/rpc_warm_ingress/validator-tx-processing-avg"]
                ["latency"]
                .value
                > 7_800_000.0
        );
        // plain counts don't become benchmark measures
        assert!(!doc.contains_key("redline/rpc_warm_ingress/superseded"));
    }
}
