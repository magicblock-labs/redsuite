use std::{
    fs,
    path::{Path, PathBuf},
};

use json::Deserialize;

use super::{
    slug_of, CampaignMeta, Direction, MeasureValue, Measurement,
    PersistedFailure, ScenarioRun, Unit,
};
use crate::{stats::ObservationsStats, Result};

pub struct ReportStore {
    pub campaigns: Vec<Campaign>,
    pub legacy: Vec<LegacyRun>,
}

pub struct Campaign {
    pub dir_name: String,
    pub meta: Option<CampaignMeta>,
    pub scenarios: Vec<StoredScenario>,
    pub orphan_cells: Vec<OrphanCells>,
}

impl Campaign {
    pub fn stamp(&self) -> &str {
        self.meta
            .as_ref()
            .map(|meta| meta.started_at.as_str())
            .unwrap_or(&self.dir_name)
    }
}

pub struct StoredScenario {
    pub file: String,
    pub run: ScenarioRun,
    pub cells: Vec<ScenarioRun>,
}

pub struct OrphanCells {
    pub parent_slug: String,
    pub cells: Vec<ScenarioRun>,
}

pub struct LegacyRun {
    pub file: String,
    pub meta: CampaignMeta,
    pub run: ScenarioRun,
}

pub fn load() -> Result<ReportStore> {
    load_from(&super::reports_dir())
}

pub fn load_from(dir: &Path) -> Result<ReportStore> {
    let mut campaigns = Vec::new();
    let mut legacy = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(ReportStore { campaigns, legacy });
    };
    let mut paths: Vec<PathBuf> = entries
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| entry.path())
        .collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            campaigns.push(load_campaign(&path)?);
        } else if path.extension().is_some_and(|ext| ext == "json") {
            legacy.push(load_legacy(&path)?);
        }
    }
    campaigns.sort_by(|left, right| left.stamp().cmp(right.stamp()));
    legacy.sort_by(|left, right| left.file.cmp(&right.file));
    Ok(ReportStore { campaigns, legacy })
}

fn load_campaign(dir: &Path) -> Result<Campaign> {
    let dir_name = dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut meta = None;
    let mut runs: Vec<(String, ScenarioRun)> = Vec::new();
    let mut journals: Vec<(String, Vec<ScenarioRun>)> = Vec::new();

    let mut paths: Vec<PathBuf> = fs::read_dir(dir)?
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| entry.path())
        .collect();
    paths.sort();
    for path in paths {
        let Some(name) = path.file_name().map(|name| name.to_string_lossy())
        else {
            continue;
        };
        if name == "campaign.json" {
            meta = Some(
                json::from_str(&fs::read_to_string(&path)?)
                    .map_err(|error| format!("{}: {error}", path.display()))?,
            );
        } else if let Some(parent_slug) = name.strip_suffix(".cells.jsonl") {
            let mut cells = Vec::new();
            for line in fs::read_to_string(&path)?.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                cells.push(
                    json::from_str(line).map_err(|error| {
                        format!("{}: {error}", path.display())
                    })?,
                );
            }
            journals.push((parent_slug.to_owned(), cells));
        } else if name.ends_with(".json") {
            runs.push((
                name.into_owned(),
                json::from_str(&fs::read_to_string(&path)?)
                    .map_err(|error| format!("{}: {error}", path.display()))?,
            ));
        }
    }

    let (scenarios, orphan_cells) = attach_cells(runs, journals);
    Ok(Campaign {
        dir_name,
        meta,
        scenarios,
        orphan_cells,
    })
}

fn attach_cells(
    runs: Vec<(String, ScenarioRun)>,
    journals: Vec<(String, Vec<ScenarioRun>)>,
) -> (Vec<StoredScenario>, Vec<OrphanCells>) {
    let mut scenarios: Vec<StoredScenario> = runs
        .into_iter()
        .map(|(file, run)| StoredScenario {
            file,
            run,
            cells: Vec::new(),
        })
        .collect();
    let mut orphan_cells = Vec::new();
    for (parent_slug, cells) in journals {
        let cells = last_attempt_cells(cells);
        let owner = scenarios
            .iter_mut()
            .filter(|scenario| slug_of(&scenario.run.scenario) == parent_slug)
            .last();
        match owner {
            Some(scenario) => scenario.cells = cells,
            None => orphan_cells.push(OrphanCells { parent_slug, cells }),
        }
    }
    (scenarios, orphan_cells)
}

fn last_attempt_cells(cells: Vec<ScenarioRun>) -> Vec<ScenarioRun> {
    let mut deduped: Vec<ScenarioRun> = Vec::new();
    for cell in cells {
        deduped.retain(|kept| kept.scenario != cell.scenario);
        deduped.push(cell);
    }
    deduped
}

#[derive(Deserialize)]
struct V0Doc {
    meta: V0Meta,
    report: V0Report,
    #[serde(default)]
    failures: Vec<PersistedFailure>,
}

#[derive(Deserialize)]
struct V0Meta {
    recorded_at: String,
    er_bin: String,
    er_version: String,
    er_fingerprint: String,
}

#[derive(Deserialize)]
struct V0Report {
    scenario: String,
    passed: bool,
    config: Vec<(String, String)>,
    observations: Vec<(String, ObservationsStats)>,
    metrics: Vec<(String, f64)>,
}

fn load_legacy(path: &Path) -> Result<LegacyRun> {
    let doc: V0Doc = json::from_str(&fs::read_to_string(path)?)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    let file = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    Ok(decode_v0(file, doc))
}

fn decode_v0(file: String, doc: V0Doc) -> LegacyRun {
    let mut measurements = Vec::new();
    for (label, stats) in doc.report.observations {
        measurements.push(Measurement {
            unit: v0_unit(&label),
            direction: v0_direction(&label),
            value: MeasureValue::Distribution(stats),
            label,
        });
    }
    for (label, value) in doc.report.metrics {
        measurements.push(Measurement {
            unit: v0_unit(&label),
            direction: v0_direction(&label),
            value: MeasureValue::Scalar(value),
            label,
        });
    }
    LegacyRun {
        file,
        meta: CampaignMeta {
            schema: 0,
            run: String::new(),
            started_at: doc.meta.recorded_at,
            er_bin: doc.meta.er_bin,
            er_version: doc.meta.er_version,
            er_fingerprint: doc.meta.er_fingerprint,
        },
        run: ScenarioRun {
            schema: 0,
            run: String::new(),
            scenario: doc.report.scenario,
            passed: doc.report.passed,
            config: doc.report.config,
            measurements,
            failures: doc.failures,
            launches: Vec::new(),
        },
    }
}

fn v0_unit(label: &str) -> Unit {
    let lowered = label.to_ascii_lowercase();
    if lowered.ends_with(" us") {
        Unit::Micros
    } else if lowered.ends_with(" ms") {
        Unit::Millis
    } else if lowered.ends_with(" s") || lowered.ends_with(" seconds") {
        Unit::Seconds
    } else if lowered.ends_with(" tps") {
        Unit::Tps
    } else if lowered.ends_with(" rps") {
        Unit::Rps
    } else if lowered.ends_with("/s") {
        Unit::PerSecond
    } else if lowered.ends_with(" kb") {
        Unit::Kilobytes
    } else if lowered.ends_with(" mb") {
        Unit::Megabytes
    } else if lowered.contains("lamports") {
        Unit::Lamports
    } else if lowered.ends_with(" ratio") || lowered.ends_with(" x") {
        Unit::Ratio
    } else {
        Unit::Count
    }
}

fn v0_direction(label: &str) -> Direction {
    let lowered = label.to_ascii_lowercase();
    if lowered.contains("rps") || lowered.contains("tps") {
        Direction::HigherIsBetter
    } else if lowered.ends_with(" us")
        || lowered.contains("lag")
        || lowered.contains("latency")
    {
        Direction::LowerIsBetter
    } else {
        Direction::Info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{ScenarioReport, SCHEMA_VERSION};

    fn run_named(scenario: &str, passed: bool) -> ScenarioRun {
        ScenarioRun {
            schema: SCHEMA_VERSION,
            run: "test-run".into(),
            scenario: scenario.into(),
            passed,
            config: Vec::new(),
            measurements: Vec::new(),
            failures: Vec::new(),
            launches: Vec::new(),
        }
    }

    #[test]
    fn cells_attach_to_the_last_attempt_of_their_parent() {
        let runs = vec![
            (
                "redline-high_cu.json".to_owned(),
                run_named("redline/high_cu", false),
            ),
            (
                "redline-high_cu-2.json".to_owned(),
                run_named("redline/high_cu", true),
            ),
        ];
        let journals = vec![(
            "redline-high_cu".to_owned(),
            vec![
                run_named("redline/high_cu/light", true),
                run_named("redline/high_cu/heavy", true),
                run_named("redline/high_cu/light", true),
            ],
        )];
        let (scenarios, orphan_cells) = attach_cells(runs, journals);
        assert!(orphan_cells.is_empty());
        assert!(scenarios[0].cells.is_empty());
        let attached = &scenarios[1].cells;
        assert_eq!(attached.len(), 2);
        assert_eq!(attached[0].scenario, "redline/high_cu/heavy");
        assert_eq!(attached[1].scenario, "redline/high_cu/light");
    }

    #[test]
    fn cells_without_a_parent_run_are_orphans() {
        let journals = vec![(
            "redline-high_cu".to_owned(),
            vec![run_named("redline/high_cu/light", true)],
        )];
        let (scenarios, orphan_cells) = attach_cells(Vec::new(), journals);
        assert!(scenarios.is_empty());
        assert_eq!(orphan_cells.len(), 1);
        assert_eq!(orphan_cells[0].parent_slug, "redline-high_cu");
        assert_eq!(orphan_cells[0].cells.len(), 1);
    }

    #[test]
    fn v0_documents_decode_into_typed_measurements() {
        let text = r#"{"meta":{"recorded_at":"20260708T120000Z","er_bin":"/x/er","er_version":"magicblock-config 0.12.1","er_fingerprint":"123-456"},"report":{"scenario":"redline/rpc_warm_ingress","passed":true,"config":[["profile","lite"]],"observations":[["delivery us",{"count":400,"median":8480,"min":8030,"max":10485,"avg":8531,"quantile95":9022,"stddev":274}]],"metrics":[["achieved tps",198.8],["superseded",0.0],["clone visibility ms",12.0],["measured wall s",30.0]]}}"#;
        let doc: V0Doc = json::from_str(text).unwrap();
        let legacy = decode_v0("f.json".into(), doc);

        assert_eq!(legacy.meta.schema, 0);
        assert_eq!(legacy.meta.started_at, "20260708T120000Z");
        assert_eq!(legacy.run.schema, 0);
        assert!(legacy.run.passed);

        let by_label = |label: &str| {
            legacy
                .run
                .measurements
                .iter()
                .find(|measurement| measurement.label == label)
                .unwrap()
        };
        let delivery = by_label("delivery us");
        assert_eq!(delivery.unit, Unit::Micros);
        assert_eq!(delivery.direction, Direction::LowerIsBetter);
        assert_eq!(delivery.distribution().unwrap().median, 8480);
        let throughput = by_label("achieved tps");
        assert_eq!(throughput.unit, Unit::Tps);
        assert_eq!(throughput.direction, Direction::HigherIsBetter);
        assert_eq!(throughput.scalar(), Some(198.8));
        let superseded = by_label("superseded");
        assert_eq!(superseded.unit, Unit::Count);
        assert_eq!(superseded.direction, Direction::Info);
        let visibility = by_label("clone visibility ms");
        assert_eq!(visibility.unit, Unit::Millis);
        assert_eq!(visibility.direction, Direction::Info);
        let wall = by_label("measured wall s");
        assert_eq!(wall.unit, Unit::Seconds);
        assert_eq!(wall.direction, Direction::Info);
    }

    #[test]
    fn a_directory_of_campaigns_and_legacy_files_loads_together() {
        let base = std::env::temp_dir()
            .join(format!("redsuite-store-load-{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        let campaign = base.join("run-a");
        fs::create_dir_all(&campaign).unwrap();

        fs::write(
            campaign.join("campaign.json"),
            r#"{"schema":1,"run":"run-a","started_at":"20260824T120000Z","er_bin":"/x/er","er_version":"v","er_fingerprint":"1-2"}"#,
        )
        .unwrap();
        let report = ScenarioReport::ok("redline/high_cu");
        let doc = ScenarioRun {
            schema: SCHEMA_VERSION,
            run: "run-a".into(),
            scenario: report.scenario.clone(),
            passed: true,
            config: Vec::new(),
            measurements: Vec::new(),
            failures: Vec::new(),
            launches: Vec::new(),
        };
        fs::write(
            campaign.join("redline-high_cu.json"),
            json::to_string(&doc).unwrap(),
        )
        .unwrap();
        fs::write(
            campaign.join("redline-high_cu.cells.jsonl"),
            format!(
                "{}\n",
                json::to_string(&run_named("redline/high_cu/light", true))
                    .unwrap()
            ),
        )
        .unwrap();
        fs::write(
            base.join("20260708T120000Z-redline-old.json"),
            r#"{"meta":{"recorded_at":"20260708T120000Z","er_bin":"/x/er","er_version":"v0","er_fingerprint":"0-0"},"report":{"scenario":"redline/old","passed":true,"config":[],"observations":[],"metrics":[]}}"#,
        )
        .unwrap();

        let store = load_from(&base).unwrap();
        assert_eq!(store.campaigns.len(), 1);
        assert_eq!(store.campaigns[0].stamp(), "20260824T120000Z");
        assert_eq!(store.campaigns[0].scenarios.len(), 1);
        assert_eq!(store.campaigns[0].scenarios[0].cells.len(), 1);
        assert_eq!(store.legacy.len(), 1);
        assert_eq!(store.legacy[0].run.scenario, "redline/old");

        fs::remove_dir_all(&base).unwrap();
    }
}
