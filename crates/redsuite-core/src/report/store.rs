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
