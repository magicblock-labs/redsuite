use std::{
    fs,
    path::{Path, PathBuf},
};

use super::{slug_of, CampaignMeta, ScenarioRun};
use crate::Result;

pub struct Campaign {
    dir_name: String,
    meta: Option<CampaignMeta>,
    pub scenarios: Vec<StoredScenario>,
    pub orphan_cells: Vec<String>,
}

impl Campaign {
    fn stamp(&self) -> &str {
        self.meta
            .as_ref()
            .map(|meta| meta.started_at.as_str())
            .unwrap_or(&self.dir_name)
    }
}

pub struct StoredScenario {
    pub run: ScenarioRun,
    pub cells: Vec<ScenarioRun>,
}

pub fn load() -> Result<Vec<Campaign>> {
    let mut campaigns = Vec::new();
    let Ok(entries) = fs::read_dir(super::reports_dir()) else {
        return Ok(campaigns);
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
        }
    }
    campaigns.sort_by(|left, right| left.stamp().cmp(right.stamp()));
    Ok(campaigns)
}

fn load_campaign(dir: &Path) -> Result<Campaign> {
    let dir_name = dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut meta = None;
    let mut runs = Vec::new();
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
            runs.push(
                json::from_str(&fs::read_to_string(&path)?)
                    .map_err(|error| format!("{}: {error}", path.display()))?,
            );
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
    runs: Vec<ScenarioRun>,
    journals: Vec<(String, Vec<ScenarioRun>)>,
) -> (Vec<StoredScenario>, Vec<String>) {
    let mut scenarios: Vec<StoredScenario> = runs
        .into_iter()
        .map(|run| StoredScenario {
            run,
            cells: Vec::new(),
        })
        .collect();
    let mut orphan_cells = Vec::new();
    for (parent_slug, cells) in journals {
        let owner = scenarios
            .iter_mut()
            .filter(|scenario| slug_of(&scenario.run.scenario) == parent_slug)
            .last();
        match owner {
            Some(scenario) => scenario.cells = last_attempt_cells(cells),
            None => orphan_cells.push(parent_slug),
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
