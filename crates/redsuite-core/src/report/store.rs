use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{Mutex, OnceLock},
};

use anyhow::Context;
use json::{Deserialize, Serialize};

use super::{CampaignMeta, ScenarioRun};
use crate::Result;

#[derive(Serialize, Deserialize)]
pub struct Campaign {
    meta: CampaignMeta,
    pub scenarios: BTreeMap<String, StoredScenario>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct StoredScenario {
    pub run: Option<ScenarioRun>,
    pub cells: Vec<ScenarioRun>,
}

pub fn persist(parent: Option<&str>, run: ScenarioRun) -> Result<PathBuf> {
    static CAMPAIGN: OnceLock<Mutex<Campaign>> = OnceLock::new();
    let mut campaign = CAMPAIGN
        .get_or_init(|| {
            Mutex::new(Campaign {
                meta: super::campaign_meta(
                    run.config.iter().any(|(key, value)| key == "target" && value == "attached"),
                ),
                scenarios: BTreeMap::new(),
            })
        })
        .lock()
        .unwrap();
    let name = parent.unwrap_or(&run.scenario).to_owned();
    let scenario = campaign.scenarios.entry(name).or_default();
    if parent.is_some() {
        scenario.cells.push(run);
    } else {
        scenario.run = Some(run);
    }
    let dir = super::campaign_dir();
    fs::create_dir_all(&dir)?;
    let path = dir.join("campaign.json");
    let staging = dir.join("campaign.tmp");
    fs::write(&staging, json::to_string_pretty(&*campaign)?)?;
    fs::rename(staging, &path)?;
    let latest = super::reports_dir().join("latest");
    let staging = dir.join("latest.tmp");
    fs::write(&staging, super::run_id())?;
    fs::rename(staging, latest)?;
    Ok(path)
}

pub fn load() -> Result<Campaign> {
    let root = super::reports_dir();
    let run = fs::read_to_string(root.join("latest"))
        .context("cannot read latest report campaign; run a scenario first")?;
    let contents = fs::read_to_string(root.join(run).join("campaign.json"))?;
    Ok(json::from_str(&contents)?)
}
