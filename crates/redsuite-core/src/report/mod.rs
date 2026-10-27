mod bmf;
mod store;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

pub use bmf::bmf;
use json::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::{
    resources::LaunchRecord, runner::RunOutcome, scenario::RunRecord, stats::ObservationsStats,
    topology, Result,
};

pub const SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Unit {
    Micros,
    Millis,
    Seconds,
    Tps,
    Rps,
    PerSecond,
    Count,
    Kilobytes,
    Megabytes,
    Ratio,
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MeasureValue {
    Scalar(f64),
    Distribution(ObservationsStats),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Measurement {
    pub label: String,
    pub unit: Unit,
    pub value: MeasureValue,
}

#[derive(Debug)]
pub struct ScenarioReport {
    pub passed: bool,
    pub config: Vec<(String, String)>,
    pub measurements: Vec<Measurement>,
}

impl ScenarioReport {
    pub fn ok() -> Self {
        Self {
            passed: true,
            config: Vec::new(),
            measurements: Vec::new(),
        }
    }

    pub fn setting(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.config.push((key.into(), value.to_string()));
        self
    }

    pub fn observe(
        mut self,
        label: impl Into<String>,
        unit: Unit,
        stats: ObservationsStats,
    ) -> Self {
        self.measurements.push(Measurement {
            label: label.into(),
            unit,
            value: MeasureValue::Distribution(stats),
        });
        self
    }

    pub fn metric(mut self, label: impl Into<String>, unit: Unit, value: f64) -> Self {
        self.measurements.push(Measurement {
            label: label.into(),
            unit,
            value: MeasureValue::Scalar(value),
        });
        self
    }

    pub fn metric_if(self, label: impl Into<String>, unit: Unit, value: Option<f64>) -> Self {
        match value {
            Some(value) => self.metric(label, unit, value),
            None => self,
        }
    }

    pub fn client_side(self, outcome: &RunOutcome) -> Self {
        self.client_side_as("scheduling delay us", outcome)
    }

    pub fn client_side_as(self, label: impl Into<String>, outcome: &RunOutcome) -> Self {
        if outcome.scheduling.count == 0 {
            return self.with_flags(outcome);
        }
        self.observe(label, Unit::Micros, outcome.scheduling).with_flags(outcome)
    }

    pub fn latency_origin(mut self, origin: &str) -> Self {
        if origin.is_empty() {
            return self;
        }
        match self.config.iter_mut().find(|(key, _)| key == LATENCY_ORIGIN) {
            Some((_, origins)) => {
                if !origins.split("; ").any(|known| known == origin) {
                    origins.push_str("; ");
                    origins.push_str(origin);
                }
            }
            None => self.config.push((LATENCY_ORIGIN.into(), origin.into())),
        }
        self
    }

    pub fn with_flags(self, outcome: &RunOutcome) -> Self {
        let mut report = self.latency_origin(outcome.origin);
        for (flag, reason) in outcome.flags() {
            warn!(flag, %reason, "workload validity warning");
            match report.config.iter_mut().find(|(key, _)| key == flag) {
                Some((_, reasons)) => {
                    reasons.push_str("; ");
                    reasons.push_str(&reason);
                }
                None => report.config.push((flag.into(), reason)),
            }
        }
        report
    }
}

pub const LATENCY_ORIGIN: &str = "latency origin";

pub const LOAD_FLAGS: [&str; 2] = ["client saturated", "offered rate not sustained"];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignMeta {
    pub schema: u32,
    pub run: String,
    pub started_at: String,
    pub er_bin: String,
    pub er_version: String,
    pub er_fingerprint: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScenarioRun {
    pub schema: u32,
    pub run: String,
    pub scenario: String,
    pub passed: bool,
    pub config: Vec<(String, String)>,
    pub measurements: Vec<Measurement>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<PersistedFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub launches: Vec<LaunchRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedFailure {
    pub phase: String,
    pub message: String,
}

impl PersistedFailure {
    fn new(phase: &str, message: impl Into<String>) -> Self {
        Self {
            phase: phase.to_owned(),
            message: message.into(),
        }
    }
}

pub fn reports_dir() -> PathBuf {
    topology::workspace_root().join("target/redsuite-reports")
}

pub fn run_id() -> &'static str {
    static RUN_ID: OnceLock<String> = OnceLock::new();
    RUN_ID.get_or_init(|| format!("{}-{}", utc_stamp(), std::process::id()))
}

fn campaign_dir() -> PathBuf {
    reports_dir().join(run_id())
}

pub fn persist_run(record: &RunRecord) -> Result<PathBuf> {
    let failures = run_failures(record);

    let fallback =
        ScenarioReport::ok().metric_if("wall seconds", Unit::Seconds, record.wall_seconds);
    let report = record.report.as_ref().unwrap_or(&fallback);
    let mut doc = scenario_run_doc(&record.name, report, &failures, &record.launches);
    doc.passed = record.passed();

    store::persist(None, doc)
}

pub fn persist_summary(name: &str, mut report: ScenarioReport, outcome: &Result<()>) -> Result<()> {
    report.passed = outcome.is_ok();
    let failures: Vec<_> = outcome
        .as_ref()
        .err()
        .map(|error| PersistedFailure::new("suite", format!("{error:#}")))
        .into_iter()
        .collect();
    store::persist(None, scenario_run_doc(name, &report, &failures, &[]))?;
    Ok(())
}

pub(crate) fn persist_cell(parent: &str, label: &str, report: &ScenarioReport) {
    let name = format!("{parent}/{label}");
    let persisted = store::persist(Some(parent), scenario_run_doc(&name, report, &[], &[]));
    if let Err(error) = persisted {
        warn!(error = %format_args!("{error:#}"), "cell report not persisted");
    }
}

fn scenario_run_doc(
    name: &str,
    report: &ScenarioReport,
    failures: &[PersistedFailure],
    launches: &[LaunchRecord],
) -> ScenarioRun {
    ScenarioRun {
        schema: SCHEMA_VERSION,
        run: run_id().to_owned(),
        scenario: name.to_owned(),
        passed: report.passed,
        config: report.config.clone(),
        measurements: report.measurements.clone(),
        failures: failures.to_vec(),
        launches: launches.to_vec(),
    }
}

fn run_failures(record: &RunRecord) -> Vec<PersistedFailure> {
    let Err(error) = &record.outcome else {
        return Vec::new();
    };
    vec![PersistedFailure::new(record.phase, format!("{error:#}"))]
}

fn campaign_meta(attached: bool) -> CampaignMeta {
    let (er_bin, er_version, er_fingerprint) = if attached {
        (
            "attached".to_owned(),
            "unobserved".to_owned(),
            "unobserved".to_owned(),
        )
    } else {
        er_identity()
    };
    CampaignMeta {
        schema: SCHEMA_VERSION,
        run: run_id().to_owned(),
        started_at: utc_stamp(),
        er_bin,
        er_version,
        er_fingerprint,
    }
}

fn er_identity() -> (String, String, String) {
    match topology::er_bin_path() {
        Ok(er) => (
            er.display().to_string(),
            binary_version(&er),
            fingerprint(&er),
        ),
        Err(_) => ("unknown".into(), "unknown".into(), "unknown".into()),
    }
}

pub(crate) fn binary_provenance(path: &Path) -> (String, String) {
    (binary_version(path), fingerprint(path))
}

pub fn binary_hash(path: &Path) -> std::io::Result<String> {
    let mut hash = Sha256::new();
    std::io::copy(&mut fs::File::open(path)?, &mut hash)?;
    Ok(format!("{:x}", hash.finalize()))
}

// A validator prints its version and then its shutdown log lines; only the
// first line names the build, and terminal colour codes are noise.
fn binary_version(path: &Path) -> String {
    Command::new(path)
        .arg("--version")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| version_line(&String::from_utf8_lossy(&out.stdout)))
        .unwrap_or_else(|| "unknown".into())
}

fn version_line(output: &str) -> Option<String> {
    let line = output.lines().map(str::trim).find(|line| !line.is_empty())?;
    let mut cleaned = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            if chars.peek() == Some(&'[') {
                for next in chars.by_ref() {
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        cleaned.push(ch);
    }
    Some(cleaned)
}

fn fingerprint(path: &Path) -> String {
    match fs::metadata(path) {
        Ok(meta) => {
            let mtime = meta
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs())
                .unwrap_or(0);
            format!("{}-{}", meta.len(), mtime)
        }
        Err(_) => "unknown".into(),
    }
}

pub(crate) fn utc_stamp() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let (hours, minutes, seconds) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    let days = (secs / 86_400) as i64 + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 { month_index + 3 } else { month_index - 9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!("{year:04}{month:02}{day:02}T{hours:02}{minutes:02}{seconds:02}Z")
}
