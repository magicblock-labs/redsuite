mod diff;
use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

pub use diff::{bmf, compare, list};
use json::{Deserialize, Serialize};

use crate::{
    scenario::{failed_check, RunRecord, ScenarioOutcome},
    stats::ObservationsStats,
    topology, DynError, Result,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct ScenarioReport {
    pub scenario: String,
    pub passed: bool,
    pub config: Vec<(String, String)>,
    pub observations: Vec<(String, ObservationsStats)>,
    pub metrics: Vec<(String, f64)>,
}

impl ScenarioReport {
    pub fn ok(name: &str) -> Self {
        Self {
            scenario: name.to_owned(),
            passed: true,
            config: Vec::new(),
            observations: Vec::new(),
            metrics: Vec::new(),
        }
    }

    pub fn failed(name: &str) -> Self {
        Self {
            passed: false,
            ..Self::ok(name)
        }
    }

    pub fn setting(
        mut self,
        key: impl Into<String>,
        value: impl ToString,
    ) -> Self {
        self.config.push((key.into(), value.to_string()));
        self
    }

    pub fn observe(
        mut self,
        label: impl Into<String>,
        stats: ObservationsStats,
    ) -> Self {
        self.observations.push((label.into(), stats));
        self
    }

    pub fn metric(mut self, label: impl Into<String>, value: f64) -> Self {
        self.metrics.push((label.into(), value));
        self
    }

    pub fn metric_if(
        mut self,
        label: impl Into<String>,
        value: Option<f64>,
    ) -> Self {
        if let Some(value) = value {
            self.metrics.push((label.into(), value));
        }
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunMeta {
    pub recorded_at: String, // YYYYMMDDThhmmssZ (campaign naming)
    pub er_bin: String,
    pub er_version: String,
    pub er_fingerprint: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedReport {
    pub meta: RunMeta,
    pub report: ScenarioReport,
    #[serde(default)]
    pub failures: Vec<PersistedFailure>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedFailure {
    pub phase: String,
    #[serde(default)]
    pub kind: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<(String, String)>,
}

impl PersistedFailure {
    fn new(phase: &str, kind: &str, message: impl Into<String>) -> Self {
        Self {
            phase: phase.to_owned(),
            kind: kind.to_owned(),
            message: message.into(),
            expected: None,
            actual: None,
            context: Vec::new(),
        }
    }
}

pub fn reports_dir() -> PathBuf {
    topology::workspace_root().join("target/redsuite-reports")
}

pub fn persist(report: &ScenarioReport) -> Result<PathBuf> {
    write_report(report, &[])
}

pub fn persist_run(record: &RunRecord) -> Result<PathBuf> {
    let failures = run_failures(record);

    let fallback;
    let report = match &record.scenario {
        ScenarioOutcome::Passed(report) => report,
        ScenarioOutcome::Skipped(reason) => {
            return Err(
                format!("skipped runs are not persisted ({reason})").into()
            )
        }
        ScenarioOutcome::Failed(_)
        | ScenarioOutcome::Panicked(_)
        | ScenarioOutcome::NotReached => {
            fallback = ScenarioReport::failed(&record.name)
                .metric_if("wall seconds", record.wall_seconds);
            &fallback
        }
    };
    write_report(report, &failures)
}

fn run_failures(record: &RunRecord) -> Vec<PersistedFailure> {
    let mut failures = Vec::new();
    match &record.scenario {
        ScenarioOutcome::Failed(error) => {
            failures.push(scenario_failure(error))
        }
        ScenarioOutcome::Panicked(message) => {
            failures.push(PersistedFailure::new("scenario", "panic", message))
        }
        ScenarioOutcome::Passed(_)
        | ScenarioOutcome::Skipped(_)
        | ScenarioOutcome::NotReached => {}
    }
    for outcome in &record.phases {
        if let Some(error) = &outcome.error {
            failures.push(PersistedFailure::new(
                outcome.phase.name(),
                "infrastructure",
                error.error().to_string(),
            ));
        }
    }
    failures
}

fn scenario_failure(error: &DynError) -> PersistedFailure {
    match failed_check(error) {
        Some(check) => PersistedFailure {
            expected: check.expected.clone(),
            actual: check.actual.clone(),
            context: check.context.clone(),
            ..PersistedFailure::new("scenario", "check", &check.check)
        },
        None => PersistedFailure::new(
            "scenario",
            "infrastructure",
            error.to_string(),
        ),
    }
}

fn write_report(
    report: &ScenarioReport,
    failures: &[PersistedFailure],
) -> Result<PathBuf> {
    let meta = run_meta();
    let dir = reports_dir();
    fs::create_dir_all(&dir)?;

    #[derive(Serialize)]
    struct Doc<'a> {
        meta: &'a RunMeta,
        report: &'a ScenarioReport,
        #[serde(skip_serializing_if = "<[_]>::is_empty")]
        failures: &'a [PersistedFailure],
    }
    let body = json::to_string_pretty(&Doc {
        meta: &meta,
        report,
        failures,
    })?;

    let slug = report.scenario.replace(['/', ' '], "-");
    let mut path = dir.join(format!("{}-{slug}.json", meta.recorded_at));
    // same scenario twice within a second — keep both runs
    for attempt in 2.. {
        if !path.exists() {
            break;
        }
        path = dir.join(format!("{}-{slug}-{attempt}.json", meta.recorded_at));
    }
    fs::write(&path, body)?;
    Ok(path)
}

fn run_meta() -> RunMeta {
    let running_exe = topology::current_state()
        .map(|state| PathBuf::from(format!("/proc/{}/exe", state.er_pid)))
        .filter(|exe| exe.exists());
    let resolved = topology::er_bin_path().ok();

    if let (Some(running), Some(resolved)) = (&running_exe, &resolved) {
        if fingerprint(running) != fingerprint(resolved) {
            eprintln!(
                "[redsuite] warning: the shared stack is not running {} — \
                 rebuilt or re-pointed since boot; `cargo xtask stack down` to pick it up",
                resolved.display()
            );
        }
    }

    let er = running_exe.clone().or(resolved);
    RunMeta {
        recorded_at: utc_stamp(),
        er_bin: running_exe
            .as_deref()
            .and_then(|exe| fs::read_link(exe).ok())
            .or(er.clone())
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "unknown".into()),
        er_version: er
            .as_deref()
            .and_then(|bin| Command::new(bin).arg("--version").output().ok())
            .filter(|out| out.status.success())
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
            .unwrap_or_else(|| "unknown".into()),
        er_fingerprint: er
            .as_deref()
            .map(fingerprint)
            .unwrap_or_else(|| "unknown".into()),
    }
}

fn fingerprint(path: &std::path::Path) -> String {
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

fn utc_stamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (hours, minutes, seconds) =
        ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    let days = (secs / 86_400) as i64 + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36524
        - day_of_era / 146_096)
        / 365;
    let day_of_year =
        day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!("{year:04}{month:02}{day:02}T{hours:02}{minutes:02}{seconds:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_document_round_trips() {
        let report = ScenarioReport::ok("redline/some_scenario")
            .setting("rate", 200)
            .observe("delivery us", ObservationsStats::default())
            .metric("achieved tps", 199.0);
        let doc = PersistedReport {
            meta: RunMeta {
                recorded_at: "20260708T120000Z".into(),
                er_bin: "/x/magicblock-validator".into(),
                er_version: "magicblock-config 0.12.1".into(),
                er_fingerprint: "123-456".into(),
            },
            report,
            failures: vec![PersistedFailure::new(
                "teardown",
                "infrastructure",
                "private ER `x` is still running",
            )],
        };
        let text = json::to_string(&doc).unwrap();
        let back: PersistedReport = json::from_str(&text).unwrap();
        assert_eq!(back.report.scenario, "redline/some_scenario");
        assert_eq!(back.report.config[0].1, "200");
        assert_eq!(back.meta.er_fingerprint, "123-456");
        assert_eq!(back.failures[0].phase, "teardown");
        assert_eq!(back.failures[0].kind, "infrastructure");
    }

    #[test]
    fn a_failed_check_persists_its_evidence() {
        let error: DynError = Box::new(
            crate::check::CheckError::new("the clone matches base")
                .expected("128 bytes")
                .actual("0 bytes")
                .context("account", "abc123"),
        );
        let record = RunRecord {
            name: "redshift/example".into(),
            phases: Vec::new(),
            scenario: ScenarioOutcome::Failed(error),
            wall_seconds: Some(1.0),
        };
        let failures = run_failures(&record);
        assert_eq!(failures[0].kind, "check");
        assert_eq!(failures[0].message, "the clone matches base");
        assert_eq!(failures[0].expected.as_deref(), Some("128 bytes"));
        assert_eq!(failures[0].actual.as_deref(), Some("0 bytes"));
        assert_eq!(failures[0].context[0].0, "account");
        assert!(record.failure().unwrap().contains("check failed"));
    }

    #[test]
    fn a_panic_is_classified_apart_from_checks_and_errors() {
        let record = RunRecord {
            name: "redshift/example".into(),
            phases: Vec::new(),
            scenario: ScenarioOutcome::Panicked("index out of bounds".into()),
            wall_seconds: Some(1.0),
        };
        let failures = run_failures(&record);
        assert_eq!(failures[0].phase, "scenario");
        assert_eq!(failures[0].kind, "panic");
        assert_eq!(failures[0].message, "index out of bounds");
        assert!(!record.passed());
        assert!(record.failure().unwrap().contains("scenario panicked"));
    }

    #[test]
    fn run_failures_keep_the_primary_first_and_tag_their_phase() {
        use crate::scenario::{Phase, PhaseOutcome, RunError, RunRecord};

        let record = RunRecord {
            name: "redshift/example".into(),
            phases: vec![
                PhaseOutcome {
                    phase: Phase::Preflight,
                    error: None,
                },
                PhaseOutcome {
                    phase: Phase::Topology,
                    error: None,
                },
                PhaseOutcome {
                    phase: Phase::Teardown,
                    error: Some(RunError::Teardown(
                        "private ER `x` is still running".into(),
                    )),
                },
            ],
            scenario: ScenarioOutcome::Failed("assert blew up".into()),
            wall_seconds: Some(1.5),
        };

        let failures = run_failures(&record);
        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0].phase, "scenario");
        assert_eq!(failures[0].message, "assert blew up");
        assert_eq!(failures[1].phase, "teardown");

        assert!(!record.passed());
        let failure = record.failure().unwrap();
        assert!(failure.starts_with("redshift/example: scenario failed"));
        assert!(failure.contains("also: teardown failed"));
    }

    #[test]
    fn setup_failures_alone_fail_the_record() {
        use crate::scenario::{Phase, PhaseOutcome, RunError, RunRecord};

        let record = RunRecord {
            name: "redshift/example".into(),
            phases: vec![PhaseOutcome {
                phase: Phase::Topology,
                error: Some(RunError::Topology(
                    "base never got healthy".into(),
                )),
            }],
            scenario: ScenarioOutcome::NotReached,
            wall_seconds: None,
        };

        let failures = run_failures(&record);
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].phase, "topology");
        assert!(!record.passed());
        assert!(record
            .failure()
            .unwrap()
            .contains("topology failed: base never got healthy"));
    }

    #[test]
    fn documents_without_failures_still_parse() {
        let text = r#"{"meta":{"recorded_at":"20260708T120000Z","er_bin":"x","er_version":"y","er_fingerprint":"z"},"report":{"scenario":"redshift/example","passed":true,"config":[],"observations":[],"metrics":[]}}"#;
        let back: PersistedReport = json::from_str(text).unwrap();
        assert!(back.failures.is_empty());
        assert!(back.report.passed);
    }

    #[test]
    fn utc_stamp_shape() {
        let stamp = utc_stamp();
        assert_eq!(stamp.len(), 16);
        assert!(stamp.starts_with("20"));
        assert_eq!(&stamp[8..9], "T");
        assert!(stamp.ends_with('Z'));
    }
}
