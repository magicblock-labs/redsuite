mod diff;
mod store;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        OnceLock,
    },
    time::{SystemTime, UNIX_EPOCH},
};

pub use diff::{bmf, compare, list};
use json::{Deserialize, Serialize};

use crate::{
    api,
    scenario::{failed_check, RunRecord, ScenarioOutcome},
    stats::ObservationsStats,
    topology,
    transport::http,
    DynError, Result,
};

pub const SCHEMA_VERSION: u32 = 1;

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
    Lamports,
    Ratio,
}

impl Unit {
    pub fn default_direction(self) -> Direction {
        match self {
            Unit::Micros | Unit::Millis => Direction::LowerIsBetter,
            Unit::Tps | Unit::Rps => Direction::HigherIsBetter,
            Unit::Seconds
            | Unit::PerSecond
            | Unit::Count
            | Unit::Kilobytes
            | Unit::Megabytes
            | Unit::Lamports
            | Unit::Ratio => Direction::Info,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Direction {
    LowerIsBetter,
    HigherIsBetter,
    Info,
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
    pub direction: Direction,
    pub value: MeasureValue,
}

impl Measurement {
    pub fn scalar(&self) -> Option<f64> {
        match self.value {
            MeasureValue::Scalar(value) => Some(value),
            MeasureValue::Distribution(_) => None,
        }
    }

    pub fn distribution(&self) -> Option<&ObservationsStats> {
        match &self.value {
            MeasureValue::Distribution(stats) => Some(stats),
            MeasureValue::Scalar(_) => None,
        }
    }
}

#[derive(Debug)]
pub struct ScenarioReport {
    pub scenario: String,
    pub passed: bool,
    pub config: Vec<(String, String)>,
    pub measurements: Vec<Measurement>,
}

impl ScenarioReport {
    pub fn ok(name: &str) -> Self {
        Self {
            scenario: name.to_owned(),
            passed: true,
            config: Vec::new(),
            measurements: Vec::new(),
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
        unit: Unit,
        stats: ObservationsStats,
    ) -> Self {
        self.measurements.push(Measurement {
            label: label.into(),
            unit,
            direction: unit.default_direction(),
            value: MeasureValue::Distribution(stats),
        });
        self
    }

    pub fn metric(
        mut self,
        label: impl Into<String>,
        unit: Unit,
        value: f64,
    ) -> Self {
        self.measurements.push(Measurement {
            label: label.into(),
            unit,
            direction: unit.default_direction(),
            value: MeasureValue::Scalar(value),
        });
        self
    }

    pub fn metric_if(
        self,
        label: impl Into<String>,
        unit: Unit,
        value: Option<f64>,
    ) -> Self {
        match value {
            Some(value) => self.metric(label, unit, value),
            None => self,
        }
    }
}

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

pub fn run_id() -> &'static str {
    static RUN_ID: OnceLock<String> = OnceLock::new();
    RUN_ID.get_or_init(|| {
        std::env::var("NEXTEST_RUN_ID")
            .ok()
            .map(|inherited| {
                inherited
                    .chars()
                    .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-')
                    .collect::<String>()
            })
            .filter(|inherited| !inherited.is_empty())
            .unwrap_or_else(|| {
                format!("{}-{}", utc_stamp(), std::process::id())
            })
    })
}

fn campaign_dir() -> PathBuf {
    reports_dir().join(run_id())
}

fn slug_of(name: &str) -> String {
    name.replace(['/', ' '], "-")
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
            fallback = ScenarioReport::failed(&record.name).metric_if(
                "wall seconds",
                Unit::Seconds,
                record.wall_seconds,
            );
            &fallback
        }
    };

    let dir = campaign_dir();
    ensure_campaign(&dir)?;
    warn_on_stack_skew();
    write_scenario_run(&dir, &scenario_run_doc(report, &failures))
}

pub fn persist_cell(parent: &str, report: &ScenarioReport) -> Result<PathBuf> {
    let dir = campaign_dir();
    ensure_campaign(&dir)?;
    append_cell(&dir, parent, &scenario_run_doc(report, &[]))
}

fn scenario_run_doc(
    report: &ScenarioReport,
    failures: &[PersistedFailure],
) -> ScenarioRun {
    ScenarioRun {
        schema: SCHEMA_VERSION,
        run: run_id().to_owned(),
        scenario: report.scenario.clone(),
        passed: report.passed,
        config: report.config.clone(),
        measurements: report.measurements.clone(),
        failures: failures.to_vec(),
    }
}

fn ensure_campaign(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    if dir.join("campaign.json").exists() {
        return Ok(());
    }
    let (er_bin, er_version, er_fingerprint) = er_identity();
    write_campaign_meta(
        dir,
        &CampaignMeta {
            schema: SCHEMA_VERSION,
            run: run_id().to_owned(),
            started_at: utc_stamp(),
            er_bin,
            er_version,
            er_fingerprint,
        },
    )
}

fn staging_path(dir: &Path, prefix: &str) -> PathBuf {
    static STAGING_NONCE: AtomicUsize = AtomicUsize::new(0);
    let nonce = STAGING_NONCE.fetch_add(1, Ordering::Relaxed);
    dir.join(format!("{prefix}.tmp{}-{nonce}", std::process::id()))
}

fn write_campaign_meta(dir: &Path, meta: &CampaignMeta) -> Result<()> {
    let staging = staging_path(dir, "campaign");
    fs::write(&staging, json::to_string_pretty(meta)?)?;
    fs::rename(&staging, dir.join("campaign.json"))?;
    Ok(())
}

fn write_scenario_run(dir: &Path, doc: &ScenarioRun) -> Result<PathBuf> {
    let body = json::to_string_pretty(doc)?;
    let slug = slug_of(&doc.scenario);
    let mut path = dir.join(format!("{slug}.json"));
    // a retried scenario within one campaign keeps both attempts
    for attempt in 2.. {
        if !path.exists() {
            break;
        }
        path = dir.join(format!("{slug}-{attempt}.json"));
    }
    let staging = staging_path(dir, &slug);
    fs::write(&staging, body)?;
    fs::rename(&staging, &path)?;
    Ok(path)
}

fn append_cell(dir: &Path, parent: &str, doc: &ScenarioRun) -> Result<PathBuf> {
    let line = json::to_string(doc)?;
    let path = dir.join(format!("{}.cells.jsonl", slug_of(parent)));
    let mut journal = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    writeln!(journal, "{line}")?;
    Ok(path)
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
    if let Some(check) = failed_check(error) {
        return PersistedFailure {
            expected: check.expected.clone(),
            actual: check.actual.clone(),
            context: check.context.clone(),
            ..PersistedFailure::new("scenario", "check", &check.check)
        };
    }
    if let Some(tx) = error.downcast_ref::<api::TxError>() {
        return PersistedFailure {
            context: vec![
                ("signature".to_owned(), tx.signature.to_string()),
                ("error".to_owned(), format!("{:?}", tx.err)),
            ],
            ..PersistedFailure::new("scenario", "transaction", tx.to_string())
        };
    }
    if let Some(timeout) = error.downcast_ref::<api::ConfirmTimeout>() {
        return PersistedFailure {
            context: vec![(
                "signature".to_owned(),
                timeout.signature.to_string(),
            )],
            ..PersistedFailure::new(
                "scenario",
                "confirm-timeout",
                timeout.to_string(),
            )
        };
    }
    if let Some(rpc) = error.downcast_ref::<api::RpcError>() {
        let mut context = vec![
            ("code".to_owned(), rpc.code.to_string()),
            ("method".to_owned(), rpc.method.clone()),
            ("url".to_owned(), rpc.url.clone()),
        ];
        if let Some(data) = &rpc.data {
            context.push(("data".to_owned(), format!("{data:?}")));
        }
        return PersistedFailure {
            context,
            ..PersistedFailure::new("scenario", "rpc", rpc.to_string())
        };
    }
    if let Some(transport) = error.downcast_ref::<http::TransportError>() {
        let mut context = vec![("url".to_owned(), transport.url.clone())];
        if let Some(status) = transport.status {
            context.push(("status".to_owned(), status.to_string()));
        }
        return PersistedFailure {
            context,
            ..PersistedFailure::new(
                "scenario",
                "transport",
                transport.to_string(),
            )
        };
    }
    PersistedFailure::new("scenario", "infrastructure", error.to_string())
}

fn running_stack_exe() -> Option<PathBuf> {
    topology::current_state()
        .map(|state| PathBuf::from(format!("/proc/{}/exe", state.er_pid)))
        .filter(|exe| exe.exists())
}

fn er_identity() -> (String, String, String) {
    let running_exe = running_stack_exe();
    let resolved = topology::er_bin_path().ok();
    let er = running_exe.clone().or(resolved);

    let er_bin = running_exe
        .as_deref()
        .and_then(|exe| fs::read_link(exe).ok())
        .or(er.clone())
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "unknown".into());
    let er_version = er
        .as_deref()
        .and_then(|bin| Command::new(bin).arg("--version").output().ok())
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".into());
    let er_fingerprint = er
        .as_deref()
        .map(fingerprint)
        .unwrap_or_else(|| "unknown".into());
    (er_bin, er_version, er_fingerprint)
}

fn warn_on_stack_skew() {
    let running_exe = running_stack_exe();
    let resolved = topology::er_bin_path().ok();
    if let (Some(running), Some(resolved)) =
        (running_exe.as_deref(), resolved.as_deref())
    {
        if fingerprint(running) != fingerprint(resolved) {
            eprintln!(
                "[redsuite] warning: the shared stack is not running {} — \
                 rebuilt or re-pointed since boot; `cargo xtask stack down` to pick it up",
                resolved.display()
            );
        }
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

    fn sample_report() -> ScenarioReport {
        ScenarioReport::ok("redline/some_scenario")
            .setting("rate", 200)
            .observe("delivery us", Unit::Micros, ObservationsStats::default())
            .metric("achieved tps", Unit::Tps, 199.0)
    }

    #[test]
    fn scenario_run_round_trips() {
        let doc = scenario_run_doc(
            &sample_report(),
            &[PersistedFailure::new(
                "teardown",
                "infrastructure",
                "private ER `x` is still running",
            )],
        );
        let text = json::to_string(&doc).unwrap();
        let back: ScenarioRun = json::from_str(&text).unwrap();
        assert_eq!(back.schema, SCHEMA_VERSION);
        assert_eq!(back.run, run_id());
        assert_eq!(back.scenario, "redline/some_scenario");
        assert_eq!(back.config[0].1, "200");
        assert_eq!(back.measurements.len(), 2);
        assert_eq!(back.measurements[0].unit, Unit::Micros);
        assert_eq!(back.measurements[0].direction, Direction::LowerIsBetter);
        assert!(back.measurements[0].distribution().is_some());
        assert_eq!(back.measurements[1].unit, Unit::Tps);
        assert_eq!(back.measurements[1].direction, Direction::HigherIsBetter);
        assert_eq!(back.measurements[1].scalar(), Some(199.0));
        assert_eq!(back.failures[0].phase, "teardown");
        assert_eq!(back.failures[0].kind, "infrastructure");
    }

    #[test]
    fn units_carry_their_default_direction() {
        assert_eq!(Unit::Micros.default_direction(), Direction::LowerIsBetter);
        assert_eq!(Unit::Millis.default_direction(), Direction::LowerIsBetter);
        assert_eq!(Unit::Tps.default_direction(), Direction::HigherIsBetter);
        assert_eq!(Unit::Rps.default_direction(), Direction::HigherIsBetter);
        assert_eq!(Unit::Seconds.default_direction(), Direction::Info);
        assert_eq!(Unit::PerSecond.default_direction(), Direction::Info);
        assert_eq!(Unit::Count.default_direction(), Direction::Info);
        assert_eq!(Unit::Ratio.default_direction(), Direction::Info);
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
    fn structured_errors_classify_into_typed_failure_kinds() {
        use signature::Signature;

        let tx: DynError = Box::new(api::TxError {
            signature: Signature::default(),
            err: json::from_str(r#"{"InstructionError":[0,{"Custom":1}]}"#)
                .unwrap(),
        });
        let failure = scenario_failure(&tx);
        assert_eq!(failure.kind, "transaction");
        assert_eq!(failure.context[0].0, "signature");
        assert!(failure.context[1].1.contains("InstructionError"));

        let timeout: DynError = Box::new(api::ConfirmTimeout {
            signature: Signature::default(),
            deadline: std::time::Duration::from_secs(20),
        });
        let failure = scenario_failure(&timeout);
        assert_eq!(failure.kind, "confirm-timeout");
        assert!(failure.message.contains("execution outcome unknown"));

        let rpc: DynError = Box::new(api::RpcError {
            code: -32003,
            message: "tx verification error".into(),
            data: None,
            method: "sendTransaction".into(),
            url: "http://127.0.0.1:1".into(),
        });
        let failure = scenario_failure(&rpc);
        assert_eq!(failure.kind, "rpc");
        assert!(failure
            .context
            .iter()
            .any(|(key, value)| key == "code" && value == "-32003"));
        assert!(failure
            .context
            .iter()
            .any(|(key, value)| key == "method" && value == "sendTransaction"));

        let transport: DynError = Box::new(http::TransportError {
            url: "http://127.0.0.1:1".into(),
            status: Some(502),
            detail: "bad gateway".into(),
        });
        let failure = scenario_failure(&transport);
        assert_eq!(failure.kind, "transport");
        assert!(failure
            .context
            .iter()
            .any(|(key, value)| key == "status" && value == "502"));

        let plain: DynError = "connection reset".into();
        assert_eq!(scenario_failure(&plain).kind, "infrastructure");
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
        let text = r#"{"schema":1,"run":"abc","scenario":"redshift/example","passed":true,"config":[],"measurements":[]}"#;
        let back: ScenarioRun = json::from_str(text).unwrap();
        assert!(back.failures.is_empty());
        assert!(back.passed);
    }

    #[test]
    fn campaign_and_cell_files_round_trip_on_disk() {
        let dir = std::env::temp_dir()
            .join(format!("redsuite-report-write-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        write_campaign_meta(
            &dir,
            &CampaignMeta {
                schema: SCHEMA_VERSION,
                run: "test-run".into(),
                started_at: "20260824T120000Z".into(),
                er_bin: "/x/magicblock-validator".into(),
                er_version: "magicblock-config 0.12.1".into(),
                er_fingerprint: "123-456".into(),
            },
        )
        .unwrap();
        let first =
            write_scenario_run(&dir, &scenario_run_doc(&sample_report(), &[]))
                .unwrap();
        let second =
            write_scenario_run(&dir, &scenario_run_doc(&sample_report(), &[]))
                .unwrap();
        let cell = ScenarioReport::ok("redline/some_scenario/light").metric(
            "achieved tps",
            Unit::Tps,
            88.0,
        );
        let journal = append_cell(
            &dir,
            "redline/some_scenario",
            &scenario_run_doc(&cell, &[]),
        )
        .unwrap();

        assert!(first
            .to_string_lossy()
            .ends_with("redline-some_scenario.json"));
        assert!(second
            .to_string_lossy()
            .ends_with("redline-some_scenario-2.json"));
        assert!(journal
            .to_string_lossy()
            .ends_with("redline-some_scenario.cells.jsonl"));

        let meta: CampaignMeta = json::from_str(
            &fs::read_to_string(dir.join("campaign.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(meta.run, "test-run");
        let back: ScenarioRun =
            json::from_str(&fs::read_to_string(&first).unwrap()).unwrap();
        assert_eq!(back.scenario, "redline/some_scenario");
        let journal_lines = fs::read_to_string(&journal).unwrap();
        let cell_back: ScenarioRun =
            json::from_str(journal_lines.lines().next().unwrap()).unwrap();
        assert_eq!(cell_back.scenario, "redline/some_scenario/light");

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn parallel_test_threads_share_one_campaign_without_rename_races() {
        let dir = std::env::temp_dir()
            .join(format!("redsuite-report-race-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let meta = CampaignMeta {
            schema: SCHEMA_VERSION,
            run: "race-run".into(),
            started_at: "20260824T120000Z".into(),
            er_bin: "/x/er".into(),
            er_version: "v".into(),
            er_fingerprint: "1-2".into(),
        };
        std::thread::scope(|scope| {
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        for _ in 0..25 {
                            write_campaign_meta(&dir, &meta).unwrap();
                        }
                    })
                })
                .collect();
            for worker in workers {
                worker.join().unwrap();
            }
        });

        let back: CampaignMeta = json::from_str(
            &fs::read_to_string(dir.join("campaign.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(back.run, "race-run");
        let leftovers = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name() != "campaign.json")
            .count();
        assert_eq!(leftovers, 0);

        fs::remove_dir_all(&dir).unwrap();
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
