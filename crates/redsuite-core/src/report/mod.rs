mod bmf;
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

pub use bmf::bmf;
use json::{Deserialize, Serialize};

use crate::{
    api, console,
    resources::LaunchRecord,
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
    Ratio,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub launches: Vec<LaunchRecord>,
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
    RUN_ID.get_or_init(|| format!("{}-{}", utc_stamp(), std::process::id()))
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
        ScenarioOutcome::Passed(Some(report)) => report,
        _ => {
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
    write_scenario_run(
        &dir,
        &scenario_run_doc(report, &failures, &record.launches),
    )
}

pub fn persist_summary(
    mut report: ScenarioReport,
    outcome: &Result<()>,
) -> Result<()> {
    let dir = campaign_dir();
    ensure_campaign(&dir)?;
    report.passed = outcome.is_ok();
    let failures: Vec<_> = outcome
        .as_ref()
        .err()
        .map(|error| {
            PersistedFailure::new("suite", "infrastructure", error.to_string())
        })
        .into_iter()
        .collect();
    write_scenario_run(&dir, &scenario_run_doc(&report, &failures, &[]))?;
    Ok(())
}

pub fn persist_cell(parent: &str, report: &ScenarioReport) {
    let dir = campaign_dir();
    let persisted = ensure_campaign(&dir).and_then(|()| {
        append_cell(&dir, parent, &scenario_run_doc(report, &[], &[]))
    });
    match persisted {
        Ok(path) => {
            console::detail(format_args!("cell report: {}", path.display()))
        }
        Err(error) => console::detail(format_args!(
            "warning: cell report not persisted: {error}"
        )),
    }
}

fn scenario_run_doc(
    report: &ScenarioReport,
    failures: &[PersistedFailure],
    launches: &[LaunchRecord],
) -> ScenarioRun {
    ScenarioRun {
        schema: SCHEMA_VERSION,
        run: run_id().to_owned(),
        scenario: report.scenario.clone(),
        passed: report.passed,
        config: report.config.clone(),
        measurements: report.measurements.clone(),
        failures: failures.to_vec(),
        launches: launches.to_vec(),
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
    for error in &record.errors {
        failures.push(PersistedFailure::new(
            error.phase(),
            "infrastructure",
            error.error().to_string(),
        ));
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
        if let Some(method) = &transport.method {
            context.push(("method".to_owned(), method.clone()));
        }
        if let Some(status) = transport.status {
            context.push(("status".to_owned(), status.to_string()));
        }
        if let Some(kind) = transport.kind {
            context.push(("transport-kind".to_owned(), kind.to_owned()));
        }
        if let Some(cause) = &transport.cause {
            context.push(("cause".to_owned(), cause.clone()));
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
        .map(binary_version)
        .unwrap_or_else(|| "unknown".into());
    let er_fingerprint = er
        .as_deref()
        .map(fingerprint)
        .unwrap_or_else(|| "unknown".into());
    (er_bin, er_version, er_fingerprint)
}

pub(crate) fn warn_on_stack_skew() {
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

pub(crate) fn binary_provenance(path: &std::path::Path) -> (String, String) {
    (binary_version(path), fingerprint(path))
}

// A validator prints its version and then its shutdown log lines; only the
// first line names the build, and terminal colour codes are noise.
fn binary_version(path: &std::path::Path) -> String {
    Command::new(path)
        .arg("--version")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| version_line(&String::from_utf8_lossy(&out.stdout)))
        .unwrap_or_else(|| "unknown".into())
}

fn version_line(output: &str) -> Option<String> {
    let line = output
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())?;
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

pub(crate) fn utc_stamp() -> String {
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
