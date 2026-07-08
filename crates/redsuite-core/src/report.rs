use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use json::{Deserialize, Serialize};

use crate::{stats::ObservationsStats, topology, Result};

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
}

pub fn reports_dir() -> PathBuf {
    topology::workspace_root().join("target/redsuite-reports")
}

pub fn persist(report: &ScenarioReport) -> Result<PathBuf> {
    let meta = run_meta();
    let dir = reports_dir();
    fs::create_dir_all(&dir)?;

    #[derive(Serialize)]
    struct Doc<'a> {
        meta: &'a RunMeta,
        report: &'a ScenarioReport,
    }
    let body = json::to_string_pretty(&Doc {
        meta: &meta,
        report,
    })?;

    let slug = report.scenario.replace(['/', ' '], "-");
    let mut path = dir.join(format!("{}-{slug}.json", meta.recorded_at));
    // same scenario twice within a second — keep both runs
    for n in 2.. {
        if !path.exists() {
            break;
        }
        path = dir.join(format!("{}-{slug}-{n}.json", meta.recorded_at));
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
            .map(|p| p.display().to_string())
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
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
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
    let (hh, mm, ss) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    let z = (secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
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
        };
        let text = json::to_string(&doc).unwrap();
        let back: PersistedReport = json::from_str(&text).unwrap();
        assert_eq!(back.report.scenario, "redline/some_scenario");
        assert_eq!(back.report.config[0].1, "200");
        assert_eq!(back.meta.er_fingerprint, "123-456");
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
