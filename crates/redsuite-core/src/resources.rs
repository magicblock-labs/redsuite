use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use json::{Deserialize, Serialize};

use crate::{host, DynError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchRecord {
    pub label: String,
    pub role: String,
    pub bin: String,
    pub bin_version: String,
    pub bin_fingerprint: String,
    pub identity: String,
    pub launched_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_port: Option<u16>,
    pub metrics_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    pub storage_dir: String,
    pub log: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_set: Option<String>,
    #[serde(default)]
    pub pid: u32,
    #[serde(default)]
    pub relaunches: u32,
}

#[derive(Default)]
pub struct Resources {
    records: RefCell<Vec<Rc<ResourceRecord>>>,
}

impl Resources {
    #[cfg(test)]
    pub(crate) fn register(&self, label: &str, pid: u32) -> Rc<ResourceRecord> {
        let record = Rc::new(ResourceRecord {
            label: label.to_owned(),
            pid: Cell::new(pid),
            finished: Cell::new(false),
            finish_error: RefCell::new(None),
            launch: None,
            relaunches: Cell::new(0),
        });
        self.records.borrow_mut().push(record.clone());
        record
    }

    pub(crate) fn register_launch(
        &self,
        launch: LaunchRecord,
    ) -> Rc<ResourceRecord> {
        let record = Rc::new(ResourceRecord {
            label: launch.label.clone(),
            pid: Cell::new(launch.pid),
            finished: Cell::new(false),
            finish_error: RefCell::new(None),
            launch: Some(launch),
            relaunches: Cell::new(0),
        });
        self.records.borrow_mut().push(record.clone());
        record
    }

    pub(crate) fn launches(&self) -> Vec<LaunchRecord> {
        self.records
            .borrow()
            .iter()
            .filter_map(|record| {
                let mut launch = record.launch.clone()?;
                launch.pid = record.pid.get();
                launch.relaunches = record.relaunches.get();
                Some(launch)
            })
            .collect()
    }

    // Audits every private ER booted against this run's base: an explicit
    // finish() failure and a process that survived teardown both come back
    // as errors, kept apart from the scenario's own outcome.
    pub(crate) fn audit(&self) -> Vec<DynError> {
        let mut errors: Vec<DynError> = Vec::new();
        for record in self.records.borrow().iter() {
            if let Some(message) = record.finish_error.borrow().as_deref() {
                errors.push(
                    format!("private ER `{}`: {message}", record.label).into(),
                );
            } else if !record.finished.get()
                && host::proc_running(record.pid.get())
            {
                errors.push(
                    format!(
                        "private ER `{}` (pid {}) is still running after the \
                         scenario",
                        record.label,
                        record.pid.get()
                    )
                    .into(),
                );
            }
        }
        errors
    }
}

pub(crate) struct ResourceRecord {
    label: String,
    pid: Cell<u32>,
    finished: Cell<bool>,
    finish_error: RefCell<Option<String>>,
    launch: Option<LaunchRecord>,
    relaunches: Cell<u32>,
}

impl ResourceRecord {
    pub(crate) fn relaunched(&self, pid: u32) {
        self.pid.set(pid);
        self.finished.set(false);
        self.relaunches.set(self.relaunches.get() + 1);
    }

    pub(crate) fn mark_finished(&self) {
        self.finished.set(true);
    }

    pub(crate) fn record_finish_error(&self, message: String) {
        *self.finish_error.borrow_mut() = Some(message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_live_unfinished_process_fails_the_audit() {
        let resources = Resources::default();
        resources.register("leaky", std::process::id());
        let errors = resources.audit();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("`leaky`"));
        assert!(errors[0].to_string().contains("still running"));
    }

    #[test]
    fn finished_and_dead_processes_pass_the_audit() {
        let resources = Resources::default();
        resources
            .register("stopped", std::process::id())
            .mark_finished();
        resources.register("reaped", u32::MAX - 1);
        assert!(resources.audit().is_empty());
    }

    #[test]
    fn finish_errors_surface_even_after_the_process_died() {
        let resources = Resources::default();
        let record = resources.register("wedged", u32::MAX - 1);
        record.mark_finished();
        record.record_finish_error("SIGTERM timed out".to_owned());
        let errors = resources.audit();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("SIGTERM timed out"));
    }

    #[test]
    fn a_relaunch_tracks_the_new_pid() {
        let resources = Resources::default();
        let record = resources.register("restarted", u32::MAX - 1);
        record.mark_finished();
        record.relaunched(std::process::id());
        assert_eq!(resources.audit().len(), 1);
    }

    fn launch(label: &str, pid: u32) -> LaunchRecord {
        LaunchRecord {
            label: label.to_owned(),
            role: "verifier".to_owned(),
            bin: "/x/magicblock-verifier".to_owned(),
            bin_version: "v".to_owned(),
            bin_fingerprint: "1-2".to_owned(),
            identity: "id".to_owned(),
            launched_at: "20260903T080000Z".to_owned(),
            rpc_port: None,
            metrics_port: 9001,
            replication_port: None,
            upstream: Some("127.0.0.1:7802".to_owned()),
            storage_dir: "/x/storage".to_owned(),
            log: "/x/verifier.log".to_owned(),
            config: Some("/x/storage/verifier.toml".to_owned()),
            cpu_set: None,
            pid,
            relaunches: 0,
        }
    }

    #[test]
    fn launches_report_the_current_pid_and_relaunch_count() {
        let resources = Resources::default();
        resources.register("plain", u32::MAX - 1).mark_finished();
        let record = resources.register_launch(launch("v0", u32::MAX - 1));
        record.mark_finished();
        record.relaunched(u32::MAX - 2);
        record.mark_finished();
        let launches = resources.launches();
        assert_eq!(launches.len(), 1);
        assert_eq!(launches[0].label, "v0");
        assert_eq!(launches[0].pid, u32::MAX - 2);
        assert_eq!(launches[0].relaunches, 1);
        assert!(resources.audit().is_empty());
    }
}
