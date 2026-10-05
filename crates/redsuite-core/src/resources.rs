use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use json::{Deserialize, Serialize};

use crate::{host, topology, DynError};

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ws_port: Option<u16>,
    pub metrics_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    pub storage_dir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accountsdb_dir: Option<String>,
    pub log: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<String>,
    #[serde(default)]
    pub pid: u32,
    #[serde(default)]
    pub relaunches: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<String>,
}

#[derive(Default)]
pub struct Resources {
    records: RefCell<Vec<Rc<ResourceRecord>>>,
}

impl Resources {
    pub(crate) fn register_launch(
        &self,
        launch: LaunchRecord,
    ) -> Rc<ResourceRecord> {
        let record = Rc::new(ResourceRecord {
            label: launch.label.clone(),
            pid: Cell::new(launch.pid),
            finished: Cell::new(false),
            finish_error: RefCell::new(None),
            exit: RefCell::new(None),
            launch,
            relaunches: Cell::new(0),
        });
        self.records.borrow_mut().push(record.clone());
        record
    }

    pub(crate) fn launches(&self) -> Vec<LaunchRecord> {
        self.records
            .borrow()
            .iter()
            .map(|record| {
                let mut launch = record.launch.clone();
                launch.pid = record.pid.get();
                launch.relaunches = record.relaunches.get();
                launch.exit = record.exit.borrow().clone();
                launch
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reclaimed {
    pub label: String,
    pub storage_dir: String,
    pub killed: bool,
    pub removed: bool,
}

impl Resources {
    pub(crate) fn reclaim(&self) -> Vec<Reclaimed> {
        let mut reclaimed = Vec::new();
        for record in self.records.borrow().iter() {
            let launch = &record.launch;
            let pid = record.pid.get();
            let killed = !record.finished.get() && host::proc_running(pid);
            if killed {
                topology::kill_pid(pid);
                record.mark_finished();
            }
            let mut removed = false;
            for dir in std::iter::once(&launch.storage_dir)
                .chain(launch.accountsdb_dir.iter())
            {
                match std::fs::remove_dir_all(dir) {
                    Ok(()) => removed = true,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => record.record_finish_error(format!(
                        "reclaiming storage {dir}: {error}"
                    )),
                }
            }
            if killed || removed {
                reclaimed.push(Reclaimed {
                    label: record.label.clone(),
                    storage_dir: launch.storage_dir.clone(),
                    killed,
                    removed,
                });
            }
        }
        reclaimed
    }
}

pub(crate) struct ResourceRecord {
    label: String,
    pid: Cell<u32>,
    finished: Cell<bool>,
    finish_error: RefCell<Option<String>>,
    exit: RefCell<Option<String>>,
    launch: LaunchRecord,
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

    pub(crate) fn record_exit(&self, message: String) {
        *self.exit.borrow_mut() = Some(message);
    }
}
