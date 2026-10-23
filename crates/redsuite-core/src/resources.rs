use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use json::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use tracing::warn;

use crate::{host, report, topology};

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchRecord {
    pub label: String,
    pub role: String,
    pub bin: String,
    pub bin_version: String,
    pub bin_fingerprint: String,
    pub identity: String,
    pub launched_at: String,
    pub rpc_port: Option<u16>,
    pub ws_port: Option<u16>,
    pub metrics_port: u16,
    pub replication_port: Option<u16>,
    pub upstream: Option<String>,
    pub storage_dir: String,
    pub accountsdb_dir: Option<String>,
    pub log: String,
    pub config: Option<String>,
    #[serde(default)]
    pub pid: u32,
    #[serde(default)]
    pub relaunches: u32,
    pub exit: Option<String>,
}

#[derive(Default)]
pub struct Resources {
    records: RefCell<Vec<Rc<ResourceRecord>>>,
}

impl Resources {
    pub(crate) fn register_launch(&self, mut launch: LaunchRecord) -> Rc<ResourceRecord> {
        launch.launched_at = report::utc_stamp();
        let record = Rc::new(ResourceRecord {
            finished: Cell::new(false),
            launch: RefCell::new(launch),
        });
        self.records.borrow_mut().push(record.clone());
        record
    }

    pub(crate) fn launches(&self) -> Vec<LaunchRecord> {
        self.records
            .borrow()
            .iter()
            .map(|record| record.launch.borrow().clone())
            .collect()
    }

    pub(crate) fn reclaim(&self, owner: &str) {
        for record in self.records.borrow().iter() {
            let launch = record.launch.borrow();
            if !record.finished.get() && host::proc_running(launch.pid) {
                topology::kill_pid(launch.pid);
                record.mark_finished();
                warn!(owner, %launch.label, "reclaimed running rollup");
            }
            for dir in std::iter::once(&launch.storage_dir).chain(launch.accountsdb_dir.iter()) {
                match std::fs::remove_dir_all(dir) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        warn!(owner, dir, %error, "storage cleanup failed")
                    }
                }
            }
        }
    }
}

pub(crate) struct ResourceRecord {
    finished: Cell<bool>,
    launch: RefCell<LaunchRecord>,
}

impl ResourceRecord {
    pub(crate) fn relaunched(&self, pid: u32) {
        let mut launch = self.launch.borrow_mut();
        launch.pid = pid;
        launch.relaunches += 1;
        self.finished.set(false);
    }

    pub(crate) fn mark_finished(&self) {
        self.finished.set(true);
    }

    pub(crate) fn record_exit(&self, message: String) {
        self.launch.borrow_mut().exit = Some(message);
    }
}
