use std::{cell::RefCell, path::Path, rc::Rc};

use json::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::report;

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

impl LaunchRecord {
    pub(crate) fn set_storage(&mut self, ledger: &Path, accountsdb: &Path) {
        self.storage_dir = ledger.display().to_string();
        self.accountsdb_dir = Some(accountsdb.display().to_string());
    }
}

#[derive(Default)]
pub(crate) struct Launches {
    records: RefCell<Vec<Rc<Launch>>>,
}

impl Launches {
    pub(crate) fn record(&self, mut launch: LaunchRecord) -> Rc<Launch> {
        launch.launched_at = report::utc_stamp();
        let record = Rc::new(Launch(RefCell::new(launch)));
        self.records.borrow_mut().push(record.clone());
        record
    }

    pub(crate) fn snapshot(&self) -> Vec<LaunchRecord> {
        self.records.borrow().iter().map(|record| record.0.borrow().clone()).collect()
    }
}

pub(crate) struct Launch(RefCell<LaunchRecord>);

impl Launch {
    pub(crate) fn relaunched(&self, pid: u32, ledger: &Path, accountsdb: &Path) {
        let mut launch = self.0.borrow_mut();
        launch.pid = pid;
        launch.relaunches += 1;
        launch.set_storage(ledger, accountsdb);
        launch.exit = None;
    }

    pub(crate) fn record_exit(&self, message: String) {
        self.0.borrow_mut().exit = Some(message);
    }
}
