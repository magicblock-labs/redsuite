use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use crate::{host, DynError};

#[derive(Default)]
pub struct Resources {
    records: RefCell<Vec<Rc<ResourceRecord>>>,
}

impl Resources {
    pub(crate) fn register(&self, label: &str, pid: u32) -> Rc<ResourceRecord> {
        let record = Rc::new(ResourceRecord {
            label: label.to_owned(),
            pid: Cell::new(pid),
            finished: Cell::new(false),
            finish_error: RefCell::new(None),
        });
        self.records.borrow_mut().push(record.clone());
        record
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
}

impl ResourceRecord {
    pub(crate) fn relaunched(&self, pid: u32) {
        self.pid.set(pid);
        self.finished.set(false);
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
}
