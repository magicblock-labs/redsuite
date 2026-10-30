pub mod continuous;
mod group;
use group::GroupEr;

use std::{
    cell::Cell,
    env,
    env::consts::{ARCH, OS},
    mem,
    num::NonZeroUsize,
    process::Command,
    result, thread,
    time::Instant,
};

use anyhow::ensure;

use futures_util::{future::join_all, stream::FuturesUnordered, StreamExt};
use redsuite_core::{
    catalog::{BasePreparation, Er, ErGroup, Family, ScenarioEntry},
    config::ExecutionConfig,
    report,
    report::{ScenarioReport, Unit},
    topology, BaseCtx, ErCtx, Result, RunRecord,
};
use tracing::{error, info};

use crate::{args::Run, catalog};

type LaneResult = (&'static str, f64, Vec<RunRecord>);

/// Compatible ER-group scenarios, or fresh scenarios whose concurrency is bounded independently.
struct Lane {
    label: &'static str,
    group: Option<&'static ErGroup>,
    entries: Vec<ScenarioEntry>,
}

impl Lane {
    async fn run(self, suite: &Suite, base: &Result<BaseCtx>) -> LaneResult {
        let started = Instant::now();
        let group = match self.group {
            Some(group) => Some(GroupEr::boot(group, base).await),
            None => None,
        };
        let er = match &group {
            Some(Ok(group)) => Some(Ok(group.ctx().clone())),
            Some(Err(error)) => Some(Err(format!("{error:#}"))),
            None => None,
        };
        let mut records = if suite.serial || self.group.is_none() {
            let limit = if suite.serial { NonZeroUsize::MIN } else { suite.fresh_concurrency };
            suite.run_entries(self.entries, limit, er).await
        } else {
            let (shared, exclusive): (Vec<_>, Vec<_>) =
                self.entries.into_iter().partition(|entry| matches!(entry.er, Er::Shared(_)));
            let mut records = suite.run_entries(shared, suite.shared_concurrency, er.clone()).await;
            if records.iter().all(RunRecord::passed) {
                records.extend(suite.run_entries(exclusive, NonZeroUsize::MIN, er).await);
            } else {
                records.extend(
                    exclusive.into_iter().map(|entry| {
                        suite.not_run(entry, "earlier scenario in this ER group failed")
                    }),
                );
            }
            records
        };
        let elapsed = started.elapsed().as_secs_f64();
        if let Some(Ok(group)) = group {
            if let Err(error) = group.finish().await {
                records.push(RunRecord::failed(
                    format!("group/{}/cleanup", self.label),
                    "cleanup",
                    format!("group teardown failed: {error:#}"),
                ));
            }
        }
        (self.label, elapsed, records)
    }
}

/// Selected scenarios and their scheduling/reporting policy for one run.
/// Progress counts terminal scenario records, including skipped entries,
/// but not group-cleanup records.
pub struct Suite {
    family: Family,
    selection: String,
    entries: Vec<ScenarioEntry>,
    total: usize,
    finished: Cell<usize>,
    config: ExecutionConfig,
    serial: bool,
    fresh_concurrency: NonZeroUsize,
    shared_concurrency: NonZeroUsize,
    cleanup: bool,
    report: Option<ScenarioReport>,
}

impl Suite {
    pub fn new(
        family: Family,
        args: Run,
        config: ExecutionConfig,
        serial: bool,
        fresh_concurrency: NonZeroUsize,
        shared_concurrency: NonZeroUsize,
    ) -> Result<Self> {
        let selection = if args.selections.is_empty() {
            family.prefix().to_owned()
        } else {
            args.selections.join(" ")
        };
        let entries = catalog::select(family, &args.selections)?;
        let mut suite = Self {
            family,
            selection,
            total: entries.len(),
            finished: Cell::new(0),
            entries,
            config,
            serial: serial || family == Family::Redline,
            fresh_concurrency,
            shared_concurrency,
            cleanup: args.cleanup,
            report: None,
        };
        if family == Family::Redline {
            suite.report = Some(suite.summary());
        }
        Ok(suite)
    }

    fn summary(&self) -> ScenarioReport {
        let mut names: Vec<_> = self.entries.iter().map(|entry| entry.name(self.family)).collect();
        names.sort();
        let hostname = env::var("HOSTNAME")
            .ok()
            .or_else(|| {
                let output = Command::new("hostname").output().ok()?;
                Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
            })
            .unwrap_or_else(|| "unknown".to_owned());
        ScenarioReport::ok()
            .setting(
                "target",
                if self.config.environment.as_ref().is_some_and(|env| env.er.is_some()) {
                    "attached"
                } else {
                    "managed"
                },
            )
            .setting("loop", self.config.loop_mode.name())
            .setting(
                "host",
                format!(
                    "{} {} {} {} cpus",
                    hostname,
                    OS,
                    ARCH,
                    thread::available_parallelism().map_or(0, |n| n.get())
                ),
            )
            .setting("serial", self.serial)
            .setting("cleanup", self.cleanup)
            .setting("scenarios", names.join(","))
            .setting("redline profile", self.config.profile.name())
    }

    fn lanes(&mut self) -> Vec<Lane> {
        self.entries.sort_by_key(|entry| entry.order);
        let mut lanes: Vec<Lane> = Vec::new();
        for entry in mem::take(&mut self.entries) {
            let group = entry.er.group();
            let label = group.map_or("fresh", |group| group.label);
            let lane = lanes.iter_mut().find(|lane| lane.label == label);
            match lane {
                Some(lane) => lane.entries.push(entry),
                None => lanes.push(Lane {
                    label,
                    group,
                    entries: vec![entry],
                }),
            }
        }
        lanes
    }

    /// Advances once per selected scenario; synthetic cleanup failures do
    /// not consume progress slots.
    fn scenario_finished(&self) {
        let finished = self.finished.get() + 1;
        self.finished.set(finished);
        info!(
            finished,
            total = self.total,
            remaining = self.total - finished,
            "suite progress"
        );
    }

    fn not_run(&self, entry: ScenarioEntry, reason: &str) -> RunRecord {
        let record = RunRecord::not_run(entry.name(self.family), reason);
        self.scenario_finished();
        record
    }

    /// Drains started work before marking pending entries skipped after
    /// interruption or group failure.
    async fn run_entries(
        &self,
        entries: Vec<ScenarioEntry>,
        limit: NonZeroUsize,
        er: Option<result::Result<ErCtx, String>>,
    ) -> Vec<RunRecord> {
        let stop_on_failure = er.is_some();
        let mut pending = entries.into_iter();
        let mut running = FuturesUnordered::new();
        let mut records = Vec::new();
        let mut failed = false;
        loop {
            while !failed && !self.config.stopped() && running.len() < limit.get() {
                let Some(entry) = pending.next() else { break };
                running.push(entry.run(self.family, self.config.clone(), er.clone()));
            }
            let Some(record) = running.next().await else { break };
            failed |= stop_on_failure && !record.passed();
            self.scenario_finished();
            records.push(record);
        }
        let reason = if self.config.stopped() {
            "run interrupted"
        } else {
            "earlier scenario in this ER group failed"
        };
        records.extend(pending.map(|entry| self.not_run(entry, reason)));
        records
    }

    async fn execute(&mut self, base: &Result<BaseCtx>) -> Vec<LaneResult> {
        let mut lanes = self.lanes();
        if !self.serial {
            lanes.sort_by_key(|lane| lane.group.is_none());
            return join_all(lanes.into_iter().map(|lane| lane.run(self, base))).await;
        }
        let started = Instant::now();
        let mut records = Vec::new();
        for lane in lanes {
            records.extend(lane.run(self, base).await.2);
        }
        vec![("serial", started.elapsed().as_secs_f64(), records)]
    }

    pub async fn run(mut self) -> Result<()> {
        let started = Instant::now();
        info!(selection = %self.selection, total = self.total, "suite started");
        let mut base = topology::base_only(self.config.clone()).await;
        if let Ok(ctx) = &base {
            if let Err(error) = prepare(self.entries.iter(), ctx).await {
                base = Err(error);
            }
        }
        let lanes = self.execute(&base).await;
        let mut records = Vec::new();
        for (label, seconds, mut runs) in lanes {
            if !runs.is_empty() {
                self.report = self.report.map(|report| {
                    report.metric(format!("{label} seconds"), Unit::Seconds, seconds)
                });
            }
            records.append(&mut runs);
        }
        if let Ok(base) = &base {
            base.reclaim(&self.selection);
        }
        let mut outcome = self.finish(&records);
        if let Some(report) = self.report {
            let report = report.metric(
                "wall seconds",
                Unit::Seconds,
                started.elapsed().as_secs_f64(),
            );
            let persisted = report::persist_summary(
                &format!("suite/{}", self.selection),
                report,
                &outcome,
            )
            .inspect_err(
                |error| error!(error = %format_args!("{error:#}"), "suite report not persisted"),
            );
            outcome = outcome.and(persisted);
        }
        info!(
            selection = %self.selection,
            passed = outcome.is_ok(),
            "suite finished"
        );
        outcome
    }

    /// Runs requested cleanup while keeping synthetic records out of the selected-scenario count.
    fn finish(&self, records: &[RunRecord]) -> Result<()> {
        let cleanup = if self.cleanup && self.config.environment.is_none() {
            topology::down()
        } else {
            Ok(())
        }
        .inspect_err(|error| error!(error = %format_args!("{error:#}"), "stack cleanup failed"));
        let not_run = records.iter().filter(|record| record.phase == "not run").count();
        let failed = records
            .iter()
            .filter(|record| record.phase != "not run" && !record.passed())
            .count();
        ensure!(
            failed == 0 && not_run == 0,
            "{failed} failed, {not_run} not run ({} scenarios selected)",
            self.total
        );
        cleanup
    }
}

async fn prepare<'a>(
    entries: impl Iterator<Item = &'a ScenarioEntry>,
    base: &BaseCtx,
) -> Result<()> {
    let mut prepared: Vec<&BasePreparation> = Vec::new();
    for prepare in entries.filter_map(|entry| entry.prepare) {
        if prepared.iter().any(|previous| std::ptr::eq(*previous, prepare)) {
            continue;
        }
        (prepare.run)(base).await?;
        prepared.push(prepare);
    }
    Ok(())
}
