pub mod continuous;
mod group;
use group::GroupEr;

use std::{
    env,
    env::consts::{ARCH, OS},
    mem,
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
            let limit = if suite.serial { 1 } else { suite.fresh_concurrency };
            suite.run_entries(self.entries, limit, er).await
        } else {
            let (shared, exclusive): (Vec<_>, Vec<_>) =
                self.entries.into_iter().partition(|entry| matches!(entry.er, Er::Shared(_)));
            let mut records = suite.run_entries(shared, suite.shared_concurrency, er.clone()).await;
            if records.iter().all(RunRecord::passed) {
                records.extend(suite.run_entries(exclusive, 1, er).await);
            } else {
                records.extend(exclusive.into_iter().map(|entry| {
                    RunRecord::not_run(
                        entry.name(suite.family),
                        "earlier scenario in this ER group failed",
                    )
                }));
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

pub struct Suite {
    family: Family,
    selection: String,
    entries: Vec<ScenarioEntry>,
    config: ExecutionConfig,
    serial: bool,
    fresh_concurrency: usize,
    shared_concurrency: usize,
    cleanup: bool,
    report: Option<ScenarioReport>,
}

impl Suite {
    pub fn new(
        family: Family,
        args: Run,
        config: ExecutionConfig,
        serial: bool,
        fresh_concurrency: usize,
        shared_concurrency: usize,
    ) -> Result<Self> {
        let selection = if args.selections.is_empty() {
            family.prefix().to_owned()
        } else {
            args.selections.join(" ")
        };
        let mut suite = Self {
            family,
            selection,
            entries: catalog::select(family, &args.selections)?,
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

    async fn run_entries(
        &self,
        entries: Vec<ScenarioEntry>,
        limit: usize,
        er: Option<result::Result<ErCtx, String>>,
    ) -> Vec<RunRecord> {
        let stop_on_failure = er.is_some();
        let mut pending = entries.into_iter();
        let mut running = FuturesUnordered::new();
        let mut records = Vec::new();
        let mut failed = false;
        loop {
            while !failed && !self.config.stopped() && running.len() < limit.max(1) {
                let Some(entry) = pending.next() else { break };
                running.push(entry.run(self.family, self.config.clone(), er.clone()));
            }
            let Some(record) = running.next().await else { break };
            failed |= stop_on_failure && !record.passed();
            records.push(record);
        }
        records.extend(pending.map(|entry| {
            RunRecord::not_run(
                entry.name(self.family),
                if self.config.stopped() {
                    "run interrupted"
                } else {
                    "earlier scenario in this ER group failed"
                },
            )
        }));
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
        info!(selection = %self.selection, "suite started");
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
        let mut outcome = finish(&records, self.cleanup && self.config.environment.is_none());
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

pub fn finish(records: &[RunRecord], cleanup: bool) -> Result<()> {
    let cleanup = if cleanup { topology::down() } else { Ok(()) }
        .inspect_err(|error| error!(error = %format_args!("{error:#}"), "stack cleanup failed"));
    let not_run = records.iter().filter(|record| record.phase == "not run").count();
    let failed = records
        .iter()
        .filter(|record| record.phase != "not run" && !record.passed())
        .count();
    ensure!(
        failed == 0 && not_run == 0,
        "{failed} scenarios failed, {not_run} not run ({} selected)",
        records.len()
    );
    cleanup
}
