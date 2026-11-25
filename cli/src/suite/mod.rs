pub mod continuous;

use std::{
    env,
    env::consts::{ARCH, OS},
    mem,
    num::NonZeroUsize,
    process::Command,
    result, thread,
    time::Instant,
};

use anyhow::{ensure, Context, Error};

use futures_util::{future::join_all, stream::FuturesUnordered, StreamExt};
use redsuite_core::{
    catalog::{BasePreparation, Er, ErGroup, Family, ScenarioEntry},
    config::ExecutionConfig,
    report,
    report::{ScenarioReport, Unit},
    topology::{self, GroupEr},
    BaseCtx, ErCtx, Result, RunRecord,
};
use tracing::{debug, error, info};

use crate::{args::Run, catalog};

const GROUP_FAILED: &str = "earlier scenario in this ER group failed";

struct LaneResult {
    label: &'static str,
    seconds: f64,
    records: Vec<RunRecord>,
    teardown_failed: bool,
}

/// Compatible ER-group scenarios, or fresh scenarios whose concurrency is bounded independently.
struct Lane {
    group: Option<&'static ErGroup>,
    entries: Vec<ScenarioEntry>,
}

impl Lane {
    async fn run(self, suite: &Suite, base: &Result<BaseCtx>) -> LaneResult {
        let started = Instant::now();
        let label = self.group.map_or("fresh", |group| group.label);
        let group = match (self.group, base) {
            (Some(spec), Ok(base)) => Some(GroupEr::boot(spec, base).await),
            _ => None,
        };
        if let Some(Err(error)) = &group {
            error!(group = label, seed = suite.config.seed, error = %format_args!("{error:#}"),
                "ER group setup failed");
        }
        let er = group.as_ref().map(|result| result.as_ref().map(GroupEr::ctx));
        let limit = match (suite.serial, self.group) {
            (true, _) => NonZeroUsize::MIN,
            (false, Some(_)) => suite.shared_concurrency,
            (false, None) => suite.fresh_concurrency,
        };
        let (first, exclusive) = if !suite.serial && self.group.is_some() {
            self.entries.into_iter().partition(|entry| matches!(entry.er, Er::Shared(_)))
        } else {
            (self.entries, Vec::new())
        };
        let mut records = suite.run_entries(first, limit, base, er, self.group.is_some()).await;
        if !exclusive.is_empty() {
            if records.iter().all(RunRecord::passed) {
                records
                    .extend(suite.run_entries(exclusive, NonZeroUsize::MIN, base, er, true).await);
            } else {
                records.extend(
                    exclusive
                        .into_iter()
                        .map(|entry| RunRecord::not_run(entry.name(suite.family), GROUP_FAILED)),
                );
            }
        }
        let seconds = started.elapsed().as_secs_f64();
        let teardown_failed = match group {
            Some(Ok(group)) => group
                .finish()
                .await
                .inspect_err(|error| {
                    error!(group = label, error = %format_args!("{error:#}"), "ER group teardown failed");
                })
                .is_err(),
            _ => false,
        };
        debug!(
            lane = label,
            seconds = format_args!("{seconds:.2}"),
            "lane finished"
        );
        LaneResult {
            label,
            seconds,
            records,
            teardown_failed,
        }
    }
}

/// Selected scenarios and their scheduling/reporting policy for one run.
pub struct Suite {
    family: Family,
    selection: String,
    entries: Vec<ScenarioEntry>,
    total: usize,
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
            let label = group.map(|group| group.label);
            let lane = lanes.iter_mut().find(|lane| lane.group.map(|group| group.label) == label);
            match lane {
                Some(lane) => lane.entries.push(entry),
                None => lanes.push(Lane { group, entries: vec![entry] }),
            }
        }
        lanes
    }

    /// Drains started work before marking pending entries skipped after
    /// interruption or group failure.
    async fn run_entries(
        &self,
        entries: Vec<ScenarioEntry>,
        limit: NonZeroUsize,
        base: &Result<BaseCtx>,
        er: Option<result::Result<&ErCtx, &Error>>,
        stop_on_failure: bool,
    ) -> Vec<RunRecord> {
        let mut pending = entries.into_iter();
        let mut running = FuturesUnordered::new();
        let mut records = Vec::new();
        let mut failed = false;
        loop {
            while !failed && !self.config.stopped() && running.len() < limit.get() {
                let Some(entry) = pending.next() else { break };
                running.push(entry.run(self.family, self.config.clone(), base.as_ref(), er));
            }
            let Some(record) = running.next().await else { break };
            failed |= stop_on_failure && !record.passed();
            records.push(record);
        }
        let reason = if self.config.stopped() { "run interrupted" } else { GROUP_FAILED };
        records.extend(pending.map(|entry| RunRecord::not_run(entry.name(self.family), reason)));
        records
    }

    async fn execute(&mut self, base: &Result<BaseCtx>) -> Vec<LaneResult> {
        let mut lanes = self.lanes();
        if !self.serial {
            lanes.sort_by_key(|lane| lane.group.is_none());
            return join_all(lanes.into_iter().map(|lane| lane.run(self, base))).await;
        }
        let started = Instant::now();
        let mut outcomes = Vec::with_capacity(lanes.len());
        for lane in lanes {
            outcomes.push(lane.run(self, base).await);
        }
        if outcomes.iter().any(|lane| !lane.records.is_empty()) {
            self.report = self.report.take().map(|report| {
                report.metric(
                    "serial seconds",
                    Unit::Seconds,
                    started.elapsed().as_secs_f64(),
                )
            });
        }
        outcomes
    }

    pub async fn run(mut self) -> Result<()> {
        let started = Instant::now();
        info!(selection = %self.selection, scenarios = self.total, seed = self.config.seed,
            serial = self.serial, "suite started");
        let base = async {
            let ctx = topology::base_only(self.config.clone())
                .await
                .context("connecting to base stack")?;
            prepare(self.entries.iter(), &ctx).await?;
            Ok(ctx)
        }
        .await;
        if let Err(error) = &base {
            error!(selection = %self.selection, seed = self.config.seed, error = %format_args!("{error:#}"),
                "base setup failed");
        }
        let lanes = self.execute(&base).await;
        let mut outcome = self.finish(&lanes).await;
        if let Some(mut report) = self.report {
            for lane in lanes.iter().filter(|lane| !self.serial && !lane.records.is_empty()) {
                report = report.metric(
                    format!("{} seconds", lane.label),
                    Unit::Seconds,
                    lane.seconds,
                );
            }
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
        if outcome.is_ok() {
            info!(selection = %self.selection, passed = self.total,
                seconds = format_args!("{:.2}", started.elapsed().as_secs_f64()), "suite finished");
        }
        outcome
    }

    async fn finish(&self, lanes: &[LaneResult]) -> Result<()> {
        let cleanup = if self.cleanup && self.config.environment.is_none() {
            topology::reset_base(false).await
        } else {
            Ok(())
        }
        .inspect_err(|error| error!(error = %format_args!("{error:#}"), "stack cleanup failed"));
        let teardown_failed = lanes.iter().filter(|lane| lane.teardown_failed).count();
        let (mut passed, mut failed, mut not_run) = (0, 0, 0);
        for record in lanes.iter().flat_map(|lane| &lane.records) {
            match record.phase {
                "not run" => not_run += 1,
                _ if record.passed() => passed += 1,
                _ => failed += 1,
            }
        }
        ensure!(
            failed == 0 && not_run == 0 && teardown_failed == 0,
            "{}: {passed} passed, {failed} failed, {not_run} not run; {teardown_failed} group teardown failures",
            self.selection
        );
        cleanup
    }
}

async fn prepare<'a>(
    entries: impl Iterator<Item = &'a ScenarioEntry>,
    base: &BaseCtx,
) -> Result<()> {
    let mut prepared: Vec<&BasePreparation> = Vec::new();
    for (entry, prepare) in entries.filter_map(|entry| entry.prepare.map(|prep| (entry, prep))) {
        if prepared.iter().any(|previous| std::ptr::eq(*previous, prepare)) {
            continue;
        }
        (prepare.run)(base)
            .await
            .with_context(|| format!("preparing shared base fixtures for {}", entry.short_name))?;
        prepared.push(prepare);
    }
    Ok(())
}
