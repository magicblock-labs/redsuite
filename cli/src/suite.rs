use std::{process::Command, rc::Rc, time::Instant};

use futures_util::{future::join_all, StreamExt};
use redsuite_core::{
    catalog::{Er, ErGroup, Family, ScenarioEntry},
    netfault::BaseProxies,
    profile::ExecutionConfig,
    report::{self, ScenarioReport, Unit},
    topology::{self, PrivateEr, ER_READY_TIMEOUT},
    BaseCtx, ErCtx, Result, RunRecord,
};
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::{args::Run, catalog};

const NO_GROUP: &str = "the catalog entry declares no ER group";
type LaneResult = (&'static str, f64, Vec<RunRecord>);

struct GroupEr {
    er: Rc<Mutex<PrivateEr>>,
    proxies: Option<Rc<BaseProxies>>,
}

impl GroupEr {
    async fn boot(group: &ErGroup, base: &Result<BaseCtx>) -> Result<Self> {
        let base = base.as_ref().map_err(ToString::to_string)?;
        let mut options = group.options();
        let proxies = if group.proxied {
            let proxies = Rc::new(BaseProxies::spawn(base).await?);
            options.base_endpoints = Some(proxies.endpoints());
            Some(proxies)
        } else {
            None
        };
        let mut er = topology::private_er(base, options).await?;
        if let Some(proxies) = &proxies {
            er = er.with_proxies(proxies.clone());
        }
        er.wait_ready(ER_READY_TIMEOUT).await?;
        Ok(Self { er: er.shared(), proxies })
    }

    async fn finish(self) {
        if let Some(er) = Rc::into_inner(self.er) {
            if let Err(error) = er.into_inner().finish().await {
                error!(%error, "group ER cleanup failed");
            }
        }
        if let Some(proxies) = self.proxies.and_then(Rc::into_inner) {
            if let Err(error) = proxies.finish() {
                error!(%error, "group ER proxy cleanup failed");
            }
        }
    }
}

struct Lane {
    label: &'static str,
    group: Option<&'static ErGroup>,
    entries: Vec<&'static ScenarioEntry>,
}

impl Lane {
    async fn run(self, suite: &Suite, base: &Result<BaseCtx>) -> LaneResult {
        let started = Instant::now();
        let group = match self.group {
            Some(group) => Some(GroupEr::boot(group, base).await),
            None => None,
        };
        let er = match &group {
            Some(Ok(group)) => Ok(group.er.lock().await.ctx().clone()),
            Some(Err(error)) => Err(error.to_string()),
            None => Err(NO_GROUP.to_owned()),
        };
        let records = if suite.serial || self.group.is_none() {
            let limit = if suite.serial { 1 } else { suite.fresh_concurrency };
            run_entries(self.entries, limit, suite.config, er).await
        } else {
            let (shared, exclusive): (Vec<_>, Vec<_>) =
                self.entries.into_iter().partition(|entry| matches!(entry.er, Er::Shared(_)));
            let limit = shared.len();
            let mut records = run_entries(shared, limit, suite.config, er.clone()).await;
            records.extend(run_entries(exclusive, 1, suite.config, er).await);
            records
        };
        let elapsed = started.elapsed().as_secs_f64();
        if let Some(Ok(group)) = group {
            group.finish().await;
        }
        (self.label, elapsed, records)
    }
}

async fn run_entries(
    entries: Vec<&'static ScenarioEntry>,
    limit: usize,
    config: ExecutionConfig,
    er: std::result::Result<ErCtx, String>,
) -> Vec<RunRecord> {
    futures_util::stream::iter(entries)
        .map(|entry| (entry.run)(config, er.clone()))
        .buffer_unordered(limit.max(1))
        .collect()
        .await
}

pub struct Suite {
    selection: String,
    entries: Vec<&'static ScenarioEntry>,
    config: ExecutionConfig,
    serial: bool,
    fresh_concurrency: usize,
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
    ) -> Result<Self> {
        let selection = if args.selections.is_empty() {
            family.prefix().to_owned()
        } else {
            args.selections.join(" ")
        };
        let mut suite = Self {
            selection,
            entries: catalog::select(family, &args.selections)?,
            config,
            serial: serial || family == Family::Redline,
            fresh_concurrency,
            cleanup: args.cleanup,
            report: None,
        };
        if family == Family::Redline {
            suite.report = Some(suite.summary());
        }
        Ok(suite)
    }

    fn summary(&self) -> ScenarioReport {
        let mut names: Vec<_> = self.entries.iter().map(|entry| entry.name()).collect();
        names.sort();
        let hostname = std::env::var("HOSTNAME")
            .ok()
            .or_else(|| {
                let output = Command::new("hostname").output().ok()?;
                Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
            })
            .unwrap_or_else(|| "unknown".to_owned());
        ScenarioReport::ok(&format!("suite/{}", self.selection))
            .setting("loop", self.config.loop_mode.name())
            .setting(
                "host",
                format!(
                    "{} {} {} {} cpus",
                    hostname,
                    std::env::consts::OS,
                    std::env::consts::ARCH,
                    std::thread::available_parallelism().map_or(0, |n| n.get())
                ),
            )
            .setting("serial", self.serial)
            .setting("cleanup", self.cleanup)
            .setting("scenarios", names.join(","))
            .setting("redline profile", self.config.profile.name())
    }

    fn lanes(&self) -> Vec<Lane> {
        let mut lanes: Vec<Lane> = Vec::new();
        for &entry in &self.entries {
            let group = entry.er.group();
            let label = group.map_or("fresh", |group| group.label);
            let lane = if self.serial {
                lanes.last_mut().filter(|lane| lane.label == label)
            } else {
                lanes.iter_mut().find(|lane| lane.label == label)
            };
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

    async fn execute(&self, base: &Result<BaseCtx>) -> Vec<LaneResult> {
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
        let base = topology::base_only(self.config).await;
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
        let mut outcome = finish(&records, self.cleanup);
        if let Some(report) = self.report {
            let report = report.metric(
                "wall seconds",
                Unit::Seconds,
                started.elapsed().as_secs_f64(),
            );
            let persisted = report::persist_summary(report, &outcome)
                .inspect_err(|error| error!(%error, "suite report not persisted"));
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

pub fn finish(records: &[RunRecord], cleanup: bool) -> Result<()> {
    let cleanup = if cleanup { topology::down() } else { Ok(()) }
        .inspect_err(|error| error!(%error, "stack cleanup failed"));
    let failed = records.iter().filter(|record| !record.passed()).count();
    if failed != 0 {
        return Err(format!("{failed} of {} scenarios failed", records.len()).into());
    }
    cleanup
}
