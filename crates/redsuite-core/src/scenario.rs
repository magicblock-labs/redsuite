use std::{future::Future, panic::AssertUnwindSafe, rc::Rc, time::Instant};

use async_trait::async_trait;
use futures_util::FutureExt;

use crate::{
    catalog::Fixture,
    check::CheckError,
    console,
    context::{BaseCtx, ErCtx},
    manifest,
    profile::ExecutionConfig,
    report::{MeasureValue, ScenarioReport, Unit},
    resources::{LaunchRecord, Resources},
    runner::panic_message,
    topology, DynError, Result,
};

pub trait Verdict {
    const REPORTS: bool;
    fn into_report(self) -> Option<ScenarioReport>;
}

impl Verdict for () {
    const REPORTS: bool = false;
    fn into_report(self) -> Option<ScenarioReport> {
        None
    }
}

impl Verdict for ScenarioReport {
    const REPORTS: bool = true;
    fn into_report(self) -> Option<ScenarioReport> {
        Some(self)
    }
}

#[async_trait(?Send)]
pub trait Scenario<V: Verdict = ()> {
    fn name(&self) -> &str;
    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<V>;
}

#[async_trait(?Send)]
pub trait PrivateErScenario<V: Verdict = ()> {
    fn name(&self) -> &str;
    async fn run(&self, base: &BaseCtx) -> Result<V>;
}

#[derive(Debug)]
pub enum RunError {
    Preflight(DynError),
    Topology(DynError),
    Teardown(DynError),
    Persist(DynError),
}

impl RunError {
    pub fn phase(&self) -> &'static str {
        match self {
            RunError::Preflight(_) => "preflight",
            RunError::Topology(_) => "topology",
            RunError::Teardown(_) => "teardown",
            RunError::Persist(_) => "persist",
        }
    }

    pub fn error(&self) -> &DynError {
        match self {
            RunError::Preflight(error)
            | RunError::Topology(error)
            | RunError::Teardown(error)
            | RunError::Persist(error) => error,
        }
    }
}

impl std::fmt::Display for RunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} failed: {}", self.phase(), self.error())
    }
}

#[derive(Debug)]
pub enum ScenarioOutcome {
    Passed(Option<ScenarioReport>),
    Skipped(String),
    Failed(DynError),
    Panicked(String),
    NotReached,
}

// A failed check is scenario evidence; every other error is infrastructure.
pub fn failed_check(error: &DynError) -> Option<&CheckError> {
    error.downcast_ref::<CheckError>()
}

#[derive(Debug)]
pub struct RunRecord {
    pub name: String,
    pub errors: Vec<RunError>,
    pub scenario: ScenarioOutcome,
    pub wall_seconds: Option<f64>,
    pub launches: Vec<LaunchRecord>,
}

impl RunRecord {
    fn new(name: String) -> Self {
        Self {
            name,
            errors: Vec::new(),
            scenario: ScenarioOutcome::NotReached,
            wall_seconds: None,
            launches: Vec::new(),
        }
    }

    pub fn passed(&self) -> bool {
        let passed = match &self.scenario {
            ScenarioOutcome::Passed(report) => {
                report.as_ref().is_none_or(|report| report.passed)
            }
            ScenarioOutcome::Skipped(_) => true,
            _ => false,
        };
        passed && self.errors.is_empty()
    }

    pub fn failure(&self) -> Option<String> {
        let mut lines = Vec::new();
        match &self.scenario {
            ScenarioOutcome::Failed(error) => match failed_check(error) {
                Some(check) => lines.push(format!("check failed: {check}")),
                None => lines.push(format!("scenario failed: {error}")),
            },
            ScenarioOutcome::Panicked(message) => {
                lines.push(format!("scenario panicked: {message}"))
            }
            ScenarioOutcome::Passed(_)
            | ScenarioOutcome::Skipped(_)
            | ScenarioOutcome::NotReached => {}
        }
        lines.extend(self.errors.iter().map(ToString::to_string));
        if lines.is_empty() {
            return None;
        }
        Some(format!("{}: {}", self.name, lines.join("\n  also: ")))
    }
}

pub async fn run_shared_scenario<V: Verdict>(
    scenario: impl Scenario<V>,
    fixtures: &[Fixture],
    optional_fixtures: &[Fixture],
    config: ExecutionConfig,
) -> RunRecord {
    // A LocalSet so contexts and transports can spawn_local background work
    // (WS readers) on the test's current-thread runtime.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let name = scenario.name().to_owned();
            execute(
                name,
                fixtures,
                optional_fixtures,
                config,
                topology::shared,
                |(base, er)| async move { scenario.run(&base, &er).await },
            )
            .await
        })
        .await
}

pub async fn run_private_er_scenario<V: Verdict>(
    scenario: impl PrivateErScenario<V>,
    fixtures: &[Fixture],
    optional_fixtures: &[Fixture],
    config: ExecutionConfig,
) -> RunRecord {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let name = scenario.name().to_owned();
            execute(
                name,
                fixtures,
                optional_fixtures,
                config,
                topology::base_only,
                |base| async move { scenario.run(&base).await },
            )
            .await
        })
        .await
}

// The provisioned base carries the run's resource registry; the executor
// reads it here so it can audit teardown after the body completes.
trait ProvidesResources {
    fn resources(&self) -> Rc<Resources>;
}

impl ProvidesResources for BaseCtx {
    fn resources(&self) -> Rc<Resources> {
        BaseCtx::resources(self)
    }
}

impl ProvidesResources for (BaseCtx, ErCtx) {
    fn resources(&self) -> Rc<Resources> {
        self.0.resources()
    }
}

async fn execute<V: Verdict, Provisioned, ProvisionFut, Body, BodyFut>(
    name: String,
    fixtures: &[Fixture],
    optional_fixtures: &[Fixture],
    config: ExecutionConfig,
    provision: impl FnOnce(ExecutionConfig) -> ProvisionFut,
    body: Body,
) -> RunRecord
where
    Provisioned: ProvidesResources,
    ProvisionFut: Future<Output = Result<Provisioned>>,
    Body: FnOnce(Provisioned) -> BodyFut,
    BodyFut: Future<Output = Result<V>>,
{
    let mut record = RunRecord::new(name);

    if let Err(error) = preflight(fixtures) {
        record.errors.push(RunError::Preflight(error));
        conclude(&mut record, V::REPORTS);
        return record;
    }
    if let Some(reason) = optional_fixture_gap(optional_fixtures) {
        record.scenario = ScenarioOutcome::Skipped(reason);
        conclude(&mut record, V::REPORTS);
        return record;
    }

    let provisioned = match provision(config).await {
        Ok(provisioned) => provisioned,
        Err(error) => {
            record.errors.push(RunError::Topology(error));
            conclude(&mut record, V::REPORTS);
            return record;
        }
    };

    let resources = provisioned.resources();
    let started = Instant::now();
    // catch_unwind so an internal-invariant panic still reaches the teardown
    // audit and the persisted report instead of aborting sibling scenarios
    let outcome = AssertUnwindSafe(body(provisioned)).catch_unwind().await;
    let wall_seconds = started.elapsed().as_secs_f64();
    for reclaimed in resources.reclaim() {
        let stopped = if reclaimed.killed { "stopped" } else { "" };
        let removed = if reclaimed.removed {
            format!("removed {}", reclaimed.storage_dir)
        } else {
            String::new()
        };
        let joiner = if reclaimed.killed && reclaimed.removed {
            ", "
        } else {
            ""
        };
        console::line(format_args!(
            "{}: reclaimed private ER `{}`: {stopped}{joiner}{removed}",
            record.name, reclaimed.label
        ));
    }
    let teardown_errors = resources.audit();
    record.launches = resources.launches();
    record.wall_seconds = Some(wall_seconds);
    record.scenario = match outcome {
        Ok(Ok(verdict)) => {
            ScenarioOutcome::Passed(verdict.into_report().map(|report| {
                report.metric("wall seconds", Unit::Seconds, wall_seconds)
            }))
        }
        Ok(Err(error)) => ScenarioOutcome::Failed(error),
        Err(payload) => ScenarioOutcome::Panicked(panic_message(payload)),
    };

    record
        .errors
        .extend(teardown_errors.into_iter().map(RunError::Teardown));

    conclude(&mut record, V::REPORTS);
    record
}

fn preflight(fixtures: &[Fixture]) -> Result<()> {
    topology::er_bin_path()?;
    if !fixtures.is_empty() {
        let manifest = manifest::load()?;
        if let Some(warning) = manifest::revision_drift(&manifest) {
            console::line(format_args!("warning: {warning}"));
        }
        for fixture in fixtures {
            manifest::resolve(*fixture)?;
        }
    }
    let Some(loaded) = topology::running_base_programs() else {
        return Ok(());
    };
    for fixture in fixtures {
        if fixture.loaded_at_base_boot()
            && !loaded.iter().any(|name| name == fixture.so_name())
        {
            return Err(format!(
                "{} is staged, but the running base booted without it — \
                 run `cargo xtask stack down`, then run again",
                fixture.so_name()
            )
            .into());
        }
    }
    Ok(())
}

fn optional_fixture_gap(optional_fixtures: &[Fixture]) -> Option<String> {
    for fixture in optional_fixtures {
        if let Err(error) = manifest::resolve(*fixture) {
            return Some(format!(
                "optional fixture {} is unavailable: {error}",
                fixture.so_name()
            ));
        }
    }
    None
}

fn conclude(record: &mut RunRecord, reports: bool) {
    let passed = record.passed();
    let show_details = !passed || console::verbose();
    match &record.scenario {
        ScenarioOutcome::Passed(report) => {
            console::line(format_args!(
                "{}: {}",
                record.name,
                if passed { "passed" } else { "failed" }
            ));
            if let Some(report) = report.as_ref().filter(|_| show_details) {
                if !report.config.is_empty() {
                    let knobs: Vec<String> = report
                        .config
                        .iter()
                        .map(|(key, value)| format!("{key}={value}"))
                        .collect();
                    console::detail(format_args!(
                        "config: {}",
                        knobs.join(" ")
                    ));
                }
                for measurement in &report.measurements {
                    match &measurement.value {
                        MeasureValue::Distribution(stats) => console::detail(
                            format_args!("{}: {stats:?}", measurement.label),
                        ),
                        MeasureValue::Scalar(value) => console::detail(
                            format_args!("{}: {value}", measurement.label),
                        ),
                    }
                }
            }
        }
        ScenarioOutcome::Failed(error) => match failed_check(error) {
            Some(check) => console::line(format_args!(
                "{}: check failed: {check}",
                record.name
            )),
            None => console::line(format_args!(
                "{}: scenario failed: {error}",
                record.name
            )),
        },
        ScenarioOutcome::Panicked(message) => console::line(format_args!(
            "{}: scenario panicked: {message}",
            record.name
        )),
        ScenarioOutcome::Skipped(reason) => {
            console::line(format_args!("{}: skipped — {reason}", record.name))
        }
        ScenarioOutcome::NotReached => {
            console::line(format_args!("{}: not run", record.name))
        }
    }
    for error in &record.errors {
        console::detail(format_args!("{error}"));
    }
    if show_details {
        for launch in &record.launches {
            let exit = launch
                .exit
                .as_deref()
                .map(|exit| format!(", {exit}"))
                .unwrap_or_default();
            console::detail(format_args!(
                "launched {} `{}`: pid {} ({} relaunches), {} {}, metrics \
                 127.0.0.1:{}, storage {}{}",
                launch.role,
                launch.label,
                launch.pid,
                launch.relaunches,
                launch.bin,
                launch.bin_version,
                launch.metrics_port,
                launch.storage_dir,
                exit,
            ));
        }
    }
    if matches!(record.scenario, ScenarioOutcome::Skipped(_)) {
        return;
    }
    crate::report::warn_on_stack_skew();
    if !reports {
        return;
    }
    match crate::report::persist_run(record) {
        Ok(path) => {
            if show_details {
                console::detail(format_args!("report: {}", path.display()));
            }
        }
        Err(error) => {
            let error = RunError::Persist(error);
            console::detail(format_args!("{error}"));
            record.errors.push(error);
        }
    }
}
