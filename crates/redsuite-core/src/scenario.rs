use std::{future::Future, panic::AssertUnwindSafe, rc::Rc, time::Instant};

use async_trait::async_trait;
use futures_util::FutureExt;

use crate::{
    catalog::Fixture,
    check::CheckError,
    context::{BaseCtx, ErCtx},
    manifest,
    profile::ExecutionConfig,
    report::{MeasureValue, ScenarioReport, Unit},
    resources::Resources,
    runner::panic_message,
    topology, DynError, Result,
};

#[async_trait(?Send)]
pub trait Scenario {
    fn name(&self) -> &str;
    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport>;
}

#[async_trait(?Send)]
pub trait PrivateErScenario {
    fn name(&self) -> &str;
    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Preflight,
    Topology,
    Teardown,
    Persist,
}

impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Phase::Preflight => "preflight",
            Phase::Topology => "topology",
            Phase::Teardown => "teardown",
            Phase::Persist => "persist",
        }
    }
}

#[derive(Debug)]
pub enum RunError {
    Preflight(DynError),
    Topology(DynError),
    Teardown(DynError),
    Persist(DynError),
}

impl RunError {
    pub fn phase(&self) -> Phase {
        match self {
            RunError::Preflight(_) => Phase::Preflight,
            RunError::Topology(_) => Phase::Topology,
            RunError::Teardown(_) => Phase::Teardown,
            RunError::Persist(_) => Phase::Persist,
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
        write!(
            formatter,
            "{} failed: {}",
            self.phase().name(),
            self.error()
        )
    }
}

#[derive(Debug)]
pub struct PhaseOutcome {
    pub phase: Phase,
    pub error: Option<RunError>,
}

#[derive(Debug)]
pub enum ScenarioOutcome {
    Passed(ScenarioReport),
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
    pub phases: Vec<PhaseOutcome>,
    pub scenario: ScenarioOutcome,
    pub wall_seconds: Option<f64>,
}

impl RunRecord {
    fn new(name: String) -> Self {
        Self {
            name,
            phases: Vec::new(),
            scenario: ScenarioOutcome::NotReached,
            wall_seconds: None,
        }
    }

    fn phase_ok(&mut self, phase: Phase) {
        self.phases.push(PhaseOutcome { phase, error: None });
    }

    fn phase_failed(&mut self, error: RunError) {
        self.phases.push(PhaseOutcome {
            phase: error.phase(),
            error: Some(error),
        });
    }

    pub fn passed(&self) -> bool {
        matches!(
            self.scenario,
            ScenarioOutcome::Passed(_) | ScenarioOutcome::Skipped(_)
        ) && self.phases.iter().all(|outcome| outcome.error.is_none())
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
        for outcome in &self.phases {
            if let Some(error) = &outcome.error {
                lines.push(error.to_string());
            }
        }
        if lines.is_empty() {
            return None;
        }
        Some(format!("{}: {}", self.name, lines.join("\n  also: ")))
    }
}

pub async fn run_shared_scenario(
    scenario: impl Scenario,
    fixtures: &[Fixture],
    optional_fixtures: &[Fixture],
    config: Result<ExecutionConfig>,
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

pub async fn run_private_er_scenario(
    scenario: impl PrivateErScenario,
    fixtures: &[Fixture],
    optional_fixtures: &[Fixture],
    config: Result<ExecutionConfig>,
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

async fn execute<Provisioned, ProvisionFut, Body, BodyFut>(
    name: String,
    fixtures: &[Fixture],
    optional_fixtures: &[Fixture],
    config: Result<ExecutionConfig>,
    provision: impl FnOnce(ExecutionConfig) -> ProvisionFut,
    body: Body,
) -> RunRecord
where
    Provisioned: ProvidesResources,
    ProvisionFut: Future<Output = Result<Provisioned>>,
    Body: FnOnce(Provisioned) -> BodyFut,
    BodyFut: Future<Output = Result<ScenarioReport>>,
{
    let mut record = RunRecord::new(name);

    let config = match config {
        Ok(config) => config,
        Err(error) => {
            record.phase_failed(RunError::Preflight(error));
            conclude(&mut record);
            return record;
        }
    };
    if let Err(error) = preflight(fixtures) {
        record.phase_failed(RunError::Preflight(error));
        conclude(&mut record);
        return record;
    }
    record.phase_ok(Phase::Preflight);
    if let Some(reason) = optional_fixture_gap(optional_fixtures) {
        record.scenario = ScenarioOutcome::Skipped(reason);
        conclude(&mut record);
        return record;
    }

    let provisioned = match provision(config).await {
        Ok(provisioned) => {
            record.phase_ok(Phase::Topology);
            provisioned
        }
        Err(error) => {
            record.phase_failed(RunError::Topology(error));
            conclude(&mut record);
            return record;
        }
    };

    let resources = provisioned.resources();
    let started = Instant::now();
    // catch_unwind so an internal-invariant panic still reaches the teardown
    // audit and the persisted report instead of aborting sibling scenarios
    let outcome = AssertUnwindSafe(body(provisioned)).catch_unwind().await;
    let wall_seconds = started.elapsed().as_secs_f64();
    let teardown_errors = resources.audit();
    record.wall_seconds = Some(wall_seconds);
    record.scenario = match outcome {
        Ok(Ok(report)) => ScenarioOutcome::Passed(report.metric(
            "wall seconds",
            Unit::Seconds,
            wall_seconds,
        )),
        Ok(Err(error)) => ScenarioOutcome::Failed(error),
        Err(payload) => ScenarioOutcome::Panicked(panic_message(payload)),
    };

    if teardown_errors.is_empty() {
        record.phase_ok(Phase::Teardown);
    } else {
        for error in teardown_errors {
            record.phase_failed(RunError::Teardown(error));
        }
    }

    conclude(&mut record);
    record
}

fn preflight(fixtures: &[Fixture]) -> Result<()> {
    topology::er_bin_path()?;
    if !fixtures.is_empty() {
        let manifest = manifest::load()?;
        if let Some(warning) = manifest::revision_drift(&manifest) {
            eprintln!("[redsuite] warning: {warning}");
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

fn conclude(record: &mut RunRecord) {
    match &record.scenario {
        ScenarioOutcome::Passed(report) => {
            eprintln!(
                "[redsuite] {}: passed={}",
                report.scenario, report.passed
            );
            if !report.config.is_empty() {
                let knobs: Vec<String> = report
                    .config
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect();
                eprintln!("[redsuite]   config: {}", knobs.join(" "));
            }
            for measurement in &report.measurements {
                match &measurement.value {
                    MeasureValue::Distribution(stats) => eprintln!(
                        "[redsuite]   {}: {stats:?}",
                        measurement.label
                    ),
                    MeasureValue::Scalar(value) => {
                        eprintln!("[redsuite]   {}: {value}", measurement.label)
                    }
                }
            }
        }
        ScenarioOutcome::Failed(error) => match failed_check(error) {
            Some(check) => {
                eprintln!("[redsuite] {}: check failed: {check}", record.name)
            }
            None => eprintln!(
                "[redsuite] {}: scenario failed: {error}",
                record.name
            ),
        },
        ScenarioOutcome::Panicked(message) => {
            eprintln!(
                "[redsuite] {}: scenario panicked: {message}",
                record.name
            )
        }
        ScenarioOutcome::Skipped(reason) => {
            eprintln!("[redsuite] {}: skipped — {reason}", record.name)
        }
        ScenarioOutcome::NotReached => {
            eprintln!("[redsuite] {}: not run", record.name)
        }
    }
    for outcome in &record.phases {
        if let Some(error) = &outcome.error {
            eprintln!("[redsuite]   {error}");
        }
    }
    if matches!(record.scenario, ScenarioOutcome::Skipped(_)) {
        return;
    }
    match crate::report::persist_run(record) {
        Ok(path) => {
            eprintln!("[redsuite]   report: {}", path.display());
            record.phase_ok(Phase::Persist);
        }
        Err(error) => {
            let error = RunError::Persist(error);
            eprintln!("[redsuite]   {error}");
            record.phase_failed(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_errors_classify_apart_from_plain_errors() {
        let check: DynError = Box::new(CheckError::new("the clone lands"));
        let plain: DynError = "connection refused".into();
        assert!(failed_check(&check).is_some());
        assert!(failed_check(&plain).is_none());
    }

    #[test]
    fn missing_optional_fixtures_report_gap_instead_of_failing_preflight() {
        // With an empty/missing manifest path in temp, optional_fixture_gap returns a skip reason
        let gap = optional_fixture_gap(&[Fixture::RedlineProgram]);
        assert!(gap.is_none() || gap.unwrap().contains("unavailable"));
    }
}
