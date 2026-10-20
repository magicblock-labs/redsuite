use std::{panic::AssertUnwindSafe, time::Instant};

use async_trait::async_trait;
use futures_util::FutureExt;
use tracing::{debug, error, error_span, info, warn, Instrument};

use crate::{
    catalog::Fixture,
    check::CheckError,
    context::{BaseCtx, ErCtx},
    manifest,
    profile::ExecutionConfig,
    report::{MeasureValue, Measurement, ScenarioReport, Unit},
    resources::LaunchRecord,
    runner::panic_message,
    topology, DynError, Result,
};

/// Converts a completed scenario body into its verdict and optional measurements.
pub trait Verdict {
    const REPORTS: bool;
    fn into_report(self) -> Option<ScenarioReport>;
    /// Override when a returned value can carry both measurements and a failure.
    fn into_outcome(self) -> ScenarioOutcome
    where
        Self: Sized,
    {
        ScenarioOutcome::Passed(self.into_report())
    }
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

impl Verdict for (ScenarioReport, Result<()>) {
    const REPORTS: bool = true;

    fn into_report(self) -> Option<ScenarioReport> {
        Some(self.0)
    }

    fn into_outcome(self) -> ScenarioOutcome {
        let (mut report, outcome) = self;
        match outcome {
            Ok(()) => ScenarioOutcome::Passed(Some(report)),
            Err(error) => {
                report.passed = false;
                ScenarioOutcome::FailedReport(error, report)
            }
        }
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
    /// Measurements retained alongside a scenario error.
    FailedReport(DynError, ScenarioReport),
    Panicked(String),
    NotReached,
}

/// Distinguishes failed assertions from infrastructure errors in reports.
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
            ScenarioOutcome::Failed(error)
            | ScenarioOutcome::FailedReport(error, _) => {
                match failed_check(error) {
                    Some(check) => lines.push(format!("check failed: {check}")),
                    None => lines.push(format!("scenario failed: {error}")),
                }
            }
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
    er: std::result::Result<ErCtx, String>,
) -> RunRecord {
    // Keep spawn_local work (such as WS readers) scoped to this scenario.
    let name = scenario.name().to_owned();
    tokio::task::LocalSet::new()
        .run_until(execute(
            name,
            fixtures,
            optional_fixtures,
            config,
            async move |config| {
                let er = er?;
                Ok((topology::base_only(config).await?, er))
            },
            async move |base, er| {
                let outcome = scenario.run(&base, &er).await;
                let faults = match er.proxies() {
                    Some(proxies) => proxies.reset().await,
                    None => Ok(()),
                };
                outcome.and_then(|verdict| faults.map(|()| verdict))
            },
        ))
        .await
}

pub async fn run_private_er_scenario<V: Verdict>(
    scenario: impl PrivateErScenario<V>,
    fixtures: &[Fixture],
    optional_fixtures: &[Fixture],
    config: ExecutionConfig,
) -> RunRecord {
    let name = scenario.name().to_owned();
    tokio::task::LocalSet::new()
        .run_until(execute(
            name,
            fixtures,
            optional_fixtures,
            config,
            async |config| Ok((topology::base_only(config).await?, ())),
            async move |base, ()| scenario.run(&base).await,
        ))
        .await
}

async fn execute<V: Verdict, State>(
    name: String,
    fixtures: &[Fixture],
    optional_fixtures: &[Fixture],
    config: ExecutionConfig,
    provision: impl AsyncFnOnce(ExecutionConfig) -> Result<(BaseCtx, State)>,
    body: impl AsyncFnOnce(BaseCtx, State) -> Result<V>,
) -> RunRecord {
    // Retain failure context even with RUST_LOG=error; spans emit no events.
    let span = error_span!("scenario", %name);
    let total_started = Instant::now();
    async move {
        info!("scenario started");
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

        let (base, state) = match provision(config).await {
            Ok(provisioned) => provisioned,
            Err(error) => {
                record.errors.push(RunError::Topology(error));
                conclude(&mut record, V::REPORTS);
                return record;
            }
        };

        let resources = base.resources();
        let started = Instant::now();
        // catch_unwind so an internal-invariant panic still reaches the teardown
        // audit and the persisted report instead of aborting sibling scenarios
        let outcome = AssertUnwindSafe(body(base, state)).catch_unwind().await;
        let wall_seconds = started.elapsed().as_secs_f64();
        let teardown_errors = resources.reclaim_and_audit(&record.name);
        record.launches = resources.launches();
        record.wall_seconds = Some(wall_seconds);
        record.scenario = match outcome {
            Ok(Ok(verdict)) => verdict.into_outcome(),
            Ok(Err(error)) => ScenarioOutcome::Failed(error),
            Err(payload) => ScenarioOutcome::Panicked(panic_message(payload)),
        };

        if let ScenarioOutcome::Passed(Some(report))
        | ScenarioOutcome::FailedReport(_, report) = &mut record.scenario
        {
            report.measurements.push(Measurement {
                label: "wall seconds".to_owned(),
                unit: Unit::Seconds,
                value: MeasureValue::Scalar(wall_seconds),
            });
            report.measurements.push(Measurement {
                label: "total seconds".to_owned(),
                unit: Unit::Seconds,
                value: MeasureValue::Scalar(
                    total_started.elapsed().as_secs_f64(),
                ),
            });
        }

        record
            .errors
            .extend(teardown_errors.into_iter().map(RunError::Teardown));

        conclude(&mut record, V::REPORTS);
        record
    }
    .instrument(span)
    .await
}

fn preflight(fixtures: &[Fixture]) -> Result<()> {
    topology::er_bin_path()?;
    if !fixtures.is_empty() {
        let manifest = manifest::load()?;
        if let Some(warning) = manifest::revision_drift(&manifest) {
            warn!(%warning, "fixture revision drift");
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
    if let ScenarioOutcome::Skipped(reason) = &record.scenario {
        info!(reason, "scenario skipped");
        return;
    }
    if reports {
        match crate::report::persist_run(record) {
            Ok(path) => debug!(?path, "report persisted"),
            Err(error) => record.errors.push(RunError::Persist(error)),
        }
    }
    if record.passed() {
        info!(record.wall_seconds, "scenario passed");
        return;
    }
    let failure = record.failure();
    error!(
        failure = failure.as_deref().unwrap_or("scenario verdict failed"),
        "scenario failed"
    );
    for launch in &record.launches {
        debug!(%launch.label, %launch.log, "node log");
    }
}
