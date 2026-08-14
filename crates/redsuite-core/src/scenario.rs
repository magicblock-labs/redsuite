use std::{future::Future, rc::Rc, time::Instant};

use async_trait::async_trait;

use crate::{
    catalog::Fixture,
    context::{BaseCtx, ErCtx},
    report::ScenarioReport,
    resources::Resources,
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
    Failed(DynError),
    NotReached,
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
        matches!(self.scenario, ScenarioOutcome::Passed(_))
            && self.phases.iter().all(|outcome| outcome.error.is_none())
    }

    pub fn failure(&self) -> Option<String> {
        let mut lines = Vec::new();
        if let ScenarioOutcome::Failed(error) = &self.scenario {
            lines.push(format!("scenario failed: {error}"));
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
) -> RunRecord {
    // A LocalSet so contexts and transports can spawn_local background work
    // (WS readers) on the test's current-thread runtime.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let name = scenario.name().to_owned();
            execute(name, fixtures, topology::shared, |(base, er)| async move {
                scenario.run(&base, &er).await
            })
            .await
        })
        .await
}

pub async fn run_private_er_scenario(
    scenario: impl PrivateErScenario,
    fixtures: &[Fixture],
) -> RunRecord {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let name = scenario.name().to_owned();
            execute(name, fixtures, topology::base_only, |base| async move {
                scenario.run(&base).await
            })
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
    provision: impl FnOnce() -> ProvisionFut,
    body: Body,
) -> RunRecord
where
    Provisioned: ProvidesResources,
    ProvisionFut: Future<Output = Result<Provisioned>>,
    Body: FnOnce(Provisioned) -> BodyFut,
    BodyFut: Future<Output = Result<ScenarioReport>>,
{
    let mut record = RunRecord::new(name);

    if let Err(error) = preflight(fixtures) {
        record.phase_failed(RunError::Preflight(error));
        conclude(&mut record);
        return record;
    }
    record.phase_ok(Phase::Preflight);

    let provisioned = match provision().await {
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
    let outcome = body(provisioned).await;
    let wall_seconds = started.elapsed().as_secs_f64();
    let teardown_errors = resources.audit();
    record.wall_seconds = Some(wall_seconds);
    record.scenario = match outcome {
        Ok(report) => {
            ScenarioOutcome::Passed(report.metric("wall seconds", wall_seconds))
        }
        Err(error) => ScenarioOutcome::Failed(error),
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
    for fixture in fixtures {
        let so = topology::workspace_root()
            .join("target/deploy")
            .join(fixture.so_name());
        if !so.exists() {
            return Err(format!(
                "fixture {} is not built — `cargo xtask programs` builds it",
                so.display()
            )
            .into());
        }
    }
    Ok(())
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
            for (label, stats) in &report.observations {
                eprintln!("[redsuite]   {label}: {stats:?}");
            }
            for (label, value) in &report.metrics {
                eprintln!("[redsuite]   {label}: {value}");
            }
        }
        ScenarioOutcome::Failed(error) => {
            eprintln!("[redsuite] {}: scenario failed: {error}", record.name)
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
