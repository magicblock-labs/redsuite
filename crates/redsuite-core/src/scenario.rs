use std::{panic::AssertUnwindSafe, time::Instant};

use async_trait::async_trait;
use futures_util::FutureExt;
use tracing::{error, error_span, field, info, Instrument, Span};

use crate::{
    catalog::Fixture,
    context::{BaseCtx, ErCtx},
    profile::ExecutionConfig,
    report::{ScenarioReport, Unit},
    resources::LaunchRecord,
    runner::panic_message,
    topology, Result,
};

/// A scenario may return measurements even when its validation fails.
pub trait Verdict {
    const REPORTS: bool;
    fn into_result(self) -> (Option<ScenarioReport>, Result<()>);
}

impl Verdict for () {
    const REPORTS: bool = false;
    fn into_result(self) -> (Option<ScenarioReport>, Result<()>) {
        (None, Ok(()))
    }
}

impl Verdict for ScenarioReport {
    const REPORTS: bool = true;
    fn into_result(self) -> (Option<ScenarioReport>, Result<()>) {
        (Some(self), Ok(()))
    }
}

impl Verdict for (ScenarioReport, Result<()>) {
    const REPORTS: bool = true;
    fn into_result(self) -> (Option<ScenarioReport>, Result<()>) {
        (Some(self.0), self.1)
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
pub struct RunRecord {
    pub name: String,
    pub outcome: Result<()>,
    pub phase: &'static str,
    pub report: Option<ScenarioReport>,
    pub wall_seconds: Option<f64>,
    pub launches: Vec<LaunchRecord>,
}

impl RunRecord {
    fn new(name: String) -> Self {
        Self {
            name,
            outcome: Ok(()),
            phase: "preflight",
            report: None,
            wall_seconds: None,
            launches: Vec::new(),
        }
    }

    pub fn passed(&self) -> bool {
        self.outcome.is_ok() && self.report.as_ref().is_none_or(|r| r.passed)
    }

    async fn run<V: Verdict, State>(
        &mut self,
        fixtures: &[Fixture],
        config: ExecutionConfig,
        provision: impl AsyncFnOnce(ExecutionConfig) -> Result<(BaseCtx, State)>,
        body: impl AsyncFnOnce(BaseCtx, State) -> Result<V>,
    ) -> Result<()> {
        let total = Instant::now();
        preflight(fixtures)?;
        self.phase = "topology";
        let (base, state) = provision(config).await?;
        let resources = base.resources();
        self.phase = "scenario";
        let started = Instant::now();
        // A scenario panic must not skip cleanup or prevent sibling runs.
        let result = AssertUnwindSafe(body(base, state)).catch_unwind().await;
        let wall = started.elapsed().as_secs_f64();
        resources.reclaim(&self.name);
        self.launches = resources.launches();
        self.wall_seconds = Some(wall);
        let (report, outcome) = match result {
            Ok(Ok(verdict)) => verdict.into_result(),
            Ok(Err(error)) => (None, Err(error)),
            Err(payload) => {
                self.phase = "panic";
                (None, Err(panic_message(payload).into()))
            }
        };
        self.report = report.map(|report| {
            report.metric("wall seconds", Unit::Seconds, wall).metric(
                "total seconds",
                Unit::Seconds,
                total.elapsed().as_secs_f64(),
            )
        });
        outcome
    }

    fn conclude(&mut self, reports: bool) {
        if reports {
            if let Err(error) = crate::report::persist_run(self) {
                if self.outcome.is_ok() {
                    self.phase = "persist";
                    self.outcome = Err(error);
                } else {
                    error!(%error, "report not persisted");
                }
            }
        }
        if self.passed() {
            info!("passed");
            return;
        }
        Span::current().record("stage", self.phase);
        match &self.outcome {
            Err(error) => error!(%error, "failed"),
            Ok(()) => error!("verdict failed"),
        }
        for launch in &self.launches {
            info!(%launch.label, pid = launch.pid, %launch.log, "node log");
        }
    }
}

pub async fn run_shared_scenario<V: Verdict>(
    scenario: impl Scenario<V>,
    fixtures: &[Fixture],
    config: ExecutionConfig,
    er: std::result::Result<ErCtx, String>,
) -> RunRecord {
    execute(
        scenario.name().to_owned(),
        fixtures,
        config,
        async move |config| Ok((topology::base_only(config).await?, er?)),
        async move |base, er| {
            let outcome = scenario.run(&base, &er).await;
            if let Some(proxies) = er.proxies() {
                let reset = proxies.reset().await;
                return outcome.and_then(|verdict| reset.map(|()| verdict));
            }
            outcome
        },
    )
    .await
}

pub async fn run_private_er_scenario<V: Verdict>(
    scenario: impl PrivateErScenario<V>,
    fixtures: &[Fixture],
    config: ExecutionConfig,
) -> RunRecord {
    execute(
        scenario.name().to_owned(),
        fixtures,
        config,
        async |config| Ok((topology::base_only(config).await?, ())),
        async move |base, ()| scenario.run(&base).await,
    )
    .await
}

async fn execute<V: Verdict, State>(
    name: String,
    fixtures: &[Fixture],
    config: ExecutionConfig,
    provision: impl AsyncFnOnce(ExecutionConfig) -> Result<(BaseCtx, State)>,
    body: impl AsyncFnOnce(BaseCtx, State) -> Result<V>,
) -> RunRecord {
    // Error-level spans retain context without emitting standalone events.
    let span = error_span!("run", scenario = %name, stage = field::Empty);
    let run = async {
        let mut record = RunRecord::new(name);
        info!("started");
        record.outcome = record.run(fixtures, config, provision, body).await;
        record.conclude(V::REPORTS);
        record
    };
    // Dropping the LocalSet cancels scenario-owned readers and background tasks.
    tokio::task::LocalSet::new().run_until(run.instrument(span)).await
}

fn preflight(fixtures: &[Fixture]) -> Result<()> {
    topology::er_bin_path()?;
    for fixture in fixtures {
        fixture.resolve()?;
    }
    let Some(loaded) = topology::running_base_programs() else {
        return Ok(());
    };
    for fixture in fixtures {
        if fixture.loaded_at_base_boot() && !loaded.iter().any(|name| name == fixture.so_name()) {
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
