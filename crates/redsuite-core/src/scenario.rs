use std::{future::Future, panic::AssertUnwindSafe, rc::Rc, result, time::Instant};

use anyhow::ensure;

use futures_util::FutureExt;
use tokio::task::LocalSet;
use tracing::{error, info, instrument, Span};

use crate::{
    catalog::{Er, ErGroup, Fixture, ScenarioEntry},
    config::ExecutionConfig,
    context::{BaseCtx, ErCtx},
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

/// Setup produces the state consumed by a scenario on a harness-managed ER.
/// Options are `()` unless setup needs typed configuration.
pub trait Scenario<V = (), Options = ()>: Sized + 'static
where
    V: Verdict + 'static,
    Options: 'static,
{
    fn setup(base: Rc<BaseCtx>, er: ErCtx, options: Options) -> impl Future<Output = Result<Self>>;

    fn run(self) -> impl Future<Output = Result<V>>;

    fn shared(
        name: &'static str,
        group: &'static ErGroup,
        fixtures: &'static [Fixture],
        options: Options,
    ) -> ScenarioEntry {
        ScenarioEntry::shared::<Self, V, Options>(name, Er::Shared(group), fixtures, options)
    }

    fn exclusive(
        name: &'static str,
        group: &'static ErGroup,
        fixtures: &'static [Fixture],
        options: Options,
    ) -> ScenarioEntry {
        ScenarioEntry::shared::<Self, V, Options>(name, Er::Exclusive(group), fixtures, options)
    }
}

/// A scenario that owns its ER topology.
pub trait PrivateErScenario<V = (), Options = ()>: Sized + 'static
where
    V: Verdict + 'static,
    Options: 'static,
{
    fn setup(base: Rc<BaseCtx>, options: Options) -> impl Future<Output = Result<Self>>;

    fn run(self) -> impl Future<Output = Result<V>>;

    fn fresh(name: &'static str, fixtures: &'static [Fixture], options: Options) -> ScenarioEntry {
        ScenarioEntry::private::<Self, V, Options>(name, fixtures, options)
    }
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
    pub fn not_run(name: String, reason: &str) -> Self {
        Self::failed(name, "not run", reason.to_owned())
    }

    pub fn failed(name: String, phase: &'static str, reason: String) -> Self {
        let mut record = Self::new(name);
        record.phase = phase;
        error!(scenario = %record.name, phase, %reason, "scenario did not complete");
        record.outcome = Err(crate::DynError::msg(reason));
        record
    }

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

    async fn run<V, State, Body>(
        &mut self,
        fixtures: &[Fixture],
        config: ExecutionConfig,
        state: result::Result<State, String>,
        body: Body,
    ) -> Result<()>
    where
        V: Verdict,
        Body: AsyncFnOnce(Rc<BaseCtx>, State, &mut &'static str) -> Result<V>,
    {
        let total = Instant::now();
        let attached = config.environment.as_ref().is_some_and(|env| env.er.is_some());
        if V::REPORTS {
            self.report = Some(
                ScenarioReport::ok()
                    .setting("target", if attached { "attached" } else { "managed" }),
            );
        }
        if config.environment.is_none() {
            preflight(fixtures)?;
        }
        self.phase = "topology";
        let mut base = topology::base_only(config).await?;
        base.scenario = Some(self.name.clone());
        let base = Rc::new(base);
        let state = state.map_err(crate::DynError::msg)?;
        let resources = base.resources();
        self.phase = "setup";
        let started = Instant::now();
        // A scenario panic must not skip cleanup or prevent sibling runs.
        let result = AssertUnwindSafe(body(base, state, &mut self.phase)).catch_unwind().await;
        let wall = started.elapsed().as_secs_f64();
        resources.reclaim(&self.name);
        self.launches = resources.launches();
        self.wall_seconds = Some(wall);
        let (report, outcome) = match result {
            Ok(Ok(verdict)) => verdict.into_result(),
            Ok(Err(error)) => (None, Err(error)),
            Err(payload) => {
                self.phase = "panic";
                (None, Err(crate::DynError::msg(panic_message(payload))))
            }
        };
        self.report = report.or_else(|| self.report.take()).map(|report| {
            report
                .setting("target", if attached { "attached" } else { "managed" })
                .metric("wall seconds", Unit::Seconds, wall)
                .metric(
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
                    error!(error = %format_args!("{error:#}"), "report not persisted");
                }
            }
        }
        if self.passed() {
            info!("passed");
            return;
        }
        Span::current().record("phase", self.phase);
        match &self.outcome {
            Err(error) => error!(error = %format_args!("{error:#}"), "failed"),
            Ok(()) => error!("verdict failed"),
        }
        for launch in &self.launches {
            info!(node = %launch.label, log = %launch.log,
                version = %launch.bin_version,
                binary_hash = %crate::report::binary_hash(std::path::Path::new(&launch.bin)).unwrap_or_else(|_| "unavailable".into()),
                "node log");
        }
    }
}

pub async fn run_shared_scenario<V, Body>(
    name: String,
    fixtures: &[Fixture],
    config: ExecutionConfig,
    er: result::Result<ErCtx, String>,
    body: Body,
) -> RunRecord
where
    V: Verdict,
    Body: AsyncFnOnce(Rc<BaseCtx>, ErCtx, &mut &'static str) -> Result<V>,
{
    execute(name, fixtures, config, er, async move |base, er, stage| {
        let er = er.for_scenario()?;
        let outcome = AssertUnwindSafe(body(base, er.clone(), stage)).catch_unwind().await;
        let reset = er.reset_faults().await;
        match outcome {
            Ok(outcome) => outcome.and_then(|verdict| reset.map(|()| verdict)),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    })
    .await
}

pub async fn run_private_er_scenario<V, Body>(
    name: String,
    fixtures: &[Fixture],
    config: ExecutionConfig,
    body: Body,
) -> RunRecord
where
    V: Verdict,
    Body: AsyncFnOnce(Rc<BaseCtx>, &mut &'static str) -> Result<V>,
{
    execute(
        name,
        fixtures,
        config,
        Ok(()),
        async move |base, (), stage| body(base, stage).await,
    )
    .await
}

// Error-level spans retain context without emitting standalone events.
#[instrument(name = "run", skip_all, level = "error", fields(scenario = %name, phase))]
async fn execute<V, State, Body>(
    name: String,
    fixtures: &[Fixture],
    config: ExecutionConfig,
    state: result::Result<State, String>,
    body: Body,
) -> RunRecord
where
    V: Verdict,
    Body: AsyncFnOnce(Rc<BaseCtx>, State, &mut &'static str) -> Result<V>,
{
    let persist = V::REPORTS && !config.continuous;
    let run = async {
        let mut record = RunRecord::new(name);
        info!("started");
        record.outcome = record.run(fixtures, config, state, body).await;
        record.conclude(persist);
        record
    };
    // Dropping the LocalSet cancels scenario-owned readers and background tasks.
    LocalSet::new().run_until(run).await
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
        ensure!(
            fixture.program_id().is_none() || loaded.iter().any(|name| name == fixture.so_name()),
            "{} is staged, but the running base booted without it — \
                 run `cargo xtask stack down`, then run again",
            fixture.so_name()
        );
    }
    Ok(())
}
