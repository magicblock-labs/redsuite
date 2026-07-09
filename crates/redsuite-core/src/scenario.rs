use async_trait::async_trait;

use crate::{
    context::{BaseCtx, ErCtx},
    report::ScenarioReport,
    topology, Result,
};

#[async_trait(?Send)]
pub trait Scenario {
    fn name(&self) -> &str;
    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport>;
}

pub async fn run_scenario(scenario: impl Scenario) -> ScenarioReport {
    // A LocalSet so contexts and transports can spawn_local background work
    // (WS readers) on the test's current-thread runtime.
    let local = tokio::task::LocalSet::new();
    local.run_until(run_inner(scenario)).await
}

async fn run_inner(scenario: impl Scenario) -> ScenarioReport {
    let (base, er) = topology::shared().await.unwrap_or_else(|e| {
        panic!("failed to bring up the shared base+ER stack: {e}")
    });
    let report = scenario
        .run(&base, &er)
        .await
        .unwrap_or_else(|e| panic!("scenario {} failed: {e}", scenario.name()));
    eprintln!("[redsuite] {}: passed={}", report.scenario, report.passed);
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
    match crate::report::persist(&report) {
        Ok(path) => eprintln!("[redsuite]   report: {}", path.display()),
        Err(e) => {
            eprintln!("[redsuite]   warning: report not persisted: {e}")
        }
    }
    report
}
