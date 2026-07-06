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
    let (base, er) = topology::shared().await.unwrap_or_else(|e| {
        panic!("failed to bring up the shared base+ER stack: {e}")
    });
    let report = scenario
        .run(&base, &er)
        .await
        .unwrap_or_else(|e| panic!("scenario {} failed: {e}", scenario.name()));
    eprintln!("[redsuite] {}: passed={}", report.scenario, report.passed);
    report
}
