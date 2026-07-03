use async_trait::async_trait;

use crate::{
    context::{BaseCtx, ErCtx},
    report::ScenarioReport,
    Result,
};

#[async_trait(?Send)]
pub trait Scenario {
    fn name(&self) -> &str;
    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport>;
}

/// The only glue a test invokes: bring up the topology (phase 1), run the
/// scenario against it (phase 2), tear down, return the report.
pub async fn run_scenario(scenario: impl Scenario) -> ScenarioReport {
    todo!(
        "bring up base + ER topology, run `{}`, tear down",
        scenario.name()
    )
}
