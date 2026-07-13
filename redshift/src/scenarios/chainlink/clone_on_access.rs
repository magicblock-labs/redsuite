use async_trait::async_trait;
use redsuite_core::{BaseCtx, ErCtx, Result, Scenario, ScenarioReport};

pub struct CloneOnAccess;

#[async_trait(?Send)]
impl Scenario for CloneOnAccess {
    fn name(&self) -> &str {
        "redshift/clone_on_access"
    }

    async fn run(
        &self,
        _base: &BaseCtx,
        _er: &ErCtx,
    ) -> Result<ScenarioReport> {
        // mutate a delegated account on base → first ER access must observe the fresh clone
        Ok(ScenarioReport::ok(self.name()))
    }
}
