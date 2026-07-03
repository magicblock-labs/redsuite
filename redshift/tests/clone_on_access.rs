use async_trait::async_trait;
use redsuite_core::{run_scenario, BaseCtx, ErCtx, Result, Scenario, ScenarioReport};

struct CloneOnAccess;

#[async_trait(?Send)]
impl Scenario for CloneOnAccess {
    fn name(&self) -> &str {
        "redshift/clone_on_access"
    }

    async fn run(&self, _base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        // mutate a delegated account on base → first ER access must observe the fresh clone
        Ok(ScenarioReport::ok(self.name()))
    }
}

#[tokio::test]
#[ignore = "topology harness not implemented yet"]
async fn clone_on_access() {
    run_scenario(CloneOnAccess).await;
}
