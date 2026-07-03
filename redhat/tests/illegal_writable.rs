use async_trait::async_trait;
use redsuite_core::{run_scenario, BaseCtx, ErCtx, Result, Scenario, ScenarioReport};

struct IllegalWritable;

#[async_trait(?Send)]
impl Scenario for IllegalWritable {
    fn name(&self) -> &str {
        "redhat/illegal_writable"
    }

    async fn run(&self, _base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        // write to a non-delegated account on the ER must fail InvalidWritableAccount
        Ok(ScenarioReport::ok(self.name()))
    }
}

#[tokio::test]
#[ignore = "topology harness not implemented yet"]
async fn illegal_writable() {
    run_scenario(IllegalWritable).await;
}
