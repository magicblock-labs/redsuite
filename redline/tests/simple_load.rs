use async_trait::async_trait;
use redsuite_core::{run_scenario, BaseCtx, ErCtx, Result, Scenario, ScenarioReport};

struct SimpleLoad;

#[async_trait(?Send)]
impl Scenario for SimpleLoad {
    fn name(&self) -> &str {
        "redline/simple_load"
    }

    async fn run(&self, _base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        // sustained SimpleByteSet write load on the ER; report throughput + latency percentiles
        Ok(ScenarioReport::ok(self.name()))
    }
}

#[tokio::test]
#[ignore = "topology harness not implemented yet"]
async fn simple_load() {
    run_scenario(SimpleLoad).await;
}
