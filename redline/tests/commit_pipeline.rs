use async_trait::async_trait;
use redsuite_core::{run_scenario, BaseCtx, ErCtx, Result, Scenario, ScenarioReport};

struct CommitPipeline;

#[async_trait(?Send)]
impl Scenario for CommitPipeline {
    fn name(&self) -> &str {
        "redline/commit_pipeline"
    }

    async fn run(&self, _base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        // sustained commit load: ER → base commit pipeline throughput and backlog
        Ok(ScenarioReport::ok(self.name()))
    }
}

#[tokio::test]
#[ignore = "topology harness not implemented yet"]
async fn commit_pipeline() {
    run_scenario(CommitPipeline).await;
}
