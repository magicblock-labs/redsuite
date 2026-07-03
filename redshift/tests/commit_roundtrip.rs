use async_trait::async_trait;
use redsuite_core::{run_scenario, BaseCtx, ErCtx, Result, Scenario, ScenarioReport};

struct CommitRoundtrip;

#[async_trait(?Send)]
impl Scenario for CommitRoundtrip {
    fn name(&self) -> &str {
        "redshift/commit_roundtrip"
    }

    async fn run(&self, _base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        // init + delegate on base → mutate on ER → commit + undelegate → poll base for the final state
        Ok(ScenarioReport::ok(self.name()))
    }
}

#[tokio::test]
#[ignore = "topology harness not implemented yet"]
async fn commit_roundtrip() {
    run_scenario(CommitRoundtrip).await;
}
