use async_trait::async_trait;
use redsuite_core::{BaseCtx, ErCtx, Result, Scenario, ScenarioReport};

pub struct CommitRoundtrip;

#[async_trait(?Send)]
impl Scenario for CommitRoundtrip {
    fn name(&self) -> &str {
        "redshift/commit_roundtrip"
    }

    async fn run(
        &self,
        _base: &BaseCtx,
        _er: &ErCtx,
    ) -> Result<ScenarioReport> {
        // init + delegate on base → mutate on ER → commit + undelegate → poll base for the final state
        Ok(ScenarioReport::ok(self.name()))
    }
}
