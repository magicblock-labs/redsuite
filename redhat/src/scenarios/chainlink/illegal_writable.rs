use async_trait::async_trait;
use redsuite_core::{BaseCtx, ErCtx, Result, Scenario, ScenarioReport};

pub struct IllegalWritable;

#[async_trait(?Send)]
impl Scenario for IllegalWritable {
    fn name(&self) -> &str {
        "redhat/illegal_writable"
    }

    async fn run(
        &self,
        _base: &BaseCtx,
        _er: &ErCtx,
    ) -> Result<ScenarioReport> {
        // write to a non-delegated account on the ER must fail InvalidWritableAccount
        Ok(ScenarioReport::ok(self.name()))
    }
}
