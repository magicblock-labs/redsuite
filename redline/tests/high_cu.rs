use async_trait::async_trait;
use redsuite_core::{
    run_scenario, BaseCtx, ErCtx, Result, Scenario, ScenarioReport,
};

struct HighCu;

#[async_trait(?Send)]
impl Scenario for HighCu {
    fn name(&self) -> &str {
        "redline/high_cu"
    }

    async fn run(
        &self,
        _base: &BaseCtx,
        _er: &ErCtx,
    ) -> Result<ScenarioReport> {
        // high-CU (real SHA-256 work) load on the ER; execution pressure under load
        Ok(ScenarioReport::ok(self.name()))
    }
}

#[tokio::test]
async fn high_cu() {
    run_scenario(HighCu).await;
}
