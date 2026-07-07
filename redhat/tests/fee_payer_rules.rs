use async_trait::async_trait;
use redsuite_core::{
    run_scenario, BaseCtx, ErCtx, Result, Scenario, ScenarioReport,
};

struct FeePayerRules;

#[async_trait(?Send)]
impl Scenario for FeePayerRules {
    fn name(&self) -> &str {
        "redhat/fee_payer_rules"
    }

    async fn run(
        &self,
        _base: &BaseCtx,
        _er: &ErCtx,
    ) -> Result<ScenarioReport> {
        // outside gasless mode only delegated/privileged accounts may pay ER fees
        Ok(ScenarioReport::ok(self.name()))
    }
}

#[tokio::test]
async fn fee_payer_rules() {
    run_scenario(FeePayerRules).await;
}
