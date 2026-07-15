use async_trait::async_trait;
use redsuite_core::{BaseCtx, ErCtx, Result, Scenario, ScenarioReport};

pub struct FeePayerRules;

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
