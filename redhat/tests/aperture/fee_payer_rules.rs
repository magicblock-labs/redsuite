use redsuite_core::run_scenario;

#[tokio::test]
async fn fee_payer_rules() {
    run_scenario(redhat::scenarios::aperture::fee_payer_rules::FeePayerRules)
        .await;
}
