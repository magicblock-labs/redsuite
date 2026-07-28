use redsuite_core::run_scenario;

#[tokio::test]
async fn intent_flows() {
    run_scenario(redshift::scenarios::committor::intent_flows::IntentFlows)
        .await;
}
