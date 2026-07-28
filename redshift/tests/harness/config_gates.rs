use redsuite_core::run_scenario;

#[tokio::test]
async fn config_gates() {
    run_scenario(redshift::scenarios::harness::config_gates::ConfigGates).await;
}
