use redsuite_core::run_scenario;

#[tokio::test]
async fn aml_gate() {
    run_scenario(redshift::scenarios::chainlink::aml_gate::AmlGate).await;
}
