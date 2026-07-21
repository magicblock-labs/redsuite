use redsuite_core::run_scenario;

#[tokio::test]
async fn api_invariants() {
    run_scenario(redshift::scenarios::harness::api_invariants::ApiInvariants)
        .await;
}
