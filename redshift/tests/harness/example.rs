use redsuite_core::run_scenario;

#[tokio::test]
async fn example() {
    run_scenario(redshift::scenarios::harness::example::Example).await;
}
