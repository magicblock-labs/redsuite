use redsuite_core::run_scenario;

#[tokio::test]
async fn commits() {
    run_scenario(redshift::scenarios::committor::commits::Commits).await;
}
