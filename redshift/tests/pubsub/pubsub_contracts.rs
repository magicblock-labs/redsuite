use redsuite_core::run_scenario;

#[tokio::test]
async fn pubsub_contracts() {
    run_scenario(
        redshift::scenarios::pubsub::pubsub_contracts::PubsubContracts,
    )
    .await;
}
