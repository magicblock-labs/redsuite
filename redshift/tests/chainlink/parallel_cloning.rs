use redsuite_core::run_scenario;

#[tokio::test]
async fn parallel_cloning() {
    run_scenario(
        redshift::scenarios::chainlink::parallel_cloning::ParallelCloning,
    )
    .await;
}
