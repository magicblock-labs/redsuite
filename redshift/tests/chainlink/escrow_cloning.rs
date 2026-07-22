use redsuite_core::run_scenario;

#[tokio::test]
async fn escrow_cloning() {
    run_scenario(redshift::scenarios::chainlink::escrow_cloning::EscrowCloning)
        .await;
}
