use redsuite_core::run_scenario;

#[tokio::test]
async fn rpc_capacity_blast() {
    run_scenario(
        redline::scenarios::aperture::rpc_capacity_blast::RpcCapacityBlast,
    )
    .await;
}
