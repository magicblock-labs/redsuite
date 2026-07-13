use redsuite_core::run_scenario;

#[tokio::test]
async fn rpc_warm_ingress() {
    run_scenario(redline::scenarios::aperture::rpc_warm_ingress::WarmIngress)
        .await;
}
