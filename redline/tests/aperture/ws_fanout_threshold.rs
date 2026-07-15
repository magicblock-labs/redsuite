use redsuite_core::run_scenario;

#[tokio::test]
async fn ws_fanout_threshold() {
    run_scenario(
        redline::scenarios::aperture::ws_fanout_threshold::WsFanoutThreshold,
    )
    .await;
}
