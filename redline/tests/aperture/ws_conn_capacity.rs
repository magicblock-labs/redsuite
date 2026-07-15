use redsuite_core::run_scenario;

#[tokio::test]
async fn ws_conn_capacity() {
    run_scenario(
        redline::scenarios::aperture::ws_conn_capacity::WsConnCapacity,
    )
    .await;
}
