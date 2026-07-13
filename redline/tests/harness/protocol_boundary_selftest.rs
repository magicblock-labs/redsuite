use redsuite_core::run_scenario;

#[tokio::test]
async fn protocol_boundary_selftest() {
    run_scenario(redline::scenarios::harness::protocol_boundary_selftest::ProtocolBoundarySelftest).await;
}
