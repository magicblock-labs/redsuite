use redsuite_core::run_scenario;

#[tokio::test]
async fn ensure_gate_stall() {
    run_scenario(
        redline::scenarios::chainlink::ensure_gate_stall::EnsureGateStall,
    )
    .await;
}
