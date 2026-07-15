use redsuite_core::run_scenario;

#[tokio::test]
async fn executor_saturation() {
    run_scenario(
        redline::scenarios::scheduler::executor_saturation::ExecutorSaturation,
    )
    .await;
}
