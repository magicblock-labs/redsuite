use redsuite_core::run_scenario;

#[tokio::test]
async fn restart_under_load() {
    run_scenario(
        redline::scenarios::lifecycle::restart_under_load::RestartUnderLoad,
    )
    .await;
}
