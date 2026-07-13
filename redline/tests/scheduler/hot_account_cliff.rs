use redsuite_core::run_scenario;

#[tokio::test]
async fn hot_account_cliff() {
    run_scenario(
        redline::scenarios::scheduler::hot_account_cliff::HotAccountCliff,
    )
    .await;
}
