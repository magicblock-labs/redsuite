use redsuite_core::run_scenario;

#[tokio::test]
async fn task_scheduler() {
    run_scenario(redshift::scenarios::scheduler::task_scheduler::TaskScheduler)
        .await;
}
