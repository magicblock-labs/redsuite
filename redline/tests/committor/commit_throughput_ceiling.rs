use redsuite_core::run_scenario;

#[tokio::test]
async fn commit_throughput_ceiling() {
    run_scenario(redline::scenarios::committor::commit_throughput_ceiling::CommitThroughputCeiling).await;
}
