use redsuite_core::run_scenario;

#[tokio::test]
async fn commit_and_undelegate() {
    run_scenario(
        redshift::scenarios::committor::commit_and_undelegate::CommitAndUndelegate,
    )
    .await;
}
