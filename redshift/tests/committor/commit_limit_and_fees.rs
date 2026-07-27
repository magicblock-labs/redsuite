use redsuite_core::run_scenario;

#[tokio::test]
async fn commit_limit_and_fees() {
    run_scenario(
        redshift::scenarios::committor::commit_limit_and_fees::CommitLimitAndFees,
    )
    .await;
}
