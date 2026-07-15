use redsuite_core::run_scenario;

#[tokio::test]
async fn commit_roundtrip() {
    run_scenario(
        redshift::scenarios::committor::commit_roundtrip::CommitRoundtrip,
    )
    .await;
}
