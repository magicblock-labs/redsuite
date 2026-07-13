use redsuite_core::run_scenario;

#[tokio::test]
async fn commit_width_envelope() {
    run_scenario(redline::scenarios::committor::commit_width_envelope::CommitWidthEnvelope).await;
}
