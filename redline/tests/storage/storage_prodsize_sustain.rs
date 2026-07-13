use redsuite_core::run_scenario;

#[tokio::test]
async fn storage_prodsize_sustain() {
    run_scenario(redline::scenarios::storage::storage_prodsize_sustain::StorageProdsizeSustain).await;
}
