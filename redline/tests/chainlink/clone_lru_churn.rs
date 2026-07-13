use redsuite_core::run_scenario;

#[tokio::test]
async fn clone_lru_churn() {
    run_scenario(redline::scenarios::chainlink::clone_lru_churn::CloneLruChurn)
        .await;
}
