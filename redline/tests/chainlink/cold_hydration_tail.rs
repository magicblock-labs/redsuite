use redsuite_core::run_scenario;

#[tokio::test]
async fn cold_hydration_tail() {
    run_scenario(
        redline::scenarios::chainlink::cold_hydration_tail::ColdHydrationTail,
    )
    .await;
}
