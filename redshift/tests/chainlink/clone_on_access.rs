use redsuite_core::run_scenario;

#[tokio::test]
async fn clone_on_access() {
    run_scenario(
        redshift::scenarios::chainlink::clone_on_access::CloneOnAccess,
    )
    .await;
}
