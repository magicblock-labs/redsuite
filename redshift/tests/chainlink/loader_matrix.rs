use redsuite_core::run_scenario;

#[tokio::test]
async fn loader_matrix() {
    run_scenario(redshift::scenarios::chainlink::loader_matrix::LoaderMatrix)
        .await;
}
