use redsuite_core::run_scenario;

#[tokio::test]
async fn illegal_writable() {
    run_scenario(
        redhat::scenarios::chainlink::illegal_writable::IllegalWritable,
    )
    .await;
}
