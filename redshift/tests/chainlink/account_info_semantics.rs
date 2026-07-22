use redsuite_core::run_scenario;

#[tokio::test]
async fn account_info_semantics() {
    run_scenario(
        redshift::scenarios::chainlink::account_info_semantics::AccountInfoSemantics,
    )
    .await;
}
