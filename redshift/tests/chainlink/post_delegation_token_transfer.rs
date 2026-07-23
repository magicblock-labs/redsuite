use redsuite_core::run_scenario;

#[tokio::test]
async fn post_delegation_token_transfer() {
    run_scenario(
        redshift::scenarios::chainlink::post_delegation_token_transfer::PostDelegationTokenTransfer,
    )
    .await;
}
