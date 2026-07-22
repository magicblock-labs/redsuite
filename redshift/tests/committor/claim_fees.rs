use redsuite_core::run_scenario;

#[tokio::test]
async fn claim_fees() {
    run_scenario(redshift::scenarios::committor::claim_fees::ClaimFees).await;
}
