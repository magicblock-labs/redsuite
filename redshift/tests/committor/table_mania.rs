use redsuite_core::run_scenario;

#[tokio::test]
async fn table_mania() {
    run_scenario(
        redshift::scenarios::committor::table_mania::TableManiaScenario,
    )
    .await;
}
