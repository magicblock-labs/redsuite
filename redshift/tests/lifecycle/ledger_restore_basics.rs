use redsuite_core::run_scenario;

#[tokio::test]
async fn ledger_restore_basics() {
    run_scenario(
        redshift::scenarios::lifecycle::ledger_restore_basics::LedgerRestoreBasics,
    )
    .await;
}
