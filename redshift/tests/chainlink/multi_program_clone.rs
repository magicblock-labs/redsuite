use redsuite_core::run_scenario;

#[tokio::test]
async fn multi_program_clone() {
    run_scenario(
        redshift::scenarios::chainlink::multi_program_clone::MultiProgramClone,
    )
    .await;
}
