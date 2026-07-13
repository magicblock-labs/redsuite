use redsuite_core::run_scenario;

#[tokio::test]
async fn simple_load() {
    run_scenario(redline::scenarios::aperture::simple_load::SimpleLoad).await;
}
