use redsuite_core::run_scenario;

#[tokio::test]
async fn high_cu() {
    run_scenario(redline::scenarios::aperture::high_cu::HighCu).await;
}
