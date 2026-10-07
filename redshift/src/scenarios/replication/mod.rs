pub mod replication_recovery;
pub mod verifier_lifecycle;

use std::time::{Duration, Instant};

use redsuite_core::{
    check,
    topology::{ReplicatedTopology, Verifier},
    Result,
};

pub(super) async fn leader_metric(
    topology: &ReplicatedTopology,
    name: &str,
) -> Result<f64> {
    topology
        .leader()
        .ctx()
        .scrape_metrics()
        .await?
        .get(name)
        .ok_or_else(|| format!("the leader exposes no {name} metric").into())
}

pub(super) async fn verifier_metric(
    verifier: &Verifier,
    name: &str,
) -> Result<f64> {
    verifier.scrape_metrics().await?.get(name).ok_or_else(|| {
        format!("verifier `{}` exposes no {name} metric", verifier.label())
            .into()
    })
}

pub(super) async fn await_catch_up(
    verifier: &Verifier,
    name: &str,
    target: f64,
    moment: &str,
    timeout: Duration,
) -> Result<Duration> {
    let started = Instant::now();
    check::poll(
        &format!(
            "{moment}: verifier `{}` {name} reaching the leader's {target:.0}",
            verifier.label()
        ),
        timeout,
        || async {
            verifier_metric(verifier, name)
                .await
                .is_ok_and(|value| value >= target)
        },
    )
    .await?;
    Ok(started.elapsed())
}
