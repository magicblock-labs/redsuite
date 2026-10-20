//! Timed mixed load on a private ER, retaining measurements when validation fails.
//! Interruption is recorded separately from the verdict: a partial run may pass
//! its checks without qualifying as a completed benchmark.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use redsuite_core::{
    topology, BaseCtx, PrivateErScenario, Result, ScenarioReport,
};
use tokio::sync::watch;

use crate::scenarios::scheduler::mixed_sustained_load;

pub struct Load {
    pub duration: Duration,
    pub rate: Option<NonZeroU32>,
    pub concurrency: Option<NonZeroUsize>,
    /// `true` stops admission; already-admitted work is drained and verified.
    pub stop: watch::Receiver<bool>,
}

#[async_trait(?Send)]
impl PrivateErScenario<(ScenarioReport, Result<()>)> for Load {
    fn name(&self) -> &str {
        "redline/load"
    }

    async fn run(
        &self,
        base: &BaseCtx,
    ) -> Result<(ScenarioReport, Result<()>)> {
        let started = Instant::now();
        let (report, outcome) = async {
            let private = topology::private_er(
                base,
                topology::ErOptions {
                    label: "load".to_owned(),
                    ..Default::default()
                },
            )
            .await?;
            private.wait_ready(topology::ER_READY_TIMEOUT).await?;
            let outcome = mixed_sustained_load::run(
                base,
                private.ctx(),
                Some(self),
                started,
            )
            .await;
            let cleanup = private.finish().await;
            outcome.map(|(report, outcome)| (report, outcome.and(cleanup)))
        }
        .await
        .unwrap_or_else(|error| {
            (
                ScenarioReport::ok(self.name())
                    .setting("profile", base.config().profile.name()),
                Err(error),
            )
        });
        let report = report.setting(
            "completion",
            if *self.stop.borrow() {
                "interrupted"
            } else if outcome.is_ok() {
                "completed"
            } else {
                "failed"
            },
        );
        Ok((report, outcome))
    }
}
