//! Timed mixed load on a private ER, retaining measurements when validation fails.
//! Interruption is recorded separately from the verdict: a partial run may pass
//! its checks without qualifying as a completed benchmark.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    rc::Rc,
    time::{Duration, Instant},
};

use redsuite_core::{
    topology::{self, ErOptions, PrivateEr},
    BaseCtx, PrivateErScenario, Result, ScenarioReport,
};
use tokio::sync::watch;

use crate::scenarios::scheduler::mixed_sustained_load::Workload;

pub struct Options {
    pub duration: Duration,
    pub rate: Option<NonZeroU32>,
    pub concurrency: Option<NonZeroUsize>,
    /// `true` stops admission; already-admitted work is drained and verified.
    pub stop: watch::Receiver<bool>,
}

pub struct Load {
    options: Options,
    private: PrivateEr,
    workload: Workload,
    started: Instant,
}

impl PrivateErScenario<(ScenarioReport, Result<()>), Options> for Load {
    async fn setup(base: Rc<BaseCtx>, options: Options) -> Result<Self> {
        let started = Instant::now();
        let private = topology::private_er(
            &base,
            ErOptions {
                label: "load".to_owned(),
                ..Default::default()
            },
        )
        .await?;
        private.wait_ready(topology::ER_READY_TIMEOUT).await?;
        match Workload::setup(&base, private.ctx()).await {
            Ok(workload) => Ok(Self {
                options,
                private,
                workload,
                started,
            }),
            Err(error) => {
                let _ = private.finish().await;
                Err(error)
            }
        }
    }

    async fn run(self) -> Result<(ScenarioReport, Result<()>)> {
        let Self {
            options,
            private,
            workload,
            started,
        } = self;
        let result = workload.run(private.ctx(), Some(&options), started).await;
        let cleanup = private.finish().await;
        let (report, outcome) = result?;
        let outcome = outcome.and(cleanup);
        let report = report.setting(
            "completion",
            if *options.stop.borrow() {
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
