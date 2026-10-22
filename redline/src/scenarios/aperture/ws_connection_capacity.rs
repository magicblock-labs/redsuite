use std::{
    future,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::ensure;
use futures_util::future::join_all;
use pubkey::Pubkey;
use redsuite_core::{
    host,
    profile::ProfileValues,
    report::Unit,
    runner::{execute, Pacing, RunConfig, RunOutcome},
    topology,
    topology::{ErOptions, PrivateEr},
    transport::wsraw::RawWs,
    BaseCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use tokio::time;
use tracing::{debug, instrument};

const OPEN_CONCURRENCY: usize = 64;
const FD_TOLERANCE: usize = 64;
const FD_BASELINE_SLACK: usize = 8;
const FD_SETTLE_TIMEOUT: Duration = Duration::from_secs(90);

struct Profile {
    name: &'static str,
    ladder: [usize; 3],
    churn_ops: u64,
    churn_concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    ladder: [100, 200, 500],
    churn_ops: 500,
    churn_concurrency: 50,
};

const FULL: Profile = Profile {
    name: "full",
    ladder: [1_000, 2_000, 5_000],
    churn_ops: 5_000,
    churn_concurrency: 200,
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

struct RungOutcome {
    connections: usize,
    fds_idle: usize,
    fds_subscribed: usize,
    rss_subscribed_kb: u64,
    fds_after_close: usize,
    settle_s: f64,
    unsubscribe_failures: usize,
}

pub struct WsConnectionCapacity {
    profile: &'static Profile,
    shared_account: Pubkey,
    private: PrivateEr,
}

impl Scenario<ScenarioReport> for WsConnectionCapacity {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let shared_account: Pubkey = er.identity();
        let private = topology::private_er(
            &base,
            ErOptions {
                label: "s3".to_owned(),
                ..Default::default()
            },
        )
        .await?;

        Ok(Self { profile, shared_account, private })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let pid = self.private.pid();
        let fd_baseline = host::fd_count(pid)?;
        let rss_baseline_kb = host::rss_kb(pid)?;
        let mut rungs = Vec::new();
        for connections in self.profile.ladder {
            let rung = self.measure_rung(connections, fd_baseline).await?;
            ensure!(rung.fds_idle + FD_BASELINE_SLACK >= fd_baseline + connections);
            ensure!(rung.fds_idle <= fd_baseline + connections + FD_TOLERANCE);
            ensure!(rung.unsubscribe_failures == 0);
            ensure!(rung.fds_after_close <= fd_baseline + FD_TOLERANCE);
            rungs.push(rung);
        }
        let churn = self.churn(fd_baseline).await?;
        ensure!(churn.outcome.failed == 0);
        ensure!(churn.fds_after_churn <= fd_baseline + FD_TOLERANCE);
        Ok(self.report(&rungs, &churn, fd_baseline, rss_baseline_kb))
    }
}

struct Churn {
    outcome: RunOutcome,
    fds_after_churn: usize,
    churn_settle_s: f64,
    rss_after_churn_kb: u64,
}

impl WsConnectionCapacity {
    #[instrument(skip_all, err, fields(connections = connections))]
    async fn measure_rung(&self, connections: usize, fd_baseline: usize) -> Result<RungOutcome> {
        let pid = self.private.pid();
        let mut held = self.open_connections(connections).await?;
        let fds_idle = host::fd_count(pid)?;

        let mut subids = Vec::with_capacity(held.len());
        for conn in &mut held {
            subids.push(conn.account_subscribe(&self.shared_account).await?);
        }
        let fds_subscribed = host::fd_count(pid)?;
        let rss_subscribed_kb = host::rss_kb(pid)?;

        let mut unsubscribe_failures = 0;
        for (conn, subid) in held.iter_mut().zip(subids) {
            if !conn.account_unsubscribe(subid).await? {
                unsubscribe_failures += 1;
            }
        }
        for conn in held {
            conn.close().await?;
        }
        let (fds_after_close, settle_s) = self.await_fd_settle(fd_baseline).await?;

        let rung = RungOutcome {
            connections,
            fds_idle,
            fds_subscribed,
            rss_subscribed_kb,
            fds_after_close,
            settle_s,
            unsubscribe_failures,
        };
        debug!(rung.connections, "connection rung finished");

        Ok(rung)
    }

    #[instrument(skip_all, err)]
    async fn churn(&self, fd_baseline: usize) -> Result<Churn> {
        let pid = self.private.pid();
        let ws_url = self.private.ctx().ws_url().clone();
        let shared_account = self.shared_account;
        let churn = execute(
            RunConfig {
                iterations: self.profile.churn_ops,
                rate: Pacing::PerSecond(self.profile.churn_ops as u32),
                concurrency: self.profile.churn_concurrency,
            },
            |_| {
                let ws_url = ws_url.clone();
                future::ready(Ok(move |_: Instant| async move {
                    let mut conn = RawWs::connect(&ws_url).await?;
                    let subid = conn.account_subscribe(&shared_account).await?;
                    ensure!(conn.account_unsubscribe(subid).await?);
                    conn.close().await
                }))
            },
        )
        .await?;
        let (fds_after_churn, churn_settle_s) = self.await_fd_settle(fd_baseline).await?;
        let rss_after_churn_kb = host::rss_kb(pid)?;
        debug!(
            churn.failed,
            fds_after_churn, fd_baseline, "connection churn finished",
        );

        Ok(Churn {
            outcome: churn,
            fds_after_churn,
            churn_settle_s,
            rss_after_churn_kb,
        })
    }

    fn report(
        &self,
        rungs: &[RungOutcome],
        churn: &Churn,
        fd_baseline: usize,
        rss_baseline_kb: u64,
    ) -> ScenarioReport {
        let mut summary = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting(
                "ladder",
                self.profile
                    .ladder
                    .iter()
                    .map(|connections| connections.to_string())
                    .collect::<Vec<_>>()
                    .join("/"),
            )
            .setting("churn ops", self.profile.churn_ops)
            .setting("churn concurrency", self.profile.churn_concurrency)
            .setting("fd tolerance", FD_TOLERANCE)
            .observe("churn op us", Unit::Micros, churn.outcome.delivery)
            .client_side(&churn.outcome)
            .metric("baseline fds", Unit::Count, fd_baseline as f64)
            .metric("baseline rss kb", Unit::Kilobytes, rss_baseline_kb as f64)
            .metric(
                "churn ops /s",
                Unit::PerSecond,
                churn.outcome.achieved_rps(),
            )
            .metric("churn failed", Unit::Count, churn.outcome.failed as f64)
            .metric("fds after churn", Unit::Count, churn.fds_after_churn as f64)
            .metric("churn fd settle s", Unit::Seconds, churn.churn_settle_s)
            .metric(
                "rss after churn kb",
                Unit::Kilobytes,
                churn.rss_after_churn_kb as f64,
            );
        for rung in rungs {
            let rung_name = format!("conns{}", rung.connections);
            summary = summary
                .metric(
                    format!("{rung_name} fds idle"),
                    Unit::Count,
                    rung.fds_idle as f64,
                )
                .metric(
                    format!("{rung_name} fds subscribed"),
                    Unit::Count,
                    rung.fds_subscribed as f64,
                )
                .metric(
                    format!("{rung_name} rss kb"),
                    Unit::Kilobytes,
                    rung.rss_subscribed_kb as f64,
                )
                .metric(
                    format!("{rung_name} fds after close"),
                    Unit::Count,
                    rung.fds_after_close as f64,
                )
                .metric(
                    format!("{rung_name} fd settle s"),
                    Unit::Seconds,
                    rung.settle_s,
                );
        }
        summary
    }

    async fn open_connections(&self, count: usize) -> Result<Vec<RawWs>> {
        let mut connections = Vec::with_capacity(count);
        let mut remaining = count;
        while remaining > 0 {
            let batch = remaining.min(OPEN_CONCURRENCY);
            let opened =
                join_all((0..batch).map(|_| RawWs::connect(self.private.ctx().ws_url()))).await;
            for conn in opened {
                connections.push(conn?);
            }
            remaining -= batch;
        }
        Ok(connections)
    }

    async fn await_fd_settle(&self, baseline: usize) -> Result<(usize, f64)> {
        let started = Instant::now();
        let deadline = time::Instant::now() + FD_SETTLE_TIMEOUT;
        loop {
            let fds = host::fd_count(self.private.pid())?;
            if fds <= baseline + FD_TOLERANCE || time::Instant::now() >= deadline {
                return Ok((fds, started.elapsed().as_secs_f64()));
            }
            time::sleep(Duration::from_millis(200)).await;
        }
    }
}
