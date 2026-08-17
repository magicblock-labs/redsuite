use std::time::Duration;

use async_trait::async_trait;
use futures_util::future::join_all;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, host,
    profile::{self, ProfileValues},
    runner::{drive, RunConfig},
    topology,
    transport::wsraw::RawWs,
    BaseCtx, ErCtx, Result, Scenario, ScenarioReport,
};

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

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
    soak: None,
    deep: None,
};

async fn open_connections(url: &str, count: usize) -> Result<Vec<RawWs>> {
    let mut connections = Vec::with_capacity(count);
    let mut remaining = count;
    while remaining > 0 {
        let batch = remaining.min(OPEN_CONCURRENCY);
        let opened = join_all((0..batch).map(|_| RawWs::connect(url))).await;
        for conn in opened {
            connections.push(conn?);
        }
        remaining -= batch;
    }
    Ok(connections)
}

async fn await_fd_settle(pid: u32, baseline: usize) -> Result<(usize, f64)> {
    let started = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + FD_SETTLE_TIMEOUT;
    loop {
        let fds = host::fd_count(pid)?;
        if fds <= baseline + FD_TOLERANCE
            || tokio::time::Instant::now() >= deadline
        {
            return Ok((fds, started.elapsed().as_secs_f64()));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

struct RungOutcome {
    connections: usize,
    fds_idle: usize,
    fds_subscribed: usize,
    rss_subscribed_kb: u64,
    fds_after_close: usize,
    settle_s: f64,
    unsubscribe_failures: usize,
}

pub struct WsConnCapacity;

#[async_trait(?Send)]
impl Scenario for WsConnCapacity {
    fn name(&self) -> &str {
        "redline/ws_conn_capacity"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);
        let shared_account: Pubkey = er.identity();
        let private = topology::private_er(
            base,
            topology::ErOptions {
                label: "s3".to_owned(),
                env: Vec::new(),
                request_timeout: None,
                ..Default::default()
            },
        )
        .await?;
        let pid = private.pid();
        let ws_url = private.ctx().ws_url().to_owned();

        let fd_baseline = host::fd_count(pid)?;
        let rss_baseline_kb = host::rss_kb(pid)?;
        eprintln!(
            "[redsuite] {}: baseline fds {fd_baseline}, rss {rss_baseline_kb} kB",
            self.name()
        );

        let mut rungs: Vec<RungOutcome> = Vec::new();
        for connections in profile.ladder {
            let mut held = open_connections(&ws_url, connections).await?;
            let fds_idle = host::fd_count(pid)?;

            let mut subids = Vec::with_capacity(held.len());
            for conn in &mut held {
                subids.push(conn.account_subscribe(&shared_account).await?);
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
            let (fds_after_close, settle_s) =
                await_fd_settle(pid, fd_baseline).await?;

            let rung = RungOutcome {
                connections,
                fds_idle,
                fds_subscribed,
                rss_subscribed_kb,
                fds_after_close,
                settle_s,
                unsubscribe_failures,
            };
            eprintln!(
                "[redsuite] {}: {} conns — fds idle {} / subscribed {} \
                 (baseline {}), rss {} kB, after close {} in {:.1} s, \
                 unsub failures {}",
                self.name(),
                rung.connections,
                rung.fds_idle,
                rung.fds_subscribed,
                fd_baseline,
                rung.rss_subscribed_kb,
                rung.fds_after_close,
                rung.settle_s,
                rung.unsubscribe_failures,
            );

            check!(
                rung.fds_idle + FD_BASELINE_SLACK >= fd_baseline + connections,
                "{} conns: only {} fds at hold — connections not held open",
                connections,
                rung.fds_idle
            )?;
            check!(
                rung.fds_idle <= fd_baseline + connections + FD_TOLERANCE,
                "{} conns: {} fds at hold exceeds conns + tolerance",
                connections,
                rung.fds_idle
            )?;
            check_eq!(
                rung.unsubscribe_failures,
                0,
                "{} conns: unsubscribes failed",
                connections
            )?;
            check!(
                rung.fds_after_close <= fd_baseline + FD_TOLERANCE,
                "{} conns: {} fds after close-all — leaked descriptors \
                 (baseline {})",
                connections,
                rung.fds_after_close,
                fd_baseline
            )?;
            rungs.push(rung);
        }

        let churn = drive(
            RunConfig {
                iterations: profile.churn_ops,
                rate: profile.churn_ops as u32,
                concurrency: profile.churn_concurrency,
            },
            |_| {
                let ws_url = ws_url.clone();
                async move {
                    let mut conn = RawWs::connect(&ws_url).await?;
                    let subid = conn.account_subscribe(&shared_account).await?;
                    if !conn.account_unsubscribe(subid).await? {
                        return Err("unsubscribe returned false".into());
                    }
                    conn.close().await
                }
            },
        )
        .await;
        let (fds_after_churn, churn_settle_s) =
            await_fd_settle(pid, fd_baseline).await?;
        let rss_after_churn_kb = host::rss_kb(pid)?;
        eprintln!(
            "[redsuite] {}: churn {} ops — {} ok / {} failed in {:.1} s \
             (p50 {} us / p95 {} us), fds settle {} in {:.1} s (baseline {}), rss {} kB",
            self.name(),
            profile.churn_ops,
            churn.delivered,
            churn.failed,
            churn.wall.as_secs_f64(),
            churn.delivery.median,
            churn.delivery.quantile95,
            fds_after_churn,
            churn_settle_s,
            fd_baseline,
            rss_after_churn_kb,
        );
        check_eq!(
            churn.failed,
            0,
            "churn ops failed: {:?}",
            churn.first_error
        )?;
        check!(
            fds_after_churn <= fd_baseline + FD_TOLERANCE,
            "churn leaked descriptors: {fds_after_churn} vs baseline {fd_baseline}"
        )?;

        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting(
                "ladder",
                profile
                    .ladder
                    .iter()
                    .map(|connections| connections.to_string())
                    .collect::<Vec<_>>()
                    .join("/"),
            )
            .setting("churn ops", profile.churn_ops)
            .setting("churn concurrency", profile.churn_concurrency)
            .setting("fd tolerance", FD_TOLERANCE)
            .observe("churn op us", churn.delivery)
            .metric("baseline fds", fd_baseline as f64)
            .metric("baseline rss kb", rss_baseline_kb as f64)
            .metric("churn ops /s", churn.achieved_rps())
            .metric("churn failed", churn.failed as f64)
            .metric("fds after churn", fds_after_churn as f64)
            .metric("churn fd settle s", churn_settle_s)
            .metric("rss after churn kb", rss_after_churn_kb as f64);
        for rung in &rungs {
            let rung_name = format!("conns{}", rung.connections);
            summary = summary
                .metric(format!("{rung_name} fds idle"), rung.fds_idle as f64)
                .metric(
                    format!("{rung_name} fds subscribed"),
                    rung.fds_subscribed as f64,
                )
                .metric(
                    format!("{rung_name} rss kb"),
                    rung.rss_subscribed_kb as f64,
                )
                .metric(
                    format!("{rung_name} fds after close"),
                    rung.fds_after_close as f64,
                )
                .metric(format!("{rung_name} fd settle s"), rung.settle_s);
        }
        Ok(summary)
    }
}
