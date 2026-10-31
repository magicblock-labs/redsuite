use crate::RunControl;
use redsuite_core::{
    check,
    config::Profile,
    host,
    report::Unit,
    runner::{execute_for_raw, Pacing, SendTransaction},
    transport::wsraw::RawWs,
    BaseCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use std::{
    mem,
    rc::Rc,
    time::{Duration, Instant},
};

const FD_TOLERANCE: usize = 64;

/// Exercises connection churn; server FD reclamation is checked only for locally owned ERs.
/// Keeps an idle connection population open while measuring subscribe/unsubscribe/close cycles.
pub struct WebsocketChurn {
    base: Rc<BaseCtx>,
    er: ErCtx,
    connections: usize,
}
impl Scenario<(ScenarioReport, Result<()>)> for WebsocketChurn {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let connections = match base.config().profile {
            Profile::Lite => 100,
            Profile::Full => 1000,
        };
        Ok(Self { base, er, connections })
    }
    /// Measures churn, closes idle sockets, and allows asynchronous local FD
    /// cleanup before checking it.
    async fn run(self) -> Result<(ScenarioReport, Result<()>)> {
        let options = RunControl::from_config(&self.base.config())?;
        let baseline = self
            .er
            .local_pid()
            .map(|pid| -> Result<_> { Ok((pid, host::fd_count(pid)?, host::rss_kb(pid)?)) })
            .transpose()?;
        let mut idle = Vec::with_capacity(self.connections);
        for _ in 0..self.connections {
            idle.push(RawWs::connect(self.er.ws_url()).await?);
        }
        if let Some((pid, fds, _)) = baseline {
            let count = host::fd_count(pid)?;
            check!(count + 8 >= fds + self.connections);
            check!(count <= fds + self.connections + FD_TOLERANCE);
        }
        let deadline = options.deadline()?;
        let mut report =
            ScenarioReport::ok().setting("idle connections", self.connections).setting(
                "resource scope",
                if baseline.is_some() {
                    "warm local process"
                } else {
                    "server resources unobserved"
                },
            );
        let result: Result<()> = async {
            let outcome = execute_for_raw(
                Pacing::PerSecond(200),
                64,
                deadline,
                options.stop.clone(),
                |_| {
                    let url = self.er.ws_url().clone();
                    let account = self.er.identity();
                    async move {
                        Ok(SendTransaction(move |_: Instant| async move {
                            let mut connection = RawWs::connect(&url).await?;
                            let subscription = connection.account_subscribe(&account).await?;
                            let unsubscribed = connection.account_unsubscribe(subscription).await?;
                            check!(unsubscribed);
                            connection.close().await
                        }))
                    }
                },
            )
            .await?;
            let outcome = outcome.finalize();
            report = mem::replace(&mut report, ScenarioReport::ok())
                .observe("connection lifecycle us", Unit::Micros, outcome.delivery)
                .client_side(&outcome)
                .metric("completed cycles", Unit::Count, outcome.delivered as f64);
            check!(
                outcome.delivered > 0 || *options.stop.borrow(),
                "no measured churn completed"
            );
            outcome.check_failures();
            Ok(())
        }
        .await;
        for connection in idle {
            connection.close().await?;
        }
        // Remote/attached ERs have no owned PID; absence of resource metrics is not a leak verdict.
        let cleanup: Result<()> = async {
            if let Some((pid, fds, rss)) = baseline {
                let end = Instant::now() + Duration::from_secs(90);
                while host::fd_count(pid)? > fds + FD_TOLERANCE && Instant::now() < end {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                let count = host::fd_count(pid)?;
                report = mem::replace(&mut report, ScenarioReport::ok())
                    .metric("FDs after cleanup", Unit::Count, count as f64)
                    .metric(
                        "RSS change KiB",
                        Unit::Kilobytes,
                        host::rss_kb(pid)?.saturating_sub(rss) as f64,
                    );
                check!(count <= fds + FD_TOLERANCE);
            }
            Ok(())
        }
        .await;
        let result = result.and(cleanup);
        Ok((
            report.setting("completion", options.completion(&result)),
            result,
        ))
    }
}
