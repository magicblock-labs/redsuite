use super::{
    channels::Channels, fixture, storage::PayloadFault, BaseCtx, ErCtx, Result, Scenario, TIMEOUT,
};
use anyhow::{bail, ensure, Context};
use redshift::scenarios::replication::replication_recovery::{
    await_catch_up, leader_metric, log_mentions_mismatch, TRANSACTIONS,
};
use redsuite_core::{
    topology::{self, ReplicatedOptions, ReplicatedTopology},
    wait,
};
use std::{
    os::unix::process::ExitStatusExt, path::Path, process::ExitStatus, rc::Rc, time::Duration,
};
use tokio::time::{self, Instant};
use tracing::info;

const TRUNCATIONS: &str = r#"engine_ledger_operation_duration_micros_count{op="truncate"}"#;
const INTEGRITY: [&str; 3] = ["state mismatch", "checksum mismatch", "checkpoint mismatch"];

fn integrity_diagnostic(log: &Path) -> Result<Option<String>> {
    Ok(log_mentions_mismatch(log)?
        .filter(|line| INTEGRITY.iter().any(|needle| line.contains(needle))))
}

fn fail_stop(status: ExitStatus, diagnostic: Option<&str>) -> Result<()> {
    ensure!(
        diagnostic.is_some(),
        "process exited without a named integrity diagnostic: {status}"
    );
    ensure!(
        !status.success() && !matches!(status.signal(), Some(6 | 11)),
        "integrity fault caused an invalid exit: {status}"
    );
    Ok(())
}

pub struct DivergenceSignals {
    base: Rc<BaseCtx>,
    topology: ReplicatedTopology,
}

impl Scenario for DivergenceSignals {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        wait::retry(
            "leader pruned the genesis cursor before verifier snapshot bootstrap",
            TIMEOUT,
            async || {
                let metrics = er.scrape_metrics().await?;
                ensure!(metrics.get(TRUNCATIONS).unwrap_or(0.0) >= 1.0);
                Ok(())
            },
        )
        .await?;
        let topology = topology::replicated(
            &base,
            er,
            ReplicatedOptions {
                label: "signal-integrity".into(),
                verifiers: 1,
            },
        )
        .await?;
        Ok(Self { base, topology })
    }

    async fn run(mut self) -> Result<()> {
        let outcome = self.exercise().await;
        let cleanup = self.topology.finish().await;
        outcome.and(cleanup)
    }
}

impl DivergenceSignals {
    async fn exercise(&mut self) -> Result<()> {
        let mut fixture = fixture(
            self.base.clone(),
            self.topology.leader().clone(),
            "divergence_signals",
        )
        .await?;
        let mut channels = Channels::open(&fixture).await?;
        channels.control(&mut fixture, 11_000, &[]).await?;
        let cutoff = leader_metric(&self.topology, TRANSACTIONS).await?;
        self.topology.wait_verifiers_connected(TIMEOUT).await?;
        await_catch_up(self.topology.verifier(0), TRANSACTIONS, cutoff).await?;
        let before = fixture.snapshot(&[]).await?;
        let image = fixture.expected[1].as_ref().context("divergence target missing")?;
        let log = self.topology.verifier(0).log().to_owned();
        ensure!(
            log_mentions_mismatch(&log)?.is_none(),
            "verifier reported a mismatch before injection"
        );
        let bit = (fixture.base.config().seed % 8) as u8;
        let mut fault =
            PayloadFault::inject(self.topology.verifier(0).accountsdb_dir(), &image.data, bit)?;
        info!(bit, "divergence injected into a caught-up verifier");
        let deadline = Instant::now() + TIMEOUT;
        let mut detected = false;
        loop {
            let diagnostic = integrity_diagnostic(&log)?;
            detected |= diagnostic.is_some();
            if let Some(status) = self.topology.verifier_mut(0).exit_status()? {
                fail_stop(status, integrity_diagnostic(&log)?.as_deref())?;
                detected = true;
                break;
            }
            if detected {
                break;
            }
            match self.topology.verifier(0).scrape_metrics().await {
                Ok(_) => {}
                Err(_) if integrity_diagnostic(&log)?.is_some() => detected = true,
                Err(error) => {
                    return Err(
                        error.context("verifier lost metrics without an integrity diagnostic")
                    )
                }
            }
            if detected || Instant::now() >= deadline {
                break;
            }
            time::sleep(Duration::from_millis(50)).await;
        }
        fault.restore()?;
        fixture.unchanged(&before).await?;
        if let Some(status) = self.topology.verifier_mut(0).exit_status()? {
            fail_stop(status, integrity_diagnostic(&log)?.as_deref())?;
        }
        let stop = self.topology.verifier_mut(0).stop(false).await?;
        ensure!(
            !stop.needed_sigkill && !matches!(stop.exit_signal, Some(6 | 11)),
            "divergence caused a crash or hung shutdown: {stop:?}"
        );
        self.topology.verifier_mut(0).reset_storage()?;
        self.topology.verifier_mut(0).start(TIMEOUT).await?;
        self.topology.wait_verifiers_connected(TIMEOUT).await?;
        channels.control(&mut fixture, 11_001, &[]).await?;
        let cutoff = leader_metric(&self.topology, TRANSACTIONS).await?;
        await_catch_up(self.topology.verifier(0), TRANSACTIONS, cutoff).await?;
        if !detected {
            bail!("quiet continuation after forced leader/verifier divergence (#261): no named diagnostic or fail-stop");
        }
        Ok(())
    }
}
