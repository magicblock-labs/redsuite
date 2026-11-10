use super::{fixture::Fixture, TIMEOUT};
use anyhow::{ensure, Context};
use redsuite_core::{netfault::Selector, random, BaseCtx, ChainCtx, ErCtx, Result, Scenario};
use signer::Signer;
use std::{rc::Rc, time::Duration};
use tracing::debug;

macro_rules! signal_scenario {
    ($name:ident, $run:ident) => {
        pub struct $name {
            base: Rc<BaseCtx>,
            er: ErCtx,
        }

        impl Scenario for $name {
            async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
                Ok(Self { base, er })
            }

            async fn run(self) -> Result<()> {
                $run(self.base, self.er).await
            }
        }
    };
}

mod channels;
mod history;
mod integrity;
mod storage;
mod subscriber;

use channels::{Channels, Probe};
pub use history::{LedgerIndexSignals, PrunedReadSignals};
pub use integrity::DivergenceSignals;
pub use subscriber::SlowSubscriberSignals;

const OBSERVE: Duration = Duration::from_secs(1);

async fn fixture(base: Rc<BaseCtx>, er: ErCtx, label: &str) -> Result<Fixture> {
    let mut rng = random::rng(base.config().seed, &["redhat", label], 0);
    Fixture::new(base, er, &mut rng).await
}

fn finish(findings: Vec<String>) -> Result<()> {
    ensure!(
        findings.is_empty(),
        "public-surface findings (#261): {}",
        findings.join("; ")
    );
    Ok(())
}

signal_scenario!(OutcomeSignals, outcomes);

async fn outcomes(base: Rc<BaseCtx>, er: ErCtx) -> Result<()> {
    let mut fixture = fixture(base, er, "outcome_signals").await?;
    let mut rng = random::rng(
        fixture.base.config().seed,
        &["redhat", "outcome_signals", "probes"],
        0,
    );
    let faults = fixture.er.faults()?;
    let mut channels = Channels::open(&fixture).await?;
    channels.control(&mut fixture, 9_000, &[]).await?;

    let cold = fixture.base.seeded_payer(&mut rng, 1_000_000).await?.pubkey();
    let before = fixture.snapshot(&[cold]).await?;
    ensure!(
        before.local.last() == Some(&None),
        "refusal input was already resident"
    );
    let rule = faults.reject(
        Selector::methods(&["getAccountInfo", "getMultipleAccounts"])
            .http()
            .request()
            .account(&cold),
        "redhat-outcome-cold-read-refused",
    );
    let hash = fixture.er.api().get_latest_blockhash().await?;
    let refused = fixture.signed_with(fixture.message(9_001, 3, &[cold]).await?, hash)?;
    let watch = channels.watch(&refused).await?;
    let start = fixture.er.api().get_slot().await?;
    let error = refused
        .send(&fixture.er)
        .await?
        .err()
        .context("cold preparation unexpectedly admitted")?;
    rule.wait_hit(TIMEOUT)
        .await
        .context("refused write never reached the cold-read fault")?;
    ensure!(
        error.error.code.code() == -32003
            && (error.error.message.contains("redhat-outcome-cold-read-refused")
                || error.error.message.contains(&cold.to_string())),
        "wrong refusal: {error}"
    );
    fixture.unchanged(&before).await?;
    let probe = Probe::new(&refused, watch, None);
    channels.control_with_probe(&mut fixture, 9_101, &[], Some(&probe)).await?;
    fixture.not_admitted(&refused.signature, start, None).await?;
    debug!(signature = %refused.signature, %error, hits = rule.hits(), channel = "rpc", "refused write attributed");

    rule.remove();
    faults.reset().await?;
    channels.control(&mut fixture, 9_102, &[cold]).await?;

    let (instructions, error) = fixture.failed_write(9_002, 11, 7);
    let (failed, probe, slot) =
        channels.submit_failure(&mut fixture, &instructions, &error).await?;
    ensure!(failed.outcome(&fixture.er).await? == slot);
    let observed = channels.control_with_probe(&mut fixture, 9_103, &[], Some(&probe)).await?;
    let findings = [("signature", ""), ("logs", "validator#1889: "), ("failures", "")]
        .into_iter()
        .filter(|(channel, _)| !observed.contains(channel))
        .map(|(channel, issue)| {
            format!(
                "{issue}failed execution/{channel}: no diagnostic for {}",
                failed.signature
            )
        })
        .collect();
    channels.control(&mut fixture, 9_105, &[cold]).await?;
    finish(findings)
}
