use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::redline::Accounts;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, prep,
    profile::ProfileValues,
    runner::{execute_threaded, Pacing, ThreadRunConfig},
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
};

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const LEDGER_SETTLE_TIMEOUT: Duration = Duration::from_secs(5);
const LEDGER_SETTLE_POLL: Duration = Duration::from_millis(50);
const CATASTROPHIC_RPS_FLOOR: f64 = 1_000.0;

const TX_PROCESSING_HISTOGRAM: &str = "mbv_transaction_processing_time";

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: usize,
    threads: usize,
    requests: u64,
    offered: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    accounts: 16,
    threads: 4,
    requests: 10_000,
    offered: 10_000,
    concurrency: 512,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 16,
    accounts: 64,
    threads: 8,
    requests: 50_000,
    offered: 50_000,
    concurrency: 2_048,
};

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
};

pub struct RpcCapacityBlast;

#[async_trait(?Send)]
impl Scenario for RpcCapacityBlast {
    fn name(&self) -> &str {
        "redline/rpc_capacity_blast"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers =
            prep::funded_payers(base, profile.payers, PREP_PAYER_LAMPORTS)
                .await?;
        let pool = Arc::new(
            Accounts::new(crate::ACCOUNT_SPACE, er.identity())
                .init_batched(base, &prep_payers, profile.accounts, true)
                .await?,
        );
        prep::await_clones(
            er,
            &pool,
            crate::ACCOUNT_SPACE as usize,
            CLONE_TIMEOUT,
        )
        .await?;
        let payer_bytes: Arc<Vec<[u8; 64]>> = Arc::new(
            prep_payers.iter().map(|payer| payer.to_bytes()).collect(),
        );
        let er_rpc_url = er.api().url().to_owned();
        let threads = profile.threads;

        let before = er.scrape_metrics().await?;
        let factory = {
            let pool = pool.clone();
            let payer_bytes = payer_bytes.clone();
            move |thread_index: usize| {
                let senders = prep::worker_senders(
                    &er_rpc_url,
                    &payer_bytes,
                    thread_index,
                    threads,
                );
                let pool = pool.clone();
                move |id: u64| {
                    let sender = senders[(id as usize) % senders.len()].clone();
                    let account: Pubkey = pool[(id as usize) % pool.len()];
                    let ix =
                        crate::program::instruction::build::simple_byte_set(
                            id,
                            &[account],
                        );
                    async move { sender.submit(&[ix]).await.map(|_| ()) }
                }
            }
        };
        let outcome = execute_threaded(
            ThreadRunConfig {
                threads,
                iterations: profile.requests,
                rate: Pacing::PerSecond(profile.offered),
                concurrency: profile.concurrency,
            },
            factory,
        )?;
        if let Some(ledger_txs_before) =
            before.get(crate::metrics::ENGINE_TRANSACTIONS)
        {
            settle_ledger(er, ledger_txs_before, profile.requests as f64).await;
        }
        let after = er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);

        let delivered_rps = outcome.achieved_rps();
        eprintln!(
            "[redsuite] {}: {} requests at offered {}/s over {} threads — \
             delivered {:.0} RPS in {:.2} s, {} failed, client p50 {} us / \
             p95 {} us, validator tx avg {}",
            self.name(),
            profile.requests,
            profile.offered,
            threads,
            delivered_rps,
            outcome.wall.as_secs_f64(),
            outcome.failed,
            outcome.delivery.median,
            outcome.delivery.quantile95,
            delta
                .histogram_avg(TX_PROCESSING_HISTOGRAM)
                .map(|seconds| format!("{:.1} us", seconds * 1e6))
                .unwrap_or_else(|| "n/a".to_owned()),
        );

        check_eq!(
            outcome.failed,
            0,
            "blast requests failed: {:?}",
            outcome.first_error
        )?;
        if delivered_rps < CATASTROPHIC_RPS_FLOOR {
            eprintln!(
                "[redsuite] {}: warning: delivered only {delivered_rps:.0} \
                 RPS — catastrophic ingress regression or broken harness",
                self.name()
            );
        }
        if let Some(processed) =
            delta.counter(crate::metrics::ENGINE_TRANSACTIONS)
        {
            check!(
                processed >= profile.requests as f64,
                "validator processed {processed} txs, expected at least {}",
                profile.requests
            )?;
        }
        if let Some(failed_txs) =
            delta.counter_all(crate::metrics::FAILED_TRANSACTIONS)
        {
            check_eq!(
                failed_txs,
                0.0,
                "transactions failed on the validator during the blast"
            )?;
        }

        Ok(ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("shape", "write 1/tx byte-set over hot pool")
            .setting("threads", threads)
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("requests", profile.requests)
            .setting("offered rps", profile.offered)
            .setting("concurrency", profile.concurrency)
            .observe("delivery us", Unit::Micros, outcome.delivery)
            .observe("achieved rps", Unit::Rps, outcome.rps)
            .metric("delivered rps", Unit::Rps, delivered_rps)
            .metric("blast wall s", Unit::Seconds, outcome.wall.as_secs_f64())
            .metric("failed", Unit::Count, outcome.failed as f64)
            .metric_if(
                "validator tx processing avg us",
                Unit::Micros,
                delta
                    .histogram_avg(TX_PROCESSING_HISTOGRAM)
                    .map(|seconds| seconds * 1e6),
            )
            .metric_if(
                "validator txs in window",
                Unit::Count,
                delta.counter(crate::metrics::ENGINE_TRANSACTIONS),
            ))
    }
}

async fn settle_ledger(er: &ErCtx, baseline: f64, expected: f64) {
    let deadline = tokio::time::Instant::now() + LEDGER_SETTLE_TIMEOUT;
    loop {
        let committed = tokio::time::timeout_at(deadline, er.scrape_metrics())
            .await
            .ok()
            .and_then(|scraped| scraped.ok())
            .and_then(|metrics| {
                metrics.get(crate::metrics::ENGINE_TRANSACTIONS)
            })
            .map(|total| total - baseline)
            .unwrap_or(0.0);
        if committed >= expected || tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(LEDGER_SETTLE_POLL).await;
    }
}
