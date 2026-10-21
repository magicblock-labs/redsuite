use std::{rc::Rc, sync::Arc, time::Duration};

use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, prep,
    profile::ProfileValues,
    redline::{execute_kernel, Accounts},
    report::Unit,
    runner::{Pacing, ThreadRunConfig},
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
};
use tokio::{time, time::Instant};
use tracing::{debug, warn};

use crate::{metrics, ACCOUNT_SPACE};

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

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

pub struct RpcCapacityBlast {
    er: ErCtx,
    profile: &'static Profile,
    prep_payers: Vec<Keypair>,
    pool: Arc<Vec<Pubkey>>,
}

impl Scenario<ScenarioReport> for RpcCapacityBlast {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers = base.funded_payers(profile.payers, PREP_PAYER_LAMPORTS).await?;
        let pool = Arc::new(
            Accounts::new(ACCOUNT_SPACE, er.identity())
                .init_batched(base.as_ref(), &prep_payers, profile.accounts, true)
                .await?,
        );
        er.await_clones(&pool, ACCOUNT_SPACE as usize, CLONE_TIMEOUT).await?;
        Ok(Self { er, profile, prep_payers, pool })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let Self { er, profile, prep_payers, pool } = self;
        let threads = profile.threads;

        let before = er.scrape_metrics().await?;
        let outcome = execute_kernel(
            ThreadRunConfig {
                threads,
                iterations: profile.requests,
                rate: Pacing::PerSecond(profile.offered),
                concurrency: profile.concurrency,
            },
            er.api().url().clone(),
            prep::payer_bytes(&prep_payers),
            0,
            move |id| {
                crate::program::instruction::build::simple_byte_set(
                    id,
                    &[pool[(id as usize) % pool.len()]],
                )
            },
            None,
        )?;
        if let Some(ledger_txs_before) = before.get(metrics::ENGINE_TRANSACTIONS) {
            settle_ledger(&er, ledger_txs_before, profile.requests as f64).await;
        }
        let after = er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);

        let delivered_rps = outcome.achieved_rps();
        debug!(delivered_rps, outcome.failed, "RPC blast finished");

        check_eq!(
            outcome.failed,
            0,
            "blast requests failed: {:?}",
            outcome.first_error
        )?;
        if delivered_rps < CATASTROPHIC_RPS_FLOOR {
            warn!(delivered_rps, "RPC throughput below minimum",);
        }
        if let Some(processed) = delta.counter(metrics::ENGINE_TRANSACTIONS) {
            check!(
                processed >= profile.requests as f64,
                "validator processed {processed} txs, expected at least {}",
                profile.requests
            )?;
        }
        if let Some(failed_txs) = delta.counter_all(metrics::FAILED_TRANSACTIONS) {
            check_eq!(
                failed_txs,
                0.0,
                "transactions failed on the validator during the blast"
            )?;
        }

        Ok(ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting("shape", "write 1/tx byte-set over hot pool")
            .setting("threads", threads)
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("requests", profile.requests)
            .setting("offered rps", profile.offered)
            .setting("concurrency", profile.concurrency)
            .observe("delivery us", Unit::Micros, outcome.delivery)
            .client_side(&outcome)
            .observe("achieved rps", Unit::Rps, outcome.rps)
            .metric("delivered rps", Unit::Rps, delivered_rps)
            .metric("blast wall s", Unit::Seconds, outcome.wall.as_secs_f64())
            .metric("failed", Unit::Count, outcome.failed as f64)
            .metric_if(
                "validator tx processing avg us",
                Unit::Micros,
                delta.histogram_avg(TX_PROCESSING_HISTOGRAM).map(|seconds| seconds * 1e6),
            )
            .metric_if(
                "validator txs in window",
                Unit::Count,
                delta.counter(metrics::ENGINE_TRANSACTIONS),
            ))
    }
}

async fn settle_ledger(er: &ErCtx, baseline: f64, expected: f64) {
    let deadline = Instant::now() + LEDGER_SETTLE_TIMEOUT;
    loop {
        let committed = time::timeout_at(deadline, er.scrape_metrics())
            .await
            .ok()
            .and_then(|scraped| scraped.ok())
            .and_then(|metrics| metrics.get(metrics::ENGINE_TRANSACTIONS))
            .map(|total| total - baseline)
            .unwrap_or(0.0);
        if committed >= expected || Instant::now() >= deadline {
            return;
        }
        time::sleep(LEDGER_SETTLE_POLL).await;
    }
}
