use std::{rc::Rc, sync::Arc, time::Duration};

use anyhow::ensure;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    prep,
    profile::ProfileValues,
    programs::REDLINE,
    redline::{execute_kernel, Accounts},
    report::Unit,
    runner::{Pacing, ThreadRunConfig},
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
};
use tokio::time::{self, Instant};
use tracing::{debug, warn};

use crate::{metrics, ACCOUNT_SPACE};

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
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

pub struct RpcIngressCapacity {
    er: ErCtx,
    profile: &'static Profile,
    prep_payers: Vec<Keypair>,
    pool: Arc<Vec<Pubkey>>,
}

impl Scenario<ScenarioReport> for RpcIngressCapacity {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let prep_payers = base.funded_payers(profile.payers, PREP_PAYER_LAMPORTS).await?;
        let pool = Arc::new(
            Accounts::new(ACCOUNT_SPACE, er.identity())
                .init_batched(base.as_ref(), &prep_payers, profile.accounts, true)
                .await?,
        );
        er.await_clones(&pool, ACCOUNT_SPACE as usize).await?;
        Ok(Self { er, profile, prep_payers, pool })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let threads = self.profile.threads;

        let before = self.er.scrape_metrics().await?;
        let outcome = execute_kernel(
            ThreadRunConfig {
                threads,
                iterations: self.profile.requests,
                rate: Pacing::PerSecond(self.profile.offered),
                concurrency: self.profile.concurrency,
            },
            self.er.api().url().clone(),
            self.er.ws_url().clone(),
            prep::payer_bytes(&self.prep_payers),
            0,
            move |id| REDLINE.write(id, &[self.pool[(id as usize) % self.pool.len()]]),
            None,
        )?;
        if let Some(ledger_txs_before) = before.get(metrics::ENGINE_TRANSACTIONS) {
            self.profile.settle_ledger(&self.er, ledger_txs_before).await;
        }
        let after = self.er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);

        let delivered_rps = outcome.achieved_rps();
        debug!(delivered_rps, outcome.failed, "RPC blast finished");

        ensure!(outcome.failed == 0);
        if delivered_rps < CATASTROPHIC_RPS_FLOOR {
            warn!(delivered_rps, "RPC throughput below minimum",);
        }
        if let Some(processed) = delta.counter(metrics::ENGINE_TRANSACTIONS) {
            ensure!(processed >= self.profile.requests as f64);
        }
        if let Some(failed_txs) = delta.counter_all(metrics::FAILED_TRANSACTIONS) {
            ensure!(failed_txs == 0.0);
        }

        Ok(ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("shape", "write 1/tx pattern write over hot pool")
            .setting("threads", threads)
            .setting("payers", self.profile.payers)
            .setting("accounts", self.profile.accounts)
            .setting("requests", self.profile.requests)
            .setting("offered rps", self.profile.offered)
            .setting("concurrency", self.profile.concurrency)
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

impl Profile {
    async fn settle_ledger(&self, er: &ErCtx, baseline: f64) {
        let deadline = Instant::now() + LEDGER_SETTLE_TIMEOUT;
        loop {
            let committed = time::timeout_at(deadline, er.scrape_metrics())
                .await
                .ok()
                .and_then(|scraped| scraped.ok())
                .and_then(|metrics| metrics.get(metrics::ENGINE_TRANSACTIONS))
                .map(|total| total - baseline)
                .unwrap_or(0.0);
            if committed >= self.requests as f64 || Instant::now() >= deadline {
                return;
            }
            time::sleep(LEDGER_SETTLE_POLL).await;
        }
    }
}
