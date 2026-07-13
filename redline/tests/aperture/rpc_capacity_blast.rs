use std::{rc::Rc, sync::Arc, time::Duration};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    assert::poll_until,
    prep, run_scenario,
    runner::{drive_threads, ThreadRunConfig},
    BaseCtx, ChainCtx, ErClient, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport, TxSender,
};

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
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

fn profile() -> &'static Profile {
    match std::env::var("REDSUITE_PROFILE") {
        Ok(name) if name == "full" => &FULL,
        Ok(name) if name == "lite" => &LITE,
        Ok(name) => panic!("unknown REDSUITE_PROFILE `{name}` (lite|full)"),
        Err(_) => &LITE,
    }
}

struct RpcCapacityBlast;

#[async_trait(?Send)]
impl Scenario for RpcCapacityBlast {
    fn name(&self) -> &str {
        "redline/rpc_capacity_blast"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = profile();
        let prep_payers =
            prep::funded_payers(base, profile.payers, PREP_PAYER_LAMPORTS)
                .await?;
        let pool = Arc::new(
            redline::init_delegated_accounts_batched(
                base,
                &prep_payers,
                profile.accounts,
                redline::ACCOUNT_SPACE,
                er.identity(),
            )
            .await?,
        );
        for pda in pool.iter() {
            poll_until(CLONE_TIMEOUT, || async {
                matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == redline::ACCOUNT_SPACE as usize)
            })
            .await;
        }
        let payer_bytes: Arc<Vec<[u8; 64]>> = Arc::new(
            prep_payers.iter().map(|payer| payer.to_bytes()).collect(),
        );
        let er_rpc_url = er.api().url().to_owned();
        let threads = profile.threads;

        let before = er.scrape_metrics().await?;
        let factory =
            {
                let pool = pool.clone();
                let payer_bytes = payer_bytes.clone();
                move |thread_index: usize| {
                    let client = ErClient::new(er_rpc_url.clone());
                    let senders: Vec<TxSender> = payer_bytes
                        .iter()
                        .enumerate()
                        .filter(|(payer_index, _)| {
                            payer_index % threads == thread_index
                        })
                        .map(|(_, bytes)| {
                            let payer = Keypair::try_from(&bytes[..])
                                .expect("payer bytes round-trip");
                            client.sender(Rc::new(payer))
                        })
                        .collect();
                    let pool = pool.clone();
                    move |id: u64| {
                        let sender =
                            senders[(id as usize) % senders.len()].clone();
                        let account: Pubkey = pool[(id as usize) % pool.len()];
                        let ix = redline::program::instruction::build::
                        simple_byte_set(id, &[account]);
                        async move { sender.send(&[ix]).await.map(|_| ()) }
                    }
                }
            };
        let outcome = drive_threads(
            ThreadRunConfig {
                threads,
                iterations: profile.requests,
                rate: profile.offered,
                concurrency: profile.concurrency,
            },
            factory,
        );
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

        assert_eq!(
            outcome.failed, 0,
            "blast requests failed: {:?}",
            outcome.first_error
        );
        assert!(
            delivered_rps >= CATASTROPHIC_RPS_FLOOR,
            "INVALID: delivered only {delivered_rps:.0} RPS — catastrophic \
             ingress regression or broken harness"
        );
        if let Some(processed) = delta.counter("mbv_transaction_count") {
            assert!(
                processed >= profile.requests as f64,
                "validator processed {processed} txs, expected at least {}",
                profile.requests
            );
        }
        if let Some(failed_txs) = delta.counter("mbv_failed_transactions_count")
        {
            assert_eq!(
                failed_txs, 0.0,
                "transactions failed on the validator during the blast"
            );
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
            .observe("delivery us", outcome.delivery)
            .metric("delivered rps", delivered_rps)
            .metric("blast wall s", outcome.wall.as_secs_f64())
            .metric("failed", outcome.failed as f64)
            .metric_if(
                "validator tx processing avg us",
                delta
                    .histogram_avg(TX_PROCESSING_HISTOGRAM)
                    .map(|seconds| seconds * 1e6),
            )
            .metric_if(
                "validator txs in window",
                delta.counter("mbv_transaction_count"),
            ))
    }
}

#[tokio::test]
async fn rpc_capacity_blast() {
    run_scenario(RpcCapacityBlast).await;
}
