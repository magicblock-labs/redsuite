use std::{rc::Rc, sync::Arc, time::Duration};

use async_trait::async_trait;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    assert::poll_until,
    prep, report,
    runner::{drive_threads, RunOutcome, ThreadRunConfig},
    BaseCtx, ChainCtx, ErClient, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport, TxSender,
};

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const ARTIFACT_P50_RATIO: f64 = 5.0;
const VALIDATOR_AGREEMENT_RATIO: f64 = 3.0;

const TX_PROCESSING_HISTOGRAM: &str = "mbv_transaction_processing_time";

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: usize,
    threads: usize,
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
    expect_artifact: bool,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 16,
    accounts: 16,
    threads: 4,
    warmup: 200,
    iterations: 2_000,
    rate: 400,
    concurrency: 64,
    expect_artifact: false,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 64,
    accounts: 64,
    threads: 8,
    warmup: 2_000,
    iterations: 25_000,
    rate: 2_500,
    concurrency: 512,
    expect_artifact: true,
};

fn profile() -> &'static Profile {
    match std::env::var("REDSUITE_PROFILE") {
        Ok(name) if name == "full" => &FULL,
        Ok(name) if name == "lite" => &LITE,
        Ok(name) => panic!("unknown REDSUITE_PROFILE `{name}` (lite|full)"),
        Err(_) => &LITE,
    }
}

fn shape(pool: &[Pubkey], id: u64) -> Instruction {
    use crate::program::instruction::build;
    let len = pool.len() as u64;
    let base_index = ((id - 1) * 3) % len;
    let source = pool[base_index as usize];
    let first_dest = pool[((base_index + 1) % len) as usize];
    let second_dest = pool[((base_index + 2) % len) as usize];
    build::account_data_copy(id, &[source], &[first_dest, second_dest])
}

fn run_cell(
    er_rpc_url: String,
    config: ThreadRunConfig,
    id_offset: u64,
    pool: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
) -> RunOutcome {
    let threads = config.threads;
    let factory = move |thread_index: usize| {
        let client = ErClient::new(er_rpc_url.clone());
        let senders: Vec<TxSender> = payer_bytes
            .iter()
            .enumerate()
            .filter(|(payer_index, _)| payer_index % threads == thread_index)
            .map(|(_, bytes)| {
                let payer = Keypair::try_from(&bytes[..])
                    .expect("payer bytes round-trip");
                client.sender(Rc::new(payer))
            })
            .collect();
        let pool = pool.clone();
        move |id: u64| {
            let sender = senders[(id as usize) % senders.len()].clone();
            let ix = shape(&pool, id_offset + id);
            async move { sender.send(&[ix]).await.map(|_| ()) }
        }
    };
    drive_threads(config, factory)
}

struct CellOutcome {
    threads: usize,
    outcome: RunOutcome,
    tx_processing_avg_us: Option<f64>,
}

pub struct ProtocolBoundarySelftest;

#[async_trait(?Send)]
impl Scenario for ProtocolBoundarySelftest {
    fn name(&self) -> &str {
        "redline/protocol_boundary_selftest"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = profile();
        let prep_payers =
            prep::funded_payers(base, profile.payers, PREP_PAYER_LAMPORTS)
                .await?;
        let pool = Arc::new(
            crate::init_delegated_accounts_batched(
                base,
                &prep_payers,
                profile.accounts,
                crate::ACCOUNT_SPACE,
                er.identity(),
            )
            .await?,
        );
        for pda in pool.iter() {
            poll_until(CLONE_TIMEOUT, || async {
                matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
            })
            .await;
        }
        let payer_bytes: Arc<Vec<[u8; 64]>> = Arc::new(
            prep_payers.iter().map(|payer| payer.to_bytes()).collect(),
        );
        let er_rpc_url = er.api().url().to_owned();

        let warmup = run_cell(
            er_rpc_url.clone(),
            ThreadRunConfig {
                threads: profile.threads,
                iterations: profile.warmup,
                rate: profile.rate,
                concurrency: profile.concurrency,
            },
            0,
            pool.clone(),
            payer_bytes.clone(),
        );
        assert_eq!(
            warmup.failed, 0,
            "warmup deliveries failed: {:?}",
            warmup.first_error
        );

        let mut id_offset = profile.warmup;
        let mut cells: Vec<CellOutcome> = Vec::new();
        for threads in [1, profile.threads] {
            let before = er.scrape_metrics().await?;
            let outcome = run_cell(
                er_rpc_url.clone(),
                ThreadRunConfig {
                    threads,
                    iterations: profile.iterations,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                id_offset,
                pool.clone(),
                payer_bytes.clone(),
            );
            let after = er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);
            id_offset += profile.iterations;

            let cell = CellOutcome {
                threads,
                outcome,
                tx_processing_avg_us: delta
                    .histogram_avg(TX_PROCESSING_HISTOGRAM)
                    .map(|seconds| seconds * 1e6),
            };
            eprintln!(
                "[redsuite] {}: threads={} — {:.0} tps achieved of {} offered, \
                 client p50 {} us / p95 {} us, validator tx avg {}",
                self.name(),
                cell.threads,
                cell.outcome.achieved_rps(),
                profile.rate,
                cell.outcome.delivery.median,
                cell.outcome.delivery.quantile95,
                cell.tx_processing_avg_us
                    .map(|us| format!("{us:.1} us"))
                    .unwrap_or_else(|| "n/a".to_owned()),
            );
            assert_eq!(
                cell.outcome.failed, 0,
                "threads={}: deliveries failed: {:?}",
                cell.threads, cell.outcome.first_error
            );

            let cell_report = ScenarioReport::ok(&format!(
                "{}/threads{threads}",
                self.name()
            ))
            .setting("profile", profile.name)
            .setting("threads", threads)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("measured iters", profile.iterations)
            .setting("offered tps", profile.rate)
            .setting("concurrency", profile.concurrency)
            .observe("delivery us", cell.outcome.delivery)
            .metric("achieved tps", cell.outcome.achieved_rps())
            .metric_if(
                "validator tx processing avg us",
                cell.tx_processing_avg_us,
            );
            match report::persist(&cell_report) {
                Ok(path) => {
                    eprintln!("[redsuite]   cell report: {}", path.display())
                }
                Err(e) => eprintln!(
                    "[redsuite]   warning: cell report not persisted: {e}"
                ),
            }
            cells.push(cell);
        }

        let single = &cells[0];
        let multi = &cells[1];

        assert!(
            multi.outcome.achieved_rps() >= profile.rate as f64 * 0.9,
            "INVALID: the {}-thread driver achieved only {:.0} of {} offered — \
             the boundary rate exceeds even the multi-thread client on this host",
            profile.threads,
            multi.outcome.achieved_rps(),
            profile.rate
        );
        if let (Some(single_validator), Some(multi_validator)) =
            (single.tx_processing_avg_us, multi.tx_processing_avg_us)
        {
            let validator_ratio = (single_validator / multi_validator)
                .max(multi_validator / single_validator);
            assert!(
                validator_ratio <= VALIDATOR_AGREEMENT_RATIO,
                "validator-side work diverged across cells ({single_validator:.1} \
                 vs {multi_validator:.1} us) — cells are not comparable"
            );
        }

        let p50_ratio = single.outcome.delivery.median.max(1) as f64
            / multi.outcome.delivery.median.max(1) as f64;
        let artifact_detected = p50_ratio >= ARTIFACT_P50_RATIO;
        eprintln!(
            "[redsuite] {}: single/multi client p50 ratio {:.1}x — client \
             artifact {}",
            self.name(),
            p50_ratio,
            if artifact_detected {
                "DETECTED (single-thread latency is the harness, not the validator)"
            } else {
                "not detected"
            }
        );
        if profile.expect_artifact {
            assert!(
                artifact_detected,
                "the single-thread cell was expected to hit the client boundary \
                 at {} tps (p50 ratio {:.1}x < {ARTIFACT_P50_RATIO}x) — raise \
                 the boundary rate for this host",
                profile.rate,
                p50_ratio
            );
        } else {
            assert!(
                !artifact_detected,
                "false positive: both cells run below the client boundary yet \
                 p50 diverged {:.1}x",
                p50_ratio
            );
        }

        Ok(ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("threads boundary", profile.threads)
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("measured iters per cell", profile.iterations)
            .setting("offered tps", profile.rate)
            .setting("concurrency", profile.concurrency)
            .setting("expect artifact", profile.expect_artifact)
            .metric("single p50 us", single.outcome.delivery.median as f64)
            .metric("single p95 us", single.outcome.delivery.quantile95 as f64)
            .metric("single achieved tps", single.outcome.achieved_rps())
            .metric("multi p50 us", multi.outcome.delivery.median as f64)
            .metric("multi p95 us", multi.outcome.delivery.quantile95 as f64)
            .metric("multi achieved tps", multi.outcome.achieved_rps())
            .metric("p50 ratio", p50_ratio)
            .metric(
                "artifact detected",
                if artifact_detected { 1.0 } else { 0.0 },
            )
            .metric_if(
                "single validator tx avg us",
                single.tx_processing_avg_us,
            )
            .metric_if("multi validator tx avg us", multi.tx_processing_avg_us))
    }
}
