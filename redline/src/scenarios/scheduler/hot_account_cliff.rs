use std::{rc::Rc, sync::Arc, time::Duration};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, prep,
    profile::select as select_profile,
    report,
    runner::{drive_threads, RunOutcome, ThreadRunConfig},
    BaseCtx, ChainCtx, ErClient, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport, TxSender,
};

use crate::program::instruction::build;

const PAYER_LAMPORTS: u64 = 2_000_000_000;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(300);
const TX_COUNT: &str = "mbv_transaction_count";
// healthy cells must stay within this factor of the widest cell's p50
const FLAT_FACTOR: f64 = 3.0;
// a cell beyond this factor is the cliff
const CLIFF_FACTOR: f64 = 5.0;
// campaign's healthy client p50 bound, host-calibrated slack
const HEALTHY_P50_US: f64 = 15_000.0;

struct Profile {
    name: &'static str,
    payers: usize,
    // hot write-set sizes; widest first (it is the healthy baseline),
    // narrowest last so a wedged cell cannot poison the others
    cells: &'static [u8],
    threads: usize,
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    cells: &[16, 4],
    threads: 4,
    warmup: 1_000,
    iterations: 10_000,
    rate: 2_000,
    concurrency: 512,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    cells: &[32, 16, 8, 4],
    threads: 8,
    warmup: 10_000,
    iterations: 100_000,
    rate: 14_000,
    concurrency: 2_048,
};

fn profile(scenario: &str) -> &'static Profile {
    match select_profile(scenario, &["lite", "full"]) {
        "full" => &FULL,
        _ => &LITE,
    }
}

fn drive_cell(
    er_rpc_url: String,
    config: ThreadRunConfig,
    id_offset: u64,
    hot_set: Arc<Vec<Pubkey>>,
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
        let hot_set = hot_set.clone();
        // same read-write 3/tx shape as S1, confined to the hot set
        // (hot-set sizes are powers of two — coprime with the stride)
        move |id: u64| {
            let global_id = id_offset + id;
            let len = hot_set.len() as u64;
            let base_index = ((global_id - 1) * 3) % len;
            let ix = build::account_data_copy(
                global_id,
                &[hot_set[base_index as usize]],
                &[
                    hot_set[((base_index + 1) % len) as usize],
                    hot_set[((base_index + 2) % len) as usize],
                ],
            );
            let sender = senders[(global_id as usize) % senders.len()].clone();
            async move { sender.send(&[ix]).await.map(|_| ()) }
        }
    };
    drive_threads(config, factory)
}

async fn drain_intake(er: &ErCtx, target: f64) -> Result<()> {
    check::poll(
        &format!("the validator transaction count reaches {target:.0}"),
        DRAIN_TIMEOUT,
        || async {
            matches!(
                er.scrape_metrics().await.ok().and_then(|metrics| metrics.get(TX_COUNT)),
                Some(count) if count >= target
            )
        },
    )
    .await?;
    Ok(())
}

struct CellResult {
    hot: u8,
    p50_us: f64,
    p95_us: f64,
    achieved: f64,
}

pub struct HotAccountCliff;

#[async_trait(?Send)]
impl Scenario for HotAccountCliff {
    fn name(&self) -> &str {
        "redline/hot_account_cliff"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = profile(self.name());
        let pool = *profile.cells.iter().max().unwrap();
        let payers =
            prep::funded_payers(base, profile.payers, PAYER_LAMPORTS).await?;
        let pdas = crate::init_delegated_accounts(
            base,
            &payers[0],
            pool,
            crate::ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        for pda in &pdas {
            check::poll(
                &format!("the ER clones the delegated pda {pda}"),
                Duration::from_secs(15),
                || async {
                    matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                },
            )
            .await?;
        }
        let payer_bytes: Arc<Vec<[u8; 64]>> =
            Arc::new(payers.iter().map(|payer| payer.to_bytes()).collect());
        let er_rpc_url = er.api().url().to_owned();

        let mut offset = 0u64;
        let mut cells: Vec<CellResult> = Vec::new();
        for &hot in profile.cells {
            let hot_set: Arc<Vec<Pubkey>> =
                Arc::new(pdas[..hot as usize].to_vec());

            let count_before_warmup = er.scrape_metrics().await?.get(TX_COUNT);
            let warm = drive_cell(
                er_rpc_url.clone(),
                ThreadRunConfig {
                    threads: profile.threads,
                    iterations: profile.warmup,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                offset,
                hot_set.clone(),
                payer_bytes.clone(),
            );
            check_eq!(
                warm.failed,
                0,
                "hot{hot} warmup failed: {:?}",
                warm.first_error
            )?;
            offset += profile.warmup;
            if let Some(seen) = count_before_warmup {
                drain_intake(er, seen + profile.warmup as f64).await?;
            }

            let before = er.scrape_metrics().await?;
            let outcome = drive_cell(
                er_rpc_url.clone(),
                ThreadRunConfig {
                    threads: profile.threads,
                    iterations: profile.iterations,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                offset,
                hot_set.clone(),
                payer_bytes.clone(),
            );
            offset += profile.iterations;
            check_eq!(
                outcome.failed,
                0,
                "hot{hot} deliveries failed: {:?}",
                outcome.first_error
            )?;
            if let Some(seen) = before.get(TX_COUNT) {
                drain_intake(er, seen + profile.iterations as f64).await?;
            }
            let after = er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);
            if let Some(failed) = delta.counter("mbv_failed_transactions_count")
            {
                check_eq!(
                    failed,
                    0.0,
                    "hot{hot}: transactions failed on the validator"
                )?;
            }

            let cell = CellResult {
                hot,
                p50_us: outcome.delivery.median as f64,
                p95_us: outcome.delivery.quantile95 as f64,
                achieved: outcome.achieved_rps(),
            };
            eprintln!(
                "[redsuite] {}: hot{hot}: p50 {:.0} us / p95 {:.0} us, {:.0} tps of {} offered",
                self.name(),
                cell.p50_us,
                cell.p95_us,
                cell.achieved,
                profile.rate,
            );

            let cell_report =
                ScenarioReport::ok(&format!("{}/hot{hot}", self.name()))
                    .setting("profile", profile.name)
                    .setting("hot accounts", hot)
                    .setting(
                        "shape",
                        "read-write 3/tx (1 src + 2 dst data-copy)",
                    )
                    .setting("driver threads", profile.threads)
                    .setting("measured iters", profile.iterations)
                    .setting("offered tps", profile.rate)
                    .setting("concurrency", profile.concurrency)
                    .observe("delivery us", outcome.delivery)
                    .metric("achieved tps", cell.achieved)
                    .metric_if(
                        "validator tx processing avg us",
                        delta
                            .histogram_avg("mbv_transaction_processing_time")
                            .map(|seconds| seconds * 1e6),
                    )
                    .metric_if(
                        "max lock contention queue",
                        delta.gauge("mbv_max_lock_contention_queue_size"),
                    )
                    .metric_if(
                        "validator txs in window",
                        delta.counter("mbv_transaction_count"),
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

        // the widest cell is the healthy baseline every other cell is
        // judged against — and it must itself be healthy, else the whole
        // sweep is INVALID (saturated baseline = measuring the harness,
        // not the scheduler; check `validator tx processing avg us` in the
        // cell report to attribute, then lower the offered rate)
        let base_p50 = cells[0].p50_us;
        if base_p50 > HEALTHY_P50_US {
            eprintln!(
                "[redsuite] {}: warning: baseline hot{} p50 {:.0} us is \
                 itself unhealthy (> {HEALTHY_P50_US:.0} us) — offered {} \
                 tps exceeds what this harness+validator pair sustains",
                self.name(),
                cells[0].hot,
                base_p50,
                profile.rate,
            );
        }
        for cell in cells.iter().filter(|cell| cell.hot >= 16) {
            let bound = (base_p50 * FLAT_FACTOR).max(HEALTHY_P50_US);
            if cell.p50_us > bound {
                eprintln!(
                    "[redsuite] {}: warning: hot{} p50 {:.0} us exceeds the \
                     healthy bound {:.0} us (baseline hot{} p50 {:.0} us)",
                    self.name(),
                    cell.hot,
                    cell.p50_us,
                    bound,
                    cells[0].hot,
                    base_p50,
                );
            }
        }
        // cliff position = the widest hot-set whose p50 blows past the
        // baseline; the release-diffed headline
        let cliff = cells
            .iter()
            .find(|cell| cell.p50_us > base_p50 * CLIFF_FACTOR)
            .map(|cell| cell.hot);

        let cell_names: Vec<String> =
            profile.cells.iter().map(|hot| hot.to_string()).collect();
        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("cells", cell_names.join("/"))
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("driver threads", profile.threads)
            .setting("measured iters per cell", profile.iterations)
            .setting("offered tps", profile.rate)
            .setting("concurrency", profile.concurrency)
            .setting(
                "cliff",
                cliff
                    .map(|hot| format!("hot-set {hot}"))
                    .unwrap_or_else(|| format!("none at {} tps", profile.rate)),
            );
        for cell in &cells {
            summary = summary
                .metric(format!("hot{} delivery p50 us", cell.hot), cell.p50_us)
                .metric(format!("hot{} delivery p95 us", cell.hot), cell.p95_us)
                .metric(format!("hot{} achieved tps", cell.hot), cell.achieved);
        }
        Ok(summary)
    }
}
