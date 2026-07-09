//! S4 `sched_hot_account_cliff` — scheduler intake under hot-account
//! contention. Few hot accounts serialize execution; the campaign measured a
//! client-latency cliff between 16 and 8 hot accounts at ~13.5k delivered
//! RPS (15 ms → 170–280 ms). At the rates this single-threaded client
//! sustains the cliff may not trigger — then the cells document the healthy
//! floor and the cliff position is release-diffed: a cliff appearing at
//! these rates is a regression.

use std::{rc::Rc, time::Duration};

use async_trait::async_trait;
use redline::program::instruction::build;
use redsuite_core::{
    assert::poll_until,
    prep, report, run_scenario,
    runner::{drive, RunConfig},
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
    TxSender,
};

const PAYER_LAMPORTS: u64 = 2_000_000_000;
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
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    cells: &[16, 4],
    warmup: 100,
    iterations: 1_000,
    rate: 500,
    concurrency: 256,
};

// The rate must be one this single-threaded client serves with sub-ms
// deliveries, not merely sustains: on this host the hard throughput ceiling
// is ~2.3k tps but client-side queueing already dominates latency at 2k
// (~450 us of client CPU per request → 91% utilization). Above that the
// sweep measures the harness, not the scheduler (R1/S14) — the INVALID
// baseline gate below catches it. 1k is proven healthy by S1.
const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    cells: &[32, 16, 8, 4],
    warmup: 2_000,
    iterations: 20_000,
    rate: 1_000,
    concurrency: 1_024,
};

fn profile() -> &'static Profile {
    match std::env::var("REDSUITE_PROFILE") {
        Ok(name) if name == "full" => &FULL,
        Ok(name) if name == "lite" => &LITE,
        Ok(name) => panic!("unknown REDSUITE_PROFILE `{name}` (lite|full)"),
        Err(_) => &LITE,
    }
}

struct CellResult {
    hot: u8,
    p50_us: f64,
    p95_us: f64,
    achieved: f64,
}

struct HotAccountCliff;

#[async_trait(?Send)]
impl Scenario for HotAccountCliff {
    fn name(&self) -> &str {
        "redline/hot_account_cliff"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = profile();
        let pool = *profile.cells.iter().max().unwrap();
        let payers =
            prep::funded_payers(base, profile.payers, PAYER_LAMPORTS).await?;
        let pdas = redline::init_delegated_accounts(
            base,
            &payers[0],
            pool,
            redline::ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        for pda in &pdas {
            poll_until(Duration::from_secs(15), || async {
                matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == redline::ACCOUNT_SPACE as usize)
            })
            .await;
        }
        let senders: Vec<TxSender> = payers
            .into_iter()
            .map(|payer| er.sender(Rc::new(payer)))
            .collect();

        let mut offset = 0u64;
        let mut cells: Vec<CellResult> = Vec::new();
        for &hot in profile.cells {
            let hot_set = &pdas[..hot as usize];
            // same read-write 3/tx shape as S1, confined to the hot set
            // (hot-set sizes are powers of two — coprime with the stride)
            let shape = |id: u64| {
                let len = hot_set.len() as u64;
                let base_index = ((id - 1) * 3) % len;
                build::account_data_copy(
                    id,
                    &[hot_set[base_index as usize]],
                    &[
                        hot_set[((base_index + 1) % len) as usize],
                        hot_set[((base_index + 2) % len) as usize],
                    ],
                )
            };
            let request = |iteration: u64| {
                let id = offset + iteration;
                let sender = senders[(id as usize) % senders.len()].clone();
                let ix = shape(id);
                async move { sender.send(&[ix]).await.map(|_| ()) }
            };

            let warm = drive(
                RunConfig {
                    iterations: profile.warmup,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                request,
            )
            .await;
            assert_eq!(
                warm.failed, 0,
                "hot{hot} warmup failed: {:?}",
                warm.first_error
            );
            offset += profile.warmup;

            let before = er.scrape_metrics().await?;
            let request = |iteration: u64| {
                let id = offset + iteration;
                let sender = senders[(id as usize) % senders.len()].clone();
                let ix = shape(id);
                async move { sender.send(&[ix]).await.map(|_| ()) }
            };
            let outcome = drive(
                RunConfig {
                    iterations: profile.iterations,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                request,
            )
            .await;
            offset += profile.iterations;
            assert_eq!(
                outcome.failed, 0,
                "hot{hot} deliveries failed: {:?}",
                outcome.first_error
            );

            // delivery only proves acceptance — drain the intake queue so
            // the after-scrape covers everything the cell caused
            if let Some(seen) = before.get("mbv_transaction_count") {
                let target = seen + profile.iterations as f64;
                poll_until(Duration::from_secs(120), || async {
                    matches!(
                        er.scrape_metrics().await.ok().and_then(|metrics| metrics.get("mbv_transaction_count")),
                        Some(count) if count >= target
                    )
                })
                .await;
            }
            let after = er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);
            if let Some(failed) = delta.counter("mbv_failed_transactions_count")
            {
                assert_eq!(
                    failed, 0.0,
                    "hot{hot}: transactions failed on the validator"
                );
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
        assert!(
            base_p50 <= HEALTHY_P50_US,
            "INVALID sweep: baseline hot{} p50 {:.0} us is itself \
             unhealthy (> {HEALTHY_P50_US:.0} us) — offered {} tps exceeds \
             what this harness+validator pair sustains",
            cells[0].hot,
            base_p50,
            profile.rate,
        );
        for cell in cells.iter().filter(|cell| cell.hot >= 16) {
            let bound = (base_p50 * FLAT_FACTOR).max(HEALTHY_P50_US);
            assert!(
                cell.p50_us <= bound,
                "hot{} p50 {:.0} us exceeds the healthy bound {:.0} us \
                 (baseline hot{} p50 {:.0} us)",
                cell.hot,
                cell.p50_us,
                bound,
                cells[0].hot,
                base_p50,
            );
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

#[tokio::test]
async fn hot_account_cliff() {
    run_scenario(HotAccountCliff).await;
}
