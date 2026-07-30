use std::{rc::Rc, time::Duration};

use async_trait::async_trait;
use instruction::Instruction;
use pubkey::Pubkey;
use redsuite_core::{
    assert::poll_until,
    host, prep,
    profile::select as select_profile,
    report,
    runner::{drive, RunConfig, RunOutcome},
    topology, BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport, TxSender,
};

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const FLATNESS_P50_FACTOR: f64 = 1.5;

const TX_PROCESSING_HISTOGRAM: &str = "mbv_transaction_processing_time";

const DATABASE_SIZE_BYTES: u64 = 3 * 1024 * 1024 * 1024;
const INDEX_SIZE_BYTES: u64 = 256 * 1024 * 1024;
const PROD_SUPERBLOCK_SLOTS: u64 = 72_000;

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: usize,
    fill: u64,
    window: u64,
    rate: u32,
    concurrency: usize,
    frequent_superblock_slots: u64,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    accounts: 16,
    fill: 2_000,
    window: 2_000,
    rate: 200,
    concurrency: 64,
    frequent_superblock_slots: 150,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    accounts: 64,
    fill: 60_000,
    window: 30_000,
    rate: 1_000,
    concurrency: 256,
    frequent_superblock_slots: 400,
};

fn profile(scenario: &str) -> &'static Profile {
    match select_profile(scenario, &["lite", "full"]) {
        "full" => &FULL,
        _ => &LITE,
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

struct WindowOutcome {
    outcome: RunOutcome,
    tx_processing_avg_us: Option<f64>,
    storage_growth_bytes: u64,
}

async fn drive_window(
    er: &ErCtx,
    storage_dir: &std::path::Path,
    senders: &[TxSender],
    pool: &[Pubkey],
    first_id: u64,
    profile: &Profile,
) -> Result<WindowOutcome> {
    let storage_before = host::dir_size_bytes(storage_dir)?;
    let before = er.scrape_metrics().await?;
    let outcome = drive(
        RunConfig {
            iterations: profile.window,
            rate: profile.rate,
            concurrency: profile.concurrency,
        },
        |iteration| {
            let id = first_id + iteration;
            let sender = senders[(id as usize) % senders.len()].clone();
            let ix = shape(pool, id);
            async move { sender.send(&[ix]).await.map(|_| ()) }
        },
    )
    .await;
    let after = er.scrape_metrics().await?;
    let delta = MetricsDelta::new(before, after);
    let storage_after = host::dir_size_bytes(storage_dir)?;
    Ok(WindowOutcome {
        outcome,
        tx_processing_avg_us: delta
            .histogram_avg(TX_PROCESSING_HISTOGRAM)
            .map(|seconds| seconds * 1e6),
        storage_growth_bytes: storage_after.saturating_sub(storage_before),
    })
}

struct CellOutcome {
    name: &'static str,
    superblock_slots: u64,
    fill_growth_bytes: u64,
    windows: [WindowOutcome; 2],
}

pub struct StorageProdsizeSustain;

#[async_trait(?Send)]
impl Scenario for StorageProdsizeSustain {
    fn name(&self) -> &str {
        "redline/storage_prodsize_sustain"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = profile(self.name());
        let prep_payers =
            prep::funded_payers(base, profile.payers, PREP_PAYER_LAMPORTS)
                .await?;
        let pool = crate::init_delegated_accounts_batched(
            base,
            &prep_payers,
            profile.accounts,
            crate::ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        let payers: Vec<Rc<keypair::Keypair>> =
            prep_payers.into_iter().map(Rc::new).collect();

        let cells_spec = [
            ("prod_cadence", PROD_SUPERBLOCK_SLOTS),
            ("frequent_snapshot", profile.frequent_superblock_slots),
        ];

        let mut cells: Vec<CellOutcome> = Vec::new();
        for (cell_name, superblock_slots) in cells_spec {
            let private = topology::private_er(
                base,
                topology::ErOptions {
                    label: format!("s11-{cell_name}"),
                    env: vec![
                        (
                            "MBV_ENGINE__BLOCKSTORE__BLOCKTIME".to_owned(),
                            "50ms".to_owned(),
                        ),
                        (
                            "MBV_ENGINE__BLOCKSTORE__SUPERBLOCK".to_owned(),
                            superblock_slots.to_string(),
                        ),
                    ],
                    request_timeout: None,
                    ..Default::default()
                },
            )
            .await?;
            let cell_er = private.ctx();
            for pda in &pool {
                poll_until(CLONE_TIMEOUT, || async {
                    matches!(cell_er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                })
                .await;
            }
            let senders: Vec<TxSender> = payers
                .iter()
                .map(|payer| cell_er.sender(payer.clone()))
                .collect();

            let storage_at_boot = host::dir_size_bytes(private.storage_dir())?;
            let fill = drive(
                RunConfig {
                    iterations: profile.fill,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                |id| {
                    let sender = senders[(id as usize) % senders.len()].clone();
                    let ix = shape(&pool, id);
                    async move { sender.send(&[ix]).await.map(|_| ()) }
                },
            )
            .await;
            assert_eq!(
                fill.failed, 0,
                "{cell_name}: fill deliveries failed: {:?}",
                fill.first_error
            );
            let fill_growth_bytes =
                host::dir_size_bytes(private.storage_dir())?
                    .saturating_sub(storage_at_boot);

            let window_a = drive_window(
                cell_er,
                private.storage_dir(),
                &senders,
                &pool,
                profile.fill,
                profile,
            )
            .await?;
            let window_b = drive_window(
                cell_er,
                private.storage_dir(),
                &senders,
                &pool,
                profile.fill + profile.window,
                profile,
            )
            .await?;

            let cell = CellOutcome {
                name: cell_name,
                superblock_slots,
                fill_growth_bytes,
                windows: [window_a, window_b],
            };
            eprintln!(
                "[redsuite] {}: {} (superblock {}) — fill grew ledger {:.1} MB; \
                 window A p50 {} us / p95 {} us @ {:.0} tps, \
                 window B p50 {} us / p95 {} us @ {:.0} tps, \
                 validator tx avg {} / {} us",
                self.name(),
                cell.name,
                cell.superblock_slots,
                cell.fill_growth_bytes as f64 / 1e6,
                cell.windows[0].outcome.delivery.median,
                cell.windows[0].outcome.delivery.quantile95,
                cell.windows[0].outcome.achieved_rps(),
                cell.windows[1].outcome.delivery.median,
                cell.windows[1].outcome.delivery.quantile95,
                cell.windows[1].outcome.achieved_rps(),
                cell.windows[0]
                    .tx_processing_avg_us
                    .map(|us| format!("{us:.1}"))
                    .unwrap_or_else(|| "n/a".to_owned()),
                cell.windows[1]
                    .tx_processing_avg_us
                    .map(|us| format!("{us:.1}"))
                    .unwrap_or_else(|| "n/a".to_owned()),
            );

            let mut cell_report =
                ScenarioReport::ok(&format!("{}/{}", self.name(), cell.name))
                    .setting("profile", profile.name)
                    .setting("superblock slots", cell.superblock_slots)
                    .setting("database size", DATABASE_SIZE_BYTES)
                    .setting("index size", INDEX_SIZE_BYTES)
                    .setting("block time", "50ms")
                    .setting("payers", profile.payers)
                    .setting("accounts", profile.accounts)
                    .setting("fill iters", profile.fill)
                    .setting("window iters", profile.window)
                    .setting("offered tps", profile.rate)
                    .setting("concurrency", profile.concurrency)
                    .metric(
                        "fill storage growth mb",
                        cell.fill_growth_bytes as f64 / 1e6,
                    );
            for (window_name, window) in
                [("A", &cell.windows[0]), ("B", &cell.windows[1])]
            {
                cell_report = cell_report
                    .observe(
                        format!("window {window_name} delivery us"),
                        window.outcome.delivery,
                    )
                    .metric(
                        format!("window {window_name} achieved tps"),
                        window.outcome.achieved_rps(),
                    )
                    .metric(
                        format!("window {window_name} storage growth mb"),
                        window.storage_growth_bytes as f64 / 1e6,
                    )
                    .metric_if(
                        format!("window {window_name} validator tx avg us"),
                        window.tx_processing_avg_us,
                    );
            }
            match report::persist(&cell_report) {
                Ok(path) => {
                    eprintln!("[redsuite]   cell report: {}", path.display())
                }
                Err(e) => eprintln!(
                    "[redsuite]   warning: cell report not persisted: {e}"
                ),
            }
            cells.push(cell);
            drop(private);
        }

        for cell in &cells {
            assert!(
                cell.fill_growth_bytes > 0,
                "INVALID: {}: the fill phase did not grow the storage",
                cell.name
            );
            for window in &cell.windows {
                assert_eq!(
                    window.outcome.failed, 0,
                    "{}: measured deliveries failed: {:?}",
                    cell.name, window.outcome.first_error
                );
            }
            let p50_a = cell.windows[0].outcome.delivery.median.max(1) as f64;
            let p50_b = cell.windows[1].outcome.delivery.median as f64;
            if p50_b > p50_a * FLATNESS_P50_FACTOR {
                eprintln!(
                    "[redsuite] {}: warning: {}: p50 drifted across equal \
                     windows ({p50_a:.0} -> {p50_b:.0} us)",
                    self.name(),
                    cell.name,
                );
            }
        }

        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("database size", DATABASE_SIZE_BYTES)
            .setting("index size", INDEX_SIZE_BYTES)
            .setting("block time", "50ms")
            .setting(
                "superblocks",
                format!(
                    "prod {} / frequent {}",
                    PROD_SUPERBLOCK_SLOTS, profile.frequent_superblock_slots
                ),
            )
            .setting("offered tps", profile.rate)
            .setting("concurrency", profile.concurrency);
        for cell in &cells {
            summary = summary
                .metric(
                    format!("{} window A p50 us", cell.name),
                    cell.windows[0].outcome.delivery.median as f64,
                )
                .metric(
                    format!("{} window B p50 us", cell.name),
                    cell.windows[1].outcome.delivery.median as f64,
                )
                .metric(
                    format!("{} window p50 B/A ratio", cell.name),
                    cell.windows[1].outcome.delivery.median as f64
                        / cell.windows[0].outcome.delivery.median.max(1) as f64,
                )
                .metric(
                    format!("{} window B p95 us", cell.name),
                    cell.windows[1].outcome.delivery.quantile95 as f64,
                )
                .metric(
                    format!("{} fill growth mb", cell.name),
                    cell.fill_growth_bytes as f64 / 1e6,
                )
                .metric_if(
                    format!("{} window B validator tx avg us", cell.name),
                    cell.windows[1].tx_processing_avg_us,
                );
        }
        Ok(summary)
    }
}
