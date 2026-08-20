use std::{collections::HashMap, rc::Rc, sync::Arc, time::Duration};

use async_trait::async_trait;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, prep,
    profile::{self, ProfileValues},
    report,
    runner::{drive_threads, RunOutcome, ThreadRunConfig},
    stats::{ObservationsStats, StreamingStats},
    transport::subpool::{
        ConnReport, ExpectedWrites, ProducedLedger, SubscriberPool,
    },
    BaseCtx, ChainCtx, ErClient, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport, TxSender,
};

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;

const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(15);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const LAG_THRESHOLD: Duration = Duration::from_secs(1);
const CLIFF_P95_US: i32 = 1_000_000;

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: usize,
    ladder: &'static [usize],
    subscriber_threads: usize,
    threads: usize,
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    accounts: 16,
    ladder: &[2, 4, 8],
    subscriber_threads: 4,
    threads: 2,
    warmup: 100,
    iterations: 600,
    rate: 200,
    concurrency: 64,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    accounts: 64,
    ladder: &[16, 32, 64],
    subscriber_threads: 16,
    threads: 8,
    warmup: 10_000,
    iterations: 100_000,
    rate: 10_000,
    concurrency: 2_048,
};

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
    soak: None,
    deep: None,
};

fn shape(pool: &[Pubkey], id: u64) -> (Instruction, [Pubkey; 2]) {
    use crate::program::instruction::build;
    let len = pool.len() as u64;
    let base_index = ((id - 1) * 3) % len;
    let source = pool[base_index as usize];
    let first_dest = pool[((base_index + 1) % len) as usize];
    let second_dest = pool[((base_index + 2) % len) as usize];
    (
        build::account_data_copy(id, &[source], &[first_dest, second_dest]),
        [first_dest, second_dest],
    )
}

fn cell_expected_writes(
    pool: &[Pubkey],
    first_id: u64,
    iterations: u64,
) -> ExpectedWrites {
    let mut expected: ExpectedWrites = HashMap::new();
    for id in first_id + 1..=first_id + iterations {
        let (_, dests) = shape(pool, id);
        for dest in dests {
            expected.entry(dest).or_default().push(id);
        }
    }
    expected
}

fn drive_cell(
    er_rpc_url: String,
    config: ThreadRunConfig,
    id_offset: u64,
    pool: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
    produced: Option<Arc<ProducedLedger>>,
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
        let produced = produced.clone();
        move |id: u64| {
            let global_id = id_offset + id;
            let (ix, _) = shape(&pool, global_id);
            if let Some(ledger) = &produced {
                ledger.record(global_id);
            }
            let sender = senders[(global_id as usize) % senders.len()].clone();
            async move { sender.send(&[ix]).await.map(|_| ()) }
        }
    };
    drive_threads(config, factory)
}

struct CellOutcome {
    connections: usize,
    delivered: u64,
    failed: u64,
    achieved_tps: f64,
    missing_final: usize,
    incomplete: usize,
    received_min: u64,
    received_max: u64,
    lag: ObservationsStats,
    over_threshold: u64,
    received_total: u64,
}

pub struct WsFanoutThreshold;

#[async_trait(?Send)]
impl Scenario for WsFanoutThreshold {
    fn name(&self) -> &str {
        "redline/ws_fanout_threshold"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);
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
        for pda in &pool {
            check::poll(
                &format!("the ER clones the delegated pda {pda}"),
                CLONE_TIMEOUT,
                || async {
                    matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                },
            )
            .await?;
        }
        let payer_bytes: Arc<Vec<[u8; 64]>> = Arc::new(
            prep_payers.iter().map(|payer| payer.to_bytes()).collect(),
        );
        let pool: Arc<Vec<Pubkey>> = Arc::new(pool);
        let er_rpc_url = er.api().url().to_owned();

        let warmup = drive_cell(
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
            None,
        );
        check_eq!(
            warmup.failed,
            0,
            "warmup deliveries failed: {:?}",
            warmup.first_error
        )?;

        let mut id_cursor = profile.warmup;
        let mut cells: Vec<CellOutcome> = Vec::new();
        for &connections in profile.ladder {
            let produced = Arc::new(ProducedLedger::new(
                id_cursor + 1,
                profile.iterations as usize,
            ));
            let expected = Arc::new(cell_expected_writes(
                &pool,
                id_cursor,
                profile.iterations,
            ));
            let subscribers = SubscriberPool::start(
                er.ws_url(),
                &pool,
                connections,
                profile.subscriber_threads,
                produced.clone(),
                expected.clone(),
                Arc::new(crate::account_update_id),
                LAG_THRESHOLD,
            );
            subscribers
                .await_subscribed(pool.len(), SUBSCRIBE_TIMEOUT)
                .await?;

            let before = er.scrape_metrics().await?;
            let outcome = drive_cell(
                er_rpc_url.clone(),
                ThreadRunConfig {
                    threads: profile.threads,
                    iterations: profile.iterations,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                id_cursor,
                pool.clone(),
                payer_bytes.clone(),
                Some(produced.clone()),
            );
            check_eq!(
                outcome.failed,
                0,
                "ws{connections}: measured deliveries failed: {:?}",
                outcome.first_error
            )?;

            let missing_final =
                subscribers.await_final(&expected, DRAIN_TIMEOUT).await;
            let incomplete = subscribers.incomplete(&expected);
            let after = er.scrape_metrics().await?;
            let delta = MetricsDelta::new(before, after);
            if let Some(error) = subscribers.first_error() {
                return Err(format!(
                    "ws{connections}: subscriber pool failed: {error}"
                )
                .into());
            }
            let conn_reports: Vec<ConnReport> = subscribers.finalize();

            let received_min = conn_reports
                .iter()
                .map(|conn| conn.received)
                .min()
                .unwrap_or(0);
            let received_max = conn_reports
                .iter()
                .map(|conn| conn.received)
                .max()
                .unwrap_or(0);
            let received_total: u64 =
                conn_reports.iter().map(|conn| conn.received).sum();
            let over_threshold: u64 =
                conn_reports.iter().map(|conn| conn.over_threshold).sum();
            let mut lag_stats = StreamingStats::new();
            for conn_report in conn_reports {
                lag_stats.merge(conn_report.lag);
            }
            let lag = lag_stats.finalize(false);

            let cell_outcome = CellOutcome {
                connections,
                delivered: outcome.delivered,
                failed: outcome.failed,
                achieved_tps: outcome.achieved_rps(),
                missing_final,
                incomplete,
                received_min,
                received_max,
                lag,
                over_threshold,
                received_total,
            };
            eprintln!(
                "[redsuite] {}: ws{} — {:.0} tps, lag p50 {} us / p95 {} us / max {} us, \
                 received {}..{} per conn ({} total), missing finals {}, incomplete pairs {}, >1s {}",
                self.name(),
                connections,
                cell_outcome.achieved_tps,
                cell_outcome.lag.median,
                cell_outcome.lag.quantile95,
                cell_outcome.lag.max,
                received_min,
                received_max,
                received_total,
                missing_final,
                incomplete,
                over_threshold,
            );

            let cell_report =
                ScenarioReport::ok(&format!("{}/ws{connections}", self.name()))
                    .setting("profile", profile.name)
                    .setting("ws connections", connections)
                    .setting("subscriber threads", profile.subscriber_threads)
                    .setting("driver threads", profile.threads)
                    .setting(
                        "shape",
                        "read-write 3/tx (1 src + 2 dst data-copy)",
                    )
                    .setting("payers", profile.payers)
                    .setting("accounts", profile.accounts)
                    .setting("measured iters", profile.iterations)
                    .setting("offered tps", profile.rate)
                    .setting("concurrency", profile.concurrency)
                    .setting("drain timeout s", DRAIN_TIMEOUT.as_secs())
                    .observe("delivery us", outcome.delivery)
                    .observe("fanout lag us", cell_outcome.lag)
                    .metric("achieved tps", cell_outcome.achieved_tps)
                    .metric("writes produced", (profile.iterations * 2) as f64)
                    .metric("received total", received_total as f64)
                    .metric("received per-conn min", received_min as f64)
                    .metric("received per-conn max", received_max as f64)
                    .metric(
                        "received per-conn spread",
                        (received_max - received_min) as f64,
                    )
                    .metric("missing final states", missing_final as f64)
                    .metric("incomplete conn-account pairs", incomplete as f64)
                    .metric("notifications over 1s", over_threshold as f64)
                    .metric_if(
                        "validator txs in window",
                        delta.counter("mbv_transaction_count"),
                    )
                    .metric_if(
                        "validator tx processing avg us",
                        delta
                            .histogram_avg("mbv_transaction_processing_time")
                            .map(|seconds| seconds * 1e6),
                    );
            match report::persist(&cell_report) {
                Ok(path) => {
                    eprintln!("[redsuite]   cell report: {}", path.display())
                }
                Err(e) => eprintln!(
                    "[redsuite]   warning: cell report not persisted: {e}"
                ),
            }
            if let Some(failed_txs) =
                delta.counter("mbv_failed_transactions_count")
            {
                check_eq!(
                    failed_txs,
                    0.0,
                    "ws{connections}: transactions failed on the validator"
                )?;
            }
            cells.push(cell_outcome);
            id_cursor += profile.iterations;
        }

        let baseline = &cells[0];
        if baseline.lag.quantile95 >= CLIFF_P95_US {
            eprintln!(
                "[redsuite] {}: warning: baseline cell ws{} lag p95 {} us is \
                 already past the cliff threshold at minimum fan-out",
                self.name(),
                baseline.connections,
                baseline.lag.quantile95
            );
        }
        if baseline.missing_final > 0 {
            eprintln!(
                "[redsuite] {}: warning: baseline ws{}: {} (connection, \
                 account) pairs never received the final produced state",
                self.name(),
                baseline.connections,
                baseline.missing_final
            );
        }
        if baseline.received_min != baseline.received_max {
            eprintln!(
                "[redsuite] {}: warning: baseline ws{}: connections received \
                 unequal notification counts ({}..{})",
                self.name(),
                baseline.connections,
                baseline.received_min,
                baseline.received_max
            );
        }

        for cell in &cells {
            check!(
                cell.delivered > 0,
                "INVALID: ws{} delivered nothing",
                cell.connections
            )?;
        }

        let cliff = cells
            .iter()
            .find(|cell| {
                cell.missing_final > 0
                    || cell.received_min != cell.received_max
                    || cell.lag.quantile95 >= CLIFF_P95_US
            })
            .map(|cell| cell.connections)
            .unwrap_or(0);
        eprintln!(
            "[redsuite] {}: cliff {}",
            self.name(),
            if cliff == 0 {
                "not reached on this ladder".to_owned()
            } else {
                format!("at ws{cliff}")
            }
        );

        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting(
                "ws ladder",
                profile
                    .ladder
                    .iter()
                    .map(|connections| connections.to_string())
                    .collect::<Vec<_>>()
                    .join("/"),
            )
            .setting("subscriber threads", profile.subscriber_threads)
            .setting("driver threads", profile.threads)
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("measured iters per cell", profile.iterations)
            .setting("offered tps", profile.rate)
            .setting("concurrency", profile.concurrency)
            .metric("cliff at ws conns (0 = none)", cliff as f64)
            .metric("baseline missing finals", baseline.missing_final as f64)
            .metric(
                "baseline received spread",
                (baseline.received_max - baseline.received_min) as f64,
            );
        for cell in &cells {
            let cell_name = format!("ws{}", cell.connections);
            summary = summary
                .metric(
                    format!("{cell_name} lag p50 us"),
                    cell.lag.median as f64,
                )
                .metric(
                    format!("{cell_name} lag p95 us"),
                    cell.lag.quantile95 as f64,
                )
                .metric(format!("{cell_name} lag max us"), cell.lag.max as f64)
                .metric(format!("{cell_name} achieved tps"), cell.achieved_tps)
                .metric(
                    format!("{cell_name} received spread"),
                    (cell.received_max - cell.received_min) as f64,
                )
                .metric(
                    format!("{cell_name} missing finals"),
                    cell.missing_final as f64,
                )
                .metric(
                    format!("{cell_name} incomplete pairs"),
                    cell.incomplete as f64,
                )
                .metric(
                    format!("{cell_name} over 1s"),
                    cell.over_threshold as f64,
                )
                .metric(
                    format!("{cell_name} received total"),
                    cell.received_total as f64,
                )
                .metric(format!("{cell_name} failed"), cell.failed as f64);
        }
        Ok(summary)
    }
}
