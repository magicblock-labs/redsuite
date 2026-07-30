use std::{
    cell::RefCell,
    collections::HashSet,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::{
    api::custom_error_code,
    assert::poll_until,
    prep,
    profile::select as select_profile,
    receipt, report,
    runner::{drive, RunConfig},
    stats::StreamingStats,
    topology, Api, BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario,
    ScenarioReport,
};
use signer::Signer;

use crate::program::{instruction::build, DELEGATION_PROGRAM_ID};

const PAYER_LAMPORTS: u64 = 5_000_000_000;

const ACCOUNT_SPACE: u32 = 128;

const WARMUP_COMMITS: u64 = 2;

const RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);

const PROBE_RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);
const REJECTION_TIMEOUT: Duration = Duration::from_secs(10);
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);

struct Profile {
    name: &'static str,
    widths: &'static [usize],
    commits: u64,
    rate: u32,
    concurrency: usize,
    pool: u8,
    probes: bool,
}

const LITE: Profile = Profile {
    name: "lite",
    widths: &[1, 2, 4],
    commits: 8,
    rate: 2,
    concurrency: 4,
    pool: 16,
    probes: false,
};

const FULL: Profile = Profile {
    name: "full",
    widths: &[1, 2, 4],
    commits: 30,
    rate: 2,
    concurrency: 8,
    pool: 48,
    probes: true,
};

fn profile(scenario: &str) -> &'static Profile {
    match select_profile(scenario, &["lite", "full"]) {
        "full" => &FULL,
        _ => &LITE,
    }
}

#[derive(Default)]
struct CellTally {
    er_delivery: StreamingStats,
    round_trip: StreamingStats,
    base_signatures: usize,
    included_mismatch: usize,
}

struct CellSummary {
    width: usize,
    round_trip_p50_us: f64,
    round_trip_p95_us: f64,
    base_txs_per_commit: f64,
}

fn commit_set(pdas: &[Pubkey], width: usize, id: u64) -> Vec<Pubkey> {
    let pool_len = pdas.len();
    let start_index = ((id as usize - 1) * width) % pool_len;
    (0..width)
        .map(|position| pdas[(start_index + position) % pool_len])
        .collect()
}

enum SchedulingOutcome {
    Confirmed,
    Rejected(Option<u32>),
}

async fn scheduling_outcome(
    er_api: &Api,
    commit_signature: &signature::Signature,
    timeout: Duration,
) -> redsuite_core::Result<SchedulingOutcome> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(status) =
            er_api.get_signature_status(commit_signature).await?
        {
            if let Some(err) = &status.err {
                return Ok(SchedulingOutcome::Rejected(custom_error_code(err)));
            }
            if status.confirmed {
                return Ok(SchedulingOutcome::Confirmed);
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "no status for the probe commit {commit_signature} within \
                 {timeout:?}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

const SIGNATURE_SCAN_LIMIT: usize = 1000;

struct BaseFlowSnapshot {
    flow: HashSet<String>,
    alt: HashSet<String>,
}

impl BaseFlowSnapshot {
    fn new_flow_since(&self, earlier: &BaseFlowSnapshot) -> usize {
        self.flow.difference(&earlier.flow).count()
    }

    fn new_alt_since(&self, earlier: &BaseFlowSnapshot) -> usize {
        self.alt.difference(&earlier.alt).count()
    }
}

async fn snapshot_base_flow(base_api: &Api) -> Result<BaseFlowSnapshot> {
    let committor_program: Pubkey = topology::COMMITTOR_ID
        .parse()
        .expect("pinned committor program id parses");
    let alt_program: Pubkey = sdk_ids::address_lookup_table::ID;
    let mut flow = HashSet::new();
    flow.extend(
        base_api
            .get_signatures_for_address(
                &DELEGATION_PROGRAM_ID,
                SIGNATURE_SCAN_LIMIT,
            )
            .await?,
    );
    flow.extend(
        base_api
            .get_signatures_for_address(
                &committor_program,
                SIGNATURE_SCAN_LIMIT,
            )
            .await?,
    );
    let alt = base_api
        .get_signatures_for_address(&alt_program, SIGNATURE_SCAN_LIMIT)
        .await?
        .into_iter()
        .collect();
    Ok(BaseFlowSnapshot { flow, alt })
}

pub struct CommitWidthEnvelope;

#[async_trait(?Send)]
impl Scenario for CommitWidthEnvelope {
    fn name(&self) -> &str {
        "redline/commit_width_envelope"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = profile(self.name());
        let payer = prep::funded_payer(base, PAYER_LAMPORTS).await?;
        let payer_pubkey = payer.pubkey();
        let pdas = crate::init_delegated_accounts(
            base,
            &payer,
            profile.pool,
            ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        for pda in &pdas {
            poll_until(CLONE_TIMEOUT, || async {
                matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == ACCOUNT_SPACE as usize)
            })
            .await;
        }
        let sender = er.sender(Rc::new(payer));

        // the whole pipeline per request: deliver on the ER, await the
        // ScheduledCommitSent receipt, confirm its base-layer signatures
        let make_request =
            |width: usize, offset: u64, tally: Rc<RefCell<CellTally>>| {
                let sender = sender.clone();
                let er_api = er.api().clone();
                let base_api = base.api().clone();
                let pdas = pdas.clone();
                move |iteration: u64| {
                    let id = offset + iteration;
                    let accounts = commit_set(&pdas, width, id);
                    let ix =
                        build::commit_accounts(id, payer_pubkey, &accounts);
                    let sender = sender.clone();
                    let er_api = er_api.clone();
                    let base_api = base_api.clone();
                    let tally = tally.clone();
                    async move {
                        let started = Instant::now();
                        let tx = sender.prepare(&[ix]).await?;
                        let commit_signature = sender.deliver(&tx).await?;
                        tally
                            .borrow_mut()
                            .er_delivery
                            .push(started.elapsed().as_micros() as u32);
                        let commit_receipt = receipt::fetch_commit_receipt(
                            &er_api,
                            &commit_signature,
                            RECEIPT_TIMEOUT,
                        )
                        .await?;
                        if let Some(message) = &commit_receipt.error_message {
                            return Err(format!(
                                "commit {id} intent failed: {message}"
                            )
                            .into());
                        }
                        let round_trip_us =
                            started.elapsed().as_micros() as u32;
                        receipt::confirm_base_signatures(
                            &base_api,
                            &commit_receipt,
                            BASE_CONFIRM_TIMEOUT,
                        )
                        .await?;
                        let mut tally = tally.borrow_mut();
                        tally.round_trip.push(round_trip_us);
                        tally.base_signatures +=
                            commit_receipt.base_signatures.len();
                        let mut reported = commit_receipt.included.clone();
                        reported.sort();
                        let mut expected = accounts;
                        expected.sort();
                        if reported != expected {
                            tally.included_mismatch += 1;
                        }
                        Ok(())
                    }
                }
            };

        let mut offset = 0u64;
        let mut cells: Vec<CellSummary> = Vec::new();
        for &width in profile.widths {
            let warmup_tally = Rc::new(RefCell::new(CellTally::default()));
            let warmup = drive(
                RunConfig {
                    iterations: WARMUP_COMMITS,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                make_request(width, offset, warmup_tally),
            )
            .await;
            offset += WARMUP_COMMITS;
            assert_eq!(
                warmup.failed, 0,
                "w{width} warmup commits failed: {:?}",
                warmup.first_error
            );
            // post-apply buffer closes can trail the receipt — let warmup
            // stragglers land before the base-flow snapshot
            tokio::time::sleep(Duration::from_secs(1)).await;

            let flow_before = snapshot_base_flow(base.api()).await?;
            let before = er.scrape_metrics().await?;
            let tally = Rc::new(RefCell::new(CellTally::default()));
            let outcome = drive(
                RunConfig {
                    iterations: profile.commits,
                    rate: profile.rate,
                    concurrency: profile.concurrency,
                },
                make_request(width, offset, tally.clone()),
            )
            .await;
            offset += profile.commits;
            assert_eq!(
                outcome.failed, 0,
                "w{width} commits failed: {:?}",
                outcome.first_error
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            let after = er.scrape_metrics().await?;
            let flow_after = snapshot_base_flow(base.api()).await?;
            let delta = MetricsDelta::new(before, after);

            if let Some(intents) = delta.counter("mbv_committor_intents_count")
            {
                assert!(
                    intents >= profile.commits as f64,
                    "w{width}: only {intents} intents in the measured window, \
                     expected at least {}",
                    profile.commits
                );
            }
            let failed_intents = delta
                .counter_all("mbv_committor_failed_intents_count")
                .unwrap_or(0.0);
            assert_eq!(
                failed_intents, 0.0,
                "w{width}: {failed_intents} intents failed"
            );
            let alt_tables_used = delta
                .counter("mbv_committor_intent_alt_count_sum")
                .unwrap_or(0.0);
            assert_eq!(
                alt_tables_used, 0.0,
                "w{width} must stay on the no-LUT path"
            );

            let alt_base_txs = flow_after.new_alt_since(&flow_before);
            assert_eq!(
                alt_base_txs, 0,
                "w{width}: ALT create/extend txs appeared on base"
            );
            let backlog = delta
                .gauge("mbv_committor_intent_backlog_count")
                .unwrap_or(0.0);
            assert_eq!(
                backlog, 0.0,
                "w{width}: intent backlog not drained at window close"
            );

            let tally = Rc::try_unwrap(tally)
                .unwrap_or_else(|_| panic!("commit tasks still hold the tally"))
                .into_inner();
            assert_eq!(
                tally.included_mismatch, 0,
                "w{width}: receipts listed unexpected committed accounts"
            );
            let round_trip = tally.round_trip.finalize(false);
            let er_delivery = tally.er_delivery.finalize(false);
            assert_eq!(
                round_trip.count, profile.commits as usize,
                "w{width}: not every commit produced a receipt round-trip"
            );

            let receipt_txs_per_commit =
                tally.base_signatures as f64 / profile.commits as f64;
            assert!(
                receipt_txs_per_commit >= 1.0,
                "w{width}: commits averaged fewer than one base tx"
            );
            let flow_txs_per_commit = flow_after.new_flow_since(&flow_before)
                as f64
                / profile.commits as f64;

            eprintln!(
                "[redsuite] {}: w{width}: round-trip p50 {:.0} us / p95 {:.0} us, \
                 {:.1} base txs/commit ({:.1} receipt-listed), {:.2} commits/s achieved",
                self.name(),
                round_trip.median,
                round_trip.quantile95,
                flow_txs_per_commit,
                receipt_txs_per_commit,
                profile.commits as f64 / outcome.wall.as_secs_f64(),
            );

            let cell_report =
                ScenarioReport::ok(&format!("{}/w{width}", self.name()))
                    .setting("profile", profile.name)
                    .setting("width", width)
                    .setting("account space", ACCOUNT_SPACE)
                    .setting("commits", profile.commits)
                    .setting("offered rate /s", profile.rate)
                    .setting("concurrency", profile.concurrency)
                    .setting("pool", profile.pool)
                    .setting("receipt timeout s", RECEIPT_TIMEOUT.as_secs())
                    .observe("er delivery us", er_delivery)
                    .observe("commit round-trip us", round_trip)
                    .metric("base txs per commit", flow_txs_per_commit)
                    .metric(
                        "receipt base txs per commit",
                        receipt_txs_per_commit,
                    )
                    .metric(
                        "achieved commits /s",
                        profile.commits as f64 / outcome.wall.as_secs_f64(),
                    )
                    .metric_if(
                        "validator intent exec avg s",
                        delta.histogram_avg_all(
                            "mbv_committor_intent_execution_time_histogram_v2",
                        ),
                    )
                    .metric_if(
                        "intents in window",
                        delta.counter("mbv_committor_intents_count"),
                    );
            match report::persist(&cell_report) {
                Ok(path) => {
                    eprintln!("[redsuite]   cell report: {}", path.display())
                }
                Err(e) => eprintln!(
                    "[redsuite]   warning: cell report not persisted: {e}"
                ),
            }

            cells.push(CellSummary {
                width,
                round_trip_p50_us: round_trip.median as f64,
                round_trip_p95_us: round_trip.quantile95 as f64,
                base_txs_per_commit: flow_txs_per_commit,
            });
        }

        let mut probe_settings: Vec<(String, String)> = Vec::new();
        let mut probe_metrics: Vec<(String, f64)> = Vec::new();
        if profile.probes {
            // width 15 exceeds the no-LUT envelope: pre-gate validators
            // deliver it via ALTs, gate validators (>= 0.13.7) refuse it at
            // scheduling — both are the envelope working as designed
            {
                let accounts: Vec<Pubkey> =
                    pdas.iter().copied().take(15).collect();
                offset += 1;
                let ix =
                    build::commit_accounts(offset, payer_pubkey, &accounts);
                let flow_before = snapshot_base_flow(base.api()).await?;
                let before = er.scrape_metrics().await?;
                let started = Instant::now();
                let commit_signature = sender.send(&[ix]).await?;
                match scheduling_outcome(
                    er.api(),
                    &commit_signature,
                    REJECTION_TIMEOUT,
                )
                .await?
                {
                    SchedulingOutcome::Rejected(code) => {
                        assert_eq!(
                            code,
                            Some(receipt::INTENT_TOO_LARGE_ERR),
                            "the width-15 probe was rejected with an \
                             unexpected code"
                        );
                        eprintln!(
                            "[redsuite] {}: probe w15: refused at scheduling \
                             (intent size gate)",
                            self.name()
                        );
                        probe_settings.push((
                            "probe w15".to_owned(),
                            "refused at scheduling (intent size gate)"
                                .to_owned(),
                        ));
                    }
                    SchedulingOutcome::Confirmed => {
                        let alt_receipt = receipt::fetch_commit_receipt(
                            er.api(),
                            &commit_signature,
                            PROBE_RECEIPT_TIMEOUT,
                        )
                        .await?;
                        if let Some(message) = &alt_receipt.error_message {
                            return Err(format!(
                                "width-15 probe intent failed: {message}"
                            )
                            .into());
                        }
                        receipt::confirm_base_signatures(
                            base.api(),
                            &alt_receipt,
                            BASE_CONFIRM_TIMEOUT,
                        )
                        .await?;
                        let wall = started.elapsed();
                        let delta = MetricsDelta::new(
                            before,
                            er.scrape_metrics().await?,
                        );
                        let flow_after = snapshot_base_flow(base.api()).await?;
                        if let Some(alt_tables_used) =
                            delta.counter("mbv_committor_intent_alt_count_sum")
                        {
                            assert!(
                                alt_tables_used >= 1.0,
                                "a width-15 commit should ride ALTs"
                            );
                        }
                        let alt_base_txs =
                            flow_after.new_alt_since(&flow_before);
                        assert!(
                            alt_base_txs >= 1,
                            "a width-15 commit should create/extend ALTs on \
                             base"
                        );
                        eprintln!(
                            "[redsuite] {}: probe w15: {:.1} s, {} flow + {} alt base txs",
                            self.name(),
                            wall.as_secs_f64(),
                            flow_after.new_flow_since(&flow_before),
                            alt_base_txs,
                        );
                        probe_settings.push((
                            "probe w15".to_owned(),
                            "delivered via ALTs".to_owned(),
                        ));
                        probe_metrics.push((
                            "probe w15 round-trip s".to_owned(),
                            wall.as_secs_f64(),
                        ));
                        probe_metrics.push((
                            "probe w15 base flow txs".to_owned(),
                            flow_after.new_flow_since(&flow_before) as f64,
                        ));
                        probe_metrics.push((
                            "probe w15 alt base txs".to_owned(),
                            alt_base_txs as f64,
                        ));
                    }
                }
            }

            {
                let accounts: Vec<Pubkey> =
                    pdas.iter().copied().take(30).collect();
                offset += 1;
                let ix =
                    build::commit_accounts(offset, payer_pubkey, &accounts);
                let before = er.scrape_metrics().await?;
                let commit_signature = sender.send(&[ix]).await?;
                let outcome_note = match scheduling_outcome(
                    er.api(),
                    &commit_signature,
                    REJECTION_TIMEOUT,
                )
                .await?
                {
                    SchedulingOutcome::Rejected(code) => {
                        assert_eq!(
                            code,
                            Some(receipt::INTENT_TOO_LARGE_ERR),
                            "the width-30 probe was rejected with an \
                             unexpected code"
                        );
                        "refused at scheduling (intent size gate)".to_owned()
                    }
                    SchedulingOutcome::Confirmed => {
                        let receipt_outcome = receipt::fetch_commit_receipt(
                            er.api(),
                            &commit_signature,
                            Duration::from_secs(45),
                        )
                        .await;
                        let delta = MetricsDelta::new(
                            before,
                            er.scrape_metrics().await?,
                        );
                        let failed_intents = delta
                            .counter_all("mbv_committor_failed_intents_count")
                            .unwrap_or(0.0);
                        probe_metrics.push((
                            "probe w30 failed intents".to_owned(),
                            failed_intents,
                        ));
                        match receipt_outcome {
                            Ok(fit_receipt) => {
                                assert!(
                                    !fit_receipt.succeeded(),
                                    "a width-30 commit must exceed the fit \
                                     envelope, but the intent succeeded"
                                );
                                assert_eq!(
                                    fit_receipt.receipt_err_code,
                                    Some(receipt::INTENT_FAILED_CODE),
                                    "width-30 receipt should err with the \
                                     intent-failed code"
                                );
                                format!(
                                    "intent failed as expected: {}",
                                    fit_receipt
                                        .error_message
                                        .unwrap_or_default()
                                )
                            }
                            // a failed receipt tx may be unqueryable — then
                            // the failed-intents counter must attest to the
                            // rejection
                            Err(fetch_error) => {
                                assert!(
                                    failed_intents >= 1.0,
                                    "width-30 probe: no failed receipt and \
                                     no failed-intent count ({fetch_error})"
                                );
                                format!(
                                    "no receipt ({fetch_error}); \
                                     failed-intents delta {failed_intents}"
                                )
                            }
                        }
                    }
                };
                eprintln!(
                    "[redsuite] {}: probe w30: {outcome_note}",
                    self.name()
                );
                probe_settings.push(("probe w30".to_owned(), outcome_note));
            }

            // the 11th sponsored commit on one account is rejected on-chain
            {
                let probe_payer =
                    prep::funded_payer(base, PAYER_LAMPORTS).await?;
                let probe_payer_pubkey = probe_payer.pubkey();
                let probe_pdas = crate::init_delegated_accounts(
                    base,
                    &probe_payer,
                    1,
                    ACCOUNT_SPACE,
                    er.identity(),
                )
                .await?;
                let probe_account = probe_pdas[0];
                poll_until(CLONE_TIMEOUT, || async {
                    matches!(er.account(&probe_account).await, Ok(Some(acc)) if acc.data.len() == ACCOUNT_SPACE as usize)
                })
                .await;
                let probe_sender = er.sender(Rc::new(probe_payer));
                for commit_round in 1..=receipt::SPONSORED_COMMIT_LIMIT {
                    offset += 1;
                    let ix = build::commit_accounts(
                        offset,
                        probe_payer_pubkey,
                        &[probe_account],
                    );
                    let commit_signature = probe_sender.send(&[ix]).await?;
                    let limit_receipt = receipt::fetch_commit_receipt(
                        er.api(),
                        &commit_signature,
                        RECEIPT_TIMEOUT,
                    )
                    .await?;
                    if let Some(message) = &limit_receipt.error_message {
                        return Err(format!(
                            "sponsored commit {commit_round}/{} failed \
                             early: {message}",
                            receipt::SPONSORED_COMMIT_LIMIT
                        )
                        .into());
                    }
                }
                offset += 1;
                let ix = build::commit_accounts(
                    offset,
                    probe_payer_pubkey,
                    &[probe_account],
                );
                let rejected_tx = probe_sender.prepare(&[ix]).await?;
                let rejected_signature =
                    probe_sender.deliver(&rejected_tx).await?;
                let deadline = tokio::time::Instant::now() + REJECTION_TIMEOUT;
                let rejection_code = loop {
                    if let Some(status) = er
                        .api()
                        .get_signature_status(&rejected_signature)
                        .await?
                    {
                        if let Some(err) = &status.err {
                            break custom_error_code(err);
                        }
                        if status.confirmed {
                            return Err(format!(
                                "commit {} past the sponsored allowance \
                                 unexpectedly succeeded",
                                receipt::SPONSORED_COMMIT_LIMIT + 1
                            )
                            .into());
                        }
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(format!(
                            "no status for the over-limit commit within \
                             {REJECTION_TIMEOUT:?}"
                        )
                        .into());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                };
                assert_eq!(
                    rejection_code,
                    Some(receipt::COMMIT_LIMIT_ERR),
                    "expected the sponsored commit-limit rejection code"
                );
                let outcome_note = format!(
                    "commit {} rejected with 0x{:08X}",
                    receipt::SPONSORED_COMMIT_LIMIT + 1,
                    receipt::COMMIT_LIMIT_ERR
                );
                eprintln!(
                    "[redsuite] {}: probe commit-limit: {outcome_note}",
                    self.name()
                );
                probe_settings
                    .push(("probe commit limit".to_owned(), outcome_note));
            }
        }

        let widths_label: Vec<String> = profile
            .widths
            .iter()
            .map(|width| width.to_string())
            .collect();
        let mut summary = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("widths", widths_label.join("/"))
            .setting("account space", ACCOUNT_SPACE)
            .setting("commits per cell", profile.commits)
            .setting("offered rate /s", profile.rate)
            .setting("concurrency", profile.concurrency)
            .setting("pool", profile.pool)
            .setting("receipt timeout s", RECEIPT_TIMEOUT.as_secs());
        for (key, note) in probe_settings {
            summary = summary.setting(key, note);
        }
        for cell in &cells {
            summary = summary
                .metric(
                    format!("w{} round-trip p50 us", cell.width),
                    cell.round_trip_p50_us,
                )
                .metric(
                    format!("w{} round-trip p95 us", cell.width),
                    cell.round_trip_p95_us,
                )
                .metric(
                    format!("w{} base txs per commit", cell.width),
                    cell.base_txs_per_commit,
                );
        }
        for (label, value) in probe_metrics {
            summary = summary.metric(label, value);
        }
        Ok(summary)
    }
}
