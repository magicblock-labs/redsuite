use std::{
    cell::RefCell,
    collections::HashSet,
    rc::Rc,
    slice,
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure, Context};
use instruction::Instruction;
use pubkey::Pubkey;
use redsuite_core::{
    api::custom_error_code,
    profile::ProfileValues,
    programs::{SettlementMode, REDLINE},
    receipt,
    redline::Accounts,
    report::Unit,
    runner::{execute, Delivery, Pacing, RunConfig, RunOutcome, SendTransaction},
    stats::StreamingStats,
    topology,
    transport::NETWORK_TIMEOUT,
    Api, BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport, TxSender,
};
use signature::Signature;
use signer::Signer;
use tokio::time;
use tracing::{debug, instrument};

use crate::program::DELEGATION_PROGRAM_ID;

const PAYER_LAMPORTS: u64 = 5_000_000_000;

const ACCOUNT_SPACE: u32 = 128;

const WARMUP_COMMITS: u64 = 2;

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

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

#[derive(Default)]
struct CellTally {
    er_delivery: StreamingStats,
    round_trip: StreamingStats,
    base_txs: Vec<Signature>,
    included_mismatch: usize,
}

struct CellSummary {
    width: usize,
    outcome: RunOutcome,
    round_trip_p50_us: f64,
    round_trip_p95_us: f64,
    base_txs_per_commit: f64,
}

enum SchedulingOutcome {
    Confirmed,
    Rejected(Option<u32>),
}

const SIGNATURE_SCAN_LIMIT: usize = 1000;

struct BaseFlowSnapshot {
    flow: HashSet<Signature>,
    alt: HashSet<Signature>,
}

impl BaseFlowSnapshot {
    fn new_flow_since(&self, earlier: &BaseFlowSnapshot) -> usize {
        self.flow.difference(&earlier.flow).count()
    }

    fn new_alt_since(&self, earlier: &BaseFlowSnapshot) -> usize {
        self.alt.difference(&earlier.alt).count()
    }
}

pub struct CommitWidthEnvelope {
    base: Rc<BaseCtx>,
    er: ErCtx,
    profile: &'static Profile,
    pdas: Vec<Pubkey>,
    sender: TxSender,
}

impl Scenario<ScenarioReport> for CommitWidthEnvelope {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let profile = PROFILES.select(base.config().profile);
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let pdas = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_delegated(base.as_ref(), &payer, profile.pool)
            .await?;
        er.await_clones(&pdas, ACCOUNT_SPACE as usize).await?;
        let sender = er.sender(Rc::new(payer));

        Ok(Self { base, er, profile, pdas, sender })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let profile = self.profile;
        let mut offset = 0;
        let mut cells = Vec::new();
        for &width in profile.widths {
            cells.push(self.measure_width(width, &mut offset).await?);
        }
        let mut probes = ScenarioReport::ok();
        if profile.probes {
            probes = self.probe_alt(&mut offset, probes).await?;
            probes = self.probe_oversized(&mut offset, probes).await?;
            probes = self.probe_allowance(&mut offset, probes).await?;
        }
        let widths_label: Vec<String> =
            profile.widths.iter().map(|width| width.to_string()).collect();
        let mut summary = ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting("widths", widths_label.join("/"))
            .setting("account space", ACCOUNT_SPACE)
            .setting("commits per cell", profile.commits)
            .setting("offered rate /s", profile.rate)
            .setting("concurrency", profile.concurrency)
            .setting("pool", profile.pool)
            .setting("receipt timeout s", NETWORK_TIMEOUT.as_secs());
        summary.config.extend(probes.config);
        for cell in &cells {
            summary = summary
                .with_flags(&cell.outcome)
                .metric(
                    format!("w{} round-trip p50 us", cell.width),
                    Unit::Micros,
                    cell.round_trip_p50_us,
                )
                .metric(
                    format!("w{} round-trip p95 us", cell.width),
                    Unit::Micros,
                    cell.round_trip_p95_us,
                )
                .metric(
                    format!("w{} base txs per commit", cell.width),
                    Unit::Count,
                    cell.base_txs_per_commit,
                );
        }
        summary.measurements.extend(probes.measurements);
        Ok(summary)
    }
}

struct CommitRequest {
    accounts: Vec<Pubkey>,
    instruction: Instruction,
    sender: TxSender,
    er_api: Api,
    base_api: Api,
    tally: Rc<RefCell<CellTally>>,
}
impl CommitRequest {
    async fn prepare(self) -> Result<impl Delivery> {
        let tx = self.sender.prepare(slice::from_ref(&self.instruction)).await?;
        let sender = self.sender.clone();
        Ok(SendTransaction(move |started| async move {
            let signature = sender.submit_prepared(&tx).await?;
            self.confirm(signature, started).await
        }))
    }
    async fn confirm(self, commit_signature: Signature, started: Instant) -> Result<()> {
        let Self {
            accounts,
            er_api,
            base_api,
            tally,
            ..
        } = self;
        tally.borrow_mut().er_delivery.push(started.elapsed().as_micros() as u32);
        let commit_receipt = super::settled_receipt(&er_api, &commit_signature).await?;
        let round_trip_us = started.elapsed().as_micros() as u32;
        commit_receipt.confirm(&base_api).await?;
        let mut tally = tally.borrow_mut();
        tally.round_trip.push(round_trip_us);
        tally.base_txs.extend(commit_receipt.base_signatures);
        let mut reported = commit_receipt.included;
        reported.sort();
        let mut expected = accounts;
        expected.sort();
        if reported != expected {
            tally.included_mismatch += 1;
        }
        Ok(())
    }
}
impl CommitWidthEnvelope {
    fn request(&self, width: usize, id: u64, tally: Rc<RefCell<CellTally>>) -> CommitRequest {
        let accounts = self.commit_set(width, id);
        let instruction = REDLINE
            .batch()
            .nonce(id)
            .settle(
                self.sender.payer().pubkey(),
                &accounts,
                SettlementMode::Commit,
            )
            .build();
        CommitRequest {
            accounts,
            instruction,
            sender: self.sender.clone(),
            er_api: self.er.api().clone(),
            base_api: self.base.api().clone(),
            tally,
        }
    }
    #[instrument(skip_all, err)]
    async fn measure_width(&self, width: usize, offset: &mut u64) -> Result<CellSummary> {
        let warmup_tally = Rc::new(RefCell::new(CellTally::default()));
        let warmup = execute(
            RunConfig {
                iterations: WARMUP_COMMITS,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            |iteration| self.request(width, *offset + iteration, warmup_tally.clone()).prepare(),
        )
        .await?;
        drop(warmup_tally);
        *offset += WARMUP_COMMITS;
        ensure!(warmup.failed == 0);
        // post-apply buffer closes can trail the receipt — let warmup
        // stragglers land before the base-flow snapshot
        time::sleep(Duration::from_secs(1)).await;

        let flow_before = self.snapshot_base_flow().await?;
        let before = self.er.scrape_metrics().await?;
        let tally = Rc::new(RefCell::new(CellTally::default()));
        let outcome = execute(
            RunConfig {
                iterations: self.profile.commits,
                rate: Pacing::PerSecond(self.profile.rate),
                concurrency: self.profile.concurrency,
            },
            |iteration| self.request(width, *offset + iteration, tally.clone()).prepare(),
        )
        .await?;
        *offset += self.profile.commits;
        ensure!(outcome.failed == 0);
        time::sleep(Duration::from_secs(1)).await;
        let after = self.er.scrape_metrics().await?;
        let flow_after = self.snapshot_base_flow().await?;
        let delta = MetricsDelta::new(before, after);

        if let Some(intents) = delta.counter("mbv_committor_intents_count") {
            ensure!(intents >= self.profile.commits as f64);
        }
        let failed_intents = delta.counter_all("mbv_committor_failed_intents_count").unwrap_or(0.0);
        ensure!(failed_intents == 0.0);
        let alt_tables_used = delta.counter("mbv_committor_intent_alt_count_sum").unwrap_or(0.0);
        let alt_base_txs = flow_after.new_alt_since(&flow_before);
        let backlog = delta.gauge("mbv_committor_intent_backlog_count").unwrap_or(0.0);
        ensure!(backlog == 0.0);

        let tally = Rc::try_unwrap(tally)
            .unwrap_or_else(|_| panic!("commit tasks still hold the tally"))
            .into_inner();
        ensure!(tally.included_mismatch == 0);
        let own_alt_txs = self.own_lookup_table_txs(&tally.base_txs).await?;
        ensure!(own_alt_txs == 0);
        let round_trip = tally.round_trip.finalize(false);
        let er_delivery = tally.er_delivery.finalize(false);
        ensure!(round_trip.count == (self.profile.commits as usize));

        let receipt_txs_per_commit = tally.base_txs.len() as f64 / self.profile.commits as f64;
        ensure!(receipt_txs_per_commit >= 1.0);
        let flow_txs_per_commit =
            flow_after.new_flow_since(&flow_before) as f64 / self.profile.commits as f64;

        debug!(
            width,
            commits_per_sec = self.profile.commits as f64 / outcome.wall.as_secs_f64(),
            "commit width cell finished"
        );

        let cell_report = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting("width", width)
            .setting("account space", ACCOUNT_SPACE)
            .setting("commits", self.profile.commits)
            .setting("offered rate /s", self.profile.rate)
            .setting("concurrency", self.profile.concurrency)
            .setting("pool", self.profile.pool)
            .setting("receipt timeout s", NETWORK_TIMEOUT.as_secs())
            .observe("er delivery us", Unit::Micros, er_delivery)
            .observe("commit round-trip us", Unit::Micros, round_trip)
            .client_side(&outcome)
            .metric("base txs per commit", Unit::Count, flow_txs_per_commit)
            .metric(
                "receipt base txs per commit",
                Unit::Count,
                receipt_txs_per_commit,
            )
            .metric("window alt intents", Unit::Count, alt_tables_used)
            .metric("window alt base txs", Unit::Count, alt_base_txs as f64)
            .metric(
                "achieved commits /s",
                Unit::PerSecond,
                self.profile.commits as f64 / outcome.wall.as_secs_f64(),
            )
            .metric_if(
                "validator intent exec avg s",
                Unit::Seconds,
                delta.histogram_avg_all("mbv_committor_intent_execution_time_histogram_v2"),
            )
            .metric_if(
                "intents in window",
                Unit::Count,
                delta.counter("mbv_committor_intents_count"),
            );
        self.base.report_cell(&format!("w{width}"), &cell_report)?;

        Ok(CellSummary {
            width,
            outcome,
            round_trip_p50_us: round_trip.median as f64,
            round_trip_p95_us: round_trip.quantile95 as f64,
            base_txs_per_commit: flow_txs_per_commit,
        })
    }
    #[instrument(skip_all, err)]
    async fn probe_alt(
        &self,
        offset: &mut u64,
        mut report: ScenarioReport,
    ) -> Result<ScenarioReport> {
        let payer_pubkey = self.sender.payer().pubkey();
        let accounts: Vec<Pubkey> = self.pdas.iter().copied().take(15).collect();
        *offset += 1;
        let ix = REDLINE
            .batch()
            .nonce(*offset)
            .settle(payer_pubkey, &accounts, SettlementMode::Commit)
            .build();
        let flow_before = self.snapshot_base_flow().await?;
        let before = self.er.scrape_metrics().await?;
        let tx = self.sender.prepare(&[ix]).await?;
        let started = Instant::now();
        let commit_signature = self.sender.submit_prepared(&tx).await?;
        match self.scheduling_outcome(&commit_signature).await? {
            SchedulingOutcome::Rejected(code) => {
                ensure!(code == Some(receipt::INTENT_TOO_LARGE_ERR));
                debug!("probe refused by scheduling intent-size gate");
                report = report.setting("probe w15", "refused at scheduling (intent size gate)");
            }
            SchedulingOutcome::Confirmed => {
                let alt_receipt = super::settled_receipt(self.er.api(), &commit_signature).await?;
                alt_receipt.confirm(self.base.api()).await?;
                let wall = started.elapsed();
                let delta = MetricsDelta::new(before, self.er.scrape_metrics().await?);
                let flow_after = self.snapshot_base_flow().await?;
                let alt_base_txs = flow_after.new_alt_since(&flow_before);
                let own_alt_txs = self.own_lookup_table_txs(&alt_receipt.base_signatures).await?;
                ensure!(own_alt_txs >= 1);
                debug!(alt_base_txs, "ALT commit probe finished");
                report = report.setting("probe w15", "delivered via ALTs");
                report = report.metric("probe w15 round-trip s", Unit::Seconds, wall.as_secs_f64());
                report = report.metric(
                    "probe w15 base flow txs",
                    Unit::Count,
                    flow_after.new_flow_since(&flow_before) as f64,
                );
                report = report.metric("probe w15 alt base txs", Unit::Count, alt_base_txs as f64);
                report = report.metric("probe w15 own alt txs", Unit::Count, own_alt_txs as f64);
                report = report.metric_if(
                    "probe w15 window alt intents",
                    Unit::Count,
                    delta.counter("mbv_committor_intent_alt_count_sum"),
                );
            }
        }

        Ok(report)
    }
    #[instrument(skip_all, err)]
    async fn probe_oversized(
        &self,
        offset: &mut u64,
        mut report: ScenarioReport,
    ) -> Result<ScenarioReport> {
        let payer_pubkey = self.sender.payer().pubkey();
        let accounts: Vec<Pubkey> = self.pdas.iter().copied().take(30).collect();
        *offset += 1;
        let ix = REDLINE
            .batch()
            .nonce(*offset)
            .settle(payer_pubkey, &accounts, SettlementMode::Commit)
            .build();
        let before = self.er.scrape_metrics().await?;
        let commit_signature = self.sender.submit(&[ix]).await?;
        let outcome_note = match self.scheduling_outcome(&commit_signature).await? {
            SchedulingOutcome::Rejected(code) => {
                ensure!(code == Some(receipt::INTENT_TOO_LARGE_ERR));
                "refused at scheduling (intent size gate)".to_owned()
            }
            SchedulingOutcome::Confirmed => {
                let receipt_outcome =
                    receipt::fetch_commit_receipt(self.er.api(), &commit_signature).await;
                let delta = MetricsDelta::new(before, self.er.scrape_metrics().await?);
                let failed_intents =
                    delta.counter_all("mbv_committor_failed_intents_count").unwrap_or(0.0);
                report = report.metric("probe w30 failed intents", Unit::Count, failed_intents);
                match receipt_outcome {
                    Ok(fit_receipt) => {
                        ensure!(!fit_receipt.succeeded());
                        ensure!(fit_receipt.receipt_err_code == Some(receipt::INTENT_FAILED_CODE));
                        format!(
                            "intent failed as expected: {}",
                            fit_receipt.error_message.unwrap_or_default()
                        )
                    }
                    // a failed receipt tx may be unqueryable — then
                    // the failed-intents counter must attest to the
                    // rejection
                    Err(fetch_error) => {
                        ensure!(failed_intents >= 1.0);
                        format!(
                            "no receipt ({fetch_error}); \
                                     failed-intents delta {failed_intents}"
                        )
                    }
                }
            }
        };
        debug!(%outcome_note, "oversized commit probe finished");
        report = report.setting("probe w30", outcome_note);

        Ok(report)
    }
    #[instrument(skip_all, err)]
    async fn probe_allowance(
        &self,
        offset: &mut u64,
        mut report: ScenarioReport,
    ) -> Result<ScenarioReport> {
        let probe_payer = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let probe_payer_pubkey = probe_payer.pubkey();
        let probe_pdas = Accounts::new(ACCOUNT_SPACE, self.er.identity())
            .init_delegated(self.base.as_ref(), &probe_payer, 1)
            .await?;
        let probe_account = probe_pdas[0];
        self.er.await_clones(&[probe_account], ACCOUNT_SPACE as usize).await?;
        let probe_sender = self.er.sender(Rc::new(probe_payer));
        for _ in 0..receipt::SPONSORED_COMMIT_LIMIT {
            *offset += 1;
            let ix = REDLINE
                .batch()
                .nonce(*offset)
                .settle(probe_payer_pubkey, &[probe_account], SettlementMode::Commit)
                .build();
            let commit_signature = probe_sender.submit(&[ix]).await?;
            super::settled_receipt(self.er.api(), &commit_signature).await?;
        }
        *offset += 1;
        let ix = REDLINE
            .batch()
            .nonce(*offset)
            .settle(probe_payer_pubkey, &[probe_account], SettlementMode::Commit)
            .build();
        let rejected_signature = probe_sender.submit(&[ix]).await?;
        let rejection_code = match self.scheduling_outcome(&rejected_signature).await? {
            SchedulingOutcome::Rejected(code) => code,
            SchedulingOutcome::Confirmed => {
                return Err(anyhow!(
                    "commit {} past the sponsored allowance \
                             unexpectedly succeeded",
                    receipt::SPONSORED_COMMIT_LIMIT + 1
                ))
            }
        };
        ensure!(rejection_code == Some(receipt::COMMIT_LIMIT_ERR));
        let outcome_note = format!(
            "commit {} rejected with 0x{:08X}",
            receipt::SPONSORED_COMMIT_LIMIT + 1,
            receipt::COMMIT_LIMIT_ERR
        );
        debug!(%outcome_note, "commit limit probe finished");
        report = report.setting("probe commit limit", outcome_note);

        Ok(report)
    }

    fn commit_set(&self, width: usize, id: u64) -> Vec<Pubkey> {
        let pool_len = self.pdas.len();
        let start_index = ((id as usize - 1) * width) % pool_len;
        (0..width)
            .map(|position| self.pdas[(start_index + position) % pool_len])
            .collect()
    }

    async fn scheduling_outcome(&self, commit_signature: &Signature) -> Result<SchedulingOutcome> {
        match self.er.api().try_confirm(commit_signature).await? {
            Ok(()) => Ok(SchedulingOutcome::Confirmed),
            Err(error) => Ok(SchedulingOutcome::Rejected(custom_error_code(error.err))),
        }
    }

    async fn snapshot_base_flow(&self) -> Result<BaseFlowSnapshot> {
        let committor_program: Pubkey = topology::COMMITTOR_ID;
        let alt_program: Pubkey = sdk_ids::address_lookup_table::ID;
        let mut flow = HashSet::new();
        flow.extend(
            self.base
                .api()
                .get_signatures_for_address(&DELEGATION_PROGRAM_ID, SIGNATURE_SCAN_LIMIT)
                .await?,
        );
        flow.extend(
            self.base
                .api()
                .get_signatures_for_address(&committor_program, SIGNATURE_SCAN_LIMIT)
                .await?,
        );
        let alt = self
            .base
            .api()
            .get_signatures_for_address(&alt_program, SIGNATURE_SCAN_LIMIT)
            .await?
            .into_iter()
            .collect();
        Ok(BaseFlowSnapshot { flow, alt })
    }

    async fn own_lookup_table_txs(&self, signatures: &[Signature]) -> Result<usize> {
        let mut with_lookups = 0;
        for chain_signature in signatures {
            let tx = self.base.api().await_transaction(chain_signature).await?;
            let decoded =
                tx.transaction.transaction.decode().context("invalid encoded transaction")?;
            if decoded
                .message
                .address_table_lookups()
                .is_some_and(|lookups| !lookups.is_empty())
            {
                with_lookups += 1;
            }
        }
        Ok(with_lookups)
    }
}
