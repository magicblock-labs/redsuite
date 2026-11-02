use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    api::transaction_meta,
    prep,
    programs::{CellHeader, REDSHIFT},
    topology::RestartConfig,
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use tokio::time;
use tracing::{debug, instrument};

use crate::PAYER_LAMPORTS;

const RETENTION_EVENTS: u64 = 3;
const RETENTION_TIMEOUT: Duration = Duration::from_secs(180);
const TX_INTERVAL: Duration = Duration::from_millis(200);
const SIGNATURE_WINDOW: usize = 1_000;
const BLOCK_SETTLE_SLOTS: u64 = 2;
const TRUNCATIONS: &str = r#"engine_ledger_operation_duration_micros_count{op="truncate"}"#;
const SUPERBLOCKS: &str = "engine_ledger_superblocks";

/// A known published transaction and its original slot, retained beyond ledger pruning.
#[derive(Clone)]
struct Sent {
    signature: Signature,
    slot: u64,
}

/// Observed retention split used to compare transaction, block, and address-history RPCs.
struct HistoryView {
    pruned: Vec<Sent>,
    retained: Vec<Sent>,
}

impl HistoryView {
    fn max_pruned_slot(&self) -> Option<u64> {
        self.pruned.iter().map(|sent| sent.slot).max()
    }

    fn min_retained_slot(&self) -> Option<u64> {
        self.retained.iter().map(|sent| sent.slot).min()
    }
}

fn add_count(adds: u64) -> u8 {
    (adds % 255 + 1) as u8
}

/// Checks retained/pruned history and current state across restart and new writes.
/// Pruned records must form an older prefix; pruning history must not roll
/// back current account state.
pub struct LedgerRetentionConsistency {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
}

impl Scenario for LedgerRetentionConsistency {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        Ok(Self { base, er, payer })
    }

    /// Forces actual pruning, restarts the ER, and checks that the
    /// retention boundary stays coherent.
    async fn run(self) -> Result<()> {
        let mut history = self.prepare_history().await?;
        let before = self.prune(&mut history).await?;
        ensure!(!before.retained.is_empty());

        let timing = self.er.restart(RestartConfig::default()).await?;
        ensure!(timing.exit_code == Some(0));
        ensure!(!timing.needed_sigkill);

        let after = self.check_history(&history, before.pruned.len()).await?;
        ensure!(!after.retained.is_empty());

        let count = add_count(history.adds);
        self.send_add(count, &mut history.sent).await?;
        history.total += u64::from(count);
        self.check_history(&history, after.pruned.len()).await?;
        Ok(())
    }
}

/// Complete submitted-history oracle, including transactions no longer available through RPC.
struct History {
    cell: Pubkey,
    sent: Vec<Sent>,
    adds: u64,
    total: u64,
}

impl LedgerRetentionConsistency {
    #[instrument(skip_all, err)]
    async fn prepare_history(&self) -> Result<History> {
        let er = &self.er;
        er.await_program_clone(&redshift_interface::id()).await?;
        let cell = self.delegate_cell().await?;
        let mut sent = Vec::new();
        self.seed_history(&cell, &mut sent).await?;
        Ok(History { cell, sent, adds: 0, total: 0 })
    }

    /// Drives writes until multiple truncations include observable transaction removal.
    #[instrument(skip_all, err)]
    async fn prune(&self, history: &mut History) -> Result<HistoryView> {
        let er = &self.er;
        let mut events = 0;
        let mut previous_pruned = 0;
        let mut pruning_seen = false;
        let baseline = self.truncations().await?;
        let started = Instant::now();
        loop {
            let count = add_count(history.adds);
            self.send_add(count, &mut history.sent).await?;
            history.adds += 1;
            history.total += u64::from(count);

            let observed = self.truncations().await? - baseline;
            if observed > events {
                events = observed;
                let view = history.observe(er).await?;
                history.verify(er, &view, previous_pruned).await?;
                pruning_seen |= view.pruned.len() > previous_pruned;
                previous_pruned = view.pruned.len();
                let superblocks = self.superblocks().await?;
                debug!(
                    pruned = view.pruned.len(),
                    retained = view.retained.len(),
                    superblocks,
                    "retention advanced",
                );
            }
            if events >= RETENTION_EVENTS && pruning_seen {
                break;
            }
            ensure!(started.elapsed() < RETENTION_TIMEOUT);
            time::sleep(TX_INTERVAL).await;
        }

        let before = history.observe(er).await?;
        history.verify(er, &before, previous_pruned).await?;
        Ok(before)
    }

    #[instrument(skip_all, err)]
    async fn check_history(
        &self,
        history: &History,
        previous_pruned: usize,
    ) -> Result<HistoryView> {
        let er = &self.er;
        let view = history.observe(er).await?;
        history.verify(er, &view, previous_pruned).await?;
        Ok(view)
    }

    async fn truncations(&self) -> Result<u64> {
        let metrics = self.er.scrape_metrics().await?;
        Ok(metrics.get(TRUNCATIONS).unwrap_or(0.0) as u64)
    }

    async fn superblocks(&self) -> Result<u64> {
        let metrics = self.er.scrape_metrics().await?;
        Ok(metrics.get(SUPERBLOCKS).unwrap_or(0.0) as u64)
    }

    async fn delegate_cell(&self) -> Result<Pubkey> {
        let payer_chain = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let cell = self
            .base
            .cells(REDSHIFT, &self.payer)
            .create_primary(self.er.identity(), prep::COMMIT_FREQUENCY_MS)
            .await?;
        self.base.delegate_payer(&payer_chain, &self.payer, self.er.identity()).await?;
        Ok(cell)
    }

    /// Includes the clone transaction so address-history comparisons cover
    /// the full fixture history.
    async fn seed_history(&self, cell: &Pubkey, sent: &mut Vec<Sent>) -> Result<()> {
        let (_, listed) = self
            .er
            .api()
            .await_query(
                "the er records the clone of the delegated cell",
                async || {
                    let cloned = self.er.account(cell).await?.is_some();
                    let signatures =
                        self.er.api().get_signatures_for_address(cell, SIGNATURE_WINDOW).await?;
                    let ready = cloned && !signatures.is_empty();
                    Ok(wait::when(ready, (cloned, signatures)))
                },
            )
            .await?;
        for signature in listed.iter().rev() {
            let signature = *signature;
            let tx = self
                .er
                .api()
                .get_transaction(&signature)
                .await?
                .context("listed transaction missing")?;
            sent.push(Sent { signature, slot: tx.slot });
        }
        Ok(())
    }

    async fn send_add(&self, count: u8, sent: &mut Vec<Sent>) -> Result<()> {
        let signature = self
            .er
            .submit_and_confirm(
                &self.payer,
                &[REDSHIFT
                    .batch()
                    .add(&[REDSHIFT.primary(self.payer.pubkey())], i64::from(count))
                    .require_signer(self.payer.pubkey())
                    .build()],
            )
            .await?;
        let tx = self.er.api().await_transaction(&signature).await?;
        let meta = transaction_meta(&tx)?;
        ensure!(meta.err == None);
        sent.push(Sent { signature, slot: tx.slot });
        Ok(())
    }
}

impl History {
    /// Classifies known submissions by current transaction availability
    /// without changing the oracle.
    async fn observe(&self, er: &ErCtx) -> Result<HistoryView> {
        let mut view = HistoryView {
            pruned: Vec::new(),
            retained: Vec::new(),
        };
        for entry in &self.sent {
            match er.api().get_transaction(&entry.signature).await? {
                Some(tx) => {
                    ensure!(tx.slot == entry.slot);
                    view.retained.push(entry.clone());
                }
                None => view.pruned.push(entry.clone()),
            }
        }
        Ok(view)
    }

    /// Checks monotonic prefix pruning, cross-RPC agreement, and the sum of all executed updates.
    async fn verify(&self, er: &ErCtx, view: &HistoryView, previous_pruned: usize) -> Result<()> {
        ensure!(view.pruned.len() >= previous_pruned);
        if let (Some(max_pruned), Some(min_retained)) =
            (view.max_pruned_slot(), view.min_retained_slot())
        {
            ensure!(max_pruned < min_retained);
        }

        let tip = er.api().get_slot().await?;
        for entry in &view.pruned {
            let block = er.api().get_block(entry.slot).await?;
            ensure!(block.is_none());
        }
        for entry in &view.retained {
            if entry.slot + BLOCK_SETTLE_SLOTS > tip {
                continue;
            }
            let block = er.api().get_block(entry.slot).await?;
            ensure!(block.is_some());
        }

        let listed = er.api().get_signatures_for_address(&self.cell, SIGNATURE_WINDOW).await?;
        let expected: Vec<Signature> =
            view.retained.iter().rev().map(|entry| entry.signature).collect();
        ensure!(listed == expected);

        let account = er
            .account(&self.cell)
            .await?
            .context("the delegated cell is missing on the er")?;
        ensure!(CellHeader::try_decode(&account.data)?.value == self.total);
        Ok(())
    }
}
