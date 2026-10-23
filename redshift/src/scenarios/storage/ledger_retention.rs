use std::{
    rc::Rc,
    slice,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    api::transaction_meta,
    prep,
    programs::{CellHeader, REDSHIFT},
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signature::Signature;
use signer::Signer;
use tokio::time;
use tracing::{debug, instrument};

use crate::PAYER_LAMPORTS;

const LABEL: &str = "ledger-retention";
const SUPERBLOCK_SLOTS: u64 = 40;
const LEDGER_SIZE_LIMIT_BYTES: u64 = 1;
const RETENTION_EVENTS: u64 = 3;
const RETENTION_TIMEOUT: Duration = Duration::from_secs(180);
const TX_INTERVAL: Duration = Duration::from_millis(200);
const SIGNATURE_WINDOW: usize = 1_000;
const BLOCK_SETTLE_SLOTS: u64 = 2;
const TRUNCATIONS: &str = r#"engine_ledger_operation_duration_micros_count{op="truncate"}"#;
const SUPERBLOCKS: &str = "engine_ledger_superblocks";

#[derive(Clone)]
struct Sent {
    signature: Signature,
    slot: u64,
}

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

pub struct LedgerRetention {
    base: Rc<BaseCtx>,
    private: PrivateEr,
    payer: Keypair,
}

impl PrivateErScenario for LedgerRetention {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let private = topology::private_er(
            &base,
            ErOptions {
                label: LABEL.to_owned(),
                settings: Box::new(|params| {
                    params.engine.blockstore.superblock = SUPERBLOCK_SLOTS;
                    params.engine.ledger.size_limit = LEDGER_SIZE_LIMIT_BYTES;
                }),
                ..Default::default()
            },
        )
        .await?;

        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        Ok(Self { base, private, payer })
    }

    async fn run(mut self) -> Result<()> {
        let mut history = self.prepare_history().await?;
        let before = self.prune(&mut history).await?;
        ensure!(!before.retained.is_empty());

        let timing = self.private.restart(RestartConfig::default()).await?;
        ensure!(timing.exit_code == Some(0));
        ensure!(!timing.needed_sigkill);

        let after = self.check_history(&history, before.pruned.len()).await?;
        ensure!(!after.retained.is_empty());

        let count = add_count(history.adds);
        self.send_add(count, &mut history.sent).await?;
        history.total += u64::from(count);
        self.check_history(&history, after.pruned.len()).await?;
        self.private.finish().await
    }
}

struct History {
    cell: Pubkey,
    sent: Vec<Sent>,
    adds: u64,
    total: u64,
}

impl LedgerRetention {
    #[instrument(skip_all, err)]
    async fn prepare_history(&self) -> Result<History> {
        let er = self.private.ctx();
        er.await_program_clone(&redshift_interface::id()).await?;
        let cell = self.delegate_cell().await?;
        let mut sent = Vec::new();
        self.seed_history(&cell, &mut sent).await?;
        Ok(History { cell, sent, adds: 0, total: 0 })
    }

    #[instrument(skip_all, err)]
    async fn prune(&self, history: &mut History) -> Result<HistoryView> {
        let er = self.private.ctx();
        let mut events = 0;
        let mut previous_pruned = 0;
        let mut first_pruning_event = None;
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
                if view.pruned.len() > previous_pruned && first_pruning_event.is_none() {
                    first_pruning_event = Some(events);
                }
                previous_pruned = view.pruned.len();
                let superblocks = self.superblocks().await?;
                debug!(
                    pruned = view.pruned.len(),
                    retained = view.retained.len(),
                    superblocks,
                    "retention advanced",
                );
            }
            if events >= RETENTION_EVENTS && first_pruning_event.is_some() {
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
        let er = self.private.ctx();
        let view = history.observe(er).await?;
        ensure!(view.pruned.len() >= previous_pruned);
        history.verify(er, &view, previous_pruned).await?;
        Ok(view)
    }

    async fn truncations(&self) -> Result<u64> {
        let metrics = self.private.ctx().scrape_metrics().await?;
        Ok(metrics.get(TRUNCATIONS).unwrap_or(0.0) as u64)
    }

    async fn superblocks(&self) -> Result<u64> {
        let metrics = self.private.ctx().scrape_metrics().await?;
        Ok(metrics.get(SUPERBLOCKS).unwrap_or(0.0) as u64)
    }

    async fn delegate_cell(&self) -> Result<Pubkey> {
        let payer_chain = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let cell = self
            .base
            .cells(REDSHIFT, &self.payer)
            .create_primary(self.private.ctx().identity(), prep::COMMIT_FREQUENCY_MS)
            .await?;
        self.base
            .delegate_payer(&payer_chain, &self.payer, self.private.ctx().identity())
            .await?;
        Ok(cell)
    }

    async fn seed_history(&self, cell: &Pubkey, sent: &mut Vec<Sent>) -> Result<()> {
        let listed = self
            .private
            .ctx()
            .api()
            .await_accounts(
                slice::from_ref(cell),
                "the er records the clone of the delegated cell",
                async || {
                    ensure!(self.private.ctx().account(cell).await?.is_some());
                    let signatures = self
                        .private
                        .ctx()
                        .api()
                        .get_signatures_for_address(cell, SIGNATURE_WINDOW)
                        .await?;
                    ensure!(!signatures.is_empty());
                    Ok(signatures)
                },
            )
            .await?;
        for signature in listed.iter().rev() {
            let signature = *signature;
            let tx = self
                .private
                .ctx()
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
            .private
            .ctx()
            .submit_and_confirm(
                &self.payer,
                &[REDSHIFT
                    .batch()
                    .add(&[REDSHIFT.primary(self.payer.pubkey())], i64::from(count))
                    .require_signer(self.payer.pubkey())
                    .build()],
            )
            .await?;
        let tx = self.private.ctx().api().await_transaction(&signature).await?;
        let meta = transaction_meta(&tx)?;
        ensure!(meta.err == None);
        sent.push(Sent { signature, slot: tx.slot });
        Ok(())
    }
}

impl History {
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
