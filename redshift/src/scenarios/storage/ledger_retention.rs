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
use tracing::debug;

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

async fn truncations(er: &ErCtx) -> Result<u64> {
    let metrics = er.scrape_metrics().await?;
    Ok(metrics.get(TRUNCATIONS).unwrap_or(0.0) as u64)
}

async fn superblocks(er: &ErCtx) -> Result<u64> {
    let metrics = er.scrape_metrics().await?;
    Ok(metrics.get(SUPERBLOCKS).unwrap_or(0.0) as u64)
}

async fn delegate_cell(base: &BaseCtx, er: &ErCtx, payer: &Keypair) -> Result<Pubkey> {
    let payer_chain = base.funded_payer(PAYER_LAMPORTS).await?;
    let cell = base
        .cells(REDSHIFT, payer)
        .create_primary(er.identity(), prep::COMMIT_FREQUENCY_MS)
        .await?;
    base.delegate_payer(&payer_chain, payer, er.identity()).await?;
    Ok(cell)
}

async fn seed_history(er: &ErCtx, cell: &Pubkey, sent: &mut Vec<Sent>) -> Result<()> {
    let listed = er
        .api()
        .await_accounts(
            slice::from_ref(cell),
            "the er records the clone of the delegated cell",
            async || {
                ensure!(er.account(cell).await?.is_some());
                let signatures =
                    er.api().get_signatures_for_address(cell, SIGNATURE_WINDOW).await?;
                ensure!(!signatures.is_empty());
                Ok(signatures)
            },
        )
        .await?;
    for signature in listed.iter().rev() {
        let signature = *signature;
        let tx = er
            .api()
            .get_transaction(&signature)
            .await?
            .context("listed transaction missing")?;
        sent.push(Sent { signature, slot: tx.slot });
    }
    Ok(())
}

fn add_count(adds: u64) -> u8 {
    (adds % 255 + 1) as u8
}

async fn send_add(er: &ErCtx, payer: &Keypair, count: u8, sent: &mut Vec<Sent>) -> Result<()> {
    let signature = er
        .submit_and_confirm(
            payer,
            &[REDSHIFT
                .batch()
                .add(&[REDSHIFT.primary(payer.pubkey())], i64::from(count))
                .require_signer(payer.pubkey())
                .build()],
        )
        .await?;
    let tx = er.api().await_transaction(&signature).await?;
    let meta = transaction_meta(&tx)?;
    ensure!(meta.err == None);
    sent.push(Sent { signature, slot: tx.slot });
    Ok(())
}

async fn observe(er: &ErCtx, sent: &[Sent]) -> Result<HistoryView> {
    let mut view = HistoryView {
        pruned: Vec::new(),
        retained: Vec::new(),
    };
    for entry in sent {
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

async fn cell_value(er: &ErCtx, cell: &Pubkey) -> Result<u64> {
    let account = er.account(cell).await?.context("the delegated cell is missing on the er")?;
    Ok(CellHeader::try_decode(&account.data)?.value)
}

async fn verify(
    er: &ErCtx,
    view: &HistoryView,
    previous_pruned: usize,
    cell: &Pubkey,
    expected_count: u64,
) -> Result<()> {
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

    let listed = er.api().get_signatures_for_address(cell, SIGNATURE_WINDOW).await?;
    let expected: Vec<Signature> =
        view.retained.iter().rev().map(|entry| entry.signature).collect();
    ensure!(listed == expected);

    ensure!(cell_value(er, cell).await? == expected_count);
    Ok(())
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

    async fn run(self) -> Result<()> {
        let Self { base, mut private, payer } = self;
        let mut sent: Vec<Sent> = Vec::new();
        let mut adds: u64 = 0;
        let mut total: u64 = 0;
        let mut events: u64 = 0;
        let mut previous_pruned = 0usize;
        let mut first_pruning_event: Option<u64> = None;
        let cell;
        let before;
        {
            let er = private.ctx();
            er.await_program_clone(&redshift_interface::id()).await?;
            cell = delegate_cell(&base, er, &payer).await?;
            seed_history(er, &cell, &mut sent).await?;

            let baseline = truncations(er).await?;
            let started = Instant::now();
            loop {
                let count = add_count(adds);
                send_add(er, &payer, count, &mut sent).await?;
                adds += 1;
                total += u64::from(count);

                let observed = truncations(er).await? - baseline;
                if observed > events {
                    events = observed;
                    let view = observe(er, &sent).await?;
                    verify(er, &view, previous_pruned, &cell, total).await?;
                    if view.pruned.len() > previous_pruned && first_pruning_event.is_none() {
                        first_pruning_event = Some(events);
                    }
                    previous_pruned = view.pruned.len();
                    let superblocks = superblocks(er).await?;
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

            before = observe(er, &sent).await?;
            verify(er, &before, previous_pruned, &cell, total).await?;
            ensure!(!before.retained.is_empty());
        }

        let timing = private.restart(RestartConfig::default()).await?;
        ensure!(timing.exit_code == Some(0));
        ensure!(!timing.needed_sigkill);

        let er = private.ctx();
        let after = observe(er, &sent).await?;
        ensure!(after.pruned.len() >= before.pruned.len());
        verify(er, &after, before.pruned.len(), &cell, total).await?;
        ensure!(!after.retained.is_empty());

        let count = add_count(adds);
        send_add(er, &payer, count, &mut sent).await?;
        total += u64::from(count);
        let final_view = observe(er, &sent).await?;
        verify(er, &final_view, after.pruned.len(), &cell, total).await?;
        private.finish().await?;
        Ok(())
    }
}
