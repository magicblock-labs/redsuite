use std::{rc::Rc, time::Duration};

use anyhow::{bail, ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    api::{transaction_meta, BLOCK_CLEANED_UP},
    prep,
    programs::{CellHeader, REDSHIFT},
    rpc_types::request::RpcRequest,
    topology::RestartConfig,
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use tracing::{debug, instrument};

use crate::PAYER_LAMPORTS;

const RETENTION_TIMEOUT: Duration = Duration::from_secs(180);
const SIGNATURE_WINDOW: usize = 1_000;
const BLOCK_SETTLE_SLOTS: u64 = 2;
const TRUNCATIONS: &str = r#"engine_ledger_operation_duration_micros_count{op="truncate"}"#;

/// A known published transaction and its original slot, retained beyond ledger pruning.
struct Sent {
    signature: Signature,
    slot: u64,
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
        self.prune(&history).await?;
        self.add(&mut history, 2).await?;
        let before = history.check(&self.er, 1).await?;
        ensure!(
            before < history.sent.len(),
            "missing retained history witness"
        );

        let timing = self.er.restart(RestartConfig::default()).await?;
        ensure!(timing.exit_code == Some(0));
        ensure!(!timing.needed_sigkill);

        let after = history.check(&self.er, before).await?;
        ensure!(
            after < history.sent.len(),
            "restart lost the retained history witness"
        );

        self.add(&mut history, 3).await?;
        history.check(&self.er, after).await?;
        Ok(())
    }
}

/// Complete submitted-history oracle, including transactions no longer available through RPC.
struct History {
    cell: Pubkey,
    sent: Vec<Sent>,
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
        let mut history = History { cell, sent, total: 0 };
        self.add(&mut history, 1).await?;
        Ok(history)
    }

    #[instrument(skip_all, err)]
    async fn prune(&self, history: &History) -> Result<()> {
        let landmark = history.sent.last().context("retention landmark missing")?;
        let baseline = self.truncations().await?;
        wait::retry(
            "retention prunes the confirmed write",
            RETENTION_TIMEOUT,
            async || {
                ensure!(self.truncations().await? > baseline, "no ledger truncation");
                ensure!(
                    self.er.api().get_transaction(&landmark.signature).await?.is_none(),
                    "landmark {} is still retained",
                    landmark.slot
                );
                Ok(())
            },
        )
        .await?;
        debug!(slot = landmark.slot, "retention pruned a confirmed write");
        Ok(())
    }

    async fn truncations(&self) -> Result<u64> {
        let metrics = self.er.scrape_metrics().await?;
        Ok(metrics.get(TRUNCATIONS).unwrap_or(0.0) as u64)
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
        let listed = self
            .er
            .api()
            .await_query(
                "the er records the clone of the delegated cell",
                async || {
                    let cloned = self.er.account(cell).await?.is_some();
                    let signatures =
                        self.er.api().get_signatures_for_address(cell, SIGNATURE_WINDOW).await?;
                    let ready = cloned && !signatures.is_empty();
                    Ok(wait::when(ready, signatures))
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

    async fn add(&self, history: &mut History, count: u8) -> Result<()> {
        let signature = self
            .er
            .submit_and_confirm(
                &self.payer,
                &[REDSHIFT
                    .batch()
                    .add(&[history.cell], i64::from(count))
                    .require_signer(self.payer.pubkey())
                    .build()],
            )
            .await?;
        let tx = self.er.api().await_transaction(&signature).await?;
        let meta = transaction_meta(&tx)?;
        ensure!(meta.err == None);
        self.er.api().await_slot(tx.slot + BLOCK_SETTLE_SLOTS).await?;
        history.sent.push(Sent { signature, slot: tx.slot });
        history.total += u64::from(count);
        Ok(())
    }
}

impl History {
    /// Checks monotonic prefix pruning, cross-RPC agreement, and executed state.
    #[instrument(skip_all, err)]
    async fn check(&self, er: &ErCtx, previous_pruned: usize) -> Result<usize> {
        let api = er.api();
        let mut removed = 0;
        for (index, entry) in self.sent.iter().enumerate() {
            if let Some(tx) = api.get_transaction(&entry.signature).await? {
                ensure!(tx.slot == entry.slot);
            } else {
                ensure!(
                    removed == index,
                    "retention removed a transaction after a retained one"
                );
                removed += 1;
            }
        }
        ensure!(removed >= previous_pruned);
        let (pruned, retained) = self.sent.split_at(removed);
        let last_pruned = pruned.iter().map(|entry| entry.slot).max();
        if let (Some(last), Some(first)) =
            (last_pruned, retained.iter().map(|entry| entry.slot).min())
        {
            ensure!(last < first);
        }

        let tip = er.api().get_slot().await?;
        if let Some(last_pruned) = last_pruned {
            for method in [RpcRequest::GetFirstAvailableBlock, RpcRequest::MinimumLedgerSlot] {
                let first: u64 = api.call(method, &[0u8; 0]).await?;
                ensure!(
                    first > last_pruned && first <= tip,
                    "{method} advertised {first} after removing slot {last_pruned}"
                );
            }
            let start = pruned[0].slot;
            let slots: Vec<u64> = api.call(RpcRequest::GetBlocks, &(start, tip)).await?;
            for entry in pruned {
                ensure!(
                    !slots.contains(&entry.slot),
                    "getBlocks returned pruned slot {}",
                    entry.slot
                );
            }
            for entry in retained {
                if entry.slot + BLOCK_SETTLE_SLOTS <= tip {
                    ensure!(
                        slots.contains(&entry.slot),
                        "getBlocks omitted retained slot {}",
                        entry.slot
                    );
                }
            }
            let time =
                api.try_call::<Option<i64>>(RpcRequest::GetBlockTime, &(last_pruned,)).await?;
            match time {
                Ok(None) => {}
                Err(error) if error.error.code.code() == BLOCK_CLEANED_UP => {}
                other => bail!("getBlockTime for pruned slot {last_pruned}: {other:?}"),
            }
        }
        for entry in pruned {
            let error = er.api().try_get_block(entry.slot).await?.err().with_context(|| {
                format!("pruned block {} is not reported as cleaned up", entry.slot)
            })?;
            ensure!(
                error.error.code.code() == BLOCK_CLEANED_UP,
                "pruned block {}: {error}",
                entry.slot
            );
        }
        for entry in retained {
            if entry.slot + BLOCK_SETTLE_SLOTS > tip {
                continue;
            }
            let block = er.api().get_block(entry.slot).await?;
            ensure!(block.is_some());
        }

        let listed = er.api().get_signatures_for_address(&self.cell, SIGNATURE_WINDOW).await?;
        let expected: Vec<Signature> = retained.iter().rev().map(|entry| entry.signature).collect();
        ensure!(listed == expected);

        let account = er
            .account(&self.cell)
            .await?
            .context("the delegated cell is missing on the er")?;
        ensure!(CellHeader::try_decode(&account.data)?.value == self.total);
        Ok(removed)
    }
}
