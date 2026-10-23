use std::{rc::Rc, time::Duration};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    api::transaction_meta,
    programs::{CellHeader, REDSHIFT},
    rpc_types::response::TransactionError,
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    transaction_types::EncodedConfirmedTransactionWithStatusMeta as TransactionInfo,
    wait, BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signature::Signature;
use signer::Signer;
use solana_instruction_error::InstructionError;
use transaction::Transaction;

use crate::PAYER_LAMPORTS;

const ROUNDS: usize = 3;
const BLOCKTIME: Duration = Duration::from_millis(50);
const SUPERBLOCK_SLOTS: u64 = 40;
const CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, PartialEq, Eq)]
struct State {
    payer_lamports: u64,
    cell_lamports: u64,
    cell: CellHeader,
}

impl State {
    fn apply(&mut self, record: &Record, evidence: &TransactionInfo) -> Result<()> {
        let evidence = transaction_meta(evidence)?;
        self.payer_lamports =
            self.payer_lamports.checked_sub(evidence.fee).context("fee payer exhausted")?;
        if !Record::fails(record.id) {
            self.cell.value += u64::from(record.id);
            self.cell.revision += 1;
        }
        Ok(())
    }

    async fn read(er: &ErCtx, keys: &[Pubkey; 2]) -> Result<State> {
        let accounts = er.accounts(keys).await?;
        let payer = accounts[0].as_ref().context("fee payer missing")?;
        let cell = accounts[1].as_ref().context("cell missing")?;
        Ok(State {
            payer_lamports: payer.lamports,
            cell_lamports: cell.lamports,
            cell: CellHeader::try_decode(&cell.data)?,
        })
    }
}

#[derive(Debug)]
struct Record {
    id: u8,
    signature: Signature,
    evidence: Option<TransactionInfo>,
}

impl Record {
    fn fails(id: u8) -> bool {
        id.is_multiple_of(2)
    }

    fn check(&self, evidence: &TransactionInfo) -> Result<()> {
        let meta = transaction_meta(evidence)?;
        let error = Self::fails(self.id)
            .then(|| TransactionError::InstructionError(1, InstructionError::Custom(0)).into());
        ensure!(meta.err == error);
        let mut balances = meta.pre_balances.clone();
        let payer = balances.first_mut().context("missing payer balance")?;
        *payer = payer.checked_sub(meta.fee).context("fee exceeds balance")?;
        ensure!(meta.post_balances == balances);
        if let Some(before) = &self.evidence {
            ensure!(evidence == before);
        }
        Ok(())
    }
}

struct Workload {
    payer: Keypair,
    owner: Pubkey,
    keys: [Pubkey; 2],
    next_id: u8,
    expected: State,
}

impl Workload {
    async fn check(&self, er: &ErCtx) -> Result<()> {
        ensure!(State::read(er, &self.keys).await? == self.expected);
        Ok(())
    }

    async fn submit(&mut self, er: &ErCtx, confirm: bool) -> Result<Record> {
        self.next_id = self.next_id.checked_add(1).context("transaction ids exhausted")?;
        let id = self.next_id;
        let mut instructions =
            vec![REDSHIFT.batch().add(&[REDSHIFT.primary(self.owner)], i64::from(id)).build()];
        if Record::fails(id) {
            instructions
                .push(REDSHIFT.batch().writable(&[REDSHIFT.primary(self.owner)]).fail(0).build());
        }
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.payer.pubkey()),
            &[&self.payer],
            er.api().get_latest_blockhash().await?,
        );
        let mut record = Record {
            id,
            signature: tx.signatures[0],
            evidence: None,
        };
        er.api().send_transaction(&tx).await?;
        if confirm {
            let evidence = er.api().await_transaction(&record.signature).await?;
            record.check(&evidence)?;
            self.expected.apply(&record, &evidence)?;
            self.check(er).await?;
            record.evidence = Some(evidence);
        }
        Ok(record)
    }
}

pub struct CheckpointDurability {
    private: PrivateEr,
    workload: Workload,
}

impl PrivateErScenario for CheckpointDurability {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let private = topology::private_er(
            &base,
            ErOptions {
                label: "checkpoint-durability".to_owned(),
                settings: Box::new(|params| {
                    params.engine.blockstore.blocktime = BLOCKTIME;
                    params.engine.blockstore.superblock = SUPERBLOCK_SLOTS;
                }),
                ..Default::default()
            },
        )
        .await?;
        let owner = base.funded_payer(PAYER_LAMPORTS).await?;
        let (cell, setup) =
            base.cells(REDSHIFT, &owner).prepare_delegated(private.identity(), u32::MAX);
        base.submit_and_confirm(&owner, &setup).await?;
        let payer = base.delegated_payer(&owner, private.identity(), PAYER_LAMPORTS).await?;
        let keys = [payer.pubkey(), cell];
        let expected = private
            .ctx()
            .api()
            .await_accounts(&keys, "accounts cloned", || {
                State::read(private.ctx(), &keys)
            })
            .await?;
        ensure!((expected.cell.value, expected.cell.revision) == (0, 0));
        let workload = Workload {
            payer,
            owner: owner.pubkey(),
            keys,
            next_id: 0,
            expected,
        };
        Ok(Self { private, workload })
    }

    async fn run(mut self) -> Result<()> {
        let mut protected = Vec::new();
        let mut previous_boundary = 0;

        for _ in 0..ROUNDS {
            let er = self.private.ctx();
            for _ in 0..8 {
                protected.push(self.workload.submit(er, true).await?);
            }
            let boundary = self.checkpoint().await?;
            ensure!(boundary > previous_boundary);
            previous_boundary = boundary;
            self.verify(&protected).await?;
            self.workload.check(er).await?;
            let baseline = self.workload.expected.clone();

            let mut tail = Vec::new();
            for index in 0..16 {
                tail.push(self.workload.submit(er, index < 4).await?);
            }
            let timing = self
                .private
                .restart(RestartConfig {
                    hard_kill: true,
                    reset: false,
                    ready_timeout: CHECKPOINT_TIMEOUT,
                })
                .await?;
            ensure!(timing.exit_signal == Some(9));
            let er = self.private.ctx();
            self.verify(&protected).await?;
            self.workload.expected = baseline;
            for mut record in tail {
                let found = er.api().get_transaction(&record.signature).await?;
                if let Some(evidence) = found {
                    record.check(&evidence)?;
                    self.workload.expected.apply(&record, &evidence)?;
                    record.evidence = Some(evidence);
                    protected.push(record);
                }
            }
            self.workload.check(er).await?;
            for _ in 0..2 {
                protected.push(self.workload.submit(er, true).await?);
            }
        }
        self.private.finish().await?;
        Ok(())
    }
}

impl CheckpointDurability {
    async fn head(&self) -> Result<u64> {
        Ok(self
            .private
            .ctx()
            .scrape_metrics()
            .await?
            .get("engine_ledger_superblocks")
            .context("missing ledger head metric")? as u64)
    }

    async fn checkpoint(&self) -> Result<u64> {
        let sealed = self.head().await?;
        let archive = self
            .private
            .storage_dir()
            .join(format!("superblock-{:09}", sealed + 1))
            .join("accountsdb.tar.zst");
        wait::retry(
            &format!("checkpoint {sealed} archived at {}", archive.display()),
            CHECKPOINT_TIMEOUT,
            async || {
                ensure!(self.head().await? > sealed);
                let metadata = archive.metadata()?;
                ensure!(metadata.is_file() && metadata.len() > 0);
                Ok(())
            },
        )
        .await?;
        Ok(sealed)
    }

    async fn verify(&self, protected: &[Record]) -> Result<()> {
        for record in protected {
            ensure!(
                self.private.ctx().api().get_transaction(&record.signature).await?
                    == record.evidence
            );
        }
        Ok(())
    }
}
