use std::{path::Path, rc::Rc, time::Duration};

use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    api::TransactionInfo,
    check, check_eq,
    programs::{CellHeader, REDSHIFT},
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    BaseCtx, ChainCtx, DynError, ErCtx, PrivateErScenario, Result,
};
use signature::Signature;
use signer::Signer;
use transaction::Transaction;

use crate::PAYER_LAMPORTS;

const ROUNDS: usize = 3;
const BLOCKTIME: Duration = Duration::from_millis(50);
const SUPERBLOCK_SLOTS: u64 = 40;
const TIMEOUT: Duration = Duration::from_secs(30);
const CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, PartialEq, Eq)]
struct State {
    payer_lamports: u64,
    cell_lamports: u64,
    cell: CellHeader,
}

impl State {
    fn apply(&mut self, record: &Record, evidence: &TransactionInfo) -> Result<()> {
        self.payer_lamports =
            self.payer_lamports.checked_sub(evidence.fee).ok_or("fee payer exhausted")?;
        if !Record::fails(record.id) {
            self.cell.value += u64::from(record.id);
            self.cell.revision += 1;
        }
        Ok(())
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
        let error =
            Self::fails(self.id).then(|| json::json!({"InstructionError": [1, {"Custom": 0}]}));
        check_eq!(evidence.err, error, "{}: execution outcome", self.signature)?;
        let mut balances = evidence.pre_balances.clone();
        let payer = balances.first_mut().ok_or("missing payer balance")?;
        *payer = payer.checked_sub(evidence.fee).ok_or("fee exceeds balance")?;
        check_eq!(
            evidence.post_balances,
            balances,
            "{}: only fees affect balances",
            self.signature
        )?;
        if let Some(before) = &self.evidence {
            check_eq!(
                evidence,
                before,
                "{}: execution evidence changed",
                self.signature
            )?;
        }
        Ok(())
    }
}

async fn state(er: &ErCtx, keys: &[Pubkey; 2]) -> Result<State> {
    let accounts = er.accounts(keys).await?;
    let payer = accounts[0].as_ref().ok_or("fee payer missing")?;
    let cell = accounts[1].as_ref().ok_or("cell missing")?;
    Ok(State {
        payer_lamports: payer.lamports,
        cell_lamports: cell.lamports,
        cell: CellHeader::try_decode(&cell.data)?,
    })
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
        check_eq!(
            state(er, &self.keys).await?,
            self.expected,
            "cell, rollback and fees must match transaction evidence"
        )?;
        Ok(())
    }

    async fn submit(&mut self, er: &ErCtx, confirm: bool) -> Result<Record> {
        self.next_id = self.next_id.checked_add(1).ok_or("transaction ids exhausted")?;
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
            let evidence = er.api().await_transaction(&record.signature, TIMEOUT).await?;
            record.check(&evidence)?;
            self.expected.apply(&record, &evidence)?;
            self.check(er).await?;
            record.evidence = Some(evidence);
        }
        Ok(record)
    }
}

async fn head(er: &ErCtx) -> Result<u64> {
    Ok(er
        .scrape_metrics()
        .await?
        .get("engine_ledger_superblocks")
        .ok_or("missing ledger head metric")? as u64)
}

async fn checkpoint(er: &ErCtx, storage: &Path) -> Result<u64> {
    let sealed = head(er).await?;
    let archive = storage.join(format!("superblock-{:09}", sealed + 1)).join("accountsdb.tar.zst");
    check::poll_for(
        &format!("checkpoint {sealed} archived at {}", archive.display()),
        CHECKPOINT_TIMEOUT,
        || async {
            check!(head(er).await? > sealed, "ledger seal pending")?;
            let metadata = archive.metadata()?;
            check!(
                metadata.is_file() && metadata.len() > 0,
                "archive incomplete"
            )?;
            Ok::<_, DynError>(())
        },
    )
    .await?;
    Ok(sealed)
}

async fn verify(er: &ErCtx, boundary: u64, protected: &[Record]) -> Result<()> {
    for record in protected {
        check_eq!(
            er.api().get_transaction(&record.signature).await?,
            record.evidence,
            "checkpoint {boundary}: protected transaction {} lost or changed",
            record.signature
        )?;
    }
    Ok(())
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
        let expected =
            check::poll_for("accounts cloned", TIMEOUT, || state(private.ctx(), &keys)).await?;
        check_eq!(
            (expected.cell.value, expected.cell.revision),
            (0, 0),
            "fresh cell"
        )?;
        let workload = Workload {
            payer,
            owner: owner.pubkey(),
            keys,
            next_id: 0,
            expected,
        };
        Ok(Self { private, workload })
    }

    async fn run(self) -> Result<()> {
        let Self { mut private, mut workload } = self;
        let mut protected = Vec::new();
        let mut previous_boundary = 0;

        for round in 1..=ROUNDS {
            let er = private.ctx();
            for _ in 0..8 {
                protected.push(workload.submit(er, true).await?);
            }
            let boundary = checkpoint(er, private.storage_dir()).await?;
            check!(boundary > previous_boundary, "checkpoint must advance")?;
            previous_boundary = boundary;
            verify(er, boundary, &protected).await?;
            workload.check(er).await?;
            let baseline = workload.expected.clone();

            let mut tail = Vec::new();
            for index in 0..16 {
                tail.push(workload.submit(er, index < 4).await?);
            }
            let timing = private
                .restart(RestartConfig {
                    hard_kill: true,
                    reset: false,
                    ready_timeout: CHECKPOINT_TIMEOUT,
                })
                .await?;
            check_eq!(
                timing.exit_signal,
                Some(9),
                "round {round}: SIGKILL required"
            )?;
            let er = private.ctx();
            verify(er, boundary, &protected).await?;
            workload.expected = baseline;
            for mut record in tail {
                let found = er.api().get_transaction(&record.signature).await?;
                if let Some(evidence) = found {
                    record.check(&evidence)?;
                    workload.expected.apply(&record, &evidence)?;
                    record.evidence = Some(evidence);
                    protected.push(record);
                }
            }
            workload.check(er).await?;
            for _ in 0..2 {
                protected.push(workload.submit(er, true).await?);
            }
        }
        private.finish().await?;
        Ok(())
    }
}
