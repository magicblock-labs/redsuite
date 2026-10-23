use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    fs,
    path::Path,
    rc::Rc,
    time::Duration,
};

use anyhow::{ensure, Context};
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    programs::{CellHeader, REDLINE},
    redline::{causal, Accounts},
    topology::{self, ErOptions, PrivateEr, RestartConfig, RestartOutcome},
    wait, Api, BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, TxSender,
};
use signature::Signature;
use tokio::{sync::watch, task::JoinSet, time};
use tracing::{debug, instrument};

use crate::ACCOUNT_SPACE;

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const READY_TIMEOUT: Duration = Duration::from_secs(120);
const SUPERBLOCK_POLL: Duration = Duration::from_millis(250);
const LANE_BACKOFF: Duration = Duration::from_millis(100);
const BLOCKTIME: Duration = Duration::from_millis(50);
const SUPERBLOCK_SLOTS: u64 = 20;
const HASH_ITERS: u32 = 1;
const SIGKILL: i32 = 9;
const SUPERBLOCKS: &str = "engine_ledger_superblocks";
const ACCOUNTSDB_SNAPSHOT_FILE: &str = "accountsdb.tar.zst";
const LANES: usize = 8;
const FILL: usize = 128;
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(110);
const RESUME_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Graceful,
    Sigkill,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Mode::Graceful => "graceful",
            Mode::Sigkill => "sigkill",
        }
    }

    fn hard_kill(self) -> bool {
        matches!(self, Mode::Sigkill)
    }

    fn restart_config(self) -> RestartConfig {
        RestartConfig {
            hard_kill: self.hard_kill(),
            reset: false,
            ready_timeout: READY_TIMEOUT,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Outcome {
    Unresolved,
    Confirmed,
    Failed,
    Dropped,
}

struct Record {
    id: u64,
    lane: usize,
    signature: Signature,
    outcome: Outcome,
}

async fn superblocks(er: &ErCtx) -> Result<u64> {
    Ok(er.scrape_metrics().await?.get(SUPERBLOCKS).unwrap_or(0.0) as u64)
}

fn archived_snapshots(storage_dir: &Path) -> Result<usize> {
    let mut count = 0;
    for entry in fs::read_dir(storage_dir)? {
        let entry = entry?;
        if entry.file_name().to_str().is_some_and(|name| name.starts_with("superblock-"))
            && entry.path().join(ACCOUNTSDB_SNAPSHOT_FILE).is_file()
        {
            count += 1;
        }
    }
    Ok(count)
}

struct LaneState {
    admit: Cell<bool>,
    stop: watch::Sender<bool>,
    records: RefCell<Vec<Record>>,
    next_id: Rc<Cell<u64>>,
    api: Api,
}

impl LaneState {
    async fn run(self: Rc<Self>, lane: usize, account: Pubkey, sender: TxSender) {
        while self.admit.get() {
            if self.send(lane, account, &sender).await.is_err() {
                time::sleep(LANE_BACKOFF).await;
            }
        }
    }

    async fn send(&self, lane: usize, account: Pubkey, sender: &TxSender) -> Result<()> {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        let ix = REDLINE.hash(id, causal::HASH_INIT.to_bytes(), HASH_ITERS, &[account]);
        let tx = sender.prepare(&[ix]).await?;
        let signature = tx.signatures[0];
        let index = {
            let mut records = self.records.borrow_mut();
            let index = records.len();
            records.push(Record {
                id,
                lane,
                signature,
                outcome: Outcome::Unresolved,
            });
            index
        };
        let outcome = match sender.submit_prepared(&tx).await {
            Err(_) => Outcome::Unresolved,
            Ok(_) => await_status(&self.api, &signature, &self.stop).await,
        };
        self.records.borrow_mut()[index].outcome = outcome;
        Ok(())
    }
}

struct Lanes {
    state: Rc<LaneState>,
    tasks: JoinSet<()>,
}

impl Lanes {
    fn start(api: Api, senders: &[TxSender], pool: &[Pubkey], next_id: Rc<Cell<u64>>) -> Self {
        let state = Rc::new(LaneState {
            api,
            next_id,
            admit: Cell::new(true),
            stop: watch::channel(false).0,
            records: RefCell::new(Vec::new()),
        });
        let mut tasks = JoinSet::new();
        for (lane, sender) in senders.iter().enumerate() {
            tasks.spawn_local(state.clone().run(lane, pool[lane], sender.clone()));
        }
        Self { state, tasks }
    }

    fn halt(&self) {
        self.state.admit.set(false);
        self.state.stop.send_replace(true);
    }

    fn confirmed(&self) -> usize {
        self.state
            .records
            .borrow()
            .iter()
            .filter(|r| r.outcome == Outcome::Confirmed)
            .count()
    }

    async fn drain(self) -> Vec<Record> {
        self.state.admit.set(false);
        self.tasks.join_all().await;
        self.state.records.take()
    }
}

async fn await_status(api: &Api, signature: &Signature, stop: &watch::Sender<bool>) -> Outcome {
    let mut stopped = stop.subscribe();
    if *stopped.borrow() {
        return Outcome::Unresolved;
    }
    tokio::select! {
        _ = stopped.changed() => Outcome::Unresolved,
        outcome = api.try_confirm(signature) => match outcome {
            Ok(Ok(())) => Outcome::Confirmed,
            Ok(Err(_)) => Outcome::Failed,
            Err(_) => Outcome::Unresolved,
        }
    }
}

async fn resolve(api: &Api, records: &mut [Record]) -> Result<()> {
    for record in records.iter_mut() {
        if record.outcome != Outcome::Unresolved {
            continue;
        }
        record.outcome = match api.get_transaction(&record.signature).await? {
            Some(tx) if transaction_meta(&tx)?.err.is_none() => Outcome::Confirmed,
            Some(_) => Outcome::Failed,
            None => Outcome::Dropped,
        };
    }
    Ok(())
}

async fn check_state(er: &ErCtx, label: &str, pool: &[Pubkey], records: &[Record]) -> Result<()> {
    let mut expected = vec![None; pool.len()];
    for record in records.iter().filter(|record| record.outcome == Outcome::Confirmed) {
        let id = &mut expected[record.lane];
        *id = Some(id.unwrap_or(0).max(record.id));
    }
    for (pda, expected_id) in pool.iter().zip(expected) {
        let Some(expected_id) = expected_id else {
            continue;
        };
        let account = er
            .account(pda)
            .await?
            .with_context(|| format!("{label}: account missing on the ER"))?;
        let on_chain = CellHeader::written_id(&account.data)
            .with_context(|| format!("{label}: account data too short"))?;
        ensure!(on_chain == expected_id);
    }
    Ok(())
}

async fn settle_after_restart(api: &Api, mode: Mode, records: &mut [Record]) -> Result<()> {
    let mut per_lane: BTreeMap<usize, Vec<&mut Record>> = BTreeMap::new();
    for record in records.iter_mut().filter(|record| record.outcome == Outcome::Confirmed) {
        per_lane.entry(record.lane).or_default().push(record);
    }
    for mut lane in per_lane.into_values() {
        lane.sort_by_key(|record| record.id);
        let mut recovered = false;
        for record in lane.into_iter().rev() {
            let tx = api.get_transaction(&record.signature).await?;
            // A crash may lose a lane's tail, but never a hole before recovered history.
            if tx.is_none() && mode.hard_kill() && !recovered {
                record.outcome = Outcome::Dropped;
                continue;
            }
            let tx = tx.with_context(|| {
                format!(
                    "confirmed transaction {} missing after restart",
                    record.signature
                )
            })?;
            ensure!(
                transaction_meta(&tx)?.err.is_none(),
                "confirmed transaction {} failed after restart",
                record.signature
            );
            recovered = true;
        }
    }
    Ok(())
}

fn check_exit(mode: Mode, outcome: &RestartOutcome) -> Result<()> {
    match mode {
        Mode::Graceful => {
            ensure!(!outcome.needed_sigkill);
            ensure!(outcome.exit_code == Some(0));
        }
        Mode::Sigkill => {
            ensure!(outcome.exit_signal == Some(SIGKILL));
        }
    }
    if let (Some(before), Some(after)) = (outcome.slot_before, outcome.slot_after) {
        ensure!(after >= before / 2);
    }
    Ok(())
}

async fn run_mode(base: &BaseCtx, mode: Mode) -> Result<()> {
    let label = mode.label();
    let prep_payers = base.funded_payers(LANES, PREP_PAYER_LAMPORTS).await?;

    let mut private = topology::private_er(
        base,
        ErOptions {
            label: format!("restart-{label}"),
            settings: Box::new(|params| {
                params.engine.blockstore.blocktime = BLOCKTIME;
                params.engine.blockstore.superblock = SUPERBLOCK_SLOTS;
            }),
            ..Default::default()
        },
    )
    .await?;
    private.wait_ready(READY_TIMEOUT).await?;

    let pool = {
        let er = private.ctx();
        let pool = Accounts::new(ACCOUNT_SPACE, er.identity())
            .init_batched(base, &prep_payers, LANES, true)
            .await?;
        er.await_clones(&pool, ACCOUNT_SPACE as usize).await?;
        pool
    };
    let senders: Vec<TxSender> = prep_payers
        .into_iter()
        .map(|payer| private.ctx().sender(Rc::new(payer)))
        .collect();
    let next_id = Rc::new(Cell::new(1u64));

    let lanes = fill(&private, &senders, &pool, next_id.clone(), label).await?;

    lanes.halt();
    let first = private.restart(mode.restart_config()).await?;
    check_exit(mode, &first)?;
    let mut records = lanes.drain().await;
    ensure!(records.iter().any(|record| record.outcome == Outcome::Confirmed));

    let api = private.ctx().api().clone();
    // Restart readiness follows ledger recovery; absent submissions were not retained.
    resolve(&api, &mut records).await?;
    settle_after_restart(&api, mode, &mut records).await?;
    check_state(
        private.ctx(),
        &format!("{label} after first restart"),
        &pool,
        &records,
    )
    .await?;
    debug!(label, "first restart under load finished");

    let superblocks_before = superblocks(private.ctx()).await?;
    let resumed = Lanes::start(api.clone(), &senders, &pool, next_id.clone());
    let er = private.ctx();
    wait::retry_every(
        &format!("{label}: the resumed load crossing a superblock boundary"),
        RESUME_TIMEOUT,
        SUPERBLOCK_POLL,
        async || {
            ensure!(superblocks(er).await? > superblocks_before);
            Ok(())
        },
    )
    .await?;
    let mut resumed_records = resumed.drain().await;
    resolve(&api, &mut resumed_records).await?;
    ensure!(!resumed_records.is_empty());
    ensure!(resumed_records.iter().all(|record| record.outcome == Outcome::Confirmed));
    records.extend(resumed_records);
    check_state(er, &format!("{label} after resume"), &pool, &records).await?;

    let second = private.restart(mode.restart_config()).await?;
    check_exit(mode, &second)?;
    settle_after_restart(&api, mode, &mut records).await?;
    check_state(
        private.ctx(),
        &format!("{label} after second restart"),
        &pool,
        &records,
    )
    .await?;
    private.finish().await
}

pub struct RestartUnderLoad {
    base: Rc<BaseCtx>,
}

impl PrivateErScenario for RestartUnderLoad {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        Ok(Self { base })
    }

    async fn run(self) -> Result<()> {
        time::timeout(SCENARIO_TIMEOUT, async {
            run_mode(&self.base, Mode::Graceful).await?;
            run_mode(&self.base, Mode::Sigkill).await
        })
        .await
        .context("restart under load exceeded its 110-second budget")?
    }
}

#[instrument(skip_all, err, fields(mode = label))]
async fn fill(
    private: &PrivateEr,
    senders: &[TxSender],
    pool: &[Pubkey],
    next_id: Rc<Cell<u64>>,
    label: &str,
) -> Result<Lanes> {
    let superblocks_at_boot = superblocks(private.ctx()).await?;
    let snapshots_at_boot = archived_snapshots(private.storage_dir())?;
    let lanes = Lanes::start(private.ctx().api().clone(), senders, pool, next_id);
    let er = private.ctx();
    let storage_dir = private.storage_dir();
    wait::retry_every(
        &format!(
            "{label}: the fill of {} confirmed transactions and a sealed \
             superblock with its archived snapshot",
            FILL
        ),
        RESUME_TIMEOUT * 4,
        SUPERBLOCK_POLL,
        async || {
            ensure!(
                lanes.confirmed() >= FILL
                    && superblocks(er).await? > superblocks_at_boot
                    && archived_snapshots(storage_dir)? > snapshots_at_boot
            );
            Ok(())
        },
    )
    .await?;
    Ok(lanes)
}
