use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    fs,
    future::Future,
    path::Path,
    rc::Rc,
    time::{Duration, Instant},
};

use tokio::sync::watch;

use anyhow::{ensure, Context};
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    host,
    profile::ProfileValues,
    programs::{CellHeader, REDLINE},
    redline::{causal, Accounts},
    report::Unit,
    topology::{self, ErOptions, RestartConfig, RestartTiming},
    wait, Api, BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport, TxSender,
};
use signature::Signature;
use tokio::{task::JoinSet, time};
use tracing::debug;

use crate::ACCOUNT_SPACE;

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const READY_TIMEOUT: Duration = Duration::from_secs(120);
const SUPERBLOCK_POLL: Duration = Duration::from_millis(250);
const LANE_BACKOFF: Duration = Duration::from_millis(100);
const BLOCKTIME_MS: u64 = 50;
const BLOCKTIME: Duration = Duration::from_millis(BLOCKTIME_MS);
const SUPERBLOCK_SLOTS: u64 = 100;
const HASH_ITERS: u32 = 1;
const VERIFY_CAP: usize = 4_000;
const SIGKILL: i32 = 9;
const SUPERBLOCKS: &str = "engine_ledger_superblocks";
const ACCOUNTSDB_SNAPSHOT_FILE: &str = "accountsdb.tar.zst";
const REPLAY_STEP: &str = "\"ledger_replay\"";
const LEDGER_TIMING_ANCHOR: &str = "\"maybe_process_ledger\"";

struct Profile {
    name: &'static str,
    lanes: usize,
    fill: usize,
    resume_timeout: Duration,
}

const LITE: Profile = Profile {
    name: "lite",
    lanes: 32,
    fill: 3_000,
    resume_timeout: Duration::from_secs(60),
};

const FULL: Profile = Profile {
    name: "full",
    lanes: 64,
    fill: 30_000,
    resume_timeout: Duration::from_secs(90),
};

const PROFILES: ProfileValues<Profile> = ProfileValues { lite: LITE, full: FULL };

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
    Rejected,
    Confirmed,
    Failed,
    Dropped,
}

#[derive(Clone)]
struct Record {
    id: u64,
    lane: usize,
    signature: Signature,
    outcome: Outcome,
}

#[derive(Default, Debug)]
struct Tally {
    confirmed: usize,
    failed: usize,
    unresolved: usize,
    rejected: usize,
    dropped: usize,
}

impl Tally {
    fn of(records: &[Record]) -> Self {
        let mut tally = Self::default();
        for record in records {
            match record.outcome {
                Outcome::Confirmed => tally.confirmed += 1,
                Outcome::Failed => tally.failed += 1,
                Outcome::Unresolved => tally.unresolved += 1,
                Outcome::Rejected => tally.rejected += 1,
                Outcome::Dropped => tally.dropped += 1,
            }
        }
        tally
    }
}

fn timing_ms(log_text: &str, step: &str) -> Option<f64> {
    let at = log_text.find(step)?;
    let rest = &log_text[at + step.len()..];
    let value_at = rest.find("duration_ms")?;
    let digits: String = rest[value_at + "duration_ms".len()..]
        .chars()
        .skip_while(|character| !character.is_ascii_digit())
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
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
            Err(_) => Outcome::Rejected,
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

    async fn drain(self) -> Result<Vec<Record>> {
        self.state.admit.set(false);
        self.tasks.join_all().await;
        Ok(self.state.records.take())
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

async fn run_lanes_until<F, Fut>(what: &str, timeout: Duration, mut done: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    wait::retry_every(what, timeout, SUPERBLOCK_POLL, async || {
        ensure!(done().await?);
        Ok(())
    })
    .await
}

async fn resolve(api: &Api, records: &mut [Record]) -> Result<()> {
    for record in records.iter_mut() {
        if !matches!(record.outcome, Outcome::Unresolved | Outcome::Rejected) {
            continue;
        }
        record.outcome = match api.await_transaction(&record.signature).await {
            Ok(tx) if transaction_meta(&tx)?.err.is_none() => Outcome::Confirmed,
            Ok(_) => Outcome::Failed,
            Err(error) if error.downcast_ref::<time::error::Elapsed>().is_some() => {
                Outcome::Dropped
            }
            Err(error) => return Err(error),
        };
    }
    Ok(())
}

fn expected_ids(records: &[Record], lanes: usize) -> Vec<Option<u64>> {
    let mut expected = vec![None; lanes];
    for record in records {
        if record.outcome != Outcome::Confirmed {
            continue;
        }
        let slot = &mut expected[record.lane];
        if slot.is_none_or(|current| record.id > current) {
            *slot = Some(record.id);
        }
    }
    expected
}

async fn check_state(er: &ErCtx, label: &str, pool: &[Pubkey], records: &[Record]) -> Result<()> {
    let expected = expected_ids(records, pool.len());
    for (lane, pda) in pool.iter().enumerate() {
        let Some(expected_id) = expected[lane] else {
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

struct Settlement {
    checked: usize,
    lost: usize,
}

async fn settle_after_restart(api: &Api, mode: Mode, records: &mut [Record]) -> Result<Settlement> {
    let lost = if mode.hard_kill() { drop_lost_tail(api, records).await? } else { 0 };
    let checked = check_confirmed_present(api, records).await?;
    Ok(Settlement { checked, lost })
}

async fn drop_lost_tail(api: &Api, records: &mut [Record]) -> Result<usize> {
    let mut per_lane: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (index, record) in records.iter().enumerate() {
        if record.outcome == Outcome::Confirmed {
            per_lane.entry(record.lane).or_default().push(index);
        }
    }
    let mut lost = 0;
    for mut indices in per_lane.into_values() {
        indices.sort_by_key(|&index| records[index].id);
        while let Some(&index) = indices.last() {
            let record = &records[index];
            match api.get_transaction(&record.signature).await? {
                Some(tx) => {
                    let meta = transaction_meta(&tx)?;
                    ensure!(meta.err == None);
                    break;
                }
                None => {
                    records[index].outcome = Outcome::Dropped;
                    lost += 1;
                    indices.pop();
                }
            }
        }
    }
    Ok(lost)
}

async fn check_confirmed_present(api: &Api, records: &[Record]) -> Result<usize> {
    let confirmed: Vec<&Record> =
        records.iter().filter(|record| record.outcome == Outcome::Confirmed).collect();
    let stride = confirmed.len().div_ceil(VERIFY_CAP).max(1);
    let mut checked = 0;
    for record in confirmed.iter().step_by(stride) {
        let tx = api.get_transaction(&record.signature).await?;
        ensure!(tx
            .as_ref()
            .and_then(|tx| tx.transaction.meta.as_ref())
            .is_some_and(|meta| meta.err.is_none()));
        checked += 1;
    }
    Ok(checked)
}

fn check_exit(mode: Mode, timing: &RestartTiming) -> Result<()> {
    match mode {
        Mode::Graceful => {
            ensure!(!timing.needed_sigkill);
            ensure!(timing.exit_code == Some(0));
        }
        Mode::Sigkill => {
            ensure!(timing.exit_signal == Some(SIGKILL));
        }
    }
    if let (Some(before), Some(after)) = (timing.slot_before, timing.slot_after) {
        ensure!(after >= before / 2);
    }
    Ok(())
}

struct ModeOutcome {
    first: RestartTiming,
    second: RestartTiming,
    replay_ran: Option<bool>,
    ledger_processing_ms: Option<f64>,
    db_size_at_restart: u64,
    fill_growth: u64,
    at_kill: Tally,
    resolved: Tally,
    confirmed_checked: usize,
    lost_first: usize,
    lost_second: usize,
    resume: Tally,
    superblocks_crossed: u64,
}

async fn run_mode(base: &BaseCtx, profile: &Profile, mode: Mode) -> Result<ModeOutcome> {
    let label = mode.label();
    let prep_payers = base.funded_payers(profile.lanes, PREP_PAYER_LAMPORTS).await?;

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
            .init_batched(base, &prep_payers, profile.lanes, true)
            .await?;
        er.await_clones(&pool, ACCOUNT_SPACE as usize).await?;
        Rc::new(pool)
    };
    let senders: Vec<TxSender> = prep_payers
        .into_iter()
        .map(|payer| private.ctx().sender(Rc::new(payer)))
        .collect();
    let next_id = Rc::new(Cell::new(1u64));

    let storage_at_boot = host::dir_size_bytes(private.storage_dir())?;
    let superblocks_at_boot = superblocks(private.ctx()).await?;
    let snapshots_at_boot = archived_snapshots(private.storage_dir())?;
    let lanes = Lanes::start(
        private.ctx().api().clone(),
        &senders,
        &pool,
        next_id.clone(),
    );
    let er = private.ctx();
    let storage_dir = private.storage_dir();
    run_lanes_until(
        &format!(
            "{label}: the fill of {} confirmed transactions and a sealed \
             superblock with its archived snapshot",
            profile.fill
        ),
        profile.resume_timeout * 4,
        || async {
            Ok(lanes.confirmed() >= profile.fill
                && superblocks(er).await? > superblocks_at_boot
                && archived_snapshots(storage_dir)? > snapshots_at_boot)
        },
    )
    .await?;
    let db_size_at_restart = host::dir_size_bytes(private.storage_dir())?;
    let fill_growth = db_size_at_restart.saturating_sub(storage_at_boot);
    ensure!(fill_growth > 0);

    lanes.halt();
    let first = private.restart(mode.restart_config()).await?;
    check_exit(mode, &first)?;
    let mut records = lanes.drain().await?;
    let at_kill = Tally::of(&records);
    ensure!(at_kill.confirmed > 0);

    let log_text = fs::read_to_string(private.log()).unwrap_or_default();
    let timing_visible = log_text.contains(LEDGER_TIMING_ANCHOR);
    let replay_ran = timing_visible.then(|| log_text.contains(REPLAY_STEP));
    let ledger_processing_ms = timing_ms(&log_text, LEDGER_TIMING_ANCHOR);

    let api = private.ctx().api().clone();
    resolve(&api, &mut records).await?;
    let settled = settle_after_restart(&api, mode, &mut records).await?;
    let confirmed_checked = settled.checked;
    let lost_first = settled.lost;
    let resolved = Tally::of(&records);
    ensure!((resolved.unresolved + resolved.rejected) == 0);
    check_state(
        private.ctx(),
        &format!("{label} after first restart"),
        &pool,
        &records,
    )
    .await?;
    debug!(
        label,
        replay_ran, lost_first, "first restart under load finished"
    );

    let superblocks_before = superblocks(private.ctx()).await?;
    let resumed = Lanes::start(api.clone(), &senders, &pool, next_id.clone());
    let er = private.ctx();
    run_lanes_until(
        &format!("{label}: the resumed load crossing a superblock boundary"),
        profile.resume_timeout,
        || async { Ok(superblocks(er).await? > superblocks_before) },
    )
    .await?;
    let superblocks_crossed = superblocks(er).await? - superblocks_before;
    let mut resumed_records = resumed.drain().await?;
    resolve(&api, &mut resumed_records).await?;
    let resume = Tally::of(&resumed_records);
    ensure!(resume.confirmed > 0);
    ensure!((resume.unresolved + resume.rejected + resume.dropped + resume.failed) == 0);
    records.extend(resumed_records);
    check_state(er, &format!("{label} after resume"), &pool, &records).await?;

    let second = private.restart(mode.restart_config()).await?;
    check_exit(mode, &second)?;
    let lost_second = settle_after_restart(&api, mode, &mut records).await?.lost;
    check_state(
        private.ctx(),
        &format!("{label} after second restart"),
        &pool,
        &records,
    )
    .await?;
    private.finish().await?;

    Ok(ModeOutcome {
        first,
        second,
        replay_ran,
        ledger_processing_ms,
        db_size_at_restart,
        fill_growth,
        at_kill,
        resolved,
        confirmed_checked,
        lost_first,
        lost_second,
        resume,
        superblocks_crossed,
    })
}

fn report_mode(report: ScenarioReport, mode: Mode, outcome: &ModeOutcome) -> ScenarioReport {
    let label = mode.label();
    let millis = |duration: Duration| duration.as_secs_f64() * 1e3;
    let mut metrics: Vec<(&str, Unit, f64)> = vec![
        (
            "first restart total ms",
            Unit::Millis,
            millis(outcome.first.total),
        ),
        (
            "first restart shutdown ms",
            Unit::Millis,
            millis(outcome.first.shutdown),
        ),
        (
            "first restart startup ms",
            Unit::Millis,
            millis(outcome.first.startup),
        ),
        (
            "second restart total ms",
            Unit::Millis,
            millis(outcome.second.total),
        ),
        (
            "second restart startup ms",
            Unit::Millis,
            millis(outcome.second.startup),
        ),
        (
            "exit code",
            Unit::Count,
            outcome.first.exit_code.unwrap_or(-1) as f64,
        ),
        (
            "exit signal",
            Unit::Count,
            outcome.first.exit_signal.unwrap_or(0) as f64,
        ),
        (
            "db size at restart mb",
            Unit::Megabytes,
            outcome.db_size_at_restart as f64 / 1e6,
        ),
        (
            "fill growth mb",
            Unit::Megabytes,
            outcome.fill_growth as f64 / 1e6,
        ),
        (
            "confirmed at kill",
            Unit::Count,
            outcome.at_kill.confirmed as f64,
        ),
        ("failed at kill", Unit::Count, outcome.at_kill.failed as f64),
        (
            "unresolved at kill",
            Unit::Count,
            outcome.at_kill.unresolved as f64,
        ),
        (
            "rejected at kill",
            Unit::Count,
            outcome.at_kill.rejected as f64,
        ),
        (
            "resolved confirmed",
            Unit::Count,
            outcome.resolved.confirmed as f64,
        ),
        (
            "resolved failed",
            Unit::Count,
            outcome.resolved.failed as f64,
        ),
        (
            "resolved dropped",
            Unit::Count,
            outcome.resolved.dropped as f64,
        ),
        (
            "confirmed entries verified",
            Unit::Count,
            outcome.confirmed_checked as f64,
        ),
        (
            "confirmed lost to first restart",
            Unit::Count,
            outcome.lost_first as f64,
        ),
        (
            "confirmed lost to second restart",
            Unit::Count,
            outcome.lost_second as f64,
        ),
        (
            "resume confirmed",
            Unit::Count,
            outcome.resume.confirmed as f64,
        ),
        (
            "superblocks crossed",
            Unit::Count,
            outcome.superblocks_crossed as f64,
        ),
    ];
    if let Some(ran) = outcome.replay_ran {
        metrics.push(("replay ran", Unit::Count, ran as u8 as f64));
    }
    if let Some(ms) = outcome.ledger_processing_ms {
        metrics.push(("ledger processing ms", Unit::Millis, ms));
    }
    metrics.into_iter().fold(report, |report, (name, unit, value)| {
        report.metric(format!("{label} {name}"), unit, value)
    })
}

pub struct RestartUnderLoad {
    base: Rc<BaseCtx>,
}

impl PrivateErScenario<ScenarioReport> for RestartUnderLoad {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        Ok(Self { base })
    }

    async fn run(self) -> Result<ScenarioReport> {
        let Self { base } = self;
        let profile = PROFILES.select(base.config().profile);
        let started = Instant::now();
        let graceful = run_mode(&base, profile, Mode::Graceful).await?;
        let sigkill = run_mode(&base, profile, Mode::Sigkill).await?;

        let mut report = ScenarioReport::ok()
            .setting("profile", profile.name)
            .setting("shape", "1-iteration sha256 write, one lane per account")
            .setting("lanes", profile.lanes)
            .setting("fill confirmed txs", profile.fill)
            .setting("blocktime ms", BLOCKTIME_MS)
            .setting("superblock slots", SUPERBLOCK_SLOTS)
            .metric(
                "restart total ms",
                Unit::Millis,
                graceful.first.total.as_secs_f64() * 1e3,
            )
            .metric(
                "scenario wall s",
                Unit::Seconds,
                started.elapsed().as_secs_f64(),
            );
        report = report_mode(report, Mode::Graceful, &graceful);
        report = report_mode(report, Mode::Sigkill, &sigkill);
        Ok(report)
    }
}
