use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    fs,
    future::Future,
    path::Path,
    rc::Rc,
    time::{Duration, Instant},
};

use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, host,
    profile::ProfileValues,
    redline::{causal, Accounts},
    report::Unit,
    topology,
    topology::{ErOptions, RestartConfig, RestartTiming},
    Api, BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport, TxSender,
};
use signature::Signature;
use tokio::{task::JoinSet, time};
use tracing::debug;

use crate::{program::instruction::build, ACCOUNT_SPACE};

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(40);
const READY_TIMEOUT: Duration = Duration::from_secs(120);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);
const CONFIRM_POLL: Duration = Duration::from_millis(50);
const RESOLVE_GRACE: Duration = Duration::from_secs(3);
const RESOLVE_POLL: Duration = Duration::from_millis(200);
const SUPERBLOCK_POLL: Duration = Duration::from_millis(250);
const LANE_BACKOFF: Duration = Duration::from_millis(100);
const BLOCKTIME_MS: u64 = 50;
const BLOCKTIME: Duration = Duration::from_millis(BLOCKTIME_MS);
const SUPERBLOCK_SLOTS: u64 = 100;
const HASH_ITERS: u32 = 1;
const PROGRAM: Pubkey = crate::program::ID;
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
    stop: Cell<bool>,
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
        let ix = build::expensive_hash_compute_at(
            PROGRAM,
            id,
            causal::HASH_INIT,
            HASH_ITERS,
            &[account],
        );
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
            stop: Cell::new(false),
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
        self.state.stop.set(true);
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

async fn await_status(api: &Api, signature: &Signature, stop: &Cell<bool>) -> Outcome {
    let deadline = time::Instant::now() + CONFIRM_TIMEOUT;
    loop {
        if let Ok(Some(status)) = api.get_signature_status(signature).await {
            if status.err.is_some() {
                return Outcome::Failed;
            }
            if status.confirmed {
                return Outcome::Confirmed;
            }
        }
        if stop.get() || time::Instant::now() >= deadline {
            return Outcome::Unresolved;
        }
        time::sleep(CONFIRM_POLL).await;
    }
}

async fn run_lanes_until<F, Fut>(what: &str, timeout: Duration, mut done: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let deadline = time::Instant::now() + timeout;
    while !done().await? {
        check!(
            time::Instant::now() < deadline,
            "{what} did not happen within {timeout:?}"
        )?;
        time::sleep(SUPERBLOCK_POLL).await;
    }
    Ok(())
}

async fn resolve(api: &Api, records: &mut [Record]) -> Result<()> {
    for record in records.iter_mut() {
        if !matches!(record.outcome, Outcome::Unresolved | Outcome::Rejected) {
            continue;
        }
        record.outcome = match check::poll_until(RESOLVE_GRACE, RESOLVE_POLL, async || {
            api.get_transaction(&record.signature).await
        })
        .await?
        {
            Some(tx) if tx.err.is_none() => Outcome::Confirmed,
            Some(_) => Outcome::Failed,
            None => Outcome::Dropped,
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
            .ok_or_else(|| format!("{label}: account {pda} missing on the er"))?;
        let on_chain = crate::account_update_id(&account.data)
            .ok_or_else(|| format!("{label}: account {pda} data too short"))?;
        check_eq!(
            on_chain,
            expected_id,
            "{label}: account {pda} (lane {lane}) must hold the id of its \
             last confirmed transaction"
        )?;
    }
    Ok(())
}

struct Settlement {
    checked: usize,
    lost: usize,
}

async fn settle_after_restart(
    api: &Api,
    mode: Mode,
    label: &str,
    records: &mut [Record],
) -> Result<Settlement> {
    let lost = if mode.hard_kill() { drop_lost_tail(api, label, records).await? } else { 0 };
    let checked = check_confirmed_present(api, label, records).await?;
    Ok(Settlement { checked, lost })
}

async fn drop_lost_tail(api: &Api, label: &str, records: &mut [Record]) -> Result<usize> {
    let mut per_lane: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (index, record) in records.iter().enumerate() {
        if record.outcome == Outcome::Confirmed {
            per_lane.entry(record.lane).or_default().push(index);
        }
    }
    let mut lost = 0;
    for (lane, mut indices) in per_lane {
        indices.sort_by_key(|&index| records[index].id);
        while let Some(&index) = indices.last() {
            let record = &records[index];
            match api.get_transaction(&record.signature).await? {
                Some(tx) => {
                    check!(
                        tx.err.is_none(),
                        "{label}: surviving confirmed transaction {} (id {}, \
                         lane {lane}) replayed as a failure: {:?}",
                        record.signature,
                        record.id,
                        tx.err
                    )?;
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

async fn check_confirmed_present(api: &Api, label: &str, records: &[Record]) -> Result<usize> {
    let confirmed: Vec<&Record> =
        records.iter().filter(|record| record.outcome == Outcome::Confirmed).collect();
    let stride = confirmed.len().div_ceil(VERIFY_CAP).max(1);
    let mut checked = 0;
    for record in confirmed.iter().step_by(stride) {
        let tx = api.get_transaction(&record.signature).await?;
        check!(
            tx.as_ref().is_some_and(|tx| tx.err.is_none()),
            "{label}: confirmed transaction {} (id {}) is no longer a \
             successful ledger entry: {:?}",
            record.signature,
            record.id,
            tx.map(|tx| tx.err)
        )?;
        checked += 1;
    }
    Ok(checked)
}

fn check_exit(mode: Mode, label: &str, timing: &RestartTiming) -> Result<()> {
    match mode {
        Mode::Graceful => {
            check!(
                !timing.needed_sigkill,
                "{label}: the ER did not exit on SIGTERM within the grace \
                 window and had to be SIGKILLed"
            )?;
            check_eq!(
                timing.exit_code,
                Some(0),
                "{label}: SIGTERM did not produce a clean exit (code {:?}, \
                 signal {:?})",
                timing.exit_code,
                timing.exit_signal
            )?;
        }
        Mode::Sigkill => {
            check_eq!(
                timing.exit_signal,
                Some(SIGKILL),
                "{label}: the ER must die from SIGKILL (code {:?}, signal \
                 {:?})",
                timing.exit_code,
                timing.exit_signal
            )?;
        }
    }
    if let (Some(before), Some(after)) = (timing.slot_before, timing.slot_after) {
        check!(
            after >= before / 2,
            "INVALID: {label}: post-restart slot {after} collapsed from \
             {before} — the ER did not reopen its database"
        )?;
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
        er.await_clones(&pool, ACCOUNT_SPACE as usize, CLONE_TIMEOUT).await?;
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
    check!(
        fill_growth > 0,
        "INVALID: {label}: the fill phase did not grow the on-disk database"
    )?;

    lanes.halt();
    let first = private.restart(mode.restart_config()).await?;
    check_exit(mode, &format!("{label} first restart"), &first)?;
    let mut records = lanes.drain().await?;
    let at_kill = Tally::of(&records);
    check!(
        at_kill.confirmed > 0,
        "INVALID: {label}: no transaction was confirmed before the restart"
    )?;

    let log_text = fs::read_to_string(private.log()).unwrap_or_default();
    let timing_visible = log_text.contains(LEDGER_TIMING_ANCHOR);
    let replay_ran = timing_visible.then(|| log_text.contains(REPLAY_STEP));
    let ledger_processing_ms = timing_ms(&log_text, LEDGER_TIMING_ANCHOR);

    let api = private.ctx().api().clone();
    resolve(&api, &mut records).await?;
    let settled = settle_after_restart(
        &api,
        mode,
        &format!("{label} after first restart"),
        &mut records,
    )
    .await?;
    let confirmed_checked = settled.checked;
    let lost_first = settled.lost;
    let resolved = Tally::of(&records);
    check_eq!(
        resolved.unresolved + resolved.rejected,
        0,
        "{label}: every submitted transaction must reach a terminal outcome \
         after the restart, got {resolved:?}"
    )?;
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
    check!(
        resume.confirmed > 0,
        "INVALID: {label}: the resumed load confirmed nothing"
    )?;
    check_eq!(
        resume.unresolved + resume.rejected + resume.dropped + resume.failed,
        0,
        "{label}: the resumed load must drain cleanly, got {resume:?}"
    )?;
    records.extend(resumed_records);
    check_state(er, &format!("{label} after resume"), &pool, &records).await?;

    let second = private.restart(mode.restart_config()).await?;
    check_exit(mode, &format!("{label} second restart"), &second)?;
    let lost_second = settle_after_restart(
        &api,
        mode,
        &format!("{label} after second restart"),
        &mut records,
    )
    .await?
    .lost;
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
