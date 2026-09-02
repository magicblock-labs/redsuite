use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, host, prep,
    profile::{self, ProfileValues},
    topology::{self, RestartConfig, RestartTiming},
    Api, BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport, TxSender,
};
use signature::Signature;

use crate::program::instruction::build;

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
const SUPERBLOCK_SLOTS: u64 = 100;
const HASH_ITERS: u32 = 1;
const HASH_INIT: Pubkey = Pubkey::new_from_array([7u8; 32]);
const PROGRAM: Pubkey = crate::program::ID;
const VERIFY_CAP: usize = 4_000;
const SIGKILL: i32 = 9;
const SUPERBLOCKS: &str = "engine_ledger_superblocks";
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

const DEEP: Profile = Profile {
    name: "deep",
    lanes: 96,
    fill: 120_000,
    resume_timeout: Duration::from_secs(120),
};

const PROFILES: ProfileValues<Profile> = ProfileValues {
    lite: LITE,
    full: FULL,
    soak: None,
    deep: Some(DEEP),
};

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

struct Lanes {
    stop: Rc<Cell<bool>>,
    records: Rc<RefCell<Vec<Record>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Lanes {
    fn start(
        api: Api,
        senders: &[TxSender],
        pool: &Rc<Vec<Pubkey>>,
        next_id: Rc<Cell<u64>>,
    ) -> Self {
        let stop = Rc::new(Cell::new(false));
        let records = Rc::new(RefCell::new(Vec::new()));
        let tasks = senders
            .iter()
            .enumerate()
            .map(|(lane, sender)| {
                let account = pool[lane];
                let sender = sender.clone();
                let api = api.clone();
                let stop = stop.clone();
                let records = records.clone();
                let next_id = next_id.clone();
                tokio::task::spawn_local(async move {
                    while !stop.get() {
                        let id = next_id.get();
                        next_id.set(id + 1);
                        let ix = build::expensive_hash_compute_at(
                            PROGRAM,
                            id,
                            HASH_INIT,
                            HASH_ITERS,
                            &[account],
                        );
                        let Ok(tx) = sender.prepare(&[ix]).await else {
                            tokio::time::sleep(LANE_BACKOFF).await;
                            continue;
                        };
                        let signature = tx.signatures[0];
                        let index = {
                            let mut records = records.borrow_mut();
                            records.push(Record {
                                id,
                                lane,
                                signature,
                                outcome: Outcome::Unresolved,
                            });
                            records.len() - 1
                        };
                        let outcome = match sender.submit_prepared(&tx).await {
                            Err(_) => Outcome::Rejected,
                            Ok(_) => {
                                await_status(&api, &signature, &stop).await
                            }
                        };
                        records.borrow_mut()[index].outcome = outcome;
                    }
                })
            })
            .collect();
        Self {
            stop,
            records,
            tasks,
        }
    }

    fn halt(&self) {
        self.stop.set(true);
    }

    fn confirmed(&self) -> usize {
        self.records
            .borrow()
            .iter()
            .filter(|record| record.outcome == Outcome::Confirmed)
            .count()
    }

    async fn join(self) -> Result<Vec<Record>> {
        self.stop.set(true);
        for task in self.tasks {
            task.await.map_err(|error| format!("lane task: {error}"))?;
        }
        Ok(self.records.take())
    }
}

async fn await_status(
    api: &Api,
    signature: &Signature,
    stop: &Rc<Cell<bool>>,
) -> Outcome {
    let deadline = tokio::time::Instant::now() + CONFIRM_TIMEOUT;
    loop {
        if let Ok(Some(status)) = api.get_signature_status(signature).await {
            if status.err.is_some() {
                return Outcome::Failed;
            }
            if status.confirmed {
                return Outcome::Confirmed;
            }
        }
        if stop.get() || tokio::time::Instant::now() >= deadline {
            return Outcome::Unresolved;
        }
        tokio::time::sleep(CONFIRM_POLL).await;
    }
}

async fn run_lanes_until<F, Fut>(
    what: &str,
    timeout: Duration,
    mut done: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while !done().await? {
        check!(
            tokio::time::Instant::now() < deadline,
            "{what} did not happen within {timeout:?}"
        )?;
        tokio::time::sleep(SUPERBLOCK_POLL).await;
    }
    Ok(())
}

async fn resolve(api: &Api, records: &mut [Record]) -> Result<()> {
    let grace_ends = tokio::time::Instant::now() + RESOLVE_GRACE;
    for record in records.iter_mut() {
        if !matches!(record.outcome, Outcome::Unresolved | Outcome::Rejected) {
            continue;
        }
        loop {
            match api.get_transaction(&record.signature).await? {
                Some(tx) => {
                    record.outcome = if tx.err.is_none() {
                        Outcome::Confirmed
                    } else {
                        Outcome::Failed
                    };
                    break;
                }
                None if tokio::time::Instant::now() >= grace_ends => {
                    record.outcome = Outcome::Dropped;
                    break;
                }
                None => tokio::time::sleep(RESOLVE_POLL).await,
            }
        }
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

async fn check_state(
    er: &ErCtx,
    label: &str,
    pool: &[Pubkey],
    records: &[Record],
) -> Result<()> {
    let expected = expected_ids(records, pool.len());
    for (lane, pda) in pool.iter().enumerate() {
        let Some(expected_id) = expected[lane] else {
            continue;
        };
        let account = er.account(pda).await?.ok_or_else(|| {
            format!("{label}: account {pda} missing on the er")
        })?;
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

async fn check_confirmed_present(
    api: &Api,
    label: &str,
    records: &[Record],
) -> Result<usize> {
    let confirmed: Vec<&Record> = records
        .iter()
        .filter(|record| record.outcome == Outcome::Confirmed)
        .collect();
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
    if let (Some(before), Some(after)) = (timing.slot_before, timing.slot_after)
    {
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
    resume: Tally,
    superblocks_crossed: u64,
}

async fn run_mode(
    base: &BaseCtx,
    profile: &Profile,
    mode: Mode,
) -> Result<ModeOutcome> {
    let label = mode.label();
    let prep_payers =
        prep::funded_payers(base, profile.lanes, PREP_PAYER_LAMPORTS).await?;

    let mut private = topology::private_er(
        base,
        topology::ErOptions {
            label: format!("restart-{label}"),
            env: vec![
                (
                    "MBV_ENGINE__BLOCKSTORE__BLOCKTIME".to_owned(),
                    format!("{BLOCKTIME_MS}ms"),
                ),
                (
                    "MBV_ENGINE__BLOCKSTORE__SUPERBLOCK".to_owned(),
                    SUPERBLOCK_SLOTS.to_string(),
                ),
            ],
            request_timeout: None,
        },
    )
    .await?;
    private.wait_ready(READY_TIMEOUT).await?;

    let pool = {
        let er = private.ctx();
        let pool = crate::init_delegated_accounts_batched(
            base,
            &prep_payers,
            profile.lanes,
            crate::ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        for pda in &pool {
            check::poll(
                &format!("the ER clones the delegated pda {pda}"),
                CLONE_TIMEOUT,
                || async {
                    matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                },
            )
            .await?;
        }
        Rc::new(pool)
    };
    let senders: Vec<TxSender> = prep_payers
        .into_iter()
        .map(|payer| private.ctx().sender(Rc::new(payer)))
        .collect();
    let next_id = Rc::new(Cell::new(1u64));

    let storage_at_boot = host::dir_size_bytes(private.storage_dir())?;
    let lanes = Lanes::start(
        private.ctx().api().clone(),
        &senders,
        &pool,
        next_id.clone(),
    );
    run_lanes_until(
        &format!(
            "{label}: the fill of {} confirmed transactions",
            profile.fill
        ),
        profile.resume_timeout * 4,
        || async { Ok(lanes.confirmed() >= profile.fill) },
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
    let mut records = lanes.join().await?;
    let at_kill = Tally::of(&records);
    check!(
        at_kill.confirmed > 0,
        "INVALID: {label}: no transaction was confirmed before the restart"
    )?;

    let log_text = std::fs::read_to_string(private.log()).unwrap_or_default();
    let timing_visible = log_text.contains(LEDGER_TIMING_ANCHOR);
    let replay_ran = timing_visible.then(|| log_text.contains(REPLAY_STEP));
    let ledger_processing_ms = timing_ms(&log_text, LEDGER_TIMING_ANCHOR);

    let api = private.ctx().api().clone();
    resolve(&api, &mut records).await?;
    let resolved = Tally::of(&records);
    check_eq!(
        resolved.unresolved + resolved.rejected,
        0,
        "{label}: every submitted transaction must reach a terminal outcome \
         after the restart, got {resolved:?}"
    )?;
    let confirmed_checked = check_confirmed_present(
        &api,
        &format!("{label} after first restart"),
        &records,
    )
    .await?;
    check_state(
        private.ctx(),
        &format!("{label} after first restart"),
        &pool,
        &records,
    )
    .await?;
    eprintln!(
        "[redsuite] restart_under_load {label}: first restart total {} ms \
         (shutdown {} ms, startup {} ms), exit {:?}/sig {:?}, slot {:?} -> \
         {:?}, replay_ran {replay_ran:?}, at kill {at_kill:?}, resolved \
         {resolved:?}",
        first.total.as_millis(),
        first.shutdown.as_millis(),
        first.startup.as_millis(),
        first.exit_code,
        first.exit_signal,
        first.slot_before,
        first.slot_after,
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
    let resumed_records = resumed.join().await?;
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
    check_confirmed_present(
        &api,
        &format!("{label} after second restart"),
        &records,
    )
    .await?;
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
        resume,
        superblocks_crossed,
    })
}

fn report_mode(
    report: ScenarioReport,
    mode: Mode,
    outcome: &ModeOutcome,
) -> ScenarioReport {
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
    metrics
        .into_iter()
        .fold(report, |report, (name, unit, value)| {
            report.metric(format!("{label} {name}"), unit, value)
        })
}

pub struct RestartUnderLoad;

#[async_trait(?Send)]
impl Scenario for RestartUnderLoad {
    fn name(&self) -> &str {
        "redline/restart_under_load"
    }

    async fn run(&self, base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        let (profile, _) =
            profile::select(self.name(), base.config(), &PROFILES);
        let started = Instant::now();
        let graceful = run_mode(base, profile, Mode::Graceful).await?;
        let sigkill = run_mode(base, profile, Mode::Sigkill).await?;

        let mut report = ScenarioReport::ok(self.name())
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
