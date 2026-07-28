use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

use async_trait::async_trait;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    assert::poll_until,
    host, prep,
    runner::{drive, drive_until, RunConfig, RunOutcome},
    topology::{self, RestartConfig, RestartTiming},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport, TxSender,
};

const PREP_PAYER_LAMPORTS: u64 = 4_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(40);
const READY_TIMEOUT: Duration = Duration::from_secs(120);

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: usize,
    fill: u64,
    load_rate: u32,
    concurrency: usize,
    ramp: Duration,
    settle: Duration,
    database_size: u64,
    index_size: u64,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    accounts: 32,
    fill: 3_000,
    load_rate: 200,
    concurrency: 64,
    ramp: Duration::from_secs(3),
    settle: Duration::from_secs(2),
    database_size: 512 * 1024 * 1024,
    index_size: 64 * 1024 * 1024,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    accounts: 64,
    fill: 30_000,
    load_rate: 800,
    concurrency: 256,
    ramp: Duration::from_secs(5),
    settle: Duration::from_secs(3),
    database_size: 2 * 1024 * 1024 * 1024,
    index_size: 256 * 1024 * 1024,
};

const DEEP: Profile = Profile {
    name: "deep",
    payers: 32,
    accounts: 96,
    fill: 120_000,
    load_rate: 1_000,
    concurrency: 256,
    ramp: Duration::from_secs(8),
    settle: Duration::from_secs(4),
    database_size: 6 * 1024 * 1024 * 1024,
    index_size: 512 * 1024 * 1024,
};

fn profile() -> &'static Profile {
    match std::env::var("REDSUITE_PROFILE") {
        Ok(name) if name == "full" => &FULL,
        Ok(name) if name == "deep" => &DEEP,
        Ok(name) if name == "lite" => &LITE,
        Ok(name) => {
            panic!("unknown REDSUITE_PROFILE `{name}` (lite|full|deep)")
        }
        Err(_) => &LITE,
    }
}

fn shape(pool: &[Pubkey], id: u64) -> Instruction {
    use crate::program::instruction::build;
    let len = pool.len() as u64;
    let base_index = ((id - 1) * 3) % len;
    build::account_data_copy(
        id,
        &[pool[base_index as usize]],
        &[
            pool[((base_index + 1) % len) as usize],
            pool[((base_index + 2) % len) as usize],
        ],
    )
}

// The validator's own startup-timing lines, at info level. The quoted step
// names survive the ANSI codes tracing puts between tokens; `ledger_replay`
// is emitted only when replay actually ran, `maybe_process_ledger` on every
// boot — its presence proves the timing lines are visible at the current log
// level, so replay metrics are omitted (not reported 0) when they are not.
const REPLAY_STEP: &str = "\"ledger_replay\"";
const LEDGER_TIMING_ANCHOR: &str = "\"maybe_process_ledger\"";

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

struct LoadTally {
    delivered: u64,
    failed: u64,
    outage: Option<Duration>,
    achieved_tps: f64,
}

#[derive(Default)]
struct OutageState {
    last_ok: Option<tokio::time::Instant>,
    outage_start: Option<tokio::time::Instant>,
    max_outage: Option<Duration>,
}

impl OutageState {
    fn close_open_outage(&mut self) {
        if let Some(started) = self.outage_start.take() {
            let gap = started.elapsed();
            if self.max_outage.is_none_or(|prev| gap > prev) {
                self.max_outage = Some(gap);
            }
        }
    }
}

struct BackgroundLoad {
    stop: Rc<Cell<bool>>,
    state: Rc<RefCell<OutageState>>,
    task: tokio::task::JoinHandle<RunOutcome>,
}

impl BackgroundLoad {
    fn start(
        senders: Vec<TxSender>,
        pool: Rc<Vec<Pubkey>>,
        first_id: u64,
        rate: u32,
        concurrency: usize,
    ) -> Self {
        let stop = Rc::new(Cell::new(false));
        let state = Rc::new(RefCell::new(OutageState::default()));
        let stop_flag = stop.clone();
        let outage = state.clone();
        let task = tokio::task::spawn_local(async move {
            drive_until(rate, concurrency, stop_flag, |id| {
                let global_id = first_id + id;
                let sender =
                    senders[(global_id as usize) % senders.len()].clone();
                let ix = shape(&pool, global_id);
                let outage = outage.clone();
                async move {
                    match sender.send_fresh(&[ix]).await {
                        Ok(_) => {
                            let mut state = outage.borrow_mut();
                            state.close_open_outage();
                            state.last_ok = Some(tokio::time::Instant::now());
                            Ok(())
                        }
                        Err(e) => {
                            let mut state = outage.borrow_mut();
                            if state.outage_start.is_none() {
                                state.outage_start =
                                    Some(state.last_ok.unwrap_or_else(
                                        tokio::time::Instant::now,
                                    ));
                            }
                            Err(e)
                        }
                    }
                }
            })
            .await
        });
        Self { stop, state, task }
    }

    async fn finish(self) -> Result<LoadTally> {
        self.stop.set(true);
        let outcome = self
            .task
            .await
            .map_err(|e| format!("background load task: {e}"))?;
        // An outage still open when the load stops is real downtime the
        // client observed — record it (as a lower bound) instead of
        // reporting the worst restart as zero outage.
        let mut state = self.state.borrow_mut();
        state.close_open_outage();
        Ok(LoadTally {
            delivered: outcome.delivered,
            failed: outcome.failed,
            outage: state.max_outage,
            achieved_tps: outcome.achieved_rps(),
        })
    }
}

struct CellOutcome {
    name: &'static str,
    timing: RestartTiming,
    load: LoadTally,
    replay_ran: Option<bool>,
    ledger_processing_ms: Option<f64>,
}

async fn restart_cell(
    private: &mut topology::PrivateEr,
    senders: &[TxSender],
    pool: Rc<Vec<Pubkey>>,
    first_id: u64,
    profile: &Profile,
    name: &'static str,
    config: RestartConfig,
) -> Result<CellOutcome> {
    let load = BackgroundLoad::start(
        senders.to_vec(),
        pool,
        first_id,
        profile.load_rate,
        profile.concurrency,
    );
    tokio::time::sleep(profile.ramp).await;

    let timing = private.restart(config).await?;

    tokio::time::sleep(profile.settle).await;
    let load = load.finish().await?;

    let log_text = std::fs::read_to_string(private.log()).unwrap_or_default();
    let timing_visible = log_text.contains(LEDGER_TIMING_ANCHOR);
    let replay_ran = timing_visible.then(|| log_text.contains(REPLAY_STEP));
    let ledger_processing_ms = timing_ms(&log_text, LEDGER_TIMING_ANCHOR);

    eprintln!(
        "[redsuite] restart_under_load {name}: total {} ms (shutdown {} ms, startup {} ms), \
         exit {:?}/sig {:?}, needed_sigkill {}, slot {:?} -> {:?}, replay_ran {:?}, \
         load delivered {} failed {} @ {:.0} tps, client outage {} ms",
        timing.total.as_millis(),
        timing.shutdown.as_millis(),
        timing.startup.as_millis(),
        timing.exit_code,
        timing.exit_signal,
        timing.needed_sigkill,
        timing.slot_before,
        timing.slot_after,
        replay_ran,
        load.delivered,
        load.failed,
        load.achieved_tps,
        load.outage.map(|d| d.as_millis()).unwrap_or(0),
    );

    Ok(CellOutcome {
        name,
        timing,
        load,
        replay_ran,
        ledger_processing_ms,
    })
}

pub struct RestartUnderLoad;

#[async_trait(?Send)]
impl Scenario for RestartUnderLoad {
    fn name(&self) -> &str {
        "redline/restart_under_load"
    }

    async fn run(&self, base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        let profile = profile();
        let prep_payers =
            prep::funded_payers(base, profile.payers, PREP_PAYER_LAMPORTS)
                .await?;

        let mut private = topology::private_er(
            base,
            topology::ErOptions {
                label: "restart-under-load".to_owned(),
                env: vec![
                    (
                        "MBV_ACCOUNTSDB__DATABASE_SIZE".to_owned(),
                        profile.database_size.to_string(),
                    ),
                    (
                        "MBV_ACCOUNTSDB__INDEX_SIZE".to_owned(),
                        profile.index_size.to_string(),
                    ),
                    ("MBV_LEDGER__BLOCK_TIME".to_owned(), "50ms".to_owned()),
                ],
                request_timeout: None,
                ..Default::default()
            },
        )
        .await?;
        private.wait_ready(READY_TIMEOUT).await?;
        let cell_er = private.ctx();

        let pool = crate::init_delegated_accounts_batched(
            base,
            &prep_payers,
            profile.accounts,
            crate::ACCOUNT_SPACE,
            cell_er.identity(),
        )
        .await?;
        for pda in &pool {
            poll_until(CLONE_TIMEOUT, || async {
                matches!(cell_er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
            })
            .await;
        }
        let pool = Rc::new(pool);

        let payers: Vec<Rc<Keypair>> =
            prep_payers.into_iter().map(Rc::new).collect();
        let senders: Vec<TxSender> = payers
            .iter()
            .map(|payer| cell_er.sender(payer.clone()))
            .collect();

        let storage_at_boot = host::dir_size_bytes(private.storage_dir())?;
        let fill = drive(
            RunConfig {
                iterations: profile.fill,
                rate: profile.load_rate,
                concurrency: profile.concurrency,
            },
            |id| {
                let sender = senders[(id as usize) % senders.len()].clone();
                let ix = shape(&pool, id);
                async move { sender.send(&[ix]).await.map(|_| ()) }
            },
        )
        .await;
        assert_eq!(
            fill.failed, 0,
            "fill deliveries failed: {:?}",
            fill.first_error
        );
        let db_size_at_restart = host::dir_size_bytes(private.storage_dir())?;
        let fill_growth = db_size_at_restart.saturating_sub(storage_at_boot);

        let graceful = restart_cell(
            &mut private,
            &senders,
            pool.clone(),
            profile.fill,
            profile,
            "graceful",
            RestartConfig {
                hard_kill: false,
                reset: false,
                ready_timeout: READY_TIMEOUT,
            },
        )
        .await?;

        let crash = restart_cell(
            &mut private,
            &senders,
            pool.clone(),
            profile.fill * 2,
            profile,
            "crash",
            RestartConfig {
                hard_kill: true,
                reset: false,
                ready_timeout: READY_TIMEOUT,
            },
        )
        .await?;

        assert!(
            fill_growth > 0,
            "INVALID: the fill phase did not grow the on-disk database"
        );
        assert!(
            graceful.load.delivered > 0,
            "INVALID: no load was delivered before the graceful restart"
        );
        if let (Some(before), Some(after)) =
            (graceful.timing.slot_before, graceful.timing.slot_after)
        {
            assert!(
                after >= before / 2,
                "INVALID: post-restart slot {after} collapsed from {before} — \
                 the ER did not reopen its database"
            );
        }

        assert!(
            !graceful.timing.needed_sigkill,
            "graceful restart: the ER did not exit on SIGTERM within the grace \
             window and had to be SIGKILLed"
        );
        assert_eq!(
            graceful.timing.exit_code,
            Some(0),
            "graceful restart: SIGTERM did not produce a clean exit \
             (code {:?}, signal {:?})",
            graceful.timing.exit_code,
            graceful.timing.exit_signal,
        );

        let mut report = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("fill iters", profile.fill)
            .setting("offered load tps", profile.load_rate)
            .setting("concurrency", profile.concurrency)
            .setting("database size", profile.database_size)
            .setting("index size", profile.index_size)
            .metric("db size at restart mb", db_size_at_restart as f64 / 1e6)
            .metric("fill growth mb", fill_growth as f64 / 1e6)
            .metric(
                "restart total ms",
                graceful.timing.total.as_secs_f64() * 1e3,
            );
        for cell in [&graceful, &crash] {
            report = report
                .metric(
                    format!("{} total ms", cell.name),
                    cell.timing.total.as_secs_f64() * 1e3,
                )
                .metric(
                    format!("{} shutdown ms", cell.name),
                    cell.timing.shutdown.as_secs_f64() * 1e3,
                )
                .metric(
                    format!("{} startup ms", cell.name),
                    cell.timing.startup.as_secs_f64() * 1e3,
                )
                .metric(
                    format!("{} needed sigkill", cell.name),
                    cell.timing.needed_sigkill as u8 as f64,
                )
                .metric(
                    format!("{} exit code", cell.name),
                    cell.timing.exit_code.unwrap_or(-1) as f64,
                )
                .metric_if(
                    format!("{} replay ran", cell.name),
                    cell.replay_ran.map(|ran| ran as u8 as f64),
                )
                .metric_if(
                    format!("{} ledger processing ms", cell.name),
                    cell.ledger_processing_ms,
                )
                .metric(
                    format!("{} load delivered", cell.name),
                    cell.load.delivered as f64,
                )
                .metric(
                    format!("{} load failed", cell.name),
                    cell.load.failed as f64,
                )
                .metric(
                    format!("{} load achieved tps", cell.name),
                    cell.load.achieved_tps,
                )
                .metric(
                    format!("{} client outage ms", cell.name),
                    cell.load
                        .outage
                        .map(|d| d.as_secs_f64() * 1e3)
                        .unwrap_or(0.0),
                );
        }

        Ok(report)
    }
}
