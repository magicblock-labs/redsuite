use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use borsh::BorshDeserialize;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_program::{
    flexi::{build as flexi, FlexiCounter},
    schedulecommit::{
        build as schedulecommit, MainAccount, ScheduleCommitType,
    },
};
use redsuite_core::{
    assert::poll_until, dlp, prep, system, topology, BaseCtx, ChainCtx, ErCtx,
    Result, Scenario, ScenarioReport,
};
use rusqlite::Connection;
use signer::Signer;

const TASK_INTERVAL_MS: i64 = 100;
const CRANK_INTERVAL_MS: i64 = 10;
const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;
const LABEL: &str = "redshift task";
const CLONE_TIMEOUT: Duration = Duration::from_secs(20);
const DB_OPEN_TIMEOUT: Duration = Duration::from_secs(15);
const COUNTER_TIMEOUT: Duration = Duration::from_secs(10);
const TASK_REMOVED_TIMEOUT: Duration = Duration::from_secs(10);
const FAILED_TASK_TIMEOUT: Duration = Duration::from_secs(45);
const FAILED_SCHEDULING_TIMEOUT: Duration = Duration::from_secs(10);
const COMMIT_TIMEOUT: Duration = Duration::from_secs(15);
const SCHEDULE_SETTLE: Duration = Duration::from_millis(250);

pub struct TaskScheduler;

struct TaskDb {
    path: PathBuf,
}

struct TaskRow {
    authority: String,
    execution_interval_millis: i64,
    executions_left: i64,
}

impl TaskDb {
    fn open(&self) -> Result<Connection> {
        let connection = Connection::open(&self.path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(connection)
    }

    fn task_ids(&self) -> Result<Vec<i64>> {
        let connection = self.open()?;
        let mut statement =
            connection.prepare("SELECT id FROM tasks ORDER BY id")?;
        let ids = statement
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<i64>, _>>()?;
        Ok(ids)
    }

    fn task(&self, task_id: i64) -> Result<Option<TaskRow>> {
        let connection = self.open()?;
        let mut statement = connection.prepare(
            "SELECT authority, execution_interval_millis, executions_left \
             FROM tasks WHERE id = ?",
        )?;
        let mut rows = statement.query_map([task_id], |row| {
            Ok(TaskRow {
                authority: row.get(0)?,
                execution_interval_millis: row.get(1)?,
                executions_left: row.get(2)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    fn failures(&self, table: &str) -> Result<Vec<(Option<i64>, String)>> {
        let connection = self.open()?;
        let mut statement = connection.prepare(&format!(
            "SELECT task_id, error FROM {table} ORDER BY id"
        ))?;
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn failed_tasks(&self) -> Result<Vec<(Option<i64>, String)>> {
        self.failures("failed_tasks")
    }

    fn failed_schedulings(&self) -> Result<Vec<(Option<i64>, String)>> {
        self.failures("failed_scheduling")
    }
}

struct Actor {
    payer: Keypair,
    counter: Pubkey,
}

impl Actor {
    fn pubkey(&self) -> Pubkey {
        self.payer.pubkey()
    }
}

// init + delegate the counter on base, then wallet-delegate the payer so it
// can pay er fees, then wait for the er clone.
async fn scheduled_actor(
    base: &BaseCtx,
    er: &ErCtx,
    funder: &Keypair,
) -> Result<Actor> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let (init, counter) = flexi::init_counter(payer.pubkey(), LABEL);
    base.send(&payer, &[init]).await?;
    base.send(
        &payer,
        &[flexi::delegate_counter(
            payer.pubkey(),
            COMMIT_FREQUENCY_MS,
            Some(er.identity()),
        )],
    )
    .await?;

    let delegate_setup = [
        system::assign(&payer.pubkey(), &dlp::dlp_id()),
        dlp::delegate_account(
            &funder.pubkey(),
            &payer.pubkey(),
            &er.identity(),
        ),
    ];
    base.send_with(funder, &[&payer], &delegate_setup).await?;

    poll_until(CLONE_TIMEOUT, || async {
        matches!(
            er.account(&counter).await,
            Ok(Some(clone)) if !clone.data.is_empty()
        )
    })
    .await;
    Ok(Actor { payer, counter })
}

async fn er_count(er: &ErCtx, actor: &Actor) -> Result<u64> {
    let account = er
        .account(&actor.counter)
        .await?
        .ok_or("the counter clone is missing on the er")?;
    Ok(FlexiCounter::try_decode(&account.data)?.count)
}

async fn await_er_count(
    er: &ErCtx,
    actor: &Actor,
    expected: u64,
) -> Result<()> {
    poll_until(COUNTER_TIMEOUT, || async {
        matches!(er_count(er, actor).await, Ok(count) if count == expected)
    })
    .await;
    Ok(())
}

async fn await_task_removed(db: &TaskDb, task_id: i64) -> Result<()> {
    poll_until(TASK_REMOVED_TIMEOUT, || async {
        matches!(db.task(task_id), Ok(None))
    })
    .await;
    Ok(())
}

struct FailureCounts {
    tasks: usize,
    schedulings: usize,
}

fn failure_counts(db: &TaskDb) -> Result<FailureCounts> {
    Ok(FailureCounts {
        tasks: db.failed_tasks()?.len(),
        schedulings: db.failed_schedulings()?.len(),
    })
}

fn assert_no_new_failures(
    db: &TaskDb,
    before: &FailureCounts,
    context: &str,
) -> Result<()> {
    let after = failure_counts(db)?;
    assert_eq!(
        after.tasks, before.tasks,
        "{context}: the cell must not add failed tasks"
    );
    assert_eq!(
        after.schedulings, before.schedulings,
        "{context}: the cell must not add failed schedulings"
    );
    Ok(())
}

#[async_trait(?Send)]
impl Scenario for TaskScheduler {
    fn name(&self) -> &str {
        "redshift/task_scheduler"
    }

    async fn run(&self, base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        let private = topology::private_er(
            base,
            topology::ErOptions {
                label: "task-scheduler".to_owned(),
                env: vec![(
                    "MBV_TASK_SCHEDULER__RESET".to_owned(),
                    "true".to_owned(),
                )],
                request_timeout: None,
                ..Default::default()
            },
        )
        .await?;
        let er = private.ctx();
        let db = TaskDb {
            path: private.storage_dir().join("task_scheduler.sqlite"),
        };
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let mut report = ScenarioReport::ok(self.name());

        poll_until(DB_OPEN_TIMEOUT, || async { db.task_ids().is_ok() }).await;

        // schedule — the crank runs the task to completion and the row is
        // removed; a cancel after completion still confirms.
        {
            let actor = scheduled_actor(base, er, &funder).await?;
            let before = failure_counts(&db)?;
            er.send(
                &actor.payer,
                &[flexi::schedule_counter_task(
                    actor.pubkey(),
                    101,
                    TASK_INTERVAL_MS,
                    3,
                    false,
                    false,
                )],
            )
            .await?;
            await_er_count(er, &actor, 3).await?;
            await_task_removed(&db, 101).await?;
            assert_no_new_failures(&db, &before, "schedule")?;
            er.send(
                &actor.payer,
                &[flexi::cancel_counter_task(actor.pubkey(), 101)],
            )
            .await?;
        }

        // reschedule — the same authority reschedules the same task id; the
        // iterations reset, so the counter reaches two times the iterations.
        {
            let actor = scheduled_actor(base, er, &funder).await?;
            let before = failure_counts(&db)?;
            er.send(
                &actor.payer,
                &[flexi::schedule_counter_task(
                    actor.pubkey(),
                    102,
                    TASK_INTERVAL_MS,
                    2,
                    false,
                    false,
                )],
            )
            .await?;
            tokio::time::sleep(SCHEDULE_SETTLE).await;
            er.send(
                &actor.payer,
                &[flexi::schedule_counter_task(
                    actor.pubkey(),
                    102,
                    2 * TASK_INTERVAL_MS,
                    2,
                    false,
                    false,
                )],
            )
            .await?;
            await_er_count(er, &actor, 4).await?;
            await_task_removed(&db, 102).await?;
            assert_no_new_failures(&db, &before, "reschedule")?;
            er.send(
                &actor.payer,
                &[flexi::cancel_counter_task(actor.pubkey(), 102)],
            )
            .await?;
        }

        // signed — a task instruction that demands the payer signature is
        // refused when it is scheduled.
        {
            let actor = scheduled_actor(base, er, &funder).await?;
            let attempt = er
                .send(
                    &actor.payer,
                    &[flexi::schedule_counter_task(
                        actor.pubkey(),
                        103,
                        TASK_INTERVAL_MS,
                        3,
                        false,
                        true,
                    )],
                )
                .await;
            assert!(
                attempt.is_err(),
                "a task instruction with a payer signer must be refused"
            );
            let signed_error = format!("{:?}", attempt.unwrap_err());
            assert!(
                signed_error.contains("MissingRequiredSignature"),
                "the refusal must name MissingRequiredSignature, got \
                 {signed_error}"
            );
            report = report.setting("signed task refusal", signed_error);
        }

        // crank signer — a task scheduled through the raw magic instruction
        // executes with the per-authority crank signer pda as its signer.
        {
            let actor = scheduled_actor(base, er, &funder).await?;
            er.send(
                &actor.payer,
                &[flexi::schedule_task_direct(
                    actor.pubkey(),
                    104,
                    CRANK_INTERVAL_MS,
                    5,
                    vec![flexi::add_unsigned_with_crank(actor.pubkey(), 1)],
                )],
            )
            .await?;
            await_er_count(er, &actor, 5).await?;
            report = report.setting(
                "crank signer",
                flexi::crank_signer_pda(&actor.pubkey()),
            );
        }

        // magic cpi crank — the crank executes a program that CPIs into the
        // magic program; the scheduled commit lands on base.
        {
            let authority = prep::delegated_payer(
                base,
                &funder,
                er.identity(),
                crate::PAYER_LAMPORTS,
            )
            .await?;
            let committees = crate::init_schedulecommit_committees(
                base,
                &funder,
                er.identity(),
                1,
            )
            .await?;
            crate::await_committee_clones(er, &committees).await;
            let player = committees[0].player.pubkey();
            let committee = committees[0].pda;

            let crank = flexi::crank_signer_pda(&authority.pubkey());
            let mut commit = schedulecommit::schedule_commit_cpi(
                crank,
                vec![player],
                true,
                false,
                ScheduleCommitType::CommitFinalize,
                true,
            );
            commit.accounts[0].is_writable = false;

            er.send(
                &authority,
                &[flexi::schedule_task_direct(
                    authority.pubkey(),
                    105,
                    CRANK_INTERVAL_MS,
                    1,
                    vec![commit],
                )],
            )
            .await?;
            poll_until(COMMIT_TIMEOUT, || async {
                matches!(
                    base.account(&committee).await,
                    Ok(Some(acc))
                        if MainAccount::try_from_slice(&acc.data)
                            .map(|state| state.count)
                            .ok() == Some(1)
                )
            })
            .await;
        }

        // unauthorized reschedule — a different authority reuses the task id;
        // the transaction confirms but the scheduler refuses the replacement
        // and the original task stays intact.
        {
            let owner = scheduled_actor(base, er, &funder).await?;
            let intruder = scheduled_actor(base, er, &funder).await?;
            let before = failure_counts(&db)?;
            er.send(
                &owner.payer,
                &[flexi::schedule_counter_task(
                    owner.pubkey(),
                    106,
                    TASK_INTERVAL_MS,
                    1000,
                    false,
                    false,
                )],
            )
            .await?;
            tokio::time::sleep(SCHEDULE_SETTLE).await;
            er.send(
                &intruder.payer,
                &[flexi::schedule_counter_task(
                    intruder.pubkey(),
                    106,
                    2 * TASK_INTERVAL_MS,
                    1000,
                    false,
                    false,
                )],
            )
            .await?;

            poll_until(FAILED_SCHEDULING_TIMEOUT, || async {
                matches!(
                    db.failed_schedulings(),
                    Ok(rows) if rows.len() == before.schedulings + 1
                )
            })
            .await;
            let rows = db.failed_schedulings()?;
            let (task_id, error) =
                rows.last().ok_or("no failed scheduling row")?;
            assert_eq!(
                *task_id,
                Some(106),
                "the failed scheduling must name the reused task id"
            );
            assert!(
                error.contains("UnauthorizedReplacing"),
                "the refusal must name UnauthorizedReplacing, got {error}"
            );
            assert_eq!(
                db.failed_tasks()?.len(),
                before.tasks,
                "an unauthorized reschedule must not add failed tasks"
            );

            let task = db
                .task(106)?
                .ok_or("the original task is gone after the reschedule")?;
            assert_eq!(
                task.authority,
                owner.pubkey().to_string(),
                "the original authority must keep the task"
            );
            assert_eq!(
                task.execution_interval_millis, TASK_INTERVAL_MS,
                "the original interval must survive the reschedule"
            );
            assert!(
                task.executions_left > 0,
                "the original task must keep executions"
            );
            report = report.setting("unauthorized reschedule error", error);

            er.send(
                &owner.payer,
                &[flexi::cancel_counter_task(owner.pubkey(), 106)],
            )
            .await?;
            await_task_removed(&db, 106).await?;
        }

        // schedule error — a task that always fails exhausts its retries,
        // lands in failed_tasks, and never moves the counter.
        {
            let actor = scheduled_actor(base, er, &funder).await?;
            let before = failure_counts(&db)?;
            er.send(
                &actor.payer,
                &[flexi::schedule_counter_task(
                    actor.pubkey(),
                    107,
                    TASK_INTERVAL_MS,
                    3,
                    true,
                    false,
                )],
            )
            .await?;
            poll_until(FAILED_TASK_TIMEOUT, || async {
                matches!(
                    db.failed_tasks(),
                    Ok(rows) if rows.len() == before.tasks + 1
                )
            })
            .await;
            let rows = db.failed_tasks()?;
            let (task_id, error) = rows.last().ok_or("no failed task row")?;
            assert_eq!(
                *task_id,
                Some(107),
                "the failed task row must name the failing task"
            );
            assert_eq!(
                db.failed_schedulings()?.len(),
                before.schedulings,
                "a failing task must not add failed schedulings"
            );
            await_task_removed(&db, 107).await?;
            assert_eq!(
                er_count(er, &actor).await?,
                0,
                "a failing task must not move the counter"
            );
            report = report.setting("failed task error", error);
            er.send(
                &actor.payer,
                &[flexi::cancel_counter_task(actor.pubkey(), 107)],
            )
            .await?;
        }

        poll_until(TASK_REMOVED_TIMEOUT, || async {
            matches!(
                db.task_ids(),
                Ok(ids) if ids.iter().all(|id| !(101..=107).contains(id))
            )
        })
        .await;

        Ok(report
            .setting("task interval ms", TASK_INTERVAL_MS)
            .setting("crank interval ms", CRANK_INTERVAL_MS)
            .setting("db", db.path.display().to_string()))
    }
}
