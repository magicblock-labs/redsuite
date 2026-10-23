use std::{rc::Rc, time::Duration};

use account::Account;
use anyhow::{ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    api::ConfirmOptions,
    check, prep,
    programs::{CellId, Task, MAGIC, REDSHIFT},
    topology, BaseCtx, ChainCtx, DynError, ErCtx, Result, Scenario,
};
use signer::Signer;

use crate::PAYER_LAMPORTS;

const TASK_INTERVAL_MS: i64 = 100;
const TIMEOUT: Duration = Duration::from_secs(30);
const SCHEDULER_PROGRAM: Pubkey = topology::HYDRA_EPHEMERAL_ID;

struct Actor {
    payer: Rc<Keypair>,
    cell: Pubkey,
}

impl Actor {
    fn schedule(&self, task: Task, instruction: Instruction) -> Instruction {
        let authority = self.payer.pubkey();
        REDSHIFT
            .batch()
            .invoke_signed(
                MAGIC.schedule(authority, self.cell, task, vec![instruction]),
                &[CellId::new(authority, 0)],
            )
            .build()
    }
}

async fn scheduled_actor(base: &BaseCtx, er: &ErCtx, funder: &Keypair) -> Result<Actor> {
    let payer = Rc::new(base.funded_payer(PAYER_LAMPORTS).await?);
    let cell = base
        .cells(REDSHIFT, &payer)
        .create_primary(er.identity(), prep::COMMIT_FREQUENCY_MS)
        .await?;
    base.delegate_payer(funder, &payer, er.identity()).await?;
    check::poll("the ER clones the cell", TIMEOUT, || async {
        matches!(er.account(&cell).await, Ok(Some(account)) if !account.data.is_empty())
    })
    .await?;
    Ok(Actor { payer, cell })
}

async fn er_count(er: &ErCtx, actor: &Actor) -> Result<u64> {
    Ok(er.cells(REDSHIFT, &actor.payer).read(&actor.cell).await?.value)
}

async fn tasks(er: &ErCtx) -> Result<Vec<(Pubkey, Account)>> {
    er.api().get_program_accounts(&SCHEDULER_PROGRAM).await
}

async fn await_tasks(er: &ErCtx, expected: usize) -> Result<Vec<(Pubkey, Account)>> {
    let found = check::poll_for(
        &format!("the validator holds {expected} scheduled task account(s)"),
        TIMEOUT,
        || async {
            let found = tasks(er).await?;
            ensure!(found.len() == expected);
            Ok::<_, DynError>(found)
        },
    )
    .await?;
    for (address, account) in &found {
        ensure!(account.owner == SCHEDULER_PROGRAM);
        ensure!(
            !account.data.is_empty(),
            "task account {address} is materialized"
        );
    }
    Ok(found)
}

async fn schedule(
    er: &ErCtx,
    actor: &Actor,
    task_id: i64,
    interval_ms: i64,
    iterations: i64,
) -> Result<()> {
    er.submit_and_confirm(
        &actor.payer,
        &[actor.schedule(
            Task {
                id: task_id,
                interval_ms,
                iterations,
            },
            REDSHIFT.batch().add(&[actor.cell], 1).build(),
        )],
    )
    .await?;
    Ok(())
}

async fn cancel(er: &ErCtx, actor: &Actor, task_id: i64) -> Result<()> {
    er.submit_and_confirm(
        &actor.payer,
        &[REDSHIFT
            .batch()
            .nonce(rand::random())
            .invoke(MAGIC.cancel(actor.payer.pubkey(), task_id))
            .build()],
    )
    .await?;
    Ok(())
}

async fn test_schedule_and_cancel(er: &ErCtx, actor: &Actor) -> Result<()> {
    schedule(er, actor, 101, TASK_INTERVAL_MS, 3).await?;
    await_tasks(er, 1).await?;
    cancel(er, actor, 101).await?;
    await_tasks(er, 0).await?;
    ensure!(er_count(er, actor).await? == 0);
    Ok(())
}

async fn test_reschedule(er: &ErCtx, actor: &Actor) -> Result<()> {
    schedule(er, actor, 102, TASK_INTERVAL_MS, 2).await?;
    let (address, original) = await_tasks(er, 1).await?.remove(0);
    cancel(er, actor, 102).await?;
    await_tasks(er, 0).await?;
    schedule(er, actor, 102, 2 * TASK_INTERVAL_MS, 2).await?;
    let (readdress, replacement) = await_tasks(er, 1).await?.remove(0);
    ensure!(readdress == address);
    ensure!(
        replacement.data != original.data,
        "the reused task id carries the new schedule"
    );
    cancel(er, actor, 102).await?;
    await_tasks(er, 0).await?;
    Ok(())
}

async fn test_signed_refusal(er: &ErCtx, actor: &Actor) -> Result<()> {
    let signature = er
        .sender(actor.payer.clone())
        .submit(&[actor.schedule(
            Task {
                id: 103,
                interval_ms: TASK_INTERVAL_MS,
                iterations: 3,
            },
            REDSHIFT
                .batch()
                .add(&[actor.cell], 1)
                .require_signer(actor.payer.pubkey())
                .build(),
        )])
        .await?;
    let Err(error) = er.api().try_confirm(&signature, ConfirmOptions::default()).await? else {
        anyhow::bail!("signed task payload unexpectedly succeeded");
    };
    let detail = format!("{:?}", error.err);
    ensure!(
        detail.contains("MissingRequiredSignature"),
        "a task payload with a signer is refused as MissingRequiredSignature: {detail}"
    );
    ensure!(tasks(er).await?.len() == 0);
    Ok(())
}

async fn test_authority_isolation(
    base: &BaseCtx,
    er: &ErCtx,
    funder: &Keypair,
    owner: &Actor,
) -> Result<()> {
    let other = scheduled_actor(base, er, funder).await?;
    schedule(er, owner, 106, TASK_INTERVAL_MS, 3).await?;
    let owners = await_tasks(er, 1).await?;
    schedule(er, &other, 106, 2 * TASK_INTERVAL_MS, 3).await?;
    let both = await_tasks(er, 2).await?;
    ensure!(
        both.iter().any(|(address, _)| *address == owners[0].0),
        "task ID isolated by authority"
    );
    cancel(er, owner, 106).await?;
    let remaining = await_tasks(er, 1).await?;
    ensure!(
        remaining[0].0 != owners[0].0,
        "cancelling by the owner removes only the owner's task"
    );
    cancel(er, &other, 106).await?;
    await_tasks(er, 0).await?;
    Ok(())
}

async fn test_cancel_ongoing(er: &ErCtx, actor: &Actor) -> Result<()> {
    schedule(er, actor, 108, TASK_INTERVAL_MS, i64::MAX).await?;
    await_tasks(er, 1).await?;
    cancel(er, actor, 108).await?;
    await_tasks(er, 0).await?;
    Ok(())
}

async fn sponsor_balance(er: &ErCtx) -> Result<u64> {
    Ok(er
        .account(&er.identity())
        .await?
        .context("the validator identity is missing on the ER")?
        .lamports)
}

async fn test_sponsor_refund(er: &ErCtx, actor: &Actor) -> Result<()> {
    er.submit_and_confirm(
        &actor.payer,
        &[REDSHIFT.batch().add(&[actor.cell], 0).build()],
    )
    .await?;
    let before = sponsor_balance(er).await?;
    schedule(er, actor, 109, TASK_INTERVAL_MS, 3).await?;
    await_tasks(er, 1).await?;
    let funded = sponsor_balance(er).await?;
    ensure!(
        funded < before,
        "the validator identity sponsors the scheduled task ({funded} < {before})"
    );
    cancel(er, actor, 109).await?;
    await_tasks(er, 0).await?;
    check::poll(
        "cancellation refunds the validator identity in full",
        TIMEOUT,
        || async { sponsor_balance(er).await.ok() == Some(before) },
    )
    .await?;
    Ok(())
}

pub struct TaskScheduler {
    base: Rc<BaseCtx>,
    er: ErCtx,
    funder: Keypair,
    actor: Actor,
}

impl Scenario for TaskScheduler {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        ensure!(
            matches!(base.account(&SCHEDULER_PROGRAM).await?, Some(program) if program.executable),
            "the base chain hosts the ephemeral scheduler program"
        );
        let funder = base.funded_payer(PAYER_LAMPORTS).await?;
        ensure!(tasks(&er).await?.len() == 0);

        let actor = scheduled_actor(&base, &er, &funder).await?;
        Ok(Self { base, er, funder, actor })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er, funder, actor } = self;
        test_schedule_and_cancel(&er, &actor).await?;
        test_signed_refusal(&er, &actor).await?;
        test_authority_isolation(&base, &er, &funder, &actor).await?;
        test_cancel_ongoing(&er, &actor).await?;
        test_sponsor_refund(&er, &actor).await?;
        test_reschedule(&er, &actor).await?;
        Ok(())
    }
}
