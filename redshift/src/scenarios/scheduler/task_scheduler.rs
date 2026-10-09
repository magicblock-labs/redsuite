use std::time::Duration;

use account::Account;
use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::flexi::{build as flexi, FlexiCounter};
use redsuite_core::{
    api::{ConfirmOptions, TxError},
    check, check_eq, prep, topology, BaseCtx, ChainCtx, ErCtx,
    PrivateErScenario, Result,
};
use signer::Signer;
use transaction::Transaction;

const TASK_INTERVAL_MS: i64 = 100;
const TIMEOUT: Duration = Duration::from_secs(30);
const LABEL: &str = "redshift task";
const SCHEDULER_PROGRAM: Pubkey =
    Pubkey::from_str_const(topology::HYDRA_EPHEMERAL_ID);

pub struct TaskScheduler;

struct Actor {
    payer: Keypair,
    counter: Pubkey,
}

impl Actor {
    fn pubkey(&self) -> Pubkey {
        self.payer.pubkey()
    }
}

async fn scheduled_actor(
    base: &BaseCtx,
    er: &ErCtx,
    funder: &Keypair,
) -> Result<Actor> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let (counter, setup) = prep::flexi_counter(
        payer.pubkey(),
        LABEL,
        er.identity(),
        prep::COMMIT_FREQUENCY_MS,
    );
    for instruction in setup {
        base.submit_and_confirm(&payer, &[instruction]).await?;
    }
    prep::delegate_payer(base, funder, &payer, er.identity()).await?;
    check::poll("the ER clones the counter", TIMEOUT, || async {
        matches!(er.account(&counter).await, Ok(Some(account)) if !account.data.is_empty())
    })
    .await?;
    Ok(Actor { payer, counter })
}

async fn er_count(er: &ErCtx, actor: &Actor) -> Result<u64> {
    let account = er
        .account(&actor.counter)
        .await?
        .ok_or("the ER counter is missing")?;
    Ok(FlexiCounter::try_decode(&account.data)?.count)
}

async fn tasks(er: &ErCtx) -> Result<Vec<(Pubkey, Account)>> {
    er.api().get_program_accounts(&SCHEDULER_PROGRAM).await
}

async fn await_tasks(
    er: &ErCtx,
    expected: usize,
) -> Result<Vec<(Pubkey, Account)>> {
    let found = check::poll_for(
        &format!("the validator holds {expected} scheduled task account(s)"),
        TIMEOUT,
        || async {
            match tasks(er).await {
                Ok(found) if found.len() == expected => Ok(found),
                Ok(found) => Err(format!("{} task account(s)", found.len())),
                Err(error) => Err(error.to_string()),
            }
        },
    )
    .await?;
    for (address, account) in &found {
        check_eq!(
            account.owner,
            SCHEDULER_PROGRAM,
            "task account {address} belongs to the scheduler program"
        )?;
        check!(
            !account.data.is_empty(),
            "task account {address} is materialized"
        )?;
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
        &[flexi::schedule_counter_task(
            actor.pubkey(),
            task_id,
            interval_ms,
            iterations,
            false,
        )],
    )
    .await?;
    Ok(())
}

async fn cancel(er: &ErCtx, actor: &Actor, task_id: i64) -> Result<()> {
    let tx = Transaction::new_signed_with_payer(
        &[flexi::cancel_counter_task(actor.pubkey(), task_id)],
        Some(&actor.pubkey()),
        &[&actor.payer],
        er.api().get_latest_blockhash().await?,
    );
    let signature = er.api().send_transaction(&tx).await?;
    er.api()
        .confirm(&signature, ConfirmOptions::default())
        .await?;
    Ok(())
}

fn tx_failure(attempt: Result<()>, context: &str) -> Result<Box<TxError>> {
    let error = attempt.err().ok_or_else(|| {
        format!("{context}: transaction unexpectedly succeeded")
    })?;
    error.downcast::<TxError>().map_err(|error| {
        format!("{context}: expected an on-chain rejection, got {error}").into()
    })
}

async fn test_schedule_and_cancel(er: &ErCtx, actor: &Actor) -> Result<()> {
    schedule(er, actor, 101, TASK_INTERVAL_MS, 3).await?;
    await_tasks(er, 1).await?;
    cancel(er, actor, 101).await?;
    await_tasks(er, 0).await?;
    check_eq!(
        er_count(er, actor).await?,
        0,
        "the validator itself never executes the scheduled payload"
    )?;
    Ok(())
}

async fn test_reschedule(er: &ErCtx, actor: &Actor) -> Result<()> {
    schedule(er, actor, 102, TASK_INTERVAL_MS, 2).await?;
    let (address, original) = await_tasks(er, 1).await?.remove(0);
    cancel(er, actor, 102).await?;
    await_tasks(er, 0).await?;
    schedule(er, actor, 102, 2 * TASK_INTERVAL_MS, 2).await?;
    let (readdress, replacement) = await_tasks(er, 1).await?.remove(0);
    check_eq!(
        readdress,
        address,
        "a reused task id maps to the same task account"
    )?;
    check!(
        replacement.data != original.data,
        "the reused task id carries the new schedule"
    )?;
    cancel(er, actor, 102).await?;
    await_tasks(er, 0).await?;
    Ok(())
}

async fn test_signed_refusal(er: &ErCtx, actor: &Actor) -> Result<()> {
    let error = tx_failure(
        er.submit_and_confirm(
            &actor.payer,
            &[flexi::schedule_counter_task(
                actor.pubkey(),
                103,
                TASK_INTERVAL_MS,
                3,
                true,
            )],
        )
        .await
        .map(drop),
        "signed task payload",
    )?;
    let detail = format!("{:?}", error.err);
    check!(
        detail.contains("MissingRequiredSignature"),
        "a task payload with a signer is refused as MissingRequiredSignature: {detail}"
    )?;
    check_eq!(
        tasks(er).await?.len(),
        0,
        "a refused schedule creates no task account"
    )?;
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
    check!(
        both.iter().any(|(address, _)| *address == owners[0].0),
        "another authority scheduling the same task id leaves the original task untouched"
    )?;
    cancel(er, owner, 106).await?;
    let remaining = await_tasks(er, 1).await?;
    check!(
        remaining[0].0 != owners[0].0,
        "cancelling by the owner removes only the owner's task"
    )?;
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
        .ok_or("the validator identity is missing on the ER")?
        .lamports)
}

async fn test_sponsor_refund(er: &ErCtx, actor: &Actor) -> Result<()> {
    er.submit_and_confirm(
        &actor.payer,
        &[flexi::add_unsigned(actor.pubkey(), 0)],
    )
    .await?;
    let before = sponsor_balance(er).await?;
    schedule(er, actor, 109, TASK_INTERVAL_MS, 3).await?;
    await_tasks(er, 1).await?;
    let funded = sponsor_balance(er).await?;
    check!(
        funded < before,
        "the validator identity sponsors the scheduled task ({funded} < {before})"
    )?;
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

#[async_trait(?Send)]
impl PrivateErScenario for TaskScheduler {
    fn name(&self) -> &str {
        "redshift/task_scheduler"
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        check!(
            matches!(base.account(&SCHEDULER_PROGRAM).await?, Some(program) if program.executable),
            "the base chain hosts the ephemeral scheduler program"
        )?;
        let private = topology::private_er(
            base,
            topology::ErOptions {
                label: "task-scheduler".to_owned(),
                env: Vec::new(),
                request_timeout: None,
                base_endpoints: None,
            },
        )
        .await?;
        let er = private.ctx();
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        check_eq!(
            tasks(er).await?.len(),
            0,
            "a fresh ER holds no scheduled tasks"
        )?;

        let actor = scheduled_actor(base, er, &funder).await?;
        test_schedule_and_cancel(er, &actor).await?;
        test_signed_refusal(er, &actor).await?;
        test_authority_isolation(base, er, &funder, &actor).await?;
        test_cancel_ongoing(er, &actor).await?;
        test_sponsor_refund(er, &actor).await?;
        test_reschedule(er, &actor).await?;
        Ok(())
    }
}
