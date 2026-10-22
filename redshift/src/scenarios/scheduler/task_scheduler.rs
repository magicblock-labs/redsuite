use std::rc::Rc;

use account::Account;
use anyhow::{ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    prep,
    programs::{CellId, Task, MAGIC, REDSHIFT},
    topology, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;

use crate::PAYER_LAMPORTS;

const TASK_INTERVAL_MS: i64 = 100;
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

    async fn prepare(base: &BaseCtx, er: &ErCtx, funder: &Keypair) -> Result<Actor> {
        let payer = Rc::new(base.funded_payer(PAYER_LAMPORTS).await?);
        let cell = base
            .cells(REDSHIFT, &payer)
            .create_primary(er.identity(), prep::COMMIT_FREQUENCY_MS)
            .await?;
        base.delegate_payer(funder, &payer, er.identity()).await?;
        er.api()
            .await_accounts(&[cell], "the ER clones the cell", async || {
                let account = er.account(&cell).await?.context("cell not cloned")?;
                ensure!(!account.data.is_empty());
                Ok(())
            })
            .await?;
        Ok(Actor { payer, cell })
    }

    async fn count(&self, er: &ErCtx) -> Result<u64> {
        Ok(er.cells(REDSHIFT, &self.payer).read(&self.cell).await?.value)
    }

    async fn submit_schedule(
        &self,
        er: &ErCtx,
        task_id: i64,
        interval_ms: i64,
        iterations: i64,
    ) -> Result<()> {
        er.submit_and_confirm(
            &self.payer,
            &[self.schedule(
                Task {
                    id: task_id,
                    interval_ms,
                    iterations,
                },
                REDSHIFT.batch().add(&[self.cell], 1).build(),
            )],
        )
        .await?;
        Ok(())
    }

    async fn cancel(&self, er: &ErCtx, task_id: i64) -> Result<()> {
        er.submit_and_confirm(
            &self.payer,
            &[REDSHIFT
                .batch()
                .nonce(rand::random())
                .invoke(MAGIC.cancel(self.payer.pubkey(), task_id))
                .build()],
        )
        .await?;
        Ok(())
    }
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
            matches!(base.account(&SCHEDULER_PROGRAM).await?, Some(program) if program.executable)
        );
        let funder = base.funded_payer(PAYER_LAMPORTS).await?;
        ensure!(er.api().get_program_accounts(&SCHEDULER_PROGRAM).await?.len() == 0);

        let actor = Actor::prepare(&base, &er, &funder).await?;
        Ok(Self { base, er, funder, actor })
    }

    async fn run(self) -> Result<()> {
        self.test_schedule_and_cancel().await?;
        self.test_signed_refusal().await?;
        self.test_authority_isolation().await?;
        self.test_cancel_ongoing().await?;
        self.test_sponsor_refund().await?;
        self.test_reschedule().await?;
        Ok(())
    }
}

impl TaskScheduler {
    async fn test_schedule_and_cancel(&self) -> Result<()> {
        self.actor.submit_schedule(&self.er, 101, TASK_INTERVAL_MS, 3).await?;
        self.await_tasks(1).await?;
        self.actor.cancel(&self.er, 101).await?;
        self.await_tasks(0).await?;
        ensure!(self.actor.count(&self.er).await? == 0);
        Ok(())
    }

    async fn test_reschedule(&self) -> Result<()> {
        self.actor.submit_schedule(&self.er, 102, TASK_INTERVAL_MS, 2).await?;
        let (address, original) = self.await_tasks(1).await?.remove(0);
        self.actor.cancel(&self.er, 102).await?;
        self.await_tasks(0).await?;
        self.actor.submit_schedule(&self.er, 102, 2 * TASK_INTERVAL_MS, 2).await?;
        let (readdress, replacement) = self.await_tasks(1).await?.remove(0);
        ensure!(readdress == address);
        ensure!(replacement.data != original.data);
        self.actor.cancel(&self.er, 102).await?;
        self.await_tasks(0).await?;
        Ok(())
    }

    async fn test_signed_refusal(&self) -> Result<()> {
        let signature = self
            .er
            .sender(self.actor.payer.clone())
            .submit(&[self.actor.schedule(
                Task {
                    id: 103,
                    interval_ms: TASK_INTERVAL_MS,
                    iterations: 3,
                },
                REDSHIFT
                    .batch()
                    .add(&[self.actor.cell], 1)
                    .require_signer(self.actor.payer.pubkey())
                    .build(),
            )])
            .await?;
        let Err(error) = self.er.api().try_confirm(&signature).await? else {
            anyhow::bail!("signed task payload unexpectedly succeeded");
        };
        let detail = format!("{:?}", error.err);
        ensure!(detail.contains("MissingRequiredSignature"));
        ensure!(self.tasks().await?.len() == 0);
        Ok(())
    }

    async fn test_cancel_ongoing(&self) -> Result<()> {
        self.actor.submit_schedule(&self.er, 108, TASK_INTERVAL_MS, i64::MAX).await?;
        self.await_tasks(1).await?;
        self.actor.cancel(&self.er, 108).await?;
        self.await_tasks(0).await?;
        Ok(())
    }

    async fn test_sponsor_refund(&self) -> Result<()> {
        self.er
            .submit_and_confirm(
                &self.actor.payer,
                &[REDSHIFT.batch().add(&[self.actor.cell], 0).build()],
            )
            .await?;
        let before = self.sponsor_balance().await?;
        self.actor.submit_schedule(&self.er, 109, TASK_INTERVAL_MS, 3).await?;
        self.await_tasks(1).await?;
        let funded = self.sponsor_balance().await?;
        ensure!(funded < before);
        self.actor.cancel(&self.er, 109).await?;
        self.await_tasks(0).await?;
        self.er
            .api()
            .await_accounts(
                &[self.er.identity()],
                "cancellation refunds the validator identity in full",
                async || {
                    ensure!(self.sponsor_balance().await? == before);
                    Ok(())
                },
            )
            .await?;
        Ok(())
    }

    async fn test_authority_isolation(&self) -> Result<()> {
        let other = Actor::prepare(self.base.as_ref(), &self.er, &self.funder).await?;
        self.actor.submit_schedule(&self.er, 106, TASK_INTERVAL_MS, 3).await?;
        let owners = self.await_tasks(1).await?;
        other.submit_schedule(&self.er, 106, 2 * TASK_INTERVAL_MS, 3).await?;
        let both = self.await_tasks(2).await?;
        ensure!(both.iter().any(|(address, _)| *address == owners[0].0));
        self.actor.cancel(&self.er, 106).await?;
        let remaining = self.await_tasks(1).await?;
        ensure!(remaining[0].0 != owners[0].0);
        other.cancel(&self.er, 106).await?;
        self.await_tasks(0).await?;
        Ok(())
    }

    async fn tasks(&self) -> Result<Vec<(Pubkey, Account)>> {
        self.er.api().get_program_accounts(&SCHEDULER_PROGRAM).await
    }

    async fn await_tasks(&self, expected: usize) -> Result<Vec<(Pubkey, Account)>> {
        let found = self
            .er
            .api()
            .await_program_accounts(&SCHEDULER_PROGRAM, |found| {
                ensure!(found.len() == expected);
                Ok(found)
            })
            .await?;
        for (_, account) in &found {
            ensure!(account.owner == SCHEDULER_PROGRAM);
            ensure!(!account.data.is_empty());
        }
        Ok(found)
    }

    async fn sponsor_balance(&self) -> Result<u64> {
        Ok(self
            .er
            .account(&self.er.identity())
            .await?
            .context("the validator identity is missing on the ER")?
            .lamports)
    }
}
