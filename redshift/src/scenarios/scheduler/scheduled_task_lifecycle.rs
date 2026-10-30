use std::rc::Rc;

use account::Account;
use anyhow::{bail, ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    prep,
    programs::{content_hash, CellId, Task, MAGIC, REDSHIFT},
    rpc_types::response::TransactionError,
    topology, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;
use solana_instruction_error::InstructionError;

use crate::PAYER_LAMPORTS;

const TASK_INTERVAL_MS: i64 = 100;
const CANCEL_INTERVAL_MS: i64 = 60_000;
const SCHEDULER_PROGRAM: Pubkey = topology::HYDRA_EPHEMERAL_ID;

/// Independent task authority and signed cell used to detect cross-authority task-ID collisions.
struct Actor {
    payer: Rc<Keypair>,
    cell: Pubkey,
}

impl Actor {
    /// Derives task identity from both authority and task ID, not from the ID alone.
    fn address(&self, task_id: i64) -> Pubkey {
        let mut seed = [0; 40];
        seed[..32].copy_from_slice(self.payer.pubkey().as_ref());
        seed[32..].copy_from_slice(&task_id.to_le_bytes());
        let seed = content_hash(&seed);
        Pubkey::find_program_address(&[b"crank", seed.as_ref()], &SCHEDULER_PROGRAM).0
    }

    /// Authorizes scheduling through the cell PDA; payload signers are still independently checked.
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
        er.await_account(&cell, |account| {
            ensure!(!account.data.is_empty());
            Ok(())
        })
        .await?;
        Ok(Actor { payer, cell })
    }

    async fn count(&self, er: &ErCtx) -> Result<u64> {
        Ok(er.cells(REDSHIFT, &self.payer).read(&self.cell).await?.value)
    }

    /// Subscribes before creation so a fast scheduler cannot race the account observer.
    async fn submit_schedule(
        &self,
        er: &ErCtx,
        task_id: i64,
        interval_ms: i64,
        iterations: i64,
    ) -> Result<Account> {
        let mut task = er.api().watch_accounts(&[self.address(task_id)]).await?;
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
        task.until(&format!("task {task_id} created"), |accounts| {
            let account = accounts[0].as_ref().context("task missing")?;
            ensure!(account.owner == SCHEDULER_PROGRAM);
            Ok(account.clone())
        })
        .await
    }

    /// Waits for task-account removal, not merely acceptance of the cancellation transaction.
    async fn cancel(&self, er: &ErCtx, task_id: i64) -> Result<()> {
        let mut task = er.api().watch_accounts(&[self.address(task_id)]).await?;
        er.submit_and_confirm(
            &self.payer,
            &[REDSHIFT
                .batch()
                .nonce(rand::random())
                .invoke(MAGIC.cancel(self.payer.pubkey(), task_id))
                .build()],
        )
        .await?;
        task.until(&format!("task {task_id} cancelled"), |accounts| {
            ensure!(accounts[0]
                .as_ref()
                .is_none_or(|account| account.owner != SCHEDULER_PROGRAM || account.lamports == 0));
            Ok(())
        })
        .await
    }
}

/// Checks cancellation/refund/reuse, signed-payload refusal, and authority isolation.
pub struct ScheduledTaskLifecycle {
    base: Rc<BaseCtx>,
    er: ErCtx,
    funder: Keypair,
    actor: Actor,
}

impl Scenario for ScheduledTaskLifecycle {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        // Program materialization also spends the sponsor's lamports.
        er.await_program_clone(&SCHEDULER_PROGRAM).await?;
        let funder = base.funded_payer(PAYER_LAMPORTS).await?;

        let actor = Actor::prepare(&base, &er, &funder).await?;
        Ok(Self { base, er, funder, actor })
    }

    /// Checks refund/reuse before signed-payload refusal and same-ID authority isolation.
    async fn run(self) -> Result<()> {
        self.test_sponsor_refund().await?;
        self.test_signed_refusal().await?;
        self.test_authority_isolation().await?;
        Ok(())
    }
}

impl ScheduledTaskLifecycle {
    /// Ensures scheduling cannot smuggle a payer signature into a future task payload.
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
            bail!("signed task payload unexpectedly succeeded");
        };
        ensure!(matches!(
            error.err,
            TransactionError::InstructionError(_, InstructionError::MissingRequiredSignature)
        ));
        ensure!(self.er.account(&self.actor.address(103)).await?.is_none());
        Ok(())
    }

    /// Requires full validator-sponsor reimbursement and replacement of a cancelled task ID.
    async fn test_sponsor_refund(&self) -> Result<()> {
        self.er
            .submit_and_confirm(
                &self.actor.payer,
                &[REDSHIFT.batch().add(&[self.actor.cell], 0).build()],
            )
            .await?;
        let before = self.sponsor_balance().await?;
        let original = self.actor.submit_schedule(&self.er, 109, CANCEL_INTERVAL_MS, 3).await?;
        let funded = self.sponsor_balance().await?;
        ensure!(funded < before);
        let mut sponsor = self.er.api().watch_accounts(&[self.er.identity()]).await?;
        self.actor.cancel(&self.er, 109).await?;
        sponsor
            .until(
                "cancellation refunds the validator identity in full",
                |accounts| {
                    ensure!(
                        accounts[0].as_ref().context("validator identity missing")?.lamports
                            == before
                    );
                    Ok(())
                },
            )
            .await?;
        ensure!(self.actor.count(&self.er).await? == 0);
        let replacement =
            self.actor.submit_schedule(&self.er, 109, 2 * CANCEL_INTERVAL_MS, 2).await?;
        ensure!(replacement.data != original.data);
        self.actor.cancel(&self.er, 109).await?;
        Ok(())
    }

    /// Cancels one authority's task without altering another authority's identically numbered task.
    async fn test_authority_isolation(&self) -> Result<()> {
        let other = Actor::prepare(self.base.as_ref(), &self.er, &self.funder).await?;
        self.actor.submit_schedule(&self.er, 106, TASK_INTERVAL_MS, i64::MAX).await?;
        let original =
            other.submit_schedule(&self.er, 106, 2 * CANCEL_INTERVAL_MS, i64::MAX).await?;
        self.actor.cancel(&self.er, 106).await?;
        ensure!(self.er.account(&other.address(106)).await? == Some(original));
        other.cancel(&self.er, 106).await?;
        Ok(())
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
