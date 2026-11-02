use std::{rc::Rc, slice, time::Duration};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    netfault::{Faults, Selector, Transport},
    programs::{CellHeader, CellId, SettlementMode, REDLINE},
    topology::RestartConfig,
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;
use tokio::time;

use super::{check_lockout, write_snapshot};
use crate::{program::DELEGATION_PROGRAM_ID, ACCOUNT_SPACE, PAYER_LAMPORTS};

const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(90);
const REDELEGATE_TIMEOUT: Duration = Duration::from_secs(60);
const HIDDEN: &str = "notification hidden by redsuite";

/// Checks pending lockout and discovery of completed undelegation after transport recovery.
/// The account is rewritten on base after ownership returns, then redelegated
/// to prove new-session progress.
pub struct UndelegationRecovery {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
}

/// Ways to invalidate stale pending-undelegation knowledge after hidden base completion.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Recovery {
    Reconnect,
    Restart,
}

impl Recovery {
    fn label(self) -> &'static str {
        match self {
            Self::Reconnect => "reconnect",
            Self::Restart => "restart",
        }
    }
}

fn value(case: u64, step: u64) -> u64 {
    100 * case + step
}

impl Scenario for UndelegationRecovery {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        er.faults()?;
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;

        Ok(Self { base, er, payer })
    }

    /// Applies the same lockout/discovery oracle to reconnect and restart recovery.
    async fn run(self) -> Result<()> {
        let faults = self.er.faults()?;
        self.recover(&faults, 1, Recovery::Reconnect).await?;
        self.recover(&faults, 2, Recovery::Restart).await?;

        Ok(())
    }
}

impl UndelegationRecovery {
    /// Requires a write-lockout refusal and unchanged ER snapshot while settlement is pending.
    async fn rejected_write(
        &self,
        id: u64,
        account: &Pubkey,
        snapshot: &[u8],
        when: &str,
    ) -> Result<()> {
        let error = self
            .er
            .try_submit_and_confirm(&self.payer, &[REDLINE.write(id, &[*account])])
            .await?
            .err()
            .with_context(|| format!("ER write accepted {when}"))?;
        check_lockout(&error)?;
        let on_er =
            self.er.account(account).await?.context("er copy vanished during the lockout")?;
        ensure!(on_er.data == snapshot);
        Ok(())
    }

    async fn await_ownership_return(
        &self,
        account: &Pubkey,
        owner: &Pubkey,
        snapshot: &[u8],
    ) -> Result<()> {
        time::timeout(COMPLETION_TIMEOUT, async {
            let events = self.base.api().events().await?;
            let mut changes = events.account(account).await?;
            loop {
                let on_base = self
                    .base
                    .account(account)
                    .await?
                    .context("account vanished during undelegation")?;
                if on_base.owner == *owner {
                    ensure!(on_base.data == snapshot);
                    return Ok(());
                }
                ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
                changes.next().await?;
            }
        })
        .await
        .context("base undelegation timed out")?
    }

    async fn await_er_value(&self, account: &Pubkey, id: u64, what: &str) -> Result<()> {
        self.er
            .api()
            .await_accounts(slice::from_ref(account), what, |accounts| {
                let account = accounts[0].as_ref().context("ER cell absent")?;
                ensure!(CellHeader::try_decode(&account.data)?.value == id);
                Ok(())
            })
            .await
            .with_context(|| format!("written id {id}"))?;
        Ok(())
    }

    /// Starts a fresh session and proves recovered state is writable on the ER again.
    async fn redelegate_and_write(&self, account: &Pubkey, seed: u8, id: u64) -> Result<()> {
        self.base
            .submit_and_confirm(
                &self.payer,
                &[REDLINE.delegate(
                    self.payer.pubkey(),
                    CellId::new(self.payer.pubkey(), u64::from(seed)),
                    Some(self.er.identity()),
                    u32::MAX,
                )],
            )
            .await?;
        let on_base = self
            .base
            .account(account)
            .await?
            .context("the account vanished from base during redelegation")?;
        ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
        wait::retry(
            "the er accepts writes again once the account is redelegated",
            REDELEGATE_TIMEOUT,
            async || self.er.cells(REDLINE, &self.payer).write(id, &[*account]).await,
        )
        .await?;
        let on_er = self
            .er
            .account(account)
            .await?
            .context("er copy vanished after the redelegated write")?;
        ensure!(CellHeader::written_id(&on_er.data) == Some(id));
        Ok(())
    }

    /// Drops a base submission, hides eventual completion, then tests
    /// the selected recovery boundary.
    async fn recover(&self, faults: &Faults, seed: u8, mode: Recovery) -> Result<()> {
        let case = u64::from(seed);
        let identity = self.er.identity();
        let account = self
            .base
            .cells(REDLINE, &self.payer)
            .create_delegated(ACCOUNT_SPACE, identity, seed)
            .await?;
        self.er.await_clones(&[account], ACCOUNT_SPACE as usize).await?;
        let snapshot = write_snapshot(&self.er, &self.payer, value(case, 1), &account).await?;

        let submission = faults
            .intercept(Selector::method("sendTransaction").http().request().account(&account));
        self.er
            .cells(REDLINE, &self.payer)
            .settle(case, &[account], SettlementMode::Undelegate)
            .await?;
        let held = submission.wait(INTERCEPT_TIMEOUT).await?;
        self.rejected_write(
            value(case, 2),
            &account,
            &snapshot,
            "while base has not seen the undelegation",
        )
        .await?;

        let resubmission = faults
            .intercept(Selector::method("sendTransaction").http().request().account(&account));
        held.discard();
        let held = resubmission.wait(INTERCEPT_TIMEOUT).await?;
        self.rejected_write(
            value(case, 3),
            &account,
            &snapshot,
            "after a failed base submission",
        )
        .await?;
        let on_base = self
            .base
            .account(&account)
            .await?
            .context("the account vanished from base while its submission was held")?;
        ensure!(on_base.owner == DELEGATION_PROGRAM_ID);

        let blind = faults.reject(
            Selector::methods(&[]).ws().notification().account(&account),
            HIDDEN,
        );
        faults.disconnect(Transport::Http);
        faults.disconnect(Transport::Ws);
        held.release();
        self.await_ownership_return(&account, &crate::program::id(), &snapshot).await?;

        match mode {
            Recovery::Reconnect => {
                blind.remove();
                faults.disconnect(Transport::Http);
                faults.disconnect(Transport::Ws);
            }
            Recovery::Restart => {
                let timing = self.er.restart(RestartConfig::default()).await?;
                ensure!(timing.exit_code == Some(0));
                blind.remove();
            }
        }

        let base_write = value(case, 5);
        self.base.cells(REDLINE, &self.payer).write(base_write, &[account]).await?;
        self.await_er_value(
            &account,
            base_write,
            &format!(
                "the er discovers the completed undelegation after {} and shows \
                 the base write",
                mode.label()
            ),
        )
        .await?;
        self.redelegate_and_write(&account, seed, value(case, 6)).await?;
        Ok(())
    }
}
