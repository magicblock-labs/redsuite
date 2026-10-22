use std::{rc::Rc, slice, time::Duration};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check,
    netfault::{BaseProxies, Selector},
    programs::{CellHeader, CellId, SettlementMode, REDLINE},
    topology::RestartConfig,
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;
use tokio::time;

use super::{write_snapshot, PROXIED_GROUP};
use crate::{program::DELEGATION_PROGRAM_ID, ACCOUNT_SPACE, LOCKOUT_REJECTIONS, PAYER_LAMPORTS};

const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(90);
const REDELEGATE_TIMEOUT: Duration = Duration::from_secs(60);
const HIDDEN: &str = "notification hidden by redsuite";

pub struct UndelegationRecovery {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
}

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

async fn rejected_write(
    er: &ErCtx,
    payer: &Keypair,
    id: u64,
    account: &Pubkey,
    snapshot: &[u8],
    when: &str,
) -> Result<()> {
    let attempt = er.cells(REDLINE, payer).write(id, &[*account]).await;
    let error = check::expect_error(attempt, &format!("ER write rejected {when}"))?;
    let error = format!("{error:#}");
    crate::rejection_code(&format!("the er write {when}"), &LOCKOUT_REJECTIONS, &error)?;
    let on_er = er.account(account).await?.context("er copy vanished during the lockout")?;
    ensure!(on_er.data == snapshot);
    Ok(())
}

async fn await_ownership_return(
    base: &BaseCtx,
    account: &Pubkey,
    owner: &Pubkey,
    snapshot: &[u8],
) -> Result<()> {
    time::timeout(COMPLETION_TIMEOUT, async {
        let events = base.api().events().await?;
        let mut changes = events.account(account).await?;
        loop {
            let on_base =
                base.account(account).await?.context("account vanished during undelegation")?;
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

async fn await_er_value(er: &ErCtx, account: &Pubkey, id: u64, what: &str) -> Result<()> {
    er.api()
        .await_accounts(slice::from_ref(account), what, async || {
            let account = er.account(account).await?.context("ER cell absent")?;
            ensure!(CellHeader::try_decode(&account.data)?.value == id);
            Ok(())
        })
        .await
        .with_context(|| format!("written id {id}"))?;
    Ok(())
}

async fn redelegate_and_write(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    account: &Pubkey,
    seed: u8,
    id: u64,
) -> Result<()> {
    base.submit_and_confirm(
        payer,
        &[REDLINE.delegate(
            payer.pubkey(),
            CellId::new(payer.pubkey(), u64::from(seed)),
            Some(er.identity()),
            u32::MAX,
        )],
    )
    .await?;
    let on_base = base
        .account(account)
        .await?
        .context("the account vanished from base during redelegation")?;
    ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
    wait::retry(
        "the er accepts writes again once the account is redelegated",
        REDELEGATE_TIMEOUT,
        async || er.cells(REDLINE, payer).write(id, &[*account]).await,
    )
    .await?;
    let on_er = er
        .account(account)
        .await?
        .context("er copy vanished after the redelegated write")?;
    ensure!(CellHeader::written_id(&on_er.data) == Some(id));
    Ok(())
}

async fn recover(
    base: &BaseCtx,
    er: &ErCtx,
    proxies: &BaseProxies,
    payer: &Keypair,
    seed: u8,
    mode: Recovery,
) -> Result<()> {
    let case = u64::from(seed);
    let identity = er.identity();
    let account = base
        .cells(REDLINE, payer)
        .create_delegated(ACCOUNT_SPACE, identity, seed)
        .await?;
    er.await_clones(&[account], ACCOUNT_SPACE as usize).await?;
    let snapshot = write_snapshot(er, payer, value(case, 1), &account).await?;

    let submission =
        proxies.intercept(Selector::method("sendTransaction").http().request().account(&account));
    er.cells(REDLINE, payer)
        .settle(case, &[account], SettlementMode::Undelegate)
        .await?;
    let held = submission.wait(INTERCEPT_TIMEOUT).await?;
    rejected_write(
        er,
        payer,
        value(case, 2),
        &account,
        &snapshot,
        "while base has not seen the undelegation",
    )
    .await?;

    let resubmission =
        proxies.intercept(Selector::method("sendTransaction").http().request().account(&account));
    held.discard();
    let held = resubmission.wait(INTERCEPT_TIMEOUT).await?;
    rejected_write(
        er,
        payer,
        value(case, 3),
        &account,
        &snapshot,
        "after a failed base submission",
    )
    .await?;
    let on_base = base
        .account(&account)
        .await?
        .context("the account vanished from base while its submission was held")?;
    ensure!(on_base.owner == DELEGATION_PROGRAM_ID);

    let blind = proxies.reject(
        Selector::methods(&[]).ws().notification().account(&account),
        HIDDEN,
    );
    proxies.close_connections();
    held.release();
    await_ownership_return(base, &account, &crate::program::id(), &snapshot).await?;

    match mode {
        Recovery::Reconnect => {
            blind.remove();
            proxies.close_connections();
        }
        Recovery::Restart => {
            let timing = er.restart(RestartConfig::default()).await?;
            ensure!(timing.exit_code == Some(0));
            blind.remove();
        }
    }

    let base_write = value(case, 5);
    base.cells(REDLINE, payer).write(base_write, &[account]).await?;
    await_er_value(
        er,
        &account,
        base_write,
        &format!(
            "the er discovers the completed undelegation after {} and shows \
             the base write",
            mode.label()
        ),
    )
    .await?;
    redelegate_and_write(base, er, payer, &account, seed, value(case, 6)).await?;
    Ok(())
}

impl Scenario for UndelegationRecovery {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        er.proxies().context(PROXIED_GROUP)?;
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;

        Ok(Self { base, er, payer })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er, payer } = self;
        let proxies = er.proxies().context(PROXIED_GROUP)?;
        recover(&base, &er, proxies, &payer, 1, Recovery::Reconnect).await?;
        recover(&base, &er, proxies, &payer, 2, Recovery::Restart).await?;

        Ok(())
    }
}
