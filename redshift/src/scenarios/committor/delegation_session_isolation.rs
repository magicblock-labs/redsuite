use std::{
    rc::Rc,
    slice,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    api::transaction_meta,
    dlp,
    netfault::{BaseProxies, Selector},
    prep,
    programs::{CellHeader, CellId, SettlementMode, REDSHIFT},
    receipt,
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use tokio::time;

use crate::PAYER_LAMPORTS;

const LABEL_A: &str = "session-isolation-a";
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const STALE_WINDOW: Duration = Duration::from_secs(5);
const RECOVERY_WINDOW: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(250);

fn value(case: u64, step: u64) -> u64 {
    100 * case + step
}

fn count(data: &[u8]) -> Result<u64> {
    Ok(CellHeader::try_decode(data)?.value)
}

async fn await_clone_count(er: &ErCtx, account: &Pubkey, id: u64) -> Result<()> {
    er.api()
        .await_accounts(slice::from_ref(account), "cloned cell state", async || {
            let account = er.account(account).await?.context("ER cell absent")?;
            ensure!(count(&account.data)? == id);
            Ok(())
        })
        .await?;
    Ok(())
}

async fn write(ctx: &impl ChainCtx, payer: &Keypair, account: &Pubkey, id: u64) -> Result<()> {
    ctx.submit_and_confirm(payer, &[REDSHIFT.batch().set(&[*account], id).build()])
        .await?;
    let data = ctx.account(account).await?.context("the cell vanished after the write")?.data;
    ensure!(count(&data)? == id);
    Ok(())
}

async fn base_state(base: &BaseCtx, account: &Pubkey) -> Result<(Pubkey, Option<u64>)> {
    let on_base = base.account(account).await?.context("the cell vanished from base")?;
    Ok((on_base.owner, count(&on_base.data).ok()))
}

async fn await_base(
    base: &BaseCtx,
    account: &Pubkey,
    owner: Pubkey,
    id: u64,
    what: &str,
) -> Result<()> {
    base.api()
        .await_accounts(slice::from_ref(account), what, async || {
            ensure!(base_state(base, account).await? == (owner, Some(id)));
            Ok(())
        })
        .await?;
    Ok(())
}

struct SessionB<'a> {
    er: &'a ErCtx,
    account: Pubkey,
    base_id: u64,
    er_id: u64,
    nonce: u64,
}

async fn hold_steady(base: &BaseCtx, session: &SessionB<'_>, window: Duration) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < window {
        let (owner, written) = base_state(base, &session.account).await?;
        ensure!((owner, written) == (dlp::dlp_id(), Some(session.base_id)));
        ensure!(crate::last_commit_id(base, &session.account).await? == session.nonce);
        let on_er = session
            .er
            .account(&session.account)
            .await?
            .context("the session-b er copy vanished")?;
        ensure!(count(&on_er.data)? == session.er_id);
        time::sleep(POLL).await;
    }
    Ok(())
}

async fn schedule_commit(
    er: &ErCtx,
    payer: &Keypair,
    account: &Pubkey,
    commit_type: SettlementMode,
) -> Result<Signature> {
    er.submit_and_confirm(
        payer,
        &[REDSHIFT.batch().settle(payer.pubkey(), &[*account], commit_type).build()],
    )
    .await
}

async fn recover_session_a_commit_after_lost_response(
    base: &BaseCtx,
    er: &ErCtx,
    proxies: &BaseProxies,
    payer: &Keypair,
    account: &Pubkey,
    expected_id: u64,
) -> Result<()> {
    let submission =
        proxies.intercept(Selector::method("sendTransaction").http().response().account(account));
    let signature = schedule_commit(er, payer, account, SettlementMode::Commit).await?;
    let held = submission.wait(INTERCEPT_TIMEOUT).await?;
    let base_signature = held.operation.signature()?;
    let base_tx = base.api().await_transaction(&base_signature).await?;
    ensure!(transaction_meta(&base_tx)?.err == None);
    await_base(
        base,
        account,
        dlp::dlp_id(),
        expected_id,
        "base records the session-a commit before the response is lost",
    )
    .await?;
    held.discard();

    let receipt = receipt::fetch_commit_receipt(er.api(), &signature).await?;
    ensure!(receipt.succeeded() || receipt.failure_is_duplicate_rejection());
    Ok(())
}

async fn isolate(
    base: &BaseCtx,
    proxies: &BaseProxies,
    er_a: &mut PrivateEr,
    er_b: Option<&ErCtx>,
    payer: &Keypair,
) -> Result<()> {
    let case = u64::from(er_b.is_some()) + 1;
    let cell = base
        .cells(REDSHIFT, payer)
        .create_owned(er_a.identity(), 1)
        .await?
        .pop()
        .context("no cell was created")?;
    let authority = cell.authority.pubkey();
    let account = cell.address;
    er_a.ctx().await_cells(slice::from_ref(&cell)).await?;

    let staged = value(case, 1);
    write(er_a.ctx(), payer, &account, staged).await?;
    recover_session_a_commit_after_lost_response(
        base,
        er_a.ctx(),
        proxies,
        payer,
        &account,
        staged,
    )
    .await?;

    let delayed = proxies.intercept(Selector::methods(&[]).ws().notification().account(&account));
    let undelegate =
        schedule_commit(er_a.ctx(), payer, &account, SettlementMode::Undelegate).await?;
    let undelegated = receipt::fetch_commit_receipt(er_a.ctx().api(), &undelegate).await?;
    ensure!(undelegated.succeeded() || undelegated.failure_is_duplicate_rejection());
    await_base(
        base,
        &account,
        redshift_interface::id(),
        staged,
        "base returns the account to the program with the staged value",
    )
    .await?;
    let delayed = delayed.wait(INTERCEPT_TIMEOUT).await?;

    let base_value = value(case, 2);
    write(base, payer, &account, base_value).await?;
    let session_b = er_b.unwrap_or_else(|| er_a.ctx());
    base.submit_and_confirm_with(
        payer,
        &[&cell.authority],
        &[REDSHIFT.delegate(
            payer.pubkey(),
            CellId::new(authority, 0),
            Some(session_b.identity()),
            prep::COMMIT_FREQUENCY_MS,
        )],
    )
    .await?;
    await_base(
        base,
        &account,
        dlp::dlp_id(),
        base_value,
        "base delegates the account again with the base value",
    )
    .await?;
    await_clone_count(session_b, &account, base_value).await?;
    session_b
        .await_delegated(&account, "session-b er activates the cell as delegated")
        .await?;
    let er_value = value(case, 3);
    write(session_b, payer, &account, er_value).await?;
    let nonce = crate::last_commit_id(base, &account).await?;

    delayed.release();
    hold_steady(
        base,
        &SessionB {
            er: session_b,
            account,
            base_id: base_value,
            er_id: er_value,
            nonce,
        },
        STALE_WINDOW,
    )
    .await?;

    let restart = er_a.restart(RestartConfig::default()).await?;
    ensure!(restart.exit_code == Some(0));
    let session_b = er_b.unwrap_or_else(|| er_a.ctx());
    hold_steady(
        base,
        &SessionB {
            er: session_b,
            account,
            base_id: base_value,
            er_id: er_value,
            nonce,
        },
        RECOVERY_WINDOW,
    )
    .await?;

    if er_b.is_some() {
        let stale_write = er_a
            .ctx()
            .submit_and_confirm(
                payer,
                &[REDSHIFT.batch().set(&[account], value(case, 4)).build()],
            )
            .await;
        ensure!(stale_write.is_err());
        let stale_commit =
            schedule_commit(er_a.ctx(), payer, &account, SettlementMode::Commit).await;
        if let Ok(signature) = stale_commit {
            let receipt = receipt::fetch_commit_receipt(er_a.ctx().api(), &signature).await?;
            receipt
                .error_message
                .context("the old validator must not commit the reassigned account")?;
        }
        ensure!(base_state(base, &account).await? == (dlp::dlp_id(), Some(base_value)));
    }

    let fresh = schedule_commit(session_b, payer, &account, SettlementMode::Commit).await?;
    let fresh_receipt = receipt::fetch_commit_receipt(session_b.api(), &fresh).await?;
    ensure!(fresh_receipt.succeeded() || fresh_receipt.failure_is_duplicate_rejection());
    await_base(
        base,
        &account,
        dlp::dlp_id(),
        er_value,
        "the session-b commit reaches base",
    )
    .await?;
    Ok(())
}

pub struct DelegationSessionIsolation {
    base: Rc<BaseCtx>,
    er: ErCtx,
    proxies: BaseProxies,
    er_a: PrivateEr,
    payer: Keypair,
}

impl Scenario for DelegationSessionIsolation {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let proxies = BaseProxies::spawn(&base).await?;
        let er_a = topology::private_er(
            &base,
            ErOptions {
                label: LABEL_A.to_owned(),
                base_endpoints: Some(proxies.endpoints()),
                ..Default::default()
            },
        )
        .await?;
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;

        Ok(Self { base, er, proxies, er_a, payer })
    }

    async fn run(self) -> Result<()> {
        let Self {
            base,
            er,
            proxies,
            mut er_a,
            payer,
        } = self;
        isolate(&base, &proxies, &mut er_a, None, &payer).await?;
        isolate(&base, &proxies, &mut er_a, Some(&er), &payer).await?;

        proxies.finish()?;
        er_a.finish().await?;
        Ok(())
    }
}
