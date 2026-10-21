use std::{
    rc::Rc,
    slice,
    time::{Duration, Instant},
};

use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    check, check_eq, dlp,
    netfault::{BaseProxies, Selector},
    prep,
    programs::{CellHeader, CellId, SettlementMode, REDSHIFT},
    receipt,
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    BaseCtx, ChainCtx, DynError, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use tokio::time;

use crate::PAYER_LAMPORTS;

const LABEL_A: &str = "session-isolation-a";
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(60);
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
    check::poll_for(
        &format!("the er {} clones the cell with count {id}", er.identity()),
        CLONE_TIMEOUT,
        || async {
            let account = er.account(account).await?.ok_or("ER cell absent")?;
            check_eq!(count(&account.data)?, id, "cloned cell value")?;
            Ok::<_, DynError>(())
        },
    )
    .await
    .map_err(|error| error.expected(format!("count {id}")))?;
    Ok(())
}

async fn write(ctx: &impl ChainCtx, payer: &Keypair, account: &Pubkey, id: u64) -> Result<()> {
    ctx.submit_and_confirm(payer, &[REDSHIFT.batch().set(&[*account], id).build()])
        .await?;
    let data = ctx.account(account).await?.ok_or("the cell vanished after the write")?.data;
    check_eq!(count(&data)?, id, "the write must land")?;
    Ok(())
}

async fn base_state(base: &BaseCtx, account: &Pubkey) -> Result<(Pubkey, Option<u64>)> {
    let on_base = base.account(account).await?.ok_or("the cell vanished from base")?;
    Ok((on_base.owner, count(&on_base.data).ok()))
}

async fn await_base(
    base: &BaseCtx,
    account: &Pubkey,
    owner: Pubkey,
    id: u64,
    what: &str,
) -> Result<()> {
    check::poll_for(what, COMPLETION_TIMEOUT, || async {
        check_eq!(
            base_state(base, account).await?,
            (owner, Some(id)),
            "settled cell owner and value"
        )?;
        Ok::<_, DynError>(())
    })
    .await
    .map_err(|error| error.expected(format!("owner {owner} count {id}")))?;
    Ok(())
}

struct SessionB<'a> {
    er: &'a ErCtx,
    account: Pubkey,
    base_id: u64,
    er_id: u64,
    nonce: u64,
}

async fn hold_steady(
    base: &BaseCtx,
    session: &SessionB<'_>,
    window: Duration,
    what: &str,
) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < window {
        let (owner, written) = base_state(base, &session.account).await?;
        check_eq!(
            (owner, written),
            (dlp::dlp_id(), Some(session.base_id)),
            "{what}: base must keep the session-b delegation and value"
        )?;
        check_eq!(
            crate::last_commit_id(base, &session.account).await?,
            session.nonce,
            "{what}: the session-b nonce must not move"
        )?;
        let on_er = session
            .er
            .account(&session.account)
            .await?
            .ok_or("the session-b er copy vanished")?;
        check_eq!(
            count(&on_er.data)?,
            session.er_id,
            "{what}: the session-b er state must stay"
        )?;
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
    let base_tx = base.api().await_transaction(&base_signature, BASE_CONFIRM_TIMEOUT).await?;
    check_eq!(
        base_tx.err,
        None,
        "the intercepted session-a commit must land on base"
    )?;
    await_base(
        base,
        account,
        dlp::dlp_id(),
        expected_id,
        "base records the session-a commit before the response is lost",
    )
    .await?;
    held.discard();

    let receipt = receipt::fetch_commit_receipt(er.api(), &signature, RECEIPT_TIMEOUT).await?;
    check!(
        receipt.succeeded() || receipt.failure_is_duplicate_rejection(),
        "the session-a commit intent must converge after a lost base response, \
         got {:?}",
        receipt.error_message
    )?;
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
    let label = if er_b.is_some() { "reassignment" } else { "redelegation" };
    let cell = base
        .cells(REDSHIFT, payer)
        .create_owned(er_a.identity(), 1)
        .await?
        .pop()
        .ok_or("no cell was created")?;
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
    let undelegated =
        receipt::fetch_commit_receipt(er_a.ctx().api(), &undelegate, RECEIPT_TIMEOUT).await?;
    check!(
        undelegated.succeeded() || undelegated.failure_is_duplicate_rejection(),
        "the session-a undelegation must complete, got {:?}",
        undelegated.error_message
    )?;
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
        .await_delegated(
            &account,
            CLONE_TIMEOUT,
            "session-b er activates the cell as delegated",
        )
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
        &format!("{label}: after releasing session-a observations"),
    )
    .await?;

    let restart = er_a.restart(RestartConfig::default()).await?;
    check_eq!(
        restart.exit_code,
        Some(0),
        "the session-a validator must stop cleanly before recovery"
    )?;
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
        &format!("{label}: while the restarted validator recovers"),
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
        check!(
            stale_write.is_err(),
            "the old validator must reject writes to the reassigned \
                 account, got {stale_write:?}"
        )?;
        let stale_commit =
            schedule_commit(er_a.ctx(), payer, &account, SettlementMode::Commit).await;
        if let Ok(signature) = stale_commit {
            let receipt =
                receipt::fetch_commit_receipt(er_a.ctx().api(), &signature, RECEIPT_TIMEOUT)
                    .await?;
            receipt
                .error_message
                .ok_or("the old validator must not commit the reassigned account")?;
        }
        check_eq!(
            base_state(base, &account).await?,
            (dlp::dlp_id(), Some(base_value)),
            "rejected old work must leave base untouched"
        )?;
    }

    let fresh = schedule_commit(session_b, payer, &account, SettlementMode::Commit).await?;
    let fresh_receipt =
        receipt::fetch_commit_receipt(session_b.api(), &fresh, RECEIPT_TIMEOUT).await?;
    check!(
        fresh_receipt.succeeded() || fresh_receipt.failure_is_duplicate_rejection(),
        "a fresh session-b commit must complete, got {:?}",
        fresh_receipt.error_message
    )?;
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
