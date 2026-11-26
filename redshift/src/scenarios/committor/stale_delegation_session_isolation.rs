use std::{
    rc::Rc,
    slice,
    time::{Duration, Instant},
};

use account::Account;
use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    api::transaction_meta,
    dlp,
    netfault::{Faults, Proxy, Selector},
    prep,
    programs::{CellHeader, CellId, SettlementMode, REDSHIFT},
    receipt, system,
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
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
const CANARY_LAMPORTS: u64 = 1_000_000;

fn value(case: u64, step: u64) -> u64 {
    100 * case + step
}

fn count(data: &[u8]) -> Result<u64> {
    Ok(CellHeader::try_decode(data)?.value)
}

async fn await_clone_count(er: &ErCtx, account: &Pubkey, id: u64) -> Result<()> {
    er.api()
        .await_query("cloned cell state", async || {
            let value = crate::account_id(er.account(account).await?);
            Ok(wait::when(value == Some(id), value))
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
        .await_accounts(slice::from_ref(account), what, |accounts| {
            let account = accounts[0].as_ref().context("base cell missing")?;
            ensure!((account.owner, CellHeader::written_id(&account.data)) == (owner, Some(id)));
            Ok(())
        })
        .await?;
    Ok(())
}

/// New-session invariants that stale notifications or restarted old-validator work must not change.
struct SessionB {
    account: Pubkey,
    base_id: u64,
    local: Account,
    nonce: u64,
}

impl SessionB {
    async fn check(&self, base: &BaseCtx, er: &ErCtx) -> Result<()> {
        ensure!(
            base_state(base, &self.account).await? == (dlp::dlp_id(), Some(self.base_id)),
            "base ownership or value changed across sessions"
        );
        ensure!(
            crate::last_commit_id(base, &self.account).await? == self.nonce,
            "stale work advanced the base commit nonce"
        );
        ensure!(
            er.account(&self.account).await?.as_ref() == Some(&self.local),
            "the new session's ER image changed"
        );
        Ok(())
    }

    /// Observe delayed effects after a delivery barrier, not instead of proving delivery.
    async fn hold(&self, base: &BaseCtx, er: &ErCtx, window: Duration) -> Result<()> {
        let started = Instant::now();
        while started.elapsed() < window {
            self.check(base, er).await?;
            time::sleep(POLL).await;
        }
        self.check(base, er).await
    }
}

/// A WS-only mirror on the old ER's single socket witnesses progress past the late frame.
struct Canary {
    address: Pubkey,
    lamports: u64,
}

impl Canary {
    async fn acquire(base: &BaseCtx, er: &ErCtx) -> Result<Self> {
        let address = base.funded_payer(CANARY_LAMPORTS).await?.pubkey();
        er.api()
            .await_query("old ER acquires its WebSocket canary", async || {
                let balance = er.account(&address).await?.map(|account| account.lamports);
                Ok(wait::when(balance == Some(CANARY_LAMPORTS), balance))
            })
            .await?;
        Ok(Self {
            address,
            lamports: CANARY_LAMPORTS,
        })
    }

    async fn advance(&mut self, base: &BaseCtx, er: &ErCtx, payer: &Keypair) -> Result<()> {
        base.submit_and_confirm(
            payer,
            &[system::transfer(&payer.pubkey(), &self.address, 1)],
        )
        .await?;
        self.lamports += 1;
        er.api()
            .await_query(
                "old ER processes the frame following the stale notification",
                async || {
                    let accounts =
                        er.local_accounts(&[sdk_ids::system_program::ID], &[self.address]).await?;
                    let balance = accounts[0].as_ref().map(|account| account.lamports);
                    Ok(wait::when(balance == Some(self.lamports), balance))
                },
            )
            .await?;
        Ok(())
    }
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

/// Establishes a landed old-session snapshot even when its send response is discarded.
async fn recover_session_a_commit_after_lost_response(
    base: &BaseCtx,
    er: &ErCtx,
    faults: &Faults,
    payer: &Keypair,
    account: &Pubkey,
    expected_id: u64,
) -> Result<()> {
    let submission =
        faults.intercept(Selector::method("sendTransaction").http().response().account(account));
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

/// Reassigns an account after undelegation, then releases old notifications and
/// restarts the old ER.
/// `er_b = None` exercises reuse of the same validator; otherwise ownership moves to another ER.
#[tracing::instrument(skip_all, fields(reassigned = er_b.is_some()), err)]
async fn isolate(
    base: &BaseCtx,
    faults: &Faults,
    er_a: &mut PrivateEr,
    er_b: Option<&ErCtx>,
    payer: &Keypair,
) -> Result<()> {
    let case = u64::from(er_b.is_some()) + 1;
    let mut canary = Canary::acquire(base, er_a.ctx()).await?;
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
    recover_session_a_commit_after_lost_response(base, er_a.ctx(), faults, payer, &account, staged)
        .await?;

    // Delegated accounts have no mirror subscription. Witness the new subscription
    // created by confirmed undelegation before generating the frame to delay.
    let subscribed =
        faults.intercept(Selector::method("accountSubscribe").ws().response().account(&account));
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
    subscribed.wait(INTERCEPT_TIMEOUT).await?.release();
    faults.reset().await?;

    let delayed = faults
        .intercept(Selector::method("accountNotification").ws().notification().account(&account));
    let base_value = value(case, 2);
    write(base, payer, &account, base_value).await?;
    let delayed = delayed.wait(INTERCEPT_TIMEOUT).await?;
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
    let session = SessionB {
        account,
        base_id: base_value,
        local: session_b.account(&account).await?.context("session-b ER image missing")?,
        nonce: crate::last_commit_id(base, &account).await?,
    };

    delayed.release();
    faults.reset().await?;
    canary.advance(base, er_a.ctx(), payer).await?;
    session.hold(base, session_b, STALE_WINDOW).await?;

    let restart = er_a.restart(RestartConfig::default()).await?;
    ensure!(restart.exit_code == Some(0));
    let session_b = er_b.unwrap_or_else(|| er_a.ctx());
    session.hold(base, session_b, RECOVERY_WINDOW).await?;

    if er_b.is_some() {
        let stale_write = er_a
            .ctx()
            .try_submit_and_confirm(
                payer,
                &[REDSHIFT.batch().set(&[account], value(case, 4)).build()],
            )
            .await?;
        let error = stale_write.err().context("the old validator wrote the reassigned account")?;
        super::check_lockout(&error)?;
        let stale_commit = er_a
            .ctx()
            .try_submit_and_confirm(
                payer,
                &[REDSHIFT
                    .batch()
                    .settle(payer.pubkey(), &[account], SettlementMode::Commit)
                    .build()],
            )
            .await?;
        if let Ok(signature) = stale_commit {
            let receipt = receipt::fetch_commit_receipt(er_a.ctx().api(), &signature).await?;
            receipt
                .error_message
                .context("the old validator must not commit the reassigned account")?;
        }
        session.check(base, session_b).await?;
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
    ensure!(crate::last_commit_id(base, &account).await? > session.nonce);
    Ok(())
}

/// Checks that stale work and notifications cannot mutate a new delegation session.
/// Covers both same-validator session reuse and reassignment to a different validator.
pub struct StaleDelegationSessionIsolation {
    base: Rc<BaseCtx>,
    er: ErCtx,
    proxy: Proxy,
    faults: Faults,
    er_a: PrivateEr,
    payer: Keypair,
}

impl Scenario for StaleDelegationSessionIsolation {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let proxy = Proxy::spawn(topology::BaseEndpoints::from_base(&base)?).await?;
        let faults = proxy.scope()?;
        let er_a = topology::private_er(
            &base,
            ErOptions {
                label: LABEL_A.to_owned(),
                followers: 0,
                base_endpoints: Some(proxy.endpoints()),
                settings: Box::new(|params| {
                    params.chainsync.websocket.0[0].max_connections = 1;
                    params.chainsync.grpc.push(params.chainsync.grpc[0].clone());
                    // Keep both lifecycle feeds while making the canary exclusively WS-driven.
                    for stream in &mut params.chainsync.grpc {
                        stream.duplication_delay = Duration::from_secs(3600);
                    }
                }),
            },
        )
        .await?;
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;

        Ok(Self {
            base,
            er,
            proxy,
            faults,
            er_a,
            payer,
        })
    }

    /// Runs same-validator isolation before cross-validator reassignment using independent cells.
    async fn run(mut self) -> Result<()> {
        isolate(&self.base, &self.faults, &mut self.er_a, None, &self.payer).await?;
        isolate(
            &self.base,
            &self.faults,
            &mut self.er_a,
            Some(&self.er),
            &self.payer,
        )
        .await?;

        self.faults.reset().await?;
        drop(self.proxy);
        self.er_a.finish().await?;
        Ok(())
    }
}
