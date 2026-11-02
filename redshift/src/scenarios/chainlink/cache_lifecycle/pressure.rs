use std::{cell::Cell, rc::Rc, time::Duration};
use tokio::sync::Notify;

use account::Account;
use anyhow::ensure;
use futures_util::future::{try_join, try_join_all};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    netfault::{Held, RuleHandle, Selector, Transport, Trap},
    programs::{CellHeader, CellId, HashState, SettlementMode, REDLINE},
    rpc_types::response::TransactionError,
    system, wait, ChainCtx, Result, TxSender,
};
use signer::Signer;
use tokio::time::{self, Instant};
use tracing::instrument;

use super::{
    await_local_value, cache_evictions, churn_cache, CacheLifecycle, Mode, BASE_REFRESH,
    BASE_REWRITE, CACHE_CAPACITY, ER_WRITE, FETCHES,
};
use crate::{
    program::{self, layout, DELEGATION_PROGRAM_ID},
    ACCOUNT_OWNERS, ACCOUNT_SPACE, PAYER_LAMPORTS,
};

const READONLY_COUNT: usize = 1;
const DELEGATED_COUNT: usize = 2;
const PENDING_COUNT: usize = 2;
const FUNDING_BATCH: usize = 16;
const TIMEOUT: Duration = Duration::from_secs(20);
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(180);

impl CacheLifecycle {
    /// Runs traffic and fault stages together, retaining discovery
    /// failures until teardown completes.
    pub(super) async fn pressure(self) -> Result<()> {
        let (accounts, stale, settlement) = self.prepare_accounts().await?;
        let progress = TrafficProgress::default();
        let faults = async {
            progress.await_sent(0).await?;
            for held in stale {
                held.release();
            }
            self.churn(&accounts, &progress).await?;
            self.complete_settlement(&accounts, &progress, settlement).await
        };
        let (_, recovery) = time::timeout(
            SCENARIO_TIMEOUT,
            try_join(self.traffic(&accounts, &progress), faults),
        )
        .await??;

        let delegated = &accounts.keys[READONLY_COUNT..READONLY_COUNT + DELEGATED_COUNT];
        ensure!(self.base.accounts(delegated).await? == accounts.base_delegated);
        self.finish().await?;
        recovery
    }
}

/// Ordered readonly/delegated/pending residents, their protected images, and an eviction pool.
struct CacheAccounts {
    funder: Keypair,
    keys: Vec<Pubkey>,
    protected: Vec<Option<Account>>,
    base_delegated: Vec<Option<Account>>,
    pool: Vec<Pubkey>,
    sender: TxSender,
    evictions: f64,
}

/// Coordinates pressure against successful traffic, settlement release, and final drain.
#[derive(Default)]
struct TrafficProgress {
    sent: Cell<u64>,
    changed: Notify,
    completed: Cell<bool>,
    done: Cell<bool>,
}

impl TrafficProgress {
    async fn await_sent(&self, start: u64) -> Result<()> {
        wait::on_change(&self.changed, TIMEOUT, "warmed traffic recovers", || {
            Ok((self.sent.get() > start).then_some(()))
        })
        .await
    }
}

impl CacheLifecycle {
    fn observe(&self, method: &str, key: &Pubkey) -> Trap {
        self.faults
            .intercept(Selector::method(method).ws().response().notification().account(key))
    }

    /// Holds stale reads and undelegation delivery before exposing the residents to pressure.
    #[instrument(skip_all, err)]
    async fn prepare_accounts(&self) -> Result<(CacheAccounts, Vec<Held>, RuleHandle)> {
        let er = &self.er;
        let validator = er.identity();
        let funder = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let owner = funder.pubkey();
        let payer = self.base.delegated_payer(&funder, validator, PAYER_LAMPORTS).await?;
        let keys = self
            .base
            .cells(REDLINE, &funder)
            .create_many(
                ACCOUNT_SPACE,
                validator,
                READONLY_COUNT + DELEGATED_COUNT + PENDING_COUNT,
            )
            .await?;
        let protected_keys = &keys[READONLY_COUNT..];
        let (delegated, pending) = protected_keys.split_at(DELEGATED_COUNT);
        let er_written_keys = &protected_keys[1..];
        let stale: Vec<_> = if matches!(self.mode, Mode::UndelegationReconnectGap) {
            Vec::new()
        } else {
            protected_keys
                .iter()
                .map(|key| self.observe("programNotification", key))
                .collect()
        };
        let delegates: Vec<_> = (READONLY_COUNT..keys.len())
            .map(|index| {
                REDLINE.delegate(
                    owner,
                    CellId::new(owner, index as u64),
                    Some(validator),
                    u32::MAX,
                )
            })
            .collect();
        self.base.submit_and_confirm(&funder, &delegates).await?;
        let stale = try_join_all(stale.into_iter().map(|trap| trap.wait(TIMEOUT))).await?;
        er.accounts(protected_keys).await?;
        er.cells(REDLINE, &payer).write(ER_WRITE, er_written_keys).await?;
        let settlement = self
            .faults
            .stall(Selector::method("sendTransaction").http().request().account(&pending[0]));
        er.submit_and_confirm(
            &funder,
            &[REDLINE
                .batch()
                .nonce(1)
                .settle(owner, pending, SettlementMode::Undelegate)
                .build()],
        )
        .await?;
        settlement.wait_hit(TIMEOUT).await?;
        let protected = er.local_accounts(&ACCOUNT_OWNERS, protected_keys).await?;
        ensure!(protected.iter().all(Option::is_some));
        let base_delegated = self.base.accounts(delegated).await?;
        let pool: Vec<_> = (0..CACHE_CAPACITY * 2).map(|_| Keypair::new().pubkey()).collect();
        try_join_all(pool.chunks(FUNDING_BATCH).map(|chunk| async {
            let transfers: Vec<_> =
                chunk.iter().map(|key| system::transfer(&owner, key, 1_000_000)).collect();
            self.base.submit_and_confirm(&funder, &transfers).await
        }))
        .await?;
        let before = cache_evictions(&self.er).await?;
        let sender = er.sender(Rc::new(payer));
        Ok((
            CacheAccounts {
                funder,
                keys,
                protected,
                base_delegated,
                pool,
                sender,
                evictions: before,
            },
            stale,
            settlement,
        ))
    }

    /// Checks hash evolution and protected images on each warmed transaction during faults.
    #[instrument(skip_all, err)]
    async fn traffic(&self, accounts: &CacheAccounts, progress: &TrafficProgress) -> Result<()> {
        let er = &self.er;
        let protected_keys = &accounts.keys[READONLY_COUNT..];
        let traffic_keys = &protected_keys[..1];

        let mut expected = accounts.protected.clone();
        let mut hash = [0; layout::HASH_SIZE];
        let mut id = 0;
        while !progress.done.get() {
            id += 1;
            let ixs = [REDLINE.fold_hashes(id, 0, traffic_keys)];
            let signature = wait::retry("warmed submission recovers", TIMEOUT, async || {
                accounts.sender.submit(&ixs).await
            })
            .await?;
            let outcome = er.api().await_transaction(&signature).await?;
            let meta = transaction_meta(&outcome)?;
            if let Some(error) = &meta.err {
                ensure!(
                    TransactionError::from(error.clone())
                        == TransactionError::ProgramAccountNotFound
                );
            } else {
                hash = HashState::fold(id, &[hash], 0);
                let data = &mut expected[0].as_mut().unwrap().data;
                let mut header = CellHeader::try_decode(data)?;
                header.value = id;
                header.revision += 2;
                header.write(data)?;
                data[layout::HASH_OFFSET..layout::HASH_OFFSET + layout::HASH_SIZE]
                    .copy_from_slice(&hash);
            }
            let count =
                if progress.completed.get() { DELEGATED_COUNT } else { protected_keys.len() };
            let actual = er.local_accounts(&ACCOUNT_OWNERS, &protected_keys[..count]).await?;
            ensure!(actual == expected[..count]);
            if meta.err.is_none() {
                progress.sent.set(progress.sent.get() + 1);
                progress.changed.notify_waiters();
            }
        }
        Ok(())
    }

    /// Forces eviction plus controlled reconnect/fetch races, or installs the discovery blackout.
    #[instrument(skip_all, err)]
    async fn churn(&self, accounts: &CacheAccounts, progress: &TrafficProgress) -> Result<()> {
        let er = &self.er;
        let (readonly, protected_keys) = accounts.keys.split_at(READONLY_COUNT);
        let pending = &protected_keys[DELEGATED_COUNT..];
        let mut evictions = accounts.evictions;
        if matches!(self.mode, Mode::UndelegationReconnectGap) {
            churn_cache(&self.er, &accounts.pool, evictions).await?;
            self.faults.stall(Selector::default().ws().response().notification());
            for key in protected_keys {
                self.faults.stall(Selector::methods(&FETCHES).http().response().account(key));
            }
            self.faults.disconnect(Transport::Ws);
            return Ok(());
        }
        for (cycle, &target) in readonly.iter().enumerate() {
            evictions = churn_cache(&self.er, &accounts.pool, evictions).await?;
            let cold = er.local_accounts(&ACCOUNT_OWNERS, &[target]).await?;
            ensure!(cold[0].is_none());
            let reconnect = self.observe("accountSubscribe", &pending[0]);
            self.faults.disconnect(Transport::Ws);
            let reconnect = reconnect.wait(TIMEOUT).await?;
            let registered = self.observe("accountSubscribe", &target);
            let old = self
                .faults
                .intercept(Selector::methods(&FETCHES).http().response().account(&target));
            let latest = BASE_REFRESH + 1 + cycle as u64;
            let (read, ()) = try_join(er.account(&target), async {
                registered.wait(TIMEOUT).await?.release();
                let old = old.wait(TIMEOUT).await?;
                let notification = self.observe("accountNotification", &target);
                self.base.cells(REDLINE, &accounts.funder).write(latest, &[target]).await?;
                notification.wait(TIMEOUT).await?.release();
                await_local_value(er, "latest base value observed locally", &[target], latest)
                    .await?;
                old.release();
                progress.await_sent(progress.sent.get()).await?;
                reconnect.release();
                Ok(())
            })
            .await?;
            ensure!(crate::account_id(read) == Some(latest));
            await_local_value(er, "latest base value observed locally", &[target], latest).await?;
            ensure!(self
                .base
                .accounts(pending)
                .await?
                .iter()
                .all(|a| matches!(a, Some(a) if a.owner == DELEGATION_PROGRAM_ID)));
        }

        Ok(())
    }

    /// Releases settlement, rewrites base state, and distinguishes passive discovery from refetch.
    /// The inner result defers reconnect-gap failures until traffic and teardown finish.
    #[instrument(skip_all, err)]
    async fn complete_settlement(
        &self,
        accounts: &CacheAccounts,
        progress: &TrafficProgress,
        settlement: RuleHandle,
    ) -> Result<Result<()>> {
        let er = &self.er;
        let (readonly, protected_keys) = accounts.keys.split_at(READONLY_COUNT);
        let (delegated, pending) = protected_keys.split_at(DELEGATED_COUNT);
        progress.completed.set(true);
        settlement.remove();
        self.base
            .api()
            .await_accounts(pending, "base undelegation", |actual| {
                for (actual, expected) in actual.iter().zip(&accounts.protected[delegated.len()..])
                {
                    let actual = actual.as_ref().map(|a| (a.owner, &a.data));
                    let expected = expected.as_ref().map(|a| (program::id(), &a.data));
                    ensure!(actual == expected);
                }
                Result::Ok(())
            })
            .await?;
        let released_at = Instant::now();
        self.base.cells(REDLINE, &accounts.funder).write(BASE_REWRITE, pending).await?;
        let recovery = if matches!(self.mode, Mode::UndelegationReconnectGap) {
            // Defer recovery failures until warmed traffic and teardown finish.
            await_local_value(
                er,
                "ER discovers the completed undelegation",
                pending,
                BASE_REWRITE,
            )
            .await
            .and_then(|()| {
                let elapsed = released_at.elapsed();
                ensure!(elapsed <= TIMEOUT);
                Ok(())
            })
        } else {
            // Completed accounts are evictable mirrors. A later reconnect
            // gap can invalidate them, so verify normal access refetches them.
            er.api()
                .await_accounts(
                    pending,
                    "completed accounts refetch current base state",
                    |observed| {
                        ensure!(observed.len() == pending.len());
                        for account in observed {
                            let actual =
                                account.as_ref().and_then(|a| CellHeader::written_id(&a.data));
                            ensure!(actual == Some(BASE_REWRITE));
                        }
                        Result::Ok(())
                    },
                )
                .await?;
            Ok(())
        };
        self.base.cells(REDLINE, &accounts.funder).write(BASE_REFRESH, readonly).await?;
        // Base confirmation does not wait for ER subscription delivery.
        er.api()
            .await_accounts(
                readonly,
                "final readonly mirrors synchronized",
                |observed| {
                    for account in observed {
                        let actual = account.as_ref().and_then(|a| CellHeader::written_id(&a.data));
                        ensure!(actual == Some(BASE_REFRESH));
                    }
                    Result::Ok(())
                },
            )
            .await?;
        let local = er.local_accounts(&ACCOUNT_OWNERS, readonly).await?;
        // Readonly entries may be evicted after the fetch.
        for local in local.into_iter().flatten() {
            ensure!(CellHeader::written_id(&local.data) == Some(BASE_REFRESH));
        }
        progress.done.set(true);
        Result::Ok(recovery)
    }

    #[instrument(skip_all, err)]
    async fn finish(self) -> Result<()> {
        self.faults.reset().await?;
        Ok(())
    }
}
