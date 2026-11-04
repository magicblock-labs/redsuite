use std::{
    collections::{BTreeSet, HashMap, HashSet},
    mem,
    ops::ControlFlow::{self, Break, Continue},
    sync::Arc,
};

use anyhow::Context;
use json::LazyValue;
use pubkey::Pubkey;
use signature::Signature;
use solana_rpc_client_types::response::{Response, RpcSignatureResult};
use tokio::{sync::oneshot, time};
use url::Url;

use super::{
    clock::SendClock,
    socket::CloseReason,
    subscription::{self, Connection, SubscriptionState, Topic},
    NETWORK_TIMEOUT,
};
use crate::{
    stats::{ObservationsStats, StreamingStats},
    Result,
};

#[derive(Debug)]
pub struct UpdateOutcome {
    pub lag: ObservationsStats,
    pub observed: usize,
    pub superseded: usize,
}

#[derive(Default)]
struct AccountState {
    // Both indexes contain the same pending writes.
    pending: HashMap<u64, BTreeSet<Pubkey>>,
    by_account: HashMap<Pubkey, BTreeSet<u64>>,
    waiters: HashMap<u64, oneshot::Sender<()>>,
    lag: StreamingStats,
    observed: usize,
    superseded: usize,
}

impl SubscriptionState for AccountState {
    type Key = Pubkey;

    fn on_close(&mut self, _reason: &CloseReason) {
        // Dropping the senders errors every parked waiter.
        self.waiters.clear();
    }
}

impl AccountState {
    fn settle(&mut self, id: u64, account: Pubkey) {
        let targets = self.pending.get_mut(&id).unwrap();
        targets.remove(&account);
        self.by_account.get_mut(&account).unwrap().remove(&id);
        if targets.is_empty() {
            self.pending.remove(&id);
            if let Some(tx) = self.waiters.remove(&id) {
                let _ = tx.send(());
            }
        }
    }

    fn observe(&mut self, account: Pubkey, id: u64, clock: &SendClock) {
        if self.pending.get(&id).is_some_and(|targets| targets.contains(&account)) {
            if let Some(lag) = clock.elapsed_micros(id) {
                self.lag.push(lag);
            }
            self.observed += 1;
            self.settle(id, account);
        }
        while let Some(&pending_id) = self.by_account.get(&account).and_then(BTreeSet::first) {
            if pending_id >= id {
                break;
            }
            self.superseded += 1;
            self.settle(pending_id, account);
        }
    }
}

pub struct AccountUpdates {
    connection: Connection<AccountState>,
}

impl AccountUpdates {
    pub async fn connect<Extract>(
        url: &Url,
        extractor: Extract,
        clock: Arc<SendClock>,
    ) -> Result<Self>
    where
        Extract: Fn(&[u8]) -> Option<u64> + 'static,
    {
        let connection = Connection::connect(
            url,
            AccountState::default(),
            move |state, account, payload| {
                if let Some(id) =
                    subscription::account_data(payload).and_then(|data| extractor(&data))
                {
                    state.observe(account, id, &clock);
                }
                Continue(())
            },
        )
        .await?;
        Ok(Self { connection })
    }

    pub async fn account_subscribe(&self, account: &Pubkey) -> Result<()> {
        self.connection
            .subscribe(
                Topic::Account,
                &subscription::account_params(account),
                *account,
            )
            .await
    }

    pub async fn await_subscribed(&self, count: usize) -> Result<()> {
        self.connection.await_subscribed(count).await
    }

    pub fn track(&self, id: u64, account: Pubkey) {
        let mut state = self.connection.state_mut();
        state.pending.entry(id).or_default().insert(account);
        state.by_account.entry(account).or_default().insert(id);
    }

    pub async fn await_observed(&self, count: usize) -> Result<()> {
        self.connection
            .wait_for("account updates", |state| state.observed >= count)
            .await
    }

    // Writes coalesce per slot: newer updates settle older writes for the same account.
    pub async fn await_settled(&self) -> Result<()> {
        self.connection
            .wait_for("pending account updates", |state| state.pending.is_empty())
            .await
    }

    // Resolves immediately for settled or untracked ids.
    pub async fn await_id(&self, id: u64) -> Result<()> {
        self.connection.check_error()?;
        let rx = {
            let mut state = self.connection.state_mut();
            if !state.pending.contains_key(&id) {
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            state.waiters.insert(id, tx);
            rx
        };
        time::timeout(NETWORK_TIMEOUT, rx)
            .await
            .context("account update timed out")?
            .context("ws stream failed while awaiting update")
    }

    pub fn finalize(&self) -> UpdateOutcome {
        self.connection.stop();
        self.take_outcome()
    }

    /// Takes one settled observation window without reconnecting the subscriber.
    pub fn take_outcome(&self) -> UpdateOutcome {
        let mut state = self.connection.state_mut();
        UpdateOutcome {
            lag: mem::take(&mut state.lag).finalize(false),
            observed: mem::take(&mut state.observed),
            superseded: mem::take(&mut state.superseded),
        }
    }
}

#[derive(Debug)]
pub struct SignatureOutcome {
    pub latency: ObservationsStats,
    pub confirmed: usize,
    pub failed: usize,
    pub first_failure: Option<String>,
    // Still pending when finalized; report against the observation window.
    pub unconfirmed: usize,
}

#[derive(Default)]
struct SignatureState {
    pending: HashSet<u64>,
    waiters: HashMap<u64, oneshot::Sender<()>>,
    latency: StreamingStats,
    confirmed: usize,
    failed: usize,
    first_failure: Option<String>,
}

impl SubscriptionState for SignatureState {
    type Key = u64;

    fn on_close(&mut self, _reason: &CloseReason) {
        self.waiters.clear();
    }
}

impl SignatureState {
    fn observe(&mut self, id: u64, payload: &LazyValue<'_>, clock: &SendClock) -> ControlFlow<()> {
        let Ok(Response {
            value: RpcSignatureResult::ProcessedSignature(result),
            ..
        }) = json::from_str::<Response<RpcSignatureResult>>(payload.as_raw_str())
        else {
            return Continue(());
        };
        if !self.pending.remove(&id) {
            return Break(());
        }
        match result.err {
            None => {
                if let Some(latency) = clock.elapsed_micros(id) {
                    self.latency.push(latency);
                }
                self.confirmed += 1;
            }
            Some(err) => {
                self.failed += 1;
                self.first_failure.get_or_insert(format!("id {id}: {err}"));
            }
        }
        if let Some(tx) = self.waiters.remove(&id) {
            let _ = tx.send(());
        }
        Break(())
    }
}

// Signature subscriptions are one-shot. Subscribe before delivering the transaction
// to avoid racing confirmation (`TxSender::prepare` exposes the signature).
pub struct SignatureConfirmations {
    connection: Connection<SignatureState>,
}

impl SignatureConfirmations {
    pub async fn connect(url: &Url, clock: Arc<SendClock>) -> Result<Self> {
        let connection =
            Connection::connect(url, SignatureState::default(), move |state, id, payload| {
                state.observe(id, payload, &clock)
            })
            .await?;
        Ok(Self { connection })
    }

    pub async fn subscribe(&self, id: u64, signature: &Signature) -> Result<()> {
        self.connection.check_error()?;
        self.connection.state_mut().pending.insert(id);
        self.connection
            .subscribe(
                Topic::Signature,
                &subscription::signature_params(signature),
                id,
            )
            .await?;
        self.connection.await_ack(id).await
    }

    pub async fn await_id(&self, id: u64) -> Result<()> {
        self.connection.check_error()?;
        let rx = {
            let mut state = self.connection.state_mut();
            if !state.pending.contains(&id) {
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            state.waiters.insert(id, tx);
            rx
        };
        time::timeout(NETWORK_TIMEOUT, rx)
            .await
            .context("signature confirmation timed out")?
            .context("ws stream failed while awaiting signature")
    }

    pub async fn await_all(&self) -> Result<()> {
        self.connection
            .wait_for("signature confirmations", |state| state.pending.is_empty())
            .await
    }

    pub fn finalize(&self) -> SignatureOutcome {
        self.connection.stop();
        let mut state = self.connection.state_mut();
        SignatureOutcome {
            latency: mem::take(&mut state.latency).finalize(false),
            confirmed: state.confirmed,
            failed: state.failed,
            first_failure: state.first_failure.take(),
            unconfirmed: state.pending.len(),
        }
    }
}
