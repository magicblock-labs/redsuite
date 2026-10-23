use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    ops::ControlFlow::{self, Continue},
    rc::Rc,
    slice,
};

use anyhow::Context;
use futures_util::{stream::FuturesUnordered, StreamExt};
use json::{LazyValue, Value};
use pubkey::Pubkey;
use serde::{de::DeserializeOwned, Serialize};
use signature::Signature;
use solana_account_decoder_client_types::UiAccount;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client_types::{
    config::{RpcSignatureSubscribeConfig, RpcTransactionLogsFilter},
    response::{Response, RpcKeyedAccount, RpcLogsResponse, RpcSignatureResult, SlotInfo},
};
use tokio::{task, time, time::Instant};
use url::Url;

use super::{
    rpc::NO_PARAMS,
    subscription::{self, Connection, SubscriptionState, Topic},
    NETWORK_TIMEOUT,
};
use crate::Result;

#[derive(Default)]
struct EventBuffer {
    last_key: u64,
    by_key: HashMap<u64, VecDeque<Value>>,
    malformed: usize,
}

impl SubscriptionState for EventBuffer {
    type Key = u64;

    fn on_malformed(&mut self) {
        self.malformed += 1;
    }
}

impl EventBuffer {
    fn register(&mut self) -> u64 {
        self.last_key += 1;
        self.by_key.insert(self.last_key, VecDeque::new());
        self.last_key
    }

    fn observe(&mut self, key: u64, payload: &LazyValue<'_>) -> ControlFlow<()> {
        match json::from_str(payload.as_raw_str()) {
            Ok(event) => {
                if let Some(events) = self.by_key.get_mut(&key) {
                    events.push_back(event);
                }
            }
            Err(_) => self.malformed += 1,
        }
        Continue(())
    }
}

#[derive(Clone)]
pub struct EventSubscriptions {
    connection: Rc<Connection<EventBuffer>>,
}

impl EventSubscriptions {
    pub async fn connect(url: &Url) -> Result<Self> {
        let connection =
            Connection::connect(url, EventBuffer::default(), EventBuffer::observe).await?;
        Ok(Self { connection: Rc::new(connection) })
    }

    pub async fn slot_subscribe(&self) -> Result<u64> {
        self.subscribe(Topic::Slot, &NO_PARAMS).await
    }

    pub async fn account_subscribe(&self, account: &Pubkey) -> Result<u64> {
        self.subscribe(Topic::Account, &subscription::account_params(account)).await
    }

    pub async fn logs_subscribe_mentions(&self, account: &Pubkey) -> Result<u64> {
        let filter = RpcTransactionLogsFilter::Mentions(vec![account.to_string()]);
        let params = subscription::logs_params(filter);
        self.subscribe(Topic::Logs, &params).await
    }

    pub async fn program_subscribe(&self, program: &Pubkey) -> Result<u64> {
        self.subscribe(Topic::Program, &subscription::program_params(program)).await
    }

    pub async fn await_subscribed(&self, count: usize) -> Result<()> {
        self.connection.await_subscribed(count).await
    }

    pub async fn await_events(&self, key: u64, count: usize) -> Result<()> {
        self.connection
            .wait_for("notifications", |events| {
                events.by_key.get(&key).map_or(0, VecDeque::len) >= count
            })
            .await
    }

    pub fn events<T: DeserializeOwned>(&self, key: u64) -> Result<Vec<T>> {
        self.connection
            .state()
            .by_key
            .get(&key)
            .into_iter()
            .flatten()
            .map(|event| json::from_value(event).map_err(Into::into))
            .collect()
    }

    pub fn malformed_frames(&self) -> usize {
        self.connection.state().malformed
    }

    pub fn finalize(&self) {
        self.connection.stop();
    }

    async fn subscribe(&self, topic: Topic, params: &impl Serialize) -> Result<u64> {
        let key = self.connection.state_mut().register();
        self.connection.subscribe(topic, params, key).await?;
        Ok(key)
    }
}

pub struct Subscription<T> {
    events: EventSubscriptions,
    key: u64,
    topic: Topic,
    payload: PhantomData<T>,
}

impl<T: DeserializeOwned> Subscription<T> {
    pub async fn next(&mut self) -> Result<T> {
        self.events.await_events(self.key, 1).await?;
        let mut state = self.events.connection.state_mut();
        let queue = state.by_key.get_mut(&self.key).unwrap();
        let value = queue.pop_front().unwrap();
        Ok(json::from_value(&value)?)
    }

    pub async fn until(&mut self, matches: impl Fn(&T) -> bool) -> Result<T> {
        time::timeout(NETWORK_TIMEOUT, async {
            loop {
                let event = self.next().await?;
                if matches(&event) {
                    return Ok(event);
                }
            }
        })
        .await
        .with_context(|| format!("no matching notification within {NETWORK_TIMEOUT:?}"))?
    }

    /// Reevaluate a query on notifications, retaining the last unmet condition.
    pub async fn query<U>(
        &mut self,
        what: &str,
        observe: impl AsyncFnMut() -> Result<U>,
    ) -> Result<U> {
        Self::query_many(slice::from_mut(self), what, observe).await
    }

    pub(crate) async fn query_many<U>(
        subscriptions: &mut [Self],
        what: &str,
        mut observe: impl AsyncFnMut() -> Result<U>,
    ) -> Result<U> {
        let deadline = Instant::now() + NETWORK_TIMEOUT;
        loop {
            // The next snapshot covers updates already buffered before the query starts.
            for subscription in &mut *subscriptions {
                let connection = &subscription.events.connection;
                connection.check_error()?;
                connection.state_mut().by_key.get_mut(&subscription.key).unwrap().clear();
            }
            let error = match time::timeout_at(deadline, observe()).await {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(error)) => error,
                Err(error) => {
                    return Err(error).with_context(|| format!("{what} after {NETWORK_TIMEOUT:?}"))
                }
            };
            let changed = time::timeout_at(deadline, async {
                if let [subscription] = subscriptions {
                    subscription.next().await.map(|_| ())
                } else {
                    let mut changes: FuturesUnordered<_> =
                        subscriptions.iter_mut().map(|subscription| subscription.next()).collect();
                    changes.next().await.context("no accounts to await")?.map(|_| ())
                }
            })
            .await
            .unwrap_or_else(|cause| Err(cause.into()));
            changed.map_err(|cause| cause.context(error)).with_context(|| what.to_owned())?;
        }
    }
}

impl<T> Drop for Subscription<T> {
    fn drop(&mut self) {
        self.events.connection.state_mut().by_key.remove(&self.key);
        let connection = self.events.connection.clone();
        let key = self.key;
        let topic = self.topic;
        task::spawn_local(async move {
            let _ = connection.unsubscribe(key, topic).await;
        });
    }
}

impl EventSubscriptions {
    pub(crate) fn is_closed(&self) -> bool {
        self.connection.check_error().is_err()
    }

    async fn typed<T>(&self, topic: Topic, params: &impl Serialize) -> Result<Subscription<T>> {
        let key = self.connection.state_mut().register();
        let subscription = Subscription {
            events: self.clone(),
            key,
            topic,
            payload: PhantomData,
        };
        self.connection.subscribe(topic, params, key).await?;
        self.connection.await_ack(key).await?;
        Ok(subscription)
    }

    pub async fn account(&self, account: &Pubkey) -> Result<Subscription<Response<UiAccount>>> {
        self.typed(Topic::Account, &subscription::account_params(account)).await
    }

    pub async fn program(
        &self,
        program: &Pubkey,
    ) -> Result<Subscription<Response<RpcKeyedAccount>>> {
        self.typed(Topic::Program, &subscription::program_params(program)).await
    }

    pub async fn slots(&self) -> Result<Subscription<SlotInfo>> {
        self.typed(Topic::Slot, &NO_PARAMS).await
    }

    pub async fn logs(&self, account: &Pubkey) -> Result<Subscription<Response<RpcLogsResponse>>> {
        let filter = RpcTransactionLogsFilter::Mentions(vec![account.to_string()]);
        self.typed(Topic::Logs, &subscription::logs_params(filter)).await
    }

    pub async fn signature(
        &self,
        signature: &Signature,
        commitment: CommitmentConfig,
    ) -> Result<Subscription<Response<RpcSignatureResult>>> {
        let config = RpcSignatureSubscribeConfig {
            commitment: Some(commitment),
            ..RpcSignatureSubscribeConfig::default()
        };
        self.typed(Topic::Signature, &(signature.to_string(), config)).await
    }

    pub async fn await_event<T: DeserializeOwned>(
        &self,
        key: u64,
        matches: impl Fn(&T) -> bool,
    ) -> Result<T> {
        let mut cursor = 0;
        self.connection
            .wait_value("matching notification", |state| {
                if let Some(events) = state.by_key.get(&key) {
                    for event in events.iter().skip(cursor) {
                        let event = json::from_value(event)?;
                        if matches(&event) {
                            return Ok(Some(event));
                        }
                    }
                    cursor = events.len();
                }
                Ok(None)
            })
            .await
    }
}
