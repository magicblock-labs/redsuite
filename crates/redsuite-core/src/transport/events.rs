use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    ops::ControlFlow::{self, Break, Continue},
    rc::Rc,
};

use anyhow::{ensure, Context};
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
    http::TransportError,
    rpc::{RpcError, NO_PARAMS},
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
    type Key = (u64, Topic);

    fn on_malformed(&mut self) {
        self.malformed += 1;
    }
}

impl EventBuffer {
    fn pop<T: DeserializeOwned>(&mut self, key: u64) -> Result<Option<T>> {
        ensure!(
            self.malformed == 0,
            "malformed subscription payloads: {}",
            self.malformed
        );
        self.by_key
            .get_mut(&key)
            .and_then(VecDeque::pop_front)
            .map(|value| json::from_value(&value).map_err(Into::into))
            .transpose()
    }

    fn register(&mut self) -> u64 {
        self.last_key += 1;
        self.by_key.insert(self.last_key, VecDeque::new());
        self.last_key
    }

    fn observe(&mut self, (key, topic): (u64, Topic), payload: &LazyValue<'_>) -> ControlFlow<()> {
        match json::from_str(payload.as_raw_str()) {
            Ok(event) => {
                if let Some(events) = self.by_key.get_mut(&key) {
                    events.push_back(event);
                }
            }
            Err(_) => self.malformed += 1,
        }
        // Signature subscriptions are removed by the server after confirmation.
        if matches!(topic, Topic::Signature) {
            Break(())
        } else {
            Continue(())
        }
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
            .wait_value("notifications", |events| {
                ensure!(
                    events.malformed == 0,
                    "malformed subscription payloads: {}",
                    events.malformed
                );
                Ok((events.by_key.get(&key).map_or(0, VecDeque::len) >= count).then_some(()))
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
        self.connection.subscribe(topic, params, (key, topic)).await?;
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
    /// Consume one update from subscriptions on the same connection.
    pub(crate) async fn next_from(subscriptions: &mut [Self]) -> Result<(usize, T)> {
        let connection = subscriptions
            .first()
            .context("no subscriptions to observe")?
            .events
            .connection
            .clone();
        connection
            .wait("notifications", || {
                let mut events = connection.state_mut();
                for (index, subscription) in subscriptions.iter().enumerate() {
                    if let Some(event) = events.pop(subscription.key)? {
                        return Ok(Some((index, event)));
                    }
                }
                Ok(None)
            })
            .await
    }

    pub fn try_next(&mut self) -> Result<Option<T>> {
        self.events.connection.check_error()?;
        self.events.connection.state_mut().pop(self.key)
    }

    pub async fn next(&mut self) -> Result<T> {
        self.events
            .connection
            .wait("notifications", || {
                self.events.connection.state_mut().pop(self.key)
            })
            .await
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
}

impl Subscription<Response<RpcLogsResponse>> {
    pub async fn for_signature(&mut self, signature: &Signature) -> Result<RpcLogsResponse> {
        let signature = signature.to_string();
        Ok(self.until(|event| event.value.signature == signature).await?.value)
    }
}

impl Subscription<SlotInfo> {
    /// Query information absent from notifications after each observed slot.
    pub async fn query<U>(
        &mut self,
        what: &str,
        mut observe: impl AsyncFnMut() -> Result<U>,
    ) -> Result<U> {
        let deadline = Instant::now() + NETWORK_TIMEOUT;
        loop {
            let error = match time::timeout_at(deadline, observe()).await {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(error)) => error,
                Err(error) => return Err(error).with_context(|| what.to_owned()),
            };
            if error.downcast_ref::<TransportError>().is_some()
                || error.downcast_ref::<RpcError>().is_some()
            {
                return Err(error).with_context(|| what.to_owned());
            }
            time::timeout_at(deadline, self.next())
                .await
                .with_context(|| format!("{what}: {error:#}"))??;
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
            let _ = connection.unsubscribe((key, topic), topic).await;
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
        self.connection.subscribe(topic, params, (key, topic)).await?;
        self.connection.await_ack((key, topic)).await?;
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
