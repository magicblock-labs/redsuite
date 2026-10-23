use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    collections::HashMap,
    ops::ControlFlow,
    rc::Rc,
};

use anyhow::{anyhow, ensure};
use futures_util::{stream::SplitSink, StreamExt};
use json::LazyValue;
use pubkey::Pubkey;
use serde::Serialize;
use signature::Signature;
use solana_account_decoder_client_types::UiAccount;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client_types::{
    config::{
        RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcSignatureSubscribeConfig,
        RpcTransactionLogsConfig, RpcTransactionLogsFilter,
    },
    request::RpcRequest,
    response::Response,
};
use tokio::{
    sync::{Mutex, Notify},
    task,
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;
use url::Url;

use super::{
    rpc::account_config,
    socket::{self, CloseReason, Frame, Socket},
    NETWORK_TIMEOUT,
};
use crate::{wait, Result};

// The upstream RpcRequest enum covers HTTP methods only.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum Topic {
    Account,
    Logs,
    Program,
    Signature,
    Slot,
}

impl Topic {
    fn methods(self) -> (&'static str, &'static str) {
        match self {
            Self::Account => ("accountSubscribe", "accountUnsubscribe"),
            Self::Logs => ("logsSubscribe", "logsUnsubscribe"),
            Self::Program => ("programSubscribe", "programUnsubscribe"),
            Self::Signature => ("signatureSubscribe", "signatureUnsubscribe"),
            Self::Slot => ("slotSubscribe", "slotUnsubscribe"),
        }
    }

    pub(super) fn subscribe(self) -> RpcRequest {
        RpcRequest::Custom { method: self.methods().0 }
    }

    pub(super) fn unsubscribe(self) -> RpcRequest {
        RpcRequest::Custom { method: self.methods().1 }
    }
}

pub(super) trait SubscriptionState {
    type Key: Copy;

    fn on_subscribed(&mut self, _count: usize) {}

    fn on_malformed(&mut self) {}

    fn on_close(&mut self, _reason: &CloseReason) {}
}

pub(super) struct Router<S: SubscriptionState> {
    pub(super) state: S,
    requests: HashMap<u64, S::Key>,
    subscriptions: HashMap<u64, S::Key>,
    subscribed: usize,
    error: Option<String>,
    changed: Rc<Notify>,
}

impl<S: SubscriptionState> Router<S> {
    pub(super) fn new(state: S) -> Self {
        Self {
            state,
            requests: HashMap::new(),
            subscriptions: HashMap::new(),
            subscribed: 0,
            error: None,
            changed: Rc::new(Notify::new()),
        }
    }

    pub(super) fn register(&mut self, request: u64, key: S::Key) {
        self.requests.insert(request, key);
    }

    pub(super) fn dispatch(
        &mut self,
        frame: Frame<'_>,
        on_notification: &mut impl FnMut(&mut S, S::Key, &LazyValue<'_>) -> ControlFlow<()>,
    ) -> Result<()> {
        match frame {
            Frame::Reply { id, result } => {
                if let Some(key) = self.requests.remove(&id) {
                    let subscription = json::from_str(result.as_raw_str())?;
                    self.subscriptions.insert(subscription, key);
                    self.subscribed += 1;
                    self.state.on_subscribed(self.subscribed);
                }
            }
            Frame::Notification { params, .. } => {
                let Some(&key) = self.subscriptions.get(&params.subscription) else {
                    return Ok(());
                };
                if on_notification(&mut self.state, key, &params.result).is_break() {
                    self.subscriptions.remove(&params.subscription);
                }
            }
            Frame::Malformed => self.state.on_malformed(),
        }
        self.changed.notify_waiters();
        Ok(())
    }

    pub(super) fn close(&mut self, reason: &CloseReason) {
        self.error = Some(reason.to_string());
        self.state.on_close(reason);
        self.changed.notify_waiters();
    }

    fn check_error(&self) -> Result<()> {
        match &self.error {
            Some(error) => Err(anyhow!("ws stream: {error}")),
            None => Ok(()),
        }
    }
}

pub(super) struct Connection<S: SubscriptionState> {
    sink: Mutex<SplitSink<Socket, Message>>,
    next_id: Cell<u64>,
    router: Rc<RefCell<Router<S>>>,
    reader: JoinHandle<()>,
}

impl<S: SubscriptionState + 'static> Connection<S> {
    /// Return `Break(())` from the callback to remove a completed one-shot subscription.
    pub(super) async fn connect(
        url: &Url,
        state: S,
        mut on_notification: impl FnMut(&mut S, S::Key, &LazyValue<'_>) -> ControlFlow<()> + 'static,
    ) -> Result<Self> {
        let (sink, mut stream) = socket::connect(url).await?.split();
        let router = Rc::new(RefCell::new(Router::new(state)));
        let reader_state = router.clone();
        let reader = task::spawn_local(async move {
            let reason = socket::drive(&mut stream, |frame| {
                reader_state.borrow_mut().dispatch(frame, &mut on_notification)
            })
            .await;
            reader_state.borrow_mut().close(&reason);
        });
        Ok(Self {
            sink: Mutex::new(sink),
            next_id: Cell::new(1),
            router,
            reader,
        })
    }

    pub(super) async fn subscribe(
        &self,
        topic: Topic,
        params: &impl Serialize,
        key: S::Key,
    ) -> Result<()> {
        let id = self.next_id.replace(self.next_id.get() + 1);
        // Register before sending: the reader may see an acknowledgment as soon as send yields.
        self.router.borrow_mut().register(id, key);
        let mut sink = self.sink.lock().await;
        socket::send(&mut *sink, id, topic.subscribe(), params).await
    }

    pub(super) fn state(&self) -> Ref<'_, S> {
        Ref::map(self.router.borrow(), |router| &router.state)
    }

    pub(super) fn state_mut(&self) -> RefMut<'_, S> {
        RefMut::map(self.router.borrow_mut(), |router| &mut router.state)
    }

    pub(super) fn check_error(&self) -> Result<()> {
        self.router.borrow().check_error()?;
        ensure!(!self.reader.is_finished(), "websocket reader stopped");
        Ok(())
    }

    pub(super) async fn await_subscribed(&self, count: usize) -> Result<()> {
        self.wait("subscription acks", |router| {
            Ok((router.subscribed >= count).then_some(()))
        })
        .await
    }

    pub(super) async fn wait_for(
        &self,
        what: &str,
        mut done: impl FnMut(&S) -> bool,
    ) -> Result<()> {
        self.wait_value(what, |state| Ok(done(state).then_some(()))).await
    }

    pub(super) async fn wait_value<T>(
        &self,
        what: &str,
        mut observe: impl FnMut(&S) -> Result<Option<T>>,
    ) -> Result<T> {
        self.wait(what, |router| observe(&router.state)).await
    }

    async fn wait<T>(
        &self,
        what: &str,
        mut observe: impl FnMut(&Router<S>) -> Result<Option<T>>,
    ) -> Result<T> {
        let changed = self.router.borrow().changed.clone();
        wait::on_change(&changed, NETWORK_TIMEOUT, what, || {
            self.check_error()?;
            observe(&self.router.borrow())
        })
        .await
    }

    pub(super) async fn await_ack(&self, key: S::Key) -> Result<()>
    where
        S::Key: PartialEq,
    {
        // A one-shot route can complete before the task awaiting its acknowledgment resumes.
        self.wait("subscription acknowledgment", |router| {
            Ok((!router.requests.values().any(|pending| *pending == key)).then_some(()))
        })
        .await
    }

    pub(super) async fn unsubscribe(&self, key: S::Key, topic: Topic) -> Result<()>
    where
        S::Key: PartialEq,
    {
        if let Err(error) = self.await_ack(key).await {
            self.router.borrow_mut().requests.retain(|_, pending| *pending != key);
            return Err(error);
        }
        let subscription = self
            .router
            .borrow()
            .subscriptions
            .iter()
            .find_map(|(&id, active)| (*active == key).then_some(id));
        let Some(subscription) = subscription else {
            return Ok(());
        };
        self.router.borrow_mut().subscriptions.remove(&subscription);
        let id = self.next_id.replace(self.next_id.get() + 1);
        let mut sink = self.sink.lock().await;
        socket::send(&mut *sink, id, topic.unsubscribe(), &(subscription,)).await
    }

    pub(super) fn stop(&self) {
        self.reader.abort();
        self.router.borrow_mut().close(&CloseReason::StreamEnded);
    }
}

impl<S: SubscriptionState> Drop for Connection<S> {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

pub(super) fn account_data(payload: &LazyValue<'_>) -> Option<Vec<u8>> {
    let payload: Response<UiAccount> = json::from_str(payload.as_raw_str()).ok()?;
    payload.value.data.decode()
}

pub(super) fn account_params(account: &Pubkey) -> (String, RpcAccountInfoConfig) {
    (account.to_string(), account_config())
}

pub(super) fn program_params(program: &Pubkey) -> (String, RpcProgramAccountsConfig) {
    (
        program.to_string(),
        RpcProgramAccountsConfig {
            account_config: account_config(),
            ..RpcProgramAccountsConfig::default()
        },
    )
}

pub(super) fn logs_params(
    filter: RpcTransactionLogsFilter,
) -> (RpcTransactionLogsFilter, RpcTransactionLogsConfig) {
    (
        filter,
        RpcTransactionLogsConfig {
            commitment: Some(CommitmentConfig::confirmed()),
        },
    )
}

pub(super) fn signature_params(signature: &Signature) -> (String, RpcSignatureSubscribeConfig) {
    (
        signature.to_string(),
        RpcSignatureSubscribeConfig {
            commitment: Some(CommitmentConfig::confirmed()),
            ..RpcSignatureSubscribeConfig::default()
        },
    )
}
