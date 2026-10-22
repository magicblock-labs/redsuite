use std::{collections::VecDeque, time::Duration};

use anyhow::Context;
use json::Value;
use pubkey::Pubkey;
use serde::{de::DeserializeOwned, Serialize};
use signature::Signature;
use solana_rpc_client_types::{config::RpcTransactionLogsFilter, request::RpcRequest};
use tokio::{time, time::Instant};
use url::Url;

use super::{
    rpc::NO_PARAMS,
    socket::{self, Frame, Notification, Socket},
    subscription::{self, Topic},
    NETWORK_TIMEOUT,
};
use crate::Result;

type RawNotification = (String, u64, Value);

pub struct RawWs {
    socket: Socket,
    next_id: u64,
    queue: VecDeque<RawNotification>,
    malformed: usize,
}

impl RawWs {
    pub async fn connect(url: &Url) -> Result<Self> {
        Ok(Self {
            socket: socket::connect(url).await?,
            next_id: 1,
            queue: VecDeque::new(),
            malformed: 0,
        })
    }

    async fn request<T: DeserializeOwned>(
        &mut self,
        method: RpcRequest,
        params: &impl Serialize,
    ) -> Result<T> {
        let id = self.next_id;
        self.next_id += 1;
        socket::send(&mut self.socket, id, method, params).await?;
        let deadline = Instant::now() + NETWORK_TIMEOUT;
        loop {
            let text = time::timeout_at(deadline, socket::receive(&mut self.socket))
                .await
                .with_context(|| format!("{method}: no reply within {NETWORK_TIMEOUT:?}"))?
                .with_context(|| method.to_string())?;
            match socket::parse(&text).with_context(|| method.to_string())? {
                Some(Frame::Reply { id: reply, result }) if reply == id => {
                    return Ok(json::from_str(result.as_raw_str())?);
                }
                // A cancelled request may leave a late reply on the socket.
                Some(Frame::Reply { .. }) | None => {}
                Some(Frame::Notification { method, params }) => {
                    if let Some(notification) = self.decode_notification(method, params) {
                        self.queue.push_back(notification);
                    }
                }
                Some(Frame::Malformed) => self.malformed += 1,
            }
        }
    }

    async fn subscribe(&mut self, topic: Topic, params: &impl Serialize) -> Result<u64> {
        self.request(topic.subscribe(), params).await
    }

    async fn unsubscribe(&mut self, topic: Topic, subscription: u64) -> Result<bool> {
        self.request(topic.unsubscribe(), &(subscription,)).await
    }

    pub async fn account_subscribe(&mut self, account: &Pubkey) -> Result<u64> {
        self.subscribe(Topic::Account, &subscription::account_params(account)).await
    }

    pub async fn account_unsubscribe(&mut self, subscription: u64) -> Result<bool> {
        self.unsubscribe(Topic::Account, subscription).await
    }

    pub async fn logs_subscribe_all(&mut self) -> Result<u64> {
        let params = subscription::logs_params(RpcTransactionLogsFilter::All);
        self.subscribe(Topic::Logs, &params).await
    }

    pub async fn logs_subscribe_mentions(&mut self, account: &Pubkey) -> Result<u64> {
        let filter = RpcTransactionLogsFilter::Mentions(vec![account.to_string()]);
        let params = subscription::logs_params(filter);
        self.subscribe(Topic::Logs, &params).await
    }

    pub async fn logs_unsubscribe(&mut self, subscription: u64) -> Result<bool> {
        self.unsubscribe(Topic::Logs, subscription).await
    }

    pub async fn program_subscribe(&mut self, program: &Pubkey) -> Result<u64> {
        self.subscribe(Topic::Program, &subscription::program_params(program)).await
    }

    pub async fn program_unsubscribe(&mut self, subscription: u64) -> Result<bool> {
        self.unsubscribe(Topic::Program, subscription).await
    }

    pub async fn slot_subscribe(&mut self) -> Result<u64> {
        self.subscribe(Topic::Slot, &NO_PARAMS).await
    }

    pub async fn slot_unsubscribe(&mut self, subscription: u64) -> Result<bool> {
        self.unsubscribe(Topic::Slot, subscription).await
    }

    pub async fn signature_subscribe(&mut self, signature: &Signature) -> Result<u64> {
        self.subscribe(Topic::Signature, &subscription::signature_params(signature))
            .await
    }

    pub async fn signature_unsubscribe(&mut self, subscription: u64) -> Result<bool> {
        self.unsubscribe(Topic::Signature, subscription).await
    }

    pub async fn next_notification(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<RawNotification>> {
        if let Some(notification) = self.queue.pop_front() {
            return Ok(Some(notification));
        }
        let deadline = Instant::now() + timeout;
        loop {
            let text = match time::timeout_at(deadline, socket::receive(&mut self.socket)).await {
                Ok(text) => text.context("ws notification")?,
                Err(_) => return Ok(None),
            };
            match socket::parse(&text).context("ws notification")? {
                Some(Frame::Notification { method, params }) => {
                    if let Some(notification) = self.decode_notification(method, params) {
                        return Ok(Some(notification));
                    }
                }
                Some(Frame::Reply { .. }) | None => {}
                Some(Frame::Malformed) => self.malformed += 1,
            }
        }
    }

    fn decode_notification(
        &mut self,
        method: &str,
        params: Notification<'_>,
    ) -> Option<RawNotification> {
        match json::from_str(params.result.as_raw_str()) {
            Ok(payload) => Some((method.to_owned(), params.subscription, payload)),
            Err(_) => {
                self.malformed += 1;
                None
            }
        }
    }

    pub fn malformed_frames(&self) -> usize {
        self.malformed
    }

    pub async fn close(mut self) -> Result<()> {
        time::timeout(NETWORK_TIMEOUT, self.socket.close(None))
            .await
            .context("ws close timed out")?
            .context("ws close")?;
        Ok(())
    }
}
