use std::{collections::VecDeque, time::Duration};

use futures_util::SinkExt;
use json::JsonValueTrait;
use pubkey::Pubkey;
use tokio_tungstenite::tungstenite::Message;

use super::conn::{self, CloseReason, RawEvent, Socket};
use crate::Result;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub struct RawWs {
    socket: Socket,
    next_req_id: u64,
    queue: VecDeque<(String, u64, json::Value)>,
    malformed: usize,
}

impl RawWs {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            socket: conn::connect(url).await?,
            next_req_id: 1,
            queue: VecDeque::new(),
            malformed: 0,
        })
    }

    async fn request(
        &mut self,
        method: &str,
        params: &str,
    ) -> Result<json::Value> {
        let req_id = self.next_req_id;
        self.next_req_id += 1;
        self.socket
            .send(Message::Text(
                conn::request_text(req_id, method, params).into(),
            ))
            .await
            .map_err(|error| format!("{method} send: {error}"))?;
        let deadline = tokio::time::Instant::now() + REQUEST_TIMEOUT;
        loop {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or(Duration::ZERO);
            let event = tokio::time::timeout(
                remaining,
                conn::next_event(&mut self.socket),
            )
            .await
            .map_err(|_| {
                format!("{method}: no reply within {REQUEST_TIMEOUT:?}")
            })?;
            match event {
                Ok(RawEvent::Reply { id, result }) if id == req_id => {
                    return result.ok_or_else(|| {
                        format!("{method}: reply had no result").into()
                    });
                }
                Ok(RawEvent::Reply { .. }) => {}
                Ok(RawEvent::Notification {
                    method: notified,
                    subscription,
                    payload,
                }) => self.queue.push_back((notified, subscription, payload)),
                Ok(RawEvent::Malformed) => self.malformed += 1,
                Err(CloseReason::ServerError(error)) => {
                    return Err(format!("{method}: {error}").into());
                }
                Err(reason) => {
                    return Err(format!("{method}: {reason}").into());
                }
            }
        }
    }

    async fn subscribe(&mut self, method: &str, params: &str) -> Result<u64> {
        let result = self.request(method, params).await?;
        result.as_u64().ok_or_else(|| {
            format!("non-numeric subscription id: {result:?}").into()
        })
    }

    async fn unsubscribe(&mut self, method: &str, subid: u64) -> Result<bool> {
        let result = self.request(method, &format!("[{subid}]")).await?;
        result.as_bool().ok_or_else(|| {
            format!("non-boolean unsubscribe reply: {result:?}").into()
        })
    }

    pub async fn account_subscribe(&mut self, account: &Pubkey) -> Result<u64> {
        self.subscribe("accountSubscribe", &conn::account_params(account))
            .await
    }

    pub async fn account_unsubscribe(&mut self, subid: u64) -> Result<bool> {
        self.unsubscribe("accountUnsubscribe", subid).await
    }

    pub async fn logs_subscribe_all(&mut self) -> Result<u64> {
        self.subscribe("logsSubscribe", conn::logs_all_params())
            .await
    }

    pub async fn logs_subscribe_mentions(
        &mut self,
        account: &Pubkey,
    ) -> Result<u64> {
        self.subscribe("logsSubscribe", &conn::logs_mentions_params(account))
            .await
    }

    pub async fn logs_unsubscribe(&mut self, subid: u64) -> Result<bool> {
        self.unsubscribe("logsUnsubscribe", subid).await
    }

    pub async fn program_subscribe(&mut self, program: &Pubkey) -> Result<u64> {
        self.subscribe("programSubscribe", &conn::program_params(program))
            .await
    }

    pub async fn program_unsubscribe(&mut self, subid: u64) -> Result<bool> {
        self.unsubscribe("programUnsubscribe", subid).await
    }

    pub async fn slot_subscribe(&mut self) -> Result<u64> {
        self.subscribe("slotSubscribe", "[]").await
    }

    pub async fn slot_unsubscribe(&mut self, subid: u64) -> Result<bool> {
        self.unsubscribe("slotUnsubscribe", subid).await
    }

    pub async fn signature_subscribe(
        &mut self,
        signature: &str,
    ) -> Result<u64> {
        self.subscribe("signatureSubscribe", &conn::signature_params(signature))
            .await
    }

    pub async fn signature_unsubscribe(&mut self, subid: u64) -> Result<bool> {
        self.unsubscribe("signatureUnsubscribe", subid).await
    }

    pub async fn next_notification(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<(String, u64, json::Value)>> {
        if let Some(notification) = self.queue.pop_front() {
            return Ok(Some(notification));
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or(Duration::ZERO);
            let event = tokio::time::timeout(
                remaining,
                conn::next_event(&mut self.socket),
            )
            .await;
            match event {
                Err(_) => return Ok(None),
                Ok(Ok(RawEvent::Notification {
                    method,
                    subscription,
                    payload,
                })) => return Ok(Some((method, subscription, payload))),
                Ok(Ok(RawEvent::Reply { .. })) => {}
                Ok(Ok(RawEvent::Malformed)) => self.malformed += 1,
                Ok(Err(reason)) => return Err(format!("ws: {reason}").into()),
            }
        }
    }

    pub fn malformed_frames(&self) -> usize {
        self.malformed
    }

    pub async fn close(mut self) -> Result<()> {
        self.socket
            .close(None)
            .await
            .map_err(|error| format!("ws close: {error}"))?;
        Ok(())
    }
}
