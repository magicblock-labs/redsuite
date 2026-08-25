use std::{collections::VecDeque, time::Duration};

use futures_util::SinkExt;
use json::{JsonValueTrait, LazyValue};
use pubkey::Pubkey;
use tokio_tungstenite::tungstenite::Message;

use super::conn::{self, CloseReason, Flow, FrameHandler, Socket};
use crate::Result;

#[derive(Default)]
struct RawState {
    want_reply: Option<u64>,
    reply: Option<Option<json::Value>>,
    queue: VecDeque<(String, u64, json::Value)>,
    malformed: usize,
}

impl FrameHandler for RawState {
    fn on_reply(&mut self, id: u64, result: Option<&LazyValue<'_>>) -> Flow {
        if self.want_reply != Some(id) {
            return Flow::Continue;
        }
        self.want_reply = None;
        self.reply = Some(
            result.and_then(|value| json::from_str(value.as_raw_str()).ok()),
        );
        Flow::Stop
    }

    fn on_notification(
        &mut self,
        method: &str,
        subscription: u64,
        payload: &LazyValue<'_>,
    ) -> Flow {
        let Ok(value) = json::from_str::<json::Value>(payload.as_raw_str())
        else {
            self.malformed += 1;
            return Flow::Continue;
        };
        self.queue
            .push_back((method.to_owned(), subscription, value));
        if self.want_reply.is_none() {
            Flow::Stop
        } else {
            Flow::Continue
        }
    }

    fn on_malformed(&mut self, _text: &str) -> Flow {
        self.malformed += 1;
        Flow::Continue
    }
}

pub struct RawWs {
    socket: Socket,
    next_req_id: u64,
    state: RawState,
}

impl RawWs {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            socket: conn::connect(url).await?,
            next_req_id: 1,
            state: RawState::default(),
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
        self.state.want_reply = Some(req_id);
        self.state.reply = None;
        match conn::drive(&mut self.socket, &mut self.state).await {
            None => match self.state.reply.take() {
                Some(Some(result)) => Ok(result),
                Some(None) => {
                    Err(format!("{method}: reply had no result").into())
                }
                None => {
                    Err(format!("{method}: reader stopped without a reply")
                        .into())
                }
            },
            Some(CloseReason::ServerError(error)) => {
                Err(format!("{method}: {error}").into())
            }
            Some(reason) => Err(format!("{method}: {reason}").into()),
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
        if let Some(notification) = self.state.queue.pop_front() {
            return Ok(Some(notification));
        }
        let drained = tokio::time::timeout(
            timeout,
            conn::drive(&mut self.socket, &mut self.state),
        )
        .await;
        match drained {
            Err(_) => Ok(None),
            Ok(None) => Ok(self.state.queue.pop_front()),
            Ok(Some(reason)) => Err(format!("ws: {reason}").into()),
        }
    }

    pub fn malformed_frames(&self) -> usize {
        self.state.malformed
    }

    pub async fn close(mut self) -> Result<()> {
        self.socket
            .close(None)
            .await
            .map_err(|error| format!("ws close: {error}"))?;
        Ok(())
    }
}
