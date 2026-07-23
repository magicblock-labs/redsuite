use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use json::Deserialize;
use pubkey::Pubkey;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};

use crate::Result;

#[derive(Deserialize)]
struct Reply {
    id: Option<u64>,
    result: Option<json::Value>,
    error: Option<json::Value>,
}

#[derive(Deserialize)]
struct Notification {
    method: Option<String>,
    params: Option<NotificationParams>,
}

#[derive(Deserialize)]
struct NotificationParams {
    subscription: u64,
    result: json::Value,
}

pub struct RawWs {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next_req_id: u64,
}

impl RawWs {
    pub async fn connect(url: &str) -> Result<Self> {
        let (socket, _) = connect_async(url)
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        Ok(Self {
            socket,
            next_req_id: 1,
        })
    }

    async fn request(
        &mut self,
        method: &str,
        params: &str,
    ) -> Result<json::Value> {
        let req_id = self.next_req_id;
        self.next_req_id += 1;
        let message = format!(
            r#"{{"jsonrpc":"2.0","id":{req_id},"method":"{method}","params":{params}}}"#
        );
        self.socket
            .send(Message::Text(message.into()))
            .await
            .map_err(|e| format!("{method} send: {e}"))?;
        loop {
            let incoming = self
                .socket
                .next()
                .await
                .ok_or_else(|| format!("{method}: stream ended"))?
                .map_err(|e| format!("{method}: {e}"))?;
            let Message::Text(text) = incoming else {
                continue;
            };
            let Ok(reply) = json::from_str::<Reply>(&text) else {
                continue;
            };
            if let Some(error) = reply.error {
                return Err(format!("{method}: {error}").into());
            }
            if reply.id == Some(req_id) {
                return reply.result.ok_or_else(|| {
                    format!("{method}: reply had no result").into()
                });
            }
        }
    }

    async fn subscribe(&mut self, method: &str, params: &str) -> Result<u64> {
        use json::JsonValueTrait;
        let result = self.request(method, params).await?;
        result.as_u64().ok_or_else(|| {
            format!("non-numeric subscription id: {result:?}").into()
        })
    }

    async fn unsubscribe(&mut self, method: &str, subid: u64) -> Result<bool> {
        use json::JsonValueTrait;
        let result = self.request(method, &format!("[{subid}]")).await?;
        result.as_bool().ok_or_else(|| {
            format!("non-boolean unsubscribe reply: {result:?}").into()
        })
    }

    pub async fn account_subscribe(&mut self, account: &Pubkey) -> Result<u64> {
        let params = format!(
            r#"["{account}",{{"encoding":"base64","commitment":"confirmed"}}]"#
        );
        self.subscribe("accountSubscribe", &params).await
    }

    pub async fn account_unsubscribe(&mut self, subid: u64) -> Result<bool> {
        self.unsubscribe("accountUnsubscribe", subid).await
    }

    pub async fn logs_subscribe_all(&mut self) -> Result<u64> {
        self.subscribe("logsSubscribe", r#"["all",{"commitment":"confirmed"}]"#)
            .await
    }

    pub async fn logs_subscribe_mentions(
        &mut self,
        account: &Pubkey,
    ) -> Result<u64> {
        let params = format!(
            r#"[{{"mentions":["{account}"]}},{{"commitment":"confirmed"}}]"#
        );
        self.subscribe("logsSubscribe", &params).await
    }

    pub async fn logs_unsubscribe(&mut self, subid: u64) -> Result<bool> {
        self.unsubscribe("logsUnsubscribe", subid).await
    }

    pub async fn program_subscribe(&mut self, program: &Pubkey) -> Result<u64> {
        let params = format!(
            r#"["{program}",{{"encoding":"base64","commitment":"confirmed"}}]"#
        );
        self.subscribe("programSubscribe", &params).await
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
        let params = format!(r#"["{signature}",{{"commitment":"confirmed"}}]"#);
        self.subscribe("signatureSubscribe", &params).await
    }

    pub async fn signature_unsubscribe(&mut self, subid: u64) -> Result<bool> {
        self.unsubscribe("signatureUnsubscribe", subid).await
    }

    pub async fn next_notification(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<(String, u64, json::Value)>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or(Duration::ZERO);
            let incoming =
                match tokio::time::timeout(remaining, self.socket.next()).await
                {
                    Err(_) => return Ok(None),
                    Ok(None) => return Err("ws stream ended".into()),
                    Ok(Some(msg)) => msg.map_err(|e| format!("ws: {e}"))?,
                };
            let Message::Text(text) = incoming else {
                continue;
            };
            let Ok(frame) = json::from_str::<Notification>(&text) else {
                continue;
            };
            if let (Some(method), Some(params)) = (frame.method, frame.params) {
                return Ok(Some((method, params.subscription, params.result)));
            }
        }
    }

    pub async fn close(mut self) -> Result<()> {
        self.socket
            .close(None)
            .await
            .map_err(|e| format!("ws close: {e}"))?;
        Ok(())
    }
}
