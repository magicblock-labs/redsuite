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

    pub async fn account_subscribe(&mut self, account: &Pubkey) -> Result<u64> {
        use json::JsonValueTrait;
        let params = format!(
            r#"["{account}",{{"encoding":"base64","commitment":"confirmed"}}]"#
        );
        let result = self.request("accountSubscribe", &params).await?;
        result.as_u64().ok_or_else(|| {
            format!("non-numeric subscription id: {result:?}").into()
        })
    }

    pub async fn account_unsubscribe(&mut self, subid: u64) -> Result<bool> {
        use json::JsonValueTrait;
        let result = self
            .request("accountUnsubscribe", &format!("[{subid}]"))
            .await?;
        result.as_bool().ok_or_else(|| {
            format!("non-boolean unsubscribe reply: {result:?}").into()
        })
    }

    pub async fn close(mut self) -> Result<()> {
        self.socket
            .close(None)
            .await
            .map_err(|e| format!("ws close: {e}"))?;
        Ok(())
    }
}
