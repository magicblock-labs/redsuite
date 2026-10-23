use std::result;

use anyhow::Context;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use json::{Deserialize, LazyValue};
use serde::Serialize;
use solana_rpc_client_types::request::RpcRequest;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WsError, Message, Utf8Bytes},
    MaybeTlsStream, WebSocketStream,
};
use url::Url;

use super::{rpc, NETWORK_TIMEOUT};
use crate::Result;

pub(super) type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Error)]
pub(super) enum CloseReason {
    #[error("connection closed by server")]
    ServerClose,
    #[error("stream ended")]
    StreamEnded,
    #[error("transport error: {0}")]
    Transport(#[from] WsError),
    #[error("server error: {0:?}")]
    ServerError(#[from] jsonrpc_core::Error),
    #[error("invalid reply: {0:#}")]
    InvalidReply(#[source] anyhow::Error),
}

#[derive(Deserialize)]
struct Envelope<'a> {
    method: Option<&'a str>,
    id: Option<u64>,
    #[serde(borrow)]
    result: Option<LazyValue<'a>>,
    error: Option<jsonrpc_core::Error>,
    #[serde(borrow)]
    params: Option<Notification<'a>>,
}

#[derive(Deserialize)]
pub(super) struct Notification<'a> {
    pub subscription: u64,
    #[serde(borrow)]
    pub result: LazyValue<'a>,
}

pub(super) enum Frame<'a> {
    Reply { id: u64, result: LazyValue<'a> },
    Notification { method: &'a str, params: Notification<'a> },
    Malformed,
}

pub(super) fn parse(text: &str) -> result::Result<Option<Frame<'_>>, CloseReason> {
    let Ok(envelope) = json::from_str::<Envelope>(text) else {
        return Ok(Some(Frame::Malformed));
    };
    if let Some(error) = envelope.error {
        return Err(error.into());
    }
    let frame = match (
        envelope.method,
        envelope.params,
        envelope.id,
        envelope.result,
    ) {
        (Some(method), Some(params), _, _) => Frame::Notification { method, params },
        (None, _, Some(id), Some(result)) => Frame::Reply { id, result },
        _ => return Ok(None),
    };
    Ok(Some(frame))
}

pub(super) async fn connect(url: &Url) -> Result<Socket> {
    let (socket, _) = time::timeout(NETWORK_TIMEOUT, connect_async(url.as_str()))
        .await
        .context("websocket connect timed out")?
        .with_context(|| url.to_string())?;
    Ok(socket)
}

pub(super) async fn send<S>(
    sink: &mut S,
    id: u64,
    method: RpcRequest,
    params: &impl Serialize,
) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    let text = rpc::encode_request(id, method, params)?;
    time::timeout(NETWORK_TIMEOUT, sink.send(Message::Text(text.into())))
        .await
        .with_context(|| format!("{method} send timed out"))?
        .with_context(|| format!("{method} send"))
}

pub(super) async fn receive<S>(stream: &mut S) -> result::Result<Utf8Bytes, CloseReason>
where
    S: Stream<Item = result::Result<Message, WsError>> + Unpin,
{
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => return Ok(text),
            Some(Ok(Message::Close(_))) => return Err(CloseReason::ServerClose),
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error.into()),
            None => return Err(CloseReason::StreamEnded),
        }
    }
}

// Dispatch borrowed payloads so load observers do not allocate a JSON tree per update.
pub(super) async fn drive<S>(
    stream: &mut S,
    mut dispatch: impl FnMut(Frame<'_>) -> Result<()>,
) -> CloseReason
where
    S: Stream<Item = result::Result<Message, WsError>> + Unpin,
{
    loop {
        let text = match receive(stream).await {
            Ok(text) => text,
            Err(reason) => return reason,
        };
        match parse(&text) {
            Ok(Some(frame)) => {
                if let Err(error) = dispatch(frame) {
                    return CloseReason::InvalidReply(error);
                }
            }
            Ok(None) => {}
            Err(reason) => return reason,
        }
    }
}
