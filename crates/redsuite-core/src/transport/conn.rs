use std::{cell::Cell, fmt, time::Duration};

use futures_util::{
    stream::SplitSink, stream::SplitStream, SinkExt, Stream, StreamExt,
};
use json::{Deserialize, LazyValue};
use pubkey::Pubkey;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, Message, Utf8Bytes},
    MaybeTlsStream, WebSocketStream,
};

use crate::Result;

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(super) fn check_stream(error: &Option<String>) -> Result<()> {
    match error {
        Some(error) => Err(format!("ws stream: {error}").into()),
        None => Ok(()),
    }
}

pub(super) async fn await_condition(
    timeout: Duration,
    what: &str,
    mut condition: impl FnMut() -> Result<bool>,
) -> Result<()> {
    crate::check::poll_until(timeout, Duration::from_millis(20), async || {
        condition().map(|ready| ready.then_some(()))
    })
    .await?
    .ok_or_else(|| format!("timed out waiting for {what} ({timeout:?})").into())
}

#[derive(Debug, Clone)]
pub enum CloseReason {
    ServerClose,
    StreamEnded,
    Transport(String),
    ServerError(String),
}

impl fmt::Display for CloseReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloseReason::ServerClose => {
                write!(formatter, "connection closed by server")
            }
            CloseReason::StreamEnded => write!(formatter, "stream ended"),
            CloseReason::Transport(error) => {
                write!(formatter, "transport error: {error}")
            }
            CloseReason::ServerError(error) => {
                write!(formatter, "server error: {error}")
            }
        }
    }
}

#[derive(Deserialize)]
struct Envelope<'a> {
    method: Option<&'a str>,
    id: Option<u64>,
    #[serde(borrow)]
    result: Option<LazyValue<'a>>,
    #[serde(borrow)]
    error: Option<LazyValue<'a>>,
    #[serde(borrow)]
    params: Option<EnvelopeParams<'a>>,
}

#[derive(Deserialize)]
struct EnvelopeParams<'a> {
    subscription: u64,
    #[serde(borrow)]
    result: LazyValue<'a>,
}

enum Frame<'a> {
    Reply {
        id: u64,
        result: Option<LazyValue<'a>>,
    },
    Notification {
        method: &'a str,
        subscription: u64,
        payload: LazyValue<'a>,
    },
    ServerError(String),
    Malformed,
    Ignored,
}

fn classify(text: &str) -> Frame<'_> {
    let Ok(envelope) = json::from_str::<Envelope>(text) else {
        return Frame::Malformed;
    };
    if let Some(error) = envelope.error {
        return Frame::ServerError(error.as_raw_str().to_owned());
    }
    match (envelope.method, envelope.params, envelope.id) {
        (Some(method), Some(params), _) => Frame::Notification {
            method,
            subscription: params.subscription,
            payload: params.result,
        },
        (None, _, Some(id)) => Frame::Reply {
            id,
            result: envelope.result,
        },
        _ => Frame::Ignored,
    }
}

enum Received {
    Text(Utf8Bytes),
    Skip,
    Closed(CloseReason),
}

async fn receive<S>(stream: &mut S) -> Received
where
    S: Stream<Item = std::result::Result<Message, tungstenite::Error>> + Unpin,
{
    let Some(incoming) = stream.next().await else {
        return Received::Closed(CloseReason::StreamEnded);
    };
    match incoming {
        Ok(Message::Text(text)) => Received::Text(text),
        Ok(Message::Close(_)) => Received::Closed(CloseReason::ServerClose),
        Ok(_) => Received::Skip,
        Err(error) => {
            Received::Closed(CloseReason::Transport(error.to_string()))
        }
    }
}

pub trait FrameHandler {
    fn on_reply(&mut self, id: u64, result: Option<&LazyValue<'_>>);

    fn on_notification(
        &mut self,
        method: &str,
        subscription: u64,
        payload: &LazyValue<'_>,
    );

    fn on_malformed(&mut self, text: &str) {
        let _ = text;
    }

    fn on_closed(&mut self, reason: &CloseReason) {
        let _ = reason;
    }
}

pub async fn drive<S, H>(stream: &mut S, handler: &mut H) -> CloseReason
where
    S: Stream<Item = std::result::Result<Message, tungstenite::Error>> + Unpin,
    H: FrameHandler,
{
    loop {
        let text = match receive(stream).await {
            Received::Text(text) => text,
            Received::Skip => continue,
            Received::Closed(reason) => {
                handler.on_closed(&reason);
                return reason;
            }
        };
        match classify(&text) {
            Frame::Reply { id, result } => {
                handler.on_reply(id, result.as_ref())
            }
            Frame::Notification {
                method,
                subscription,
                payload,
            } => handler.on_notification(method, subscription, &payload),
            Frame::ServerError(error) => {
                let reason = CloseReason::ServerError(error);
                handler.on_closed(&reason);
                return reason;
            }
            Frame::Malformed => handler.on_malformed(&text),
            Frame::Ignored => {}
        }
    }
}

pub enum RawEvent {
    Reply {
        id: u64,
        result: Option<json::Value>,
    },
    Notification {
        method: String,
        subscription: u64,
        payload: json::Value,
    },
    Malformed,
}

pub async fn next_event<S>(
    stream: &mut S,
) -> std::result::Result<RawEvent, CloseReason>
where
    S: Stream<Item = std::result::Result<Message, tungstenite::Error>> + Unpin,
{
    loop {
        let text = match receive(stream).await {
            Received::Text(text) => text,
            Received::Skip => continue,
            Received::Closed(reason) => return Err(reason),
        };
        return Ok(match classify(&text) {
            Frame::Reply { id, result } => RawEvent::Reply {
                id,
                result: result
                    .and_then(|value| json::from_str(value.as_raw_str()).ok()),
            },
            Frame::Notification {
                method,
                subscription,
                payload,
            } => match json::from_str(payload.as_raw_str()) {
                Ok(payload) => RawEvent::Notification {
                    method: method.to_owned(),
                    subscription,
                    payload,
                },
                Err(_) => RawEvent::Malformed,
            },
            Frame::ServerError(error) => {
                return Err(CloseReason::ServerError(error))
            }
            Frame::Malformed => RawEvent::Malformed,
            Frame::Ignored => continue,
        });
    }
}

pub fn reply_u64(result: Option<&LazyValue<'_>>) -> Option<u64> {
    result.and_then(|value| json::from_str(value.as_raw_str()).ok())
}

pub async fn connect(url: &str) -> Result<Socket> {
    let (socket, _) = connect_async(url)
        .await
        .map_err(|error| format!("{url}: {error}"))?;
    Ok(socket)
}

pub fn split(socket: Socket) -> (Requester, SplitStream<Socket>) {
    let (sink, stream) = socket.split();
    (Requester::new(sink), stream)
}

pub fn request_text(id: u64, method: &str, params: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"{method}","params":{params}}}"#
    )
}

pub fn account_params(account: &Pubkey) -> String {
    format!(r#"["{account}",{{"encoding":"base64","commitment":"confirmed"}}]"#)
}

pub fn logs_all_params() -> &'static str {
    r#"["all",{"commitment":"confirmed"}]"#
}

pub fn logs_mentions_params(account: &Pubkey) -> String {
    format!(r#"[{{"mentions":["{account}"]}},{{"commitment":"confirmed"}}]"#)
}

pub use account_params as program_params;

pub fn signature_params(signature: &str) -> String {
    format!(r#"["{signature}",{{"commitment":"confirmed"}}]"#)
}

pub struct Requester {
    // tokio Mutex, not RefCell: subscribes come from concurrent tasks and
    // hold the sink across an await
    sink: tokio::sync::Mutex<SplitSink<Socket, Message>>,
    next_req_id: Cell<u64>,
}

impl Requester {
    pub fn new(sink: SplitSink<Socket, Message>) -> Self {
        Self {
            sink: tokio::sync::Mutex::new(sink),
            next_req_id: Cell::new(1),
        }
    }

    pub fn mint(&self) -> u64 {
        self.next_req_id.replace(self.next_req_id.get() + 1)
    }

    pub async fn send(
        &self,
        id: u64,
        method: &str,
        params: &str,
    ) -> Result<()> {
        self.sink
            .lock()
            .await
            .send(Message::Text(request_text(id, method, params).into()))
            .await
            .map_err(|error| format!("{method} send: {error}").into())
    }
}

pub struct Reader {
    handle: tokio::task::JoinHandle<CloseReason>,
}

impl Reader {
    pub fn spawn<H>(mut stream: SplitStream<Socket>, mut handler: H) -> Self
    where
        H: FrameHandler + 'static,
    {
        let handle = tokio::task::spawn_local(async move {
            drive(&mut stream, &mut handler).await
        });
        Self { handle }
    }

    pub fn stop(&self) {
        self.handle.abort();
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
