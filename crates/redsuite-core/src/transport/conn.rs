use std::{cell::Cell, fmt};

use futures_util::{
    stream::SplitSink, stream::SplitStream, SinkExt, Stream, StreamExt,
};
use json::{Deserialize, LazyValue};
use pubkey::Pubkey;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};

use crate::Result;

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

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

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Continue,
    Stop,
}

pub trait FrameHandler {
    fn on_reply(&mut self, id: u64, result: Option<&LazyValue<'_>>) -> Flow;

    fn on_notification(
        &mut self,
        method: &str,
        subscription: u64,
        payload: &LazyValue<'_>,
    ) -> Flow;

    fn on_malformed(&mut self, text: &str) -> Flow {
        let _ = text;
        Flow::Continue
    }

    fn on_closed(&mut self, reason: &CloseReason) {
        let _ = reason;
    }
}

pub async fn drive<S, H>(stream: &mut S, handler: &mut H) -> Option<CloseReason>
where
    S: Stream<Item = std::result::Result<Message, tungstenite::Error>> + Unpin,
    H: FrameHandler,
{
    loop {
        let Some(incoming) = stream.next().await else {
            return close(handler, CloseReason::StreamEnded);
        };
        let text = match incoming {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) => {
                return close(handler, CloseReason::ServerClose)
            }
            Ok(_) => continue,
            Err(error) => {
                return close(
                    handler,
                    CloseReason::Transport(error.to_string()),
                )
            }
        };
        let Ok(envelope) = json::from_str::<Envelope>(&text) else {
            match handler.on_malformed(&text) {
                Flow::Continue => continue,
                Flow::Stop => return None,
            }
        };
        if let Some(error) = envelope.error {
            let reason =
                CloseReason::ServerError(error.as_raw_str().to_owned());
            return close(handler, reason);
        }
        let flow = match (envelope.method, envelope.params, envelope.id) {
            (Some(method), Some(params), _) => handler.on_notification(
                method,
                params.subscription,
                &params.result,
            ),
            (None, _, Some(id)) => {
                handler.on_reply(id, envelope.result.as_ref())
            }
            _ => continue,
        };
        match flow {
            Flow::Continue => continue,
            Flow::Stop => return None,
        }
    }
}

fn close<H: FrameHandler>(
    handler: &mut H,
    reason: CloseReason,
) -> Option<CloseReason> {
    handler.on_closed(&reason);
    Some(reason)
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

pub fn program_params(program: &Pubkey) -> String {
    format!(r#"["{program}",{{"encoding":"base64","commitment":"confirmed"}}]"#)
}

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
    handle: tokio::task::JoinHandle<Option<CloseReason>>,
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

#[cfg(test)]
mod tests {
    use futures_util::stream;

    use super::*;

    #[derive(Default)]
    struct Recording {
        replies: Vec<(u64, Option<u64>)>,
        notifications: Vec<(String, u64, String)>,
        malformed: usize,
        closed: Option<String>,
        stop_on_notification: bool,
    }

    impl FrameHandler for Recording {
        fn on_reply(
            &mut self,
            id: u64,
            result: Option<&LazyValue<'_>>,
        ) -> Flow {
            self.replies.push((id, reply_u64(result)));
            Flow::Continue
        }

        fn on_notification(
            &mut self,
            method: &str,
            subscription: u64,
            payload: &LazyValue<'_>,
        ) -> Flow {
            self.notifications.push((
                method.to_owned(),
                subscription,
                payload.as_raw_str().to_owned(),
            ));
            if self.stop_on_notification {
                Flow::Stop
            } else {
                Flow::Continue
            }
        }

        fn on_malformed(&mut self, _text: &str) -> Flow {
            self.malformed += 1;
            Flow::Continue
        }

        fn on_closed(&mut self, reason: &CloseReason) {
            self.closed = Some(reason.to_string());
        }
    }

    fn run(frames: &[&str], handler: &mut Recording) -> Option<CloseReason> {
        let messages: Vec<std::result::Result<Message, tungstenite::Error>> =
            frames
                .iter()
                .map(|frame| Ok(Message::Text((*frame).to_owned().into())))
                .collect();
        let mut frame_stream = stream::iter(messages);
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime build is infallible")
            .block_on(drive(&mut frame_stream, handler))
    }

    #[test]
    fn classifies_replies_and_notifications() {
        let mut handler = Recording::default();
        let reason = run(
            &[
                r#"{"jsonrpc":"2.0","id":3,"result":17}"#,
                r#"{"jsonrpc":"2.0","id":4,"result":true}"#,
                r#"{"jsonrpc":"2.0","method":"slotNotification","params":{"subscription":17,"result":{"slot":9}}}"#,
                "not json",
            ],
            &mut handler,
        );
        assert_eq!(handler.replies, vec![(3, Some(17)), (4, None)]);
        assert_eq!(
            handler.notifications,
            vec![(
                "slotNotification".to_owned(),
                17,
                r#"{"slot":9}"#.to_owned()
            )]
        );
        assert_eq!(handler.malformed, 1);
        assert!(matches!(reason, Some(CloseReason::StreamEnded)));
        assert_eq!(handler.closed.as_deref(), Some("stream ended"));
    }

    #[test]
    fn error_frames_are_terminal() {
        let mut handler = Recording::default();
        let reason = run(
            &[
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"bad params"}}"#,
                r#"{"jsonrpc":"2.0","id":2,"result":5}"#,
            ],
            &mut handler,
        );
        assert!(handler.replies.is_empty());
        let Some(CloseReason::ServerError(error)) = reason else {
            panic!("expected a server error, got {reason:?}");
        };
        assert!(error.contains("bad params"));
    }

    #[test]
    fn stop_returns_without_a_reason() {
        let mut handler = Recording {
            stop_on_notification: true,
            ..Recording::default()
        };
        let reason = run(
            &[
                r#"{"jsonrpc":"2.0","method":"slotNotification","params":{"subscription":1,"result":1}}"#,
                r#"{"jsonrpc":"2.0","id":9,"result":9}"#,
            ],
            &mut handler,
        );
        assert!(reason.is_none());
        assert_eq!(handler.notifications.len(), 1);
        assert!(handler.replies.is_empty());
        assert!(handler.closed.is_none());
    }
}
