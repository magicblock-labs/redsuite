//! Notification-collecting WS client for logs / program / slot subscriptions.

use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    time::Duration,
};

use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use json::{Deserialize, JsonValueTrait};
use pubkey::Pubkey;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};

use crate::Result;

const AWAIT_POLL: Duration = Duration::from_millis(20);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Deserialize)]
struct Frame {
    method: Option<String>,
    id: Option<u64>,
    result: Option<json::Value>,
    error: Option<json::Value>,
    params: Option<Params>,
}

#[derive(Deserialize)]
struct Params {
    subscription: u64,
    result: json::Value,
}

#[derive(Default)]
struct Shared {
    error: Option<String>,
    ready_subs: usize,
    key_by_subid: HashMap<u64, u64>,
    events: HashMap<u64, Vec<json::Value>>,
}

impl Shared {
    fn fail(&mut self, msg: impl Into<String>) {
        self.error = Some(msg.into());
    }
}

pub struct EventSubscriptions {
    sink: tokio::sync::Mutex<SplitSink<Socket, Message>>,
    shared: Rc<RefCell<Shared>>,
    reader: tokio::task::JoinHandle<()>,
    next_req_id: Cell<u64>,
}

impl EventSubscriptions {
    pub async fn connect(url: &str) -> Result<Self> {
        let (socket, _) = connect_async(url)
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        let (sink, stream) = socket.split();
        let shared = Rc::new(RefCell::new(Shared::default()));
        let reader =
            tokio::task::spawn_local(read_loop(stream, shared.clone()));
        Ok(Self {
            sink: tokio::sync::Mutex::new(sink),
            shared,
            reader,
            next_req_id: Cell::new(1),
        })
    }

    pub async fn slot_subscribe(&self) -> Result<u64> {
        self.subscribe("slotSubscribe", "[]").await
    }

    pub async fn account_subscribe(&self, account: &Pubkey) -> Result<u64> {
        self.subscribe(
            "accountSubscribe",
            &format!(
                r#"["{account}",{{"encoding":"base64","commitment":"confirmed"}}]"#
            ),
        )
        .await
    }

    pub async fn logs_subscribe_all(&self) -> Result<u64> {
        self.subscribe("logsSubscribe", r#"["all",{"commitment":"confirmed"}]"#)
            .await
    }

    pub async fn logs_subscribe_mentions(
        &self,
        account: &Pubkey,
    ) -> Result<u64> {
        self.subscribe(
            "logsSubscribe",
            &format!(
                r#"[{{"mentions":["{account}"]}},{{"commitment":"confirmed"}}]"#
            ),
        )
        .await
    }

    pub async fn program_subscribe(&self, program: &Pubkey) -> Result<u64> {
        self.subscribe(
            "programSubscribe",
            &format!(
                r#"["{program}",{{"encoding":"base64","commitment":"confirmed"}}]"#
            ),
        )
        .await
    }

    pub async fn await_subscribed(
        &self,
        count: usize,
        timeout: Duration,
    ) -> Result<()> {
        self.await_shared(timeout, "subscription acks", |shared| {
            shared.ready_subs >= count
        })
        .await
    }

    pub async fn await_events(
        &self,
        key: u64,
        count: usize,
        timeout: Duration,
    ) -> Result<()> {
        self.await_shared(timeout, "notifications", |shared| {
            shared.events.get(&key).map_or(0, Vec::len) >= count
        })
        .await
    }

    pub fn events(&self, key: u64) -> Vec<json::Value> {
        self.shared
            .borrow()
            .events
            .get(&key)
            .cloned()
            .unwrap_or_default()
    }

    pub fn finalize(&self) {
        self.reader.abort();
    }

    async fn subscribe(&self, method: &str, params: &str) -> Result<u64> {
        let key = self.next_req_id.replace(self.next_req_id.get() + 1);
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":{key},"method":"{method}","params":{params}}}"#
        );
        self.sink
            .lock()
            .await
            .send(Message::Text(msg.into()))
            .await
            .map_err(|e| format!("{method} send: {e}"))?;
        Ok(key)
    }

    async fn await_shared(
        &self,
        timeout: Duration,
        what: &str,
        done: impl Fn(&Shared) -> bool,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            {
                let shared = self.shared.borrow();
                if let Some(err) = &shared.error {
                    return Err(format!("ws stream: {err}").into());
                }
                if done(&shared) {
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for {what} ({timeout:?})"
                )
                .into());
            }
            tokio::time::sleep(AWAIT_POLL).await;
        }
    }
}

async fn read_loop(
    mut stream: SplitStream<Socket>,
    shared: Rc<RefCell<Shared>>,
) {
    while let Some(msg) = stream.next().await {
        let text = match msg {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) | Err(_) => {
                shared.borrow_mut().fail("connection closed");
                return;
            }
            Ok(_) => continue,
        };
        let Ok(frame) = json::from_str::<Frame>(&text) else {
            continue;
        };
        if let Some(error) = frame.error {
            shared.borrow_mut().fail(error.to_string());
            return;
        }
        match (frame.method, frame.params, frame.id) {
            (Some(_), Some(params), _) => {
                let mut shared = shared.borrow_mut();
                let Some(key) =
                    shared.key_by_subid.get(&params.subscription).copied()
                else {
                    continue;
                };
                shared.events.entry(key).or_default().push(params.result);
            }
            (None, _, Some(req_id)) => {
                let mut shared = shared.borrow_mut();
                if let Some(subid) =
                    frame.result.as_ref().and_then(|value| value.as_u64())
                {
                    shared.key_by_subid.insert(subid, req_id);
                }
                shared.ready_subs += 1;
            }
            _ => {}
        }
    }
    shared.borrow_mut().fail("stream ended");
}
