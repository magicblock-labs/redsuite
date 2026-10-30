use super::{gate, rpc, Decision, Gate, Holding, Operation, Shared, Stage, Transport};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use json::{JsonValueTrait, Value};
use std::{collections::HashMap, result};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::{self, JoinSet},
};
use tokio_tungstenite::{
    tungstenite::{Message, Utf8Bytes},
    MaybeTlsStream, WebSocketStream,
};
use url::Url;

type Outbound = mpsc::UnboundedSender<(Message, Option<Holding>)>;
type UpstreamSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn deliver(
    outbound: &Outbound,
    decision: Decision,
    text: Utf8Bytes,
    parsed: &Value,
    holding: Option<Holding>,
) -> result::Result<(), ()> {
    let message = match decision {
        Decision::Discard => return Ok(()),
        Decision::Reject(message) => {
            if rpc::subscription(parsed).is_some() {
                return Ok(());
            }
            Message::Text(rpc::rejection(parsed, &message).into())
        }
        Decision::Release => Message::Text(text),
    };
    outbound.send((message, holding)).map_err(|_| ())
}

pub(super) async fn accept(listener: TcpListener, upstream: Url, shared: Shared) {
    loop {
        let Ok((socket, _)) = listener.accept().await else { return };
        let handle = task::spawn_local(serve_ws(socket, upstream.clone(), shared.clone()));
        shared.borrow_mut().track(Transport::Ws, handle);
    }
}

struct WsRelay {
    server: SplitSink<UpstreamSocket, Message>,
    outbound: Outbound,
    shared: Shared,
    pending: HashMap<String, (Operation, Option<u64>)>,
    subscriptions: HashMap<u64, Operation>,
    tasks: JoinSet<bool>,
}

impl WsRelay {
    async fn request(&mut self, frame: Message) -> result::Result<(), ()> {
        let text = match frame {
            Message::Text(text) => text,
            Message::Close(_) => return Err(()),
            other => return self.server.send(other).await.map_err(|_| ()),
        };
        let parsed: Value = json::from_str(&text).unwrap_or_default();
        let request = rpc::request(Transport::Ws, &parsed);
        let (decision, _hold) = gate(&self.shared, request.clone()).resolve().await;
        match decision {
            Decision::Discard => Err(()),
            Decision::Reject(message) => self
                .outbound
                .send((
                    Message::Text(rpc::rejection(&parsed, &message).into()),
                    None,
                ))
                .map_err(|_| ()),
            Decision::Release => {
                if let Some(id) = rpc::id(&parsed) {
                    let unsubscribe = if request.method.ends_with("Unsubscribe") {
                        parsed["params"][0].as_u64()
                    } else {
                        None
                    };
                    self.pending.insert(id, (request, unsubscribe));
                }
                self.server.send(Message::Text(text)).await.map_err(|_| ())
            }
        }
    }

    fn response_operation(&mut self, parsed: &Value) -> Operation {
        if let Some(subscription) = rpc::subscription(parsed) {
            return rpc::notification(parsed, self.subscriptions.get(&subscription).cloned());
        }
        let (mut request, unsubscribe) = rpc::id(parsed)
            .and_then(|id| self.pending.remove(&id))
            .unwrap_or_else(|| (rpc::request(Transport::Ws, &Value::new()), None));
        if request.method.ends_with("Subscribe") {
            if let Some(id) = parsed["result"].as_u64() {
                self.subscriptions.insert(id, request.clone());
            }
        }
        if parsed["result"].as_bool() == Some(true) {
            if let Some(id) = unsubscribe {
                self.subscriptions.remove(&id);
            }
        }
        request.stage = Stage::Response;
        request
    }

    fn response(&mut self, frame: Message) -> result::Result<(), ()> {
        let text = match frame {
            Message::Text(text) => text,
            Message::Close(_) => return Err(()),
            other => return self.outbound.send((other, None)).map_err(|_| ()),
        };
        let Ok(parsed) = json::from_str::<Value>(&text) else {
            return self.outbound.send((Message::Text(text), None)).map_err(|_| ());
        };
        let operation = self.response_operation(&parsed);
        match gate(&self.shared, operation) {
            Gate::Decided(decision) => deliver(&self.outbound, decision, text, &parsed, None),
            pending => {
                let outbound = self.outbound.clone();
                // Other replies and notifications must progress while this one is held.
                self.tasks.spawn_local(async move {
                    let (decision, holding) = pending.resolve().await;
                    deliver(&outbound, decision, text, &parsed, holding).is_ok()
                });
                Ok(())
            }
        }
    }
}

async fn serve_ws(socket: TcpStream, upstream: Url, shared: Shared) {
    let Ok(client) = tokio_tungstenite::accept_async(socket).await else { return };
    let Ok((server, _)) = tokio_tungstenite::connect_async(upstream.as_str()).await else {
        return;
    };
    let (mut client_sink, mut client_stream) = client.split();
    let (server, mut server_stream) = server.split();
    let (outbound, mut outbound_rx) = mpsc::unbounded_channel();
    let mut tasks = JoinSet::new();
    tasks.spawn_local(async move {
        while let Some((message, _hold)) = outbound_rx.recv().await {
            if client_sink.send(message).await.is_err() {
                break;
            }
        }
        false
    });
    let mut relay = WsRelay {
        server,
        outbound,
        shared,
        pending: HashMap::new(),
        subscriptions: HashMap::new(),
        tasks,
    };
    loop {
        tokio::select! {
            frame = client_stream.next() => {
                let Some(Ok(frame)) = frame else { break };
                if relay.request(frame).await.is_err() { break; }
            }
            frame = server_stream.next() => {
                let Some(Ok(frame)) = frame else { break };
                if relay.response(frame).is_err() { break; }
            }
            completed = relay.tasks.join_next() => {
                if !matches!(completed, Some(Ok(true))) { break; }
            }
        }
    }
    // JoinSet also aborts the writer and pending deliveries when the relay is cancelled.
    relay.tasks.shutdown().await;
    let _ = relay.server.close().await;
}
