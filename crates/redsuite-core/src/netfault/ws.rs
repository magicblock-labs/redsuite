use super::{
    gate, gate_finish, gate_start, merged, operation, parse, rejection, Decision, GateStart,
    Holding, Operation, Parsed, Shared, Stage, Transport,
};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use json::Value;
use std::{collections::HashMap, result};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task,
};
use tokio_tungstenite::{
    tungstenite::{Message, Utf8Bytes},
    MaybeTlsStream, WebSocketStream,
};
use url::Url;

type Outbound = mpsc::UnboundedSender<Option<(Message, Option<Holding>)>>;

fn deliver(
    outbound: &Outbound,
    decision: Decision,
    text: Utf8Bytes,
    is_notification: bool,
    id: &Option<Value>,
    holding: Option<Holding>,
) -> result::Result<(), ()> {
    let message = match decision {
        Decision::Discard => return Ok(()),
        Decision::Reject(message) => {
            if is_notification {
                return Ok(());
            }
            Message::Text(rejection(id, &message).into())
        }
        Decision::Release => Message::Text(text),
    };
    outbound.send(Some((message, holding))).map_err(|_| ())
}

pub(super) async fn accept(listener: TcpListener, upstream: Url, shared: Shared) {
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        let handle = task::spawn_local(serve_ws(socket, upstream.clone(), shared.clone()));
        shared.borrow_mut().track(handle);
    }
}

fn response_operation(
    parsed: Parsed,
    pending: &mut HashMap<String, Operation>,
    subscriptions: &mut HashMap<u64, Operation>,
) -> Operation {
    if let Some(subscription) = parsed.subscription {
        let method = parsed.method.as_deref().unwrap_or("?");
        return match subscriptions.get(&subscription).cloned() {
            Some(base) => {
                let mut op = merged(base, Stage::Notification, parsed.strings);
                op.method = method.to_owned();
                op
            }
            None => operation(Transport::Ws, Stage::Notification, method, parsed.strings),
        };
    }
    let request = parsed
        .id
        .as_ref()
        .and_then(|id| json::to_string(id).ok())
        .and_then(|id| pending.remove(&id));
    let Some(request) = request else {
        return operation(Transport::Ws, Stage::Response, "?", parsed.strings);
    };
    if request.method.ends_with("Subscribe") {
        if let Some(subid) = parsed.result {
            subscriptions.insert(subid, request.clone());
        }
    }
    merged(request, Stage::Response, parsed.strings)
}

type UpstreamSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct WsRelay {
    server: SplitSink<UpstreamSocket, Message>,
    outbound: Outbound,
    shared: Shared,
    pending: HashMap<String, Operation>,
    subscriptions: HashMap<u64, Operation>,
}

impl WsRelay {
    async fn request(&mut self, frame: Message) -> result::Result<(), ()> {
        let text = match frame {
            Message::Text(text) => text,
            Message::Close(_) => return Err(()),
            other => return self.server.send(other).await.map_err(|_| ()),
        };
        let Some(parsed) = parse(&text) else {
            return self.server.send(Message::Text(text)).await.map_err(|_| ());
        };
        let request = operation(
            Transport::Ws,
            Stage::Request,
            parsed.method.as_deref().unwrap_or("?"),
            parsed.strings,
        );
        let (decision, _hold) = gate(&self.shared, request.clone()).await;
        match decision {
            Decision::Discard => Err(()),
            Decision::Reject(message) => {
                let reply = rejection(&parsed.id, &message);
                self.outbound.send(Some((Message::Text(reply.into()), None))).map_err(|_| ())
            }
            Decision::Release => {
                if let Some(id) = parsed.id.as_ref().and_then(|id| json::to_string(id).ok()) {
                    self.pending.insert(id, request);
                }
                self.server.send(Message::Text(text)).await.map_err(|_| ())
            }
        }
    }

    fn response(&mut self, frame: Message) -> result::Result<(), ()> {
        let text = match frame {
            Message::Text(text) => text,
            Message::Close(_) => return Err(()),
            other => return self.outbound.send(Some((other, None))).map_err(|_| ()),
        };
        let Some(parsed) = parse(&text) else {
            return self.outbound.send(Some((Message::Text(text), None))).map_err(|_| ());
        };
        let is_notification = parsed.subscription.is_some();
        let id = parsed.id.clone();
        let inbound = response_operation(parsed, &mut self.pending, &mut self.subscriptions);
        match gate_start(&self.shared, inbound) {
            GateStart::Decided(decision) => {
                deliver(&self.outbound, decision, text, is_notification, &id, None)
            }
            GateStart::Pending(receiver, operation, holding) => {
                let shared = self.shared.clone();
                let outbound = self.outbound.clone();
                task::spawn_local(async move {
                    let decision = gate_finish(&shared, receiver, operation).await;
                    let _ = deliver(
                        &outbound,
                        decision,
                        text,
                        is_notification,
                        &id,
                        Some(holding),
                    );
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
    task::spawn_local(async move {
        while let Some(Some((message, _hold))) = outbound_rx.recv().await {
            if client_sink.send(message).await.is_err() {
                break;
            }
        }
        let _ = client_sink.close().await;
    });
    let mut relay = WsRelay {
        server,
        outbound,
        shared,
        pending: HashMap::new(),
        subscriptions: HashMap::new(),
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
        }
    }
    let _ = relay.outbound.send(None);
    let _ = relay.server.close().await;
}
