use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use json::{JsonContainerTrait, JsonValueTrait};
use pubkey::Pubkey;
use signature::Signature;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, Notify},
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};

use crate::{
    api::decode_transaction,
    context::{BaseCtx, ChainCtx},
    topology::BaseEndpoints,
    transport::http,
    Result,
};

const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(60);
const REJECTION_CODE: i64 = -32000;
const MAX_HEAD: usize = 64 * 1024;
const MAX_BODY: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Http,
    Ws,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Request,
    Response,
    Notification,
}

#[derive(Clone, Debug)]
pub struct Operation {
    pub transport: Transport,
    pub stage: Stage,
    pub method: String,
    pub signatures: Vec<String>,
    pub accounts: Vec<String>,
}

impl Operation {
    pub fn signature(&self) -> Result<Signature> {
        let first = self
            .signatures
            .first()
            .ok_or("the intercepted operation carries no signature")?;
        Ok(first.parse()?)
    }
}

#[derive(Clone, Debug, Default)]
pub struct Selector {
    transport: Option<Transport>,
    stages: Vec<Stage>,
    methods: Vec<String>,
    signature: Option<String>,
    account: Option<String>,
}

impl Selector {
    pub fn method(name: &str) -> Self {
        Self::methods(&[name])
    }

    pub fn methods(names: &[&str]) -> Self {
        Self {
            methods: names.iter().map(|name| (*name).to_owned()).collect(),
            ..Self::default()
        }
    }

    pub fn http(mut self) -> Self {
        self.transport = Some(Transport::Http);
        self
    }

    pub fn ws(mut self) -> Self {
        self.transport = Some(Transport::Ws);
        self
    }

    pub fn request(mut self) -> Self {
        self.stages.push(Stage::Request);
        self
    }

    pub fn response(mut self) -> Self {
        self.stages.push(Stage::Response);
        self
    }

    pub fn notification(mut self) -> Self {
        self.stages.push(Stage::Notification);
        self
    }

    pub fn signature(mut self, signature: &Signature) -> Self {
        self.signature = Some(signature.to_string());
        self
    }

    pub fn account(mut self, account: &Pubkey) -> Self {
        self.account = Some(account.to_string());
        self
    }

    fn matches(&self, operation: &Operation) -> bool {
        self.transport.is_none_or(|transport| transport == operation.transport)
            && (self.stages.is_empty() || self.stages.contains(&operation.stage))
            && (self.methods.is_empty() || self.methods.contains(&operation.method))
            && self
                .signature
                .as_ref()
                .is_none_or(|signature| operation.signatures.contains(signature))
            && self.account.as_ref().is_none_or(|account| operation.accounts.contains(account))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Held,
    Released,
    Discarded,
    Rejected,
    ConnectionsClosed,
    Restored,
}

#[derive(Clone, Debug)]
pub struct FaultEvent {
    pub action: Action,
    pub operation: Option<Operation>,
}

enum Decision {
    Release,
    Discard,
    Reject(String),
}

struct Pending {
    operation: Operation,
    held_at: Duration,
    decide: Option<oneshot::Sender<Decision>>,
}

struct TrapState {
    selector: Selector,
    fired: RefCell<Option<Pending>>,
    notify: Notify,
}

enum Effect {
    Reject(String),
    Stall,
}

struct Rule {
    id: u64,
    selector: Selector,
    effect: Effect,
}

struct State {
    started: Instant,
    traps: Vec<Rc<TrapState>>,
    unfired: Vec<String>,
    holding: usize,
    rules: Vec<Rule>,
    stalled: Vec<(u64, oneshot::Sender<Decision>)>,
    next_rule: u64,
    events: Vec<FaultEvent>,
    connections: Vec<JoinHandle<()>>,
}

type Shared = Rc<RefCell<State>>;

impl State {
    fn record(&mut self, action: Action, operation: Option<Operation>) {
        self.events.push(FaultEvent { action, operation });
    }

    fn track(&mut self, handle: JoinHandle<()>) {
        self.connections.retain(|handle| !handle.is_finished());
        self.connections.push(handle);
    }
}

enum GateStart {
    Decided(Decision),
    Pending(oneshot::Receiver<Decision>, Operation, Holding),
}

fn gate_start(shared: &Shared, operation: Operation) -> GateStart {
    let mut state = shared.borrow_mut();
    let rule = state.rules.iter().find(|rule| rule.selector.matches(&operation)).map(|rule| {
        (
            rule.id,
            match &rule.effect {
                Effect::Reject(message) => Some(message.clone()),
                Effect::Stall => None,
            },
        )
    });
    match rule {
        Some((_, Some(message))) => {
            state.record(Action::Rejected, Some(operation));
            GateStart::Decided(Decision::Reject(message))
        }
        Some((id, None)) => {
            let (sender, receiver) = oneshot::channel();
            state.stalled.push((id, sender));
            state.record(Action::Held, Some(operation.clone()));
            state.holding += 1;
            GateStart::Pending(receiver, operation, Holding(shared.clone()))
        }
        None => {
            let trap = state
                .traps
                .iter()
                .find(|trap| trap.fired.borrow().is_none() && trap.selector.matches(&operation))
                .cloned();
            match trap {
                None => GateStart::Decided(Decision::Release),
                Some(trap) => {
                    let (sender, receiver) = oneshot::channel();
                    let held_at = state.started.elapsed();
                    *trap.fired.borrow_mut() = Some(Pending {
                        operation: operation.clone(),
                        held_at,
                        decide: Some(sender),
                    });
                    state.record(Action::Held, Some(operation.clone()));
                    state.holding += 1;
                    trap.notify.notify_waiters();
                    GateStart::Pending(receiver, operation, Holding(shared.clone()))
                }
            }
        }
    }
}

struct Holding(Shared);

impl Drop for Holding {
    fn drop(&mut self) {
        self.0.borrow_mut().holding -= 1;
    }
}

async fn gate_finish(
    shared: &Shared,
    receiver: oneshot::Receiver<Decision>,
    operation: Operation,
) -> Decision {
    let decision = receiver.await.unwrap_or(Decision::Release);
    let action = match &decision {
        Decision::Release => Action::Released,
        Decision::Discard => Action::Discarded,
        Decision::Reject(_) => Action::Rejected,
    };
    shared.borrow_mut().record(action, Some(operation));
    decision
}

async fn gate(shared: &Shared, operation: Operation) -> (Decision, Option<Holding>) {
    match gate_start(shared, operation) {
        GateStart::Decided(decision) => (decision, None),
        GateStart::Pending(receiver, operation, holding) => (
            gate_finish(shared, receiver, operation).await,
            Some(holding),
        ),
    }
}

type Outbound = mpsc::UnboundedSender<Option<(Message, Option<Holding>)>>;

fn deliver(
    outbound: &Outbound,
    decision: Decision,
    text: Utf8Bytes,
    is_notification: bool,
    id: &Option<json::Value>,
    holding: Option<Holding>,
) -> std::result::Result<(), ()> {
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

pub struct Held {
    pub operation: Operation,
    pub held_at: Duration,
    decide: oneshot::Sender<Decision>,
}

impl Held {
    pub fn release(self) {
        let _ = self.decide.send(Decision::Release);
    }

    pub fn discard(self) {
        let _ = self.decide.send(Decision::Discard);
    }

    pub fn reject(self, message: &str) {
        let _ = self.decide.send(Decision::Reject(message.to_owned()));
    }
}

pub struct Trap {
    state: Rc<TrapState>,
}

impl Trap {
    pub async fn wait(&self, timeout: Duration) -> Result<Held> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(pending) = self.state.fired.borrow_mut().as_mut() {
                if let Some(decide) = pending.decide.take() {
                    return Ok(Held {
                        operation: pending.operation.clone(),
                        held_at: pending.held_at,
                        decide,
                    });
                }
                return Err(format!(
                    "the interception `{:?}` was already taken",
                    self.state.selector
                )
                .into());
            }
            let remaining =
                deadline.checked_duration_since(tokio::time::Instant::now()).ok_or_else(|| {
                    format!(
                        "the interception `{:?}` did not occur within \
                         {timeout:?}",
                        self.state.selector
                    )
                })?;
            let _ = tokio::time::timeout(remaining, self.state.notify.notified()).await;
        }
    }
}

pub struct RuleHandle {
    id: u64,
    shared: Shared,
}

impl RuleHandle {
    pub fn remove(self) {
        let mut state = self.shared.borrow_mut();
        state.rules.retain(|rule| rule.id != self.id);
        let (release, keep): (Vec<_>, Vec<_>) =
            state.stalled.drain(..).partition(|(rule, _)| *rule == self.id);
        state.stalled = keep;
        for (_, decide) in release {
            let _ = decide.send(Decision::Release);
        }
    }
}

pub struct BaseProxies {
    shared: Shared,
    endpoints: BaseEndpoints,
    listeners: Vec<JoinHandle<()>>,
}

impl BaseProxies {
    pub async fn spawn(base: &BaseCtx) -> Result<Self> {
        let shared: Shared = Rc::new(RefCell::new(State {
            started: Instant::now(),
            traps: Vec::new(),
            unfired: Vec::new(),
            holding: 0,
            rules: Vec::new(),
            stalled: Vec::new(),
            next_rule: 1,
            events: Vec::new(),
            connections: Vec::new(),
        }));
        let http_listener = TcpListener::bind("127.0.0.1:0").await?;
        let ws_listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoints = BaseEndpoints {
            rpc_url: format!("http://{}", http_listener.local_addr()?),
            ws_url: format!("ws://{}", ws_listener.local_addr()?),
            grpc_url: None,
        };
        let upstream_rpc = base.api().url().to_owned();
        let upstream_ws = base.ws_url().to_owned();
        let client = http::client_with_timeout(UPSTREAM_TIMEOUT);
        let listeners = vec![
            tokio::task::spawn_local(accept_http(
                http_listener,
                upstream_rpc,
                client,
                shared.clone(),
            )),
            tokio::task::spawn_local(accept_ws(ws_listener, upstream_ws, shared.clone())),
        ];
        Ok(Self { shared, endpoints, listeners })
    }

    pub fn endpoints(&self) -> BaseEndpoints {
        self.endpoints.clone()
    }

    pub fn intercept(&self, selector: Selector) -> Trap {
        let state = Rc::new(TrapState {
            selector,
            fired: RefCell::new(None),
            notify: Notify::new(),
        });
        self.shared.borrow_mut().traps.push(state.clone());
        Trap { state }
    }

    pub fn reject(&self, selector: Selector, message: &str) -> RuleHandle {
        self.rule(selector, Effect::Reject(message.to_owned()))
    }

    pub fn stall(&self, selector: Selector) -> RuleHandle {
        self.rule(selector, Effect::Stall)
    }

    fn rule(&self, selector: Selector, effect: Effect) -> RuleHandle {
        let mut state = self.shared.borrow_mut();
        let id = state.next_rule;
        state.next_rule += 1;
        state.rules.push(Rule { id, selector, effect });
        RuleHandle { id, shared: self.shared.clone() }
    }

    pub fn close_connections(&self) {
        let mut state = self.shared.borrow_mut();
        for handle in state.connections.drain(..) {
            handle.abort();
        }
        state.record(Action::ConnectionsClosed, None);
    }

    pub fn restore(&self) {
        let mut state = self.shared.borrow_mut();
        state.rules.clear();
        for (_, decide) in state.stalled.drain(..) {
            let _ = decide.send(Decision::Release);
        }
        let traps: Vec<Rc<TrapState>> = state.traps.drain(..).collect();
        for trap in traps {
            let mut fired = trap.fired.borrow_mut();
            match fired.as_mut() {
                Some(pending) => {
                    if let Some(decide) = pending.decide.take() {
                        let _ = decide.send(Decision::Release);
                    }
                }
                None => state.unfired.push(format!("{:?}", trap.selector)),
            }
        }
        state.record(Action::Restored, None);
    }

    pub fn events(&self) -> Vec<FaultEvent> {
        self.shared.borrow().events.clone()
    }

    pub async fn reset(&self) -> Result<()> {
        self.restore();
        crate::check::poll(
            "released interceptions resuming",
            UPSTREAM_TIMEOUT,
            || async { self.shared.borrow().holding == 0 },
        )
        .await?;
        self.shared.borrow_mut().events.clear();
        self.unfired()
    }

    pub fn finish(mut self) -> Result<()> {
        self.shutdown();
        self.unfired()
    }

    fn unfired(&self) -> Result<()> {
        let unfired = std::mem::take(&mut self.shared.borrow_mut().unfired);
        if unfired.is_empty() {
            return Ok(());
        }
        Err(format!(
            "requested interceptions never occurred: {}",
            unfired.join("; ")
        )
        .into())
    }

    fn shutdown(&mut self) {
        self.restore();
        for handle in self.listeners.drain(..) {
            handle.abort();
        }
        let mut state = self.shared.borrow_mut();
        for handle in state.connections.drain(..) {
            handle.abort();
        }
    }
}

impl Drop for BaseProxies {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn collect_strings(value: &json::Value, into: &mut Vec<String>) {
    if let Some(text) = value.as_str() {
        into.push(text.to_owned());
    } else if let Some(items) = value.as_array() {
        for item in items.iter() {
            collect_strings(item, into);
        }
    } else if let Some(fields) = value.as_object() {
        for (_, item) in fields.iter() {
            collect_strings(item, into);
        }
    }
}

fn classify(strings: Vec<String>) -> (Vec<String>, Vec<String>) {
    let mut signatures = Vec::new();
    let mut accounts = Vec::new();
    for text in strings {
        if text.parse::<Signature>().is_ok() {
            signatures.push(text);
        } else if text.parse::<Pubkey>().is_ok() {
            accounts.push(text);
        } else if let Ok(tx) = decode_transaction(&text) {
            signatures.extend(tx.signatures.iter().map(ToString::to_string));
            accounts.extend(tx.message.static_account_keys().iter().map(ToString::to_string));
        }
    }
    (signatures, accounts)
}

struct Parsed {
    id: Option<json::Value>,
    method: Option<String>,
    subscription: Option<u64>,
    result: Option<u64>,
    strings: Vec<String>,
}

fn parse(text: &str) -> Option<Parsed> {
    let value: json::Value = json::from_str(text).ok()?;
    let method = value.get("method").and_then(|m| m.as_str()).map(str::to_owned);
    let mut strings = Vec::new();
    if let Some(params) = value.get("params") {
        collect_strings(params, &mut strings);
    }
    if let Some(result) = value.get("result") {
        collect_strings(result, &mut strings);
    }
    let subscription = value
        .get("params")
        .and_then(|params| params.get("subscription"))
        .and_then(|sub| sub.as_u64());
    Some(Parsed {
        id: value.get("id").cloned(),
        method,
        subscription,
        result: value.get("result").and_then(|result| result.as_u64()),
        strings,
    })
}

fn operation(transport: Transport, stage: Stage, method: &str, strings: Vec<String>) -> Operation {
    let (signatures, accounts) = classify(strings);
    Operation {
        transport,
        stage,
        method: method.to_owned(),
        signatures,
        accounts,
    }
}

fn merged(mut base: Operation, stage: Stage, extra: Vec<String>) -> Operation {
    let (signatures, accounts) = classify(extra);
    for signature in signatures {
        if !base.signatures.contains(&signature) {
            base.signatures.push(signature);
        }
    }
    for account in accounts {
        if !base.accounts.contains(&account) {
            base.accounts.push(account);
        }
    }
    base.stage = stage;
    base
}

fn rejection(id: &Option<json::Value>, message: &str) -> String {
    let id = id
        .as_ref()
        .and_then(|id| json::to_string(id).ok())
        .unwrap_or_else(|| "null".to_owned());
    let message = json::to_string(message).unwrap_or_default();
    format!(
        "{{\"jsonrpc\":\"2.0\",\"error\":{{\"code\":{REJECTION_CODE},\
         \"message\":{message}}},\"id\":{id}}}"
    )
}

async fn accept_http(
    listener: TcpListener,
    upstream: String,
    client: reqwest::Client,
    shared: Shared,
) {
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        let handle = tokio::task::spawn_local(serve_http(
            socket,
            upstream.clone(),
            client.clone(),
            shared.clone(),
        ));
        shared.borrow_mut().track(handle);
    }
}

async fn read_request(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Option<(String, String)> {
    loop {
        if let Some(split) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buffer[..split]).into_owned();
            let content_length = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.trim()
                        .eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if content_length > MAX_BODY {
                return None;
            }
            let body_start = split + 4;
            while buffer.len() < body_start + content_length {
                let mut chunk = [0u8; 16 * 1024];
                let read = socket.read(&mut chunk).await.ok()?;
                if read == 0 {
                    return None;
                }
                buffer.extend_from_slice(&chunk[..read]);
            }
            let body = String::from_utf8_lossy(&buffer[body_start..body_start + content_length])
                .into_owned();
            buffer.drain(..body_start + content_length);
            return Some((head, body));
        }
        if buffer.len() > MAX_HEAD {
            return None;
        }
        let mut chunk = [0u8; 16 * 1024];
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

async fn write_response(socket: &mut TcpStream, status: u16, body: &str) -> bool {
    let reason = if status == 200 { "OK" } else { "Error" };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\n\
         content-length: {}\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await.is_ok()
        && socket.write_all(body.as_bytes()).await.is_ok()
        && socket.flush().await.is_ok()
}

async fn serve_http(
    mut socket: TcpStream,
    upstream: String,
    client: reqwest::Client,
    shared: Shared,
) {
    let mut buffer = Vec::new();
    loop {
        let Some((head, body)) = read_request(&mut socket, &mut buffer).await else {
            return;
        };
        let keep_alive = !head.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.trim().eq_ignore_ascii_case("connection")
                    && value.trim().eq_ignore_ascii_case("close")
            })
        });
        let parsed = parse(&body);
        let (id, request) = match &parsed {
            Some(parsed) => (
                parsed.id.clone(),
                operation(
                    Transport::Http,
                    Stage::Request,
                    parsed.method.as_deref().unwrap_or("?"),
                    parsed.strings.clone(),
                ),
            ),
            None => (
                None,
                operation(Transport::Http, Stage::Request, "?", Vec::new()),
            ),
        };
        let (decision, _request_hold) = gate(&shared, request.clone()).await;
        match decision {
            Decision::Discard => return,
            Decision::Reject(message) => {
                if !write_response(&mut socket, 200, &rejection(&id, &message)).await {
                    return;
                }
                if !keep_alive {
                    return;
                }
                continue;
            }
            Decision::Release => {}
        }
        let (status, text) = match http::post_json(&client, &upstream, body).await {
            Ok(text) => (200, text),
            Err(error) => (error.status.unwrap_or(502), error.detail),
        };
        let extra = parse(&text).map(|parsed| parsed.strings).unwrap_or_default();
        let response = merged(request, Stage::Response, extra);
        let (decision, _response_hold) = gate(&shared, response).await;
        match decision {
            Decision::Discard => return,
            Decision::Reject(message) => {
                if !write_response(&mut socket, 200, &rejection(&id, &message)).await {
                    return;
                }
            }
            Decision::Release => {
                if !write_response(&mut socket, status, &text).await {
                    return;
                }
            }
        }
        if !keep_alive {
            return;
        }
    }
}

async fn accept_ws(listener: TcpListener, upstream: String, shared: Shared) {
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        let handle = tokio::task::spawn_local(serve_ws(socket, upstream.clone(), shared.clone()));
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

async fn serve_ws(socket: TcpStream, upstream: String, shared: Shared) {
    let Ok(client) = tokio_tungstenite::accept_async(socket).await else {
        return;
    };
    let Ok((server, _)) = tokio_tungstenite::connect_async(&upstream).await else {
        return;
    };
    let (mut client_sink, mut client_stream) = client.split();
    let (mut server_sink, mut server_stream) = server.split();
    let (outbound, mut outbound_rx) = mpsc::unbounded_channel();
    tokio::task::spawn_local(async move {
        while let Some(Some((message, _hold))) = outbound_rx.recv().await {
            if client_sink.send(message).await.is_err() {
                break;
            }
        }
        let _ = client_sink.close().await;
    });
    let mut pending: HashMap<String, Operation> = HashMap::new();
    let mut subscriptions: HashMap<u64, Operation> = HashMap::new();
    loop {
        tokio::select! {
            frame = client_stream.next() => {
                let Some(Ok(frame)) = frame else { break };
                let text = match frame {
                    Message::Text(text) => text,
                    Message::Close(_) => break,
                    other => {
                        if server_sink.send(other).await.is_err() {
                            break;
                        }
                        continue;
                    }
                };
                let Some(parsed) = parse(&text) else {
                    if server_sink.send(Message::Text(text)).await.is_err() {
                        break;
                    }
                    continue;
                };
                let request = operation(
                    Transport::Ws,
                    Stage::Request,
                    parsed.method.as_deref().unwrap_or("?"),
                    parsed.strings,
                );
                let (decision, _hold) = gate(&shared, request.clone()).await;
                match decision {
                    Decision::Discard => break,
                    Decision::Reject(message) => {
                        let reply = rejection(&parsed.id, &message);
                        if outbound.send(Some((Message::Text(reply.into()), None))).is_err() {
                            break;
                        }
                    }
                    Decision::Release => {
                        if let Some(id) = parsed.id.as_ref().and_then(|id| json::to_string(id).ok()) {
                            pending.insert(id, request);
                        }
                        if server_sink.send(Message::Text(text)).await.is_err() {
                            break;
                        }
                    }
                }
            }
            frame = server_stream.next() => {
                let Some(Ok(frame)) = frame else { break };
                let text = match frame {
                    Message::Text(text) => text,
                    Message::Close(_) => break,
                    other => {
                        if outbound.send(Some((other, None))).is_err() {
                            break;
                        }
                        continue;
                    }
                };
                let Some(parsed) = parse(&text) else {
                    if outbound.send(Some((Message::Text(text), None))).is_err() {
                        break;
                    }
                    continue;
                };
                let is_notification = parsed.subscription.is_some();
                let id = parsed.id.clone();
                let inbound = response_operation(parsed, &mut pending, &mut subscriptions);
                match gate_start(&shared, inbound) {
                    GateStart::Decided(decision) => {
                        if deliver(&outbound, decision, text, is_notification, &id, None).is_err() {
                            break;
                        }
                    }
                    GateStart::Pending(receiver, operation, holding) => {
                        let shared = shared.clone();
                        let outbound = outbound.clone();
                        tokio::task::spawn_local(async move {
                            let decision = gate_finish(&shared, receiver, operation).await;
                            let _ = deliver(&outbound, decision, text, is_notification, &id, Some(holding));
                        });
                    }
                }
            }
        }
    }
    let _ = outbound.send(None);
    let _ = server_sink.close().await;
}
