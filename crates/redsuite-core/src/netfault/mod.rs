mod http;
mod ws;

use std::{
    cell::RefCell,
    mem,
    rc::Rc,
    time::{Duration, Instant},
};

use crate::{
    api::decode_transaction,
    context::{BaseCtx, ChainCtx},
    topology::BaseEndpoints,
    transport::http as rpc_http,
    transport::NETWORK_TIMEOUT,
    wait, Result,
};
use anyhow::{anyhow, Context};
use json::{JsonContainerTrait, JsonValueTrait, Value};
use pubkey::Pubkey;
use signature::Signature;
use tokio::{
    net::TcpListener,
    sync::{oneshot, Notify},
    task,
    task::JoinHandle,
};

const REJECTION_CODE: i64 = -32000;

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
            .context("the intercepted operation carries no signature")?;
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

#[derive(Clone)]
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
    observed: [usize; 3],
    started: Instant,
    changed: Rc<Notify>,
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
        self.changed.notify_waiters();
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
    let stage = match operation.stage {
        Stage::Request => 0,
        Stage::Response => 1,
        Stage::Notification => 2,
    };
    state.observed[stage] += 1;
    let rule = state
        .rules
        .iter()
        .find(|rule| rule.selector.matches(&operation))
        .map(|rule| (rule.id, rule.effect.clone()));
    if let Some((id, effect)) = rule {
        return match effect {
            Effect::Reject(message) => {
                state.record(Action::Rejected, Some(operation));
                GateStart::Decided(Decision::Reject(message))
            }
            Effect::Stall => {
                let (sender, receiver) = oneshot::channel();
                state.stalled.push((id, sender));
                state.record(Action::Held, Some(operation.clone()));
                state.holding += 1;
                GateStart::Pending(receiver, operation, Holding(shared.clone()))
            }
        };
    }
    let trap = state
        .traps
        .iter()
        .find(|trap| trap.fired.borrow().is_none() && trap.selector.matches(&operation))
        .cloned();
    let Some(trap) = trap else {
        return GateStart::Decided(Decision::Release);
    };
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

struct Holding(Shared);

impl Drop for Holding {
    fn drop(&mut self) {
        let mut state = self.0.borrow_mut();
        state.holding -= 1;
        state.changed.notify_waiters();
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
    shared: Shared,
}

impl Trap {
    pub async fn wait(&self, timeout: Duration) -> Result<Held> {
        wait::on_change(&self.state.notify, timeout, "interception", || {
            let mut fired = self.state.fired.borrow_mut();
            let Some(pending) = fired.as_mut() else { return Ok(None) };
            let decide = pending.decide.take().context("interception was already taken")?;
            Ok(Some(Held {
                operation: pending.operation.clone(),
                held_at: pending.held_at,
                decide,
            }))
        })
        .await
        .with_context(|| {
            let shared = self.shared.borrow();
            let connections = shared.connections.iter().filter(|handle| !handle.is_finished()).count();
            format!(
                "interception {:?}: connections={connections}, held={}, observed [request, response, notification]={:?}",
                self.state.selector, shared.holding, shared.observed
            )
        })
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
        for (_, decide) in state.stalled.extract_if(.., |(rule, _)| *rule == self.id) {
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
            observed: [0; 3],
            started: Instant::now(),
            changed: Rc::default(),
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
            rpc_url: format!("http://{}", http_listener.local_addr()?).parse()?,
            ws_url: format!("ws://{}", ws_listener.local_addr()?).parse()?,
            ..BaseEndpoints::from_base(base)?
        };
        let upstream_rpc = base.api().url().clone();
        let upstream_ws = base.ws_url().clone();
        let client = rpc_http::client();
        let listeners = vec![
            task::spawn_local(http::accept(
                http_listener,
                upstream_rpc,
                client,
                shared.clone(),
            )),
            task::spawn_local(ws::accept(ws_listener, upstream_ws, shared.clone())),
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
        Trap {
            state,
            shared: self.shared.clone(),
        }
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
        for trap in mem::take(&mut state.traps) {
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

    pub fn held_count(&self, selector: &Selector) -> usize {
        self.shared
            .borrow()
            .events
            .iter()
            .filter(|event| {
                event.action == Action::Held
                    && event.operation.as_ref().is_some_and(|operation| selector.matches(operation))
            })
            .count()
    }

    pub async fn await_held(&self, selector: Selector, timeout: Duration) -> Result<()> {
        let changed = self.shared.borrow().changed.clone();
        wait::on_change(&changed, timeout, "base operation held", || {
            Ok((self.held_count(&selector) > 0).then_some(()))
        })
        .await
    }

    pub async fn reset(&self) -> Result<()> {
        self.restore();
        let changed = self.shared.borrow().changed.clone();
        wait::on_change(
            &changed,
            NETWORK_TIMEOUT,
            "released interceptions resuming",
            || Ok((self.shared.borrow().holding == 0).then_some(())),
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
        let unfired = mem::take(&mut self.shared.borrow_mut().unfired);
        if unfired.is_empty() {
            return Ok(());
        }
        Err(anyhow!(
            "requested interceptions never occurred: {}",
            unfired.join("; ")
        ))
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

fn collect_strings(value: &Value, into: &mut Vec<String>) {
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
    id: Option<Value>,
    method: Option<String>,
    subscription: Option<u64>,
    result: Option<u64>,
    strings: Vec<String>,
}

fn parse(text: &str) -> Option<Parsed> {
    let value: Value = json::from_str(text).ok()?;
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

fn rejection(id: &Option<Value>, message: &str) -> String {
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
