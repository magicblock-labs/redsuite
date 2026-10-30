mod http;
mod rpc;
mod ws;

use crate::{
    topology::BaseEndpoints,
    transport::{http as rpc_http, NETWORK_TIMEOUT},
    wait, Result,
};
use anyhow::{ensure, Context};
use pubkey::Pubkey;
use signature::Signature;
use std::{
    cell::{Cell, RefCell},
    mem,
    rc::{Rc, Weak},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{oneshot, watch, Notify},
    task::{self, JoinHandle},
    time,
};

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

enum Decision {
    Release,
    Discard,
    Reject(String),
}

struct Interception {
    selector: Selector,
    deliver: Option<oneshot::Sender<Held>>,
}

#[derive(Default)]
struct Hits {
    count: Cell<usize>,
    changed: Notify,
}

impl Hits {
    fn record(&self) {
        self.count.set(self.count.get() + 1);
        self.changed.notify_waiters();
    }
}

enum Effect {
    Reject(String),
    Stall(watch::Sender<()>),
}

struct Rule {
    selector: Selector,
    effect: Effect,
    hits: Rc<Hits>,
}

#[derive(Default)]
struct Runtime {
    active: Weak<RefCell<State>>,
    connections: Vec<(Transport, JoinHandle<()>)>,
}

impl Runtime {
    fn track(&mut self, transport: Transport, handle: JoinHandle<()>) {
        self.connections.retain(|(_, handle)| !handle.is_finished());
        self.connections.push((transport, handle));
    }
}

type Shared = Rc<RefCell<Runtime>>;

struct State {
    traps: Vec<Interception>,
    rules: Vec<Rule>,
    unfired: Vec<String>,
    observed: [usize; 3],
    holding: usize,
    sequence: u64,
    reset: watch::Sender<()>,
    drained: Rc<Notify>,
}

impl State {
    fn restore(&mut self) {
        self.rules.clear();
        self.reset.send_replace(());
        for trap in self.traps.drain(..) {
            if trap.deliver.is_some() {
                self.unfired.push(format!("{:?}", trap.selector));
            }
        }
    }
}

enum Gate {
    Decided(Decision),
    Held(oneshot::Receiver<Decision>, watch::Receiver<()>, Holding),
    Stalled(watch::Receiver<()>, Holding),
}

impl Gate {
    async fn resolve(self) -> (Decision, Option<Holding>) {
        match self {
            Self::Decided(decision) => (decision, None),
            Self::Held(decision, mut reset, holding) => {
                let decision = tokio::select! {
                    biased;
                    decision = decision => decision.unwrap_or(Decision::Release),
                    _ = reset.changed() => Decision::Release,
                };
                (decision, Some(holding))
            }
            Self::Stalled(mut resume, holding) => {
                let _ = resume.changed().await;
                (Decision::Release, Some(holding))
            }
        }
    }
}

fn gate(shared: &Shared, operation: Operation) -> Gate {
    let Some(scope) = shared.borrow().active.upgrade() else {
        return Gate::Decided(Decision::Release);
    };
    let mut state = scope.borrow_mut();
    state.observed[operation.stage as usize] += 1;
    // Persistent faults take precedence over one-shot interceptions.
    let pending =
        if let Some(rule) = state.rules.iter().find(|rule| rule.selector.matches(&operation)) {
            rule.hits.record();
            match &rule.effect {
                Effect::Reject(message) => return Gate::Decided(Decision::Reject(message.clone())),
                Effect::Stall(resume) => Gate::Stalled(resume.subscribe(), Holding(scope.clone())),
            }
        } else {
            let Some(index) = state
                .traps
                .iter()
                .position(|trap| trap.deliver.is_some() && trap.selector.matches(&operation))
            else {
                return Gate::Decided(Decision::Release);
            };
            let (decide, decision) = oneshot::channel();
            state.sequence += 1;
            let held = Held {
                operation,
                sequence: state.sequence,
                decide,
            };
            let _ = state.traps[index].deliver.take().unwrap().send(held);
            Gate::Held(decision, state.reset.subscribe(), Holding(scope.clone()))
        };
    state.holding += 1;
    pending
}

// Kept until the released operation has been forwarded, not merely unblocked.
struct Holding(Rc<RefCell<State>>);

impl Drop for Holding {
    fn drop(&mut self) {
        let mut state = self.0.borrow_mut();
        state.holding -= 1;
        state.drained.notify_waiters();
    }
}

pub struct Held {
    pub operation: Operation,
    pub sequence: u64,
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
    receive: oneshot::Receiver<Held>,
    selector: Selector,
    state: Rc<RefCell<State>>,
}

impl Trap {
    pub async fn wait(self, timeout: Duration) -> Result<Held> {
        time::timeout(timeout, self.receive)
            .await
            .context("interception timed out")
            .and_then(|held| held.context("interception restored before it arrived"))
            .with_context(|| {
                let state = self.state.borrow();
                format!(
                    "interception {:?}: held={}, observed [request, response, notification]={:?}",
                    self.selector, state.holding, state.observed
                )
            })
    }
}

pub struct RuleHandle {
    state: Weak<RefCell<State>>,
    hits: Rc<Hits>,
}

impl RuleHandle {
    pub fn hits(&self) -> usize {
        self.hits.count.get()
    }

    pub async fn wait_hit(&self, timeout: Duration) -> Result<()> {
        wait::on_change(&self.hits.changed, timeout, "fault hit", || {
            Ok((self.hits() > 0).then_some(()))
        })
        .await
    }

    pub fn remove(self) {
        if let Some(state) = self.state.upgrade() {
            state.borrow_mut().rules.retain(|rule| !Rc::ptr_eq(&rule.hits, &self.hits));
        }
    }
}

/// Scenario-owned faults; dropping the scope restores traffic.
pub struct Faults {
    state: Rc<RefCell<State>>,
    runtime: Shared,
}

impl Faults {
    pub fn intercept(&self, selector: Selector) -> Trap {
        let (deliver, receive) = oneshot::channel();
        self.state.borrow_mut().traps.push(Interception {
            selector: selector.clone(),
            deliver: Some(deliver),
        });
        Trap {
            receive,
            selector,
            state: self.state.clone(),
        }
    }

    pub fn reject(&self, selector: Selector, message: &str) -> RuleHandle {
        self.rule(selector, Effect::Reject(message.to_owned()))
    }

    pub fn stall(&self, selector: Selector) -> RuleHandle {
        self.rule(selector, Effect::Stall(watch::channel(()).0))
    }

    fn rule(&self, selector: Selector, effect: Effect) -> RuleHandle {
        let mut state = self.state.borrow_mut();
        let hits = Rc::new(Hits::default());
        state.rules.push(Rule {
            selector,
            effect,
            hits: hits.clone(),
        });
        RuleHandle {
            state: Rc::downgrade(&self.state),
            hits,
        }
    }

    pub fn disconnect(&self, transport: Transport) {
        for (_, handle) in self
            .runtime
            .borrow_mut()
            .connections
            .extract_if(.., |(kind, _)| *kind == transport)
        {
            handle.abort();
        }
    }

    pub fn restore(&self) {
        self.state.borrow_mut().restore();
    }

    pub async fn reset(&self) -> Result<()> {
        self.restore();
        let drained = self.state.borrow().drained.clone();
        wait::on_change(
            &drained,
            NETWORK_TIMEOUT,
            "released interceptions resuming",
            || Ok((self.state.borrow().holding == 0).then_some(())),
        )
        .await?;
        let unfired = mem::take(&mut self.state.borrow_mut().unfired);
        ensure!(
            unfired.is_empty(),
            "requested interceptions never occurred: {}",
            unfired.join("; ")
        );
        Ok(())
    }
}

impl Drop for Faults {
    fn drop(&mut self) {
        self.restore();
        self.runtime.borrow_mut().active = Weak::new();
    }
}

/// Owns the base-chain HTTP/WS listeners and their connection tasks.
pub struct Proxy {
    runtime: Shared,
    endpoints: BaseEndpoints,
    listeners: [JoinHandle<()>; 2],
}

impl Proxy {
    pub async fn spawn(upstream: BaseEndpoints) -> Result<Self> {
        let runtime = Rc::new(RefCell::new(Runtime::default()));
        let http_listener = TcpListener::bind("127.0.0.1:0").await?;
        let ws_listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoints = BaseEndpoints {
            rpc_url: format!("http://{}", http_listener.local_addr()?).parse()?,
            ws_url: format!("ws://{}", ws_listener.local_addr()?).parse()?,
            grpc_url: upstream.grpc_url,
        };
        let listeners = [
            task::spawn_local(http::accept(
                http_listener,
                upstream.rpc_url,
                rpc_http::client(),
                runtime.clone(),
            )),
            task::spawn_local(ws::accept(ws_listener, upstream.ws_url, runtime.clone())),
        ];
        Ok(Self { runtime, endpoints, listeners })
    }

    pub fn endpoints(&self) -> BaseEndpoints {
        self.endpoints.clone()
    }

    pub fn scope(&self) -> Result<Faults> {
        let mut runtime = self.runtime.borrow_mut();
        ensure!(
            runtime.active.upgrade().is_none(),
            "proxy already has an active fault scope"
        );
        let state = Rc::new(RefCell::new(State {
            traps: Vec::new(),
            rules: Vec::new(),
            unfired: Vec::new(),
            observed: [0; 3],
            holding: 0,
            sequence: 0,
            reset: watch::channel(()).0,
            drained: Rc::default(),
        }));
        runtime.active = Rc::downgrade(&state);
        Ok(Faults {
            state,
            runtime: self.runtime.clone(),
        })
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        for listener in &self.listeners {
            listener.abort();
        }
        for (_, connection) in self.runtime.borrow_mut().connections.drain(..) {
            connection.abort();
        }
    }
}
