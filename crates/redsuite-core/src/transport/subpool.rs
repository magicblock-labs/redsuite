use std::{
    collections::HashMap,
    mem,
    ops::ControlFlow::{self, Continue},
    sync::{Arc, Mutex},
    thread,
    thread::JoinHandle,
    time::Duration,
};

use anyhow::anyhow;
use json::LazyValue;
use pubkey::Pubkey;
use tokio::{
    runtime::Builder,
    sync::{watch, Notify},
    task::LocalSet,
};
use tracing::Span;
use url::Url;

use super::{
    clock::SendClock,
    socket::{self, CloseReason},
    subscription::{self, Router, SubscriptionState, Topic},
    NETWORK_TIMEOUT,
};

use crate::{stats::StreamingStats, wait, Result};

#[derive(Debug, Clone, Default)]
pub struct AccountRecv {
    pub count: u64,
    seen: Vec<u64>,
}

impl AccountRecv {
    fn mark(&mut self, index: usize, capacity: usize) -> bool {
        if self.seen.is_empty() {
            self.seen = vec![0u64; capacity.div_ceil(64)];
        }
        let word = index / 64;
        let bit = 1u64 << (index % 64);
        if self.seen[word] & bit != 0 {
            return false;
        }
        self.seen[word] |= bit;
        self.count += 1;
        true
    }

    fn has(&self, index: usize) -> bool {
        self.seen.get(index / 64).is_some_and(|word| word & (1u64 << (index % 64)) != 0)
    }
}

#[derive(Debug)]
pub struct ConnReport {
    pub received: u64,
    pub over_threshold: u64,
    pub lag: StreamingStats,
    pub by_account: HashMap<Pubkey, AccountRecv>,
    pub error: Option<String>,
}

#[derive(Default)]
struct ConnectionState {
    subscribed: usize,
    error: Option<String>,
    received: u64,
    over_threshold: u64,
    by_account: HashMap<Pubkey, AccountRecv>,
    lag: StreamingStats,
}

pub type Extractor = Arc<dyn Fn(&[u8]) -> Option<u64> + Send + Sync>;

pub type ExpectedWrites = HashMap<Pubkey, Vec<u64>>;

pub struct SubscriberPool {
    states: Vec<Arc<Mutex<ConnectionState>>>,
    threads: Vec<JoinHandle<()>>,
    shutdown: watch::Sender<bool>,
    changed: Arc<Notify>,
}

impl SubscriberPool {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        url: &Url,
        accounts: &[Pubkey],
        connections: usize,
        threads: usize,
        clock: Arc<SendClock>,
        expected: Arc<ExpectedWrites>,
        extractor: Extractor,
        lag_threshold: Duration,
    ) -> Self {
        let states: Vec<Arc<Mutex<ConnectionState>>> = (0..connections)
            .map(|_| Arc::new(Mutex::new(ConnectionState::default())))
            .collect();
        let (shutdown, _) = watch::channel(false);
        let changed = Arc::new(Notify::new());
        let thread_count = threads.clamp(1, connections.max(1));
        let accounts: Arc<Vec<Pubkey>> = Arc::new(accounts.to_vec());
        let threshold_micros = lag_threshold.as_micros() as u64;

        let mut handles = Vec::with_capacity(thread_count);
        for thread_index in 0..thread_count {
            let thread_states: Vec<Arc<Mutex<ConnectionState>>> =
                states.iter().skip(thread_index).step_by(thread_count).cloned().collect();
            let subscriptions = Worker {
                url: url.clone(),
                accounts: accounts.clone(),
                clock: clock.clone(),
                expected: expected.clone(),
                extractor: extractor.clone(),
                threshold_micros,
                changed: changed.clone(),
            };
            let shutdown_rx = shutdown.subscribe();
            let span = Span::current();
            handles.push(thread::spawn(move || {
                let _entered = span.enter();
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("subscriber runtime build is infallible");
                runtime.block_on(
                    LocalSet::new().run_until(subscriptions.run(thread_states, shutdown_rx)),
                );
            }));
        }
        Self {
            states,
            threads: handles,
            shutdown,
            changed,
        }
    }

    pub fn first_error(&self) -> Option<String> {
        self.states.iter().enumerate().find_map(|(conn_index, state)| {
            state
                .lock()
                .unwrap()
                .error
                .as_ref()
                .map(|error| format!("conn {conn_index}: {error:#}"))
        })
    }

    pub async fn await_subscribed(&self, per_connection: usize) -> Result<()> {
        wait::on_change(&self.changed, NETWORK_TIMEOUT, "pool subscriptions", || {
            if let Some(error) = self.first_error() {
                return Err(anyhow!("subscriber pool: {error:#}"));
            }
            Ok(self
                .states
                .iter()
                .all(|state| state.lock().unwrap().subscribed >= per_connection)
                .then_some(()))
        })
        .await
    }

    pub fn incomplete(&self, expected: &ExpectedWrites) -> usize {
        self.states
            .iter()
            .map(|state| {
                let state = state.lock().unwrap();
                expected
                    .iter()
                    .filter(|(account, write_ids)| {
                        state
                            .by_account
                            .get(account)
                            .is_none_or(|recv| (recv.count as usize) < write_ids.len())
                    })
                    .count()
            })
            .sum()
    }

    pub fn missing_final(&self, expected: &ExpectedWrites) -> usize {
        self.states
            .iter()
            .map(|state| {
                let state = state.lock().unwrap();
                expected
                    .iter()
                    .filter(|(account, write_ids)| {
                        !write_ids.is_empty()
                            && !state
                                .by_account
                                .get(account)
                                .is_some_and(|recv| recv.has(write_ids.len() - 1))
                    })
                    .count()
            })
            .sum()
    }

    pub async fn await_final(&self, expected: &ExpectedWrites) -> usize {
        wait::on_change(
            &self.changed,
            NETWORK_TIMEOUT,
            "final account writes",
            || {
                if let Some(error) = self.first_error() {
                    return Err(anyhow!("subscriber pool: {error}"));
                }
                Ok((self.missing_final(expected) == 0).then_some(0))
            },
        )
        .await
        .unwrap_or_else(|_| self.missing_final(expected))
    }

    pub fn finalize(self) -> Vec<ConnReport> {
        let _ = self.shutdown.send(true);
        for handle in self.threads {
            let _ = handle.join();
        }
        self.states
            .into_iter()
            .map(|state| {
                let state = Arc::try_unwrap(state)
                    .unwrap_or_else(|_| panic!("subscriber thread still holds a conn state"))
                    .into_inner()
                    .unwrap();
                ConnReport {
                    received: state.received,
                    over_threshold: state.over_threshold,
                    lag: state.lag,
                    by_account: state.by_account,
                    error: state.error,
                }
            })
            .collect()
    }
}

struct PoolObserver {
    state: Arc<Mutex<ConnectionState>>,
    clock: Arc<SendClock>,
    expected: Arc<ExpectedWrites>,
    extractor: Extractor,
    threshold_micros: u64,
    local_lag: StreamingStats,
    changed: Arc<Notify>,
}

impl SubscriptionState for PoolObserver {
    type Key = Pubkey;

    fn on_subscribed(&mut self) {
        self.state.lock().unwrap().subscribed += 1;
        self.changed.notify_waiters();
    }

    fn on_close(&mut self, reason: &CloseReason) {
        self.state.lock().unwrap().error = Some(reason.to_string());
        self.changed.notify_waiters();
    }
}

impl PoolObserver {
    fn observe(&mut self, account: Pubkey, payload: &LazyValue<'_>) -> ControlFlow<()> {
        let Some(data) = subscription::account_data(payload) else {
            return Continue(());
        };
        let Some(id) = (self.extractor)(&data) else {
            return Continue(());
        };
        let Some(lag_micros) = self.clock.elapsed_micros(id) else {
            return Continue(());
        };
        let Some(write_ids) = self.expected.get(&account) else {
            return Continue(());
        };
        let Ok(index) = write_ids.binary_search(&id) else {
            return Continue(());
        };
        {
            let mut state = self.state.lock().unwrap();
            let recv = state.by_account.entry(account).or_default();
            if !recv.mark(index, write_ids.len()) {
                return Continue(());
            }
            state.received += 1;
            if u64::from(lag_micros) > self.threshold_micros {
                state.over_threshold += 1;
            }
        }
        self.local_lag.push(lag_micros);
        self.changed.notify_waiters();
        Continue(())
    }
}

#[derive(Clone)]
struct Worker {
    url: Url,
    accounts: Arc<Vec<Pubkey>>,
    clock: Arc<SendClock>,
    expected: Arc<ExpectedWrites>,
    extractor: Extractor,
    threshold_micros: u64,
    changed: Arc<Notify>,
}
impl Worker {
    async fn run(self, states: Vec<Arc<Mutex<ConnectionState>>>, shutdown: watch::Receiver<bool>) {
        let mut tasks = Vec::with_capacity(states.len());
        for state in states {
            tasks.push(tokio::task::spawn_local(
                self.clone().connect(state, shutdown.clone()),
            ));
        }
        for task in tasks {
            let _ = task.await;
        }
    }
    async fn connect(
        self,
        state: Arc<Mutex<ConnectionState>>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let Self {
            url,
            accounts,
            clock,
            expected,
            extractor,
            threshold_micros,
            changed,
        } = self;
        let mut socket = match socket::connect(&url).await {
            Ok(socket) => socket,
            Err(error) => {
                state.lock().unwrap().error = Some(format!("{error:#}"));
                changed.notify_waiters();
                return;
            }
        };
        let handler = PoolObserver {
            state: state.clone(),
            clock,
            expected,
            extractor,
            threshold_micros,
            local_lag: StreamingStats::new(),
            changed: changed.clone(),
        };

        let mut router = Router::new(handler);
        for (index, account) in accounts.iter().enumerate() {
            let id = index as u64 + 1;
            router.register(id, *account);
            let params = subscription::account_params(account);
            if let Err(error) =
                socket::send(&mut socket, id, Topic::Account.subscribe(), &params).await
            {
                state.lock().unwrap().error = Some(format!("{error:#}"));
                changed.notify_waiters();
                return;
            }
        }

        let mut observe = PoolObserver::observe;
        tokio::select! {
            _ = shutdown.changed() => {}
            _ = async {
                let reason = socket::drive(&mut socket, |frame| router.dispatch(frame, &mut observe)).await;
                router.close(&reason);
            } => {}
        }
        state.lock().unwrap().lag = mem::take(&mut router.state.local_lag);
    }
}
