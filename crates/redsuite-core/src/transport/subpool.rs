use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use json::LazyValue;
use pubkey::Pubkey;
use tokio::sync::watch;

use super::{
    conn::{self, CloseReason, FrameHandler},
    ws::account_update_data,
};
use crate::{stats::StreamingStats, Result};

const AWAIT_POLL: Duration = Duration::from_millis(20);

pub struct ProducedLedger {
    epoch: Instant,
    first_id: u64,
    sent_micros: Vec<AtomicU64>,
}

impl ProducedLedger {
    pub fn new(first_id: u64, capacity: usize) -> Self {
        let mut sent_micros = Vec::with_capacity(capacity);
        sent_micros.resize_with(capacity, || AtomicU64::new(0));
        Self {
            epoch: Instant::now(),
            first_id,
            sent_micros,
        }
    }

    fn slot_of(&self, id: u64) -> Option<&AtomicU64> {
        let index = id.checked_sub(self.first_id)?;
        self.sent_micros.get(index as usize)
    }

    pub fn record(&self, id: u64) {
        let slot = self
            .slot_of(id)
            .unwrap_or_else(|| panic!("id {id} outside the ledger range"));
        let stamp = self.epoch.elapsed().as_micros() as u64 + 1;
        slot.store(stamp, Ordering::Release);
    }

    pub fn lag_micros(&self, id: u64) -> Option<u64> {
        let stamp = self.slot_of(id)?.load(Ordering::Acquire);
        if stamp == 0 {
            return None;
        }
        let now = self.epoch.elapsed().as_micros() as u64;
        Some(now.saturating_sub(stamp - 1))
    }
}

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
        self.seen
            .get(index / 64)
            .map(|word| word & (1u64 << (index % 64)) != 0)
            .unwrap_or(false)
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
struct ConnState {
    ready_subs: usize,
    error: Option<String>,
    received: u64,
    over_threshold: u64,
    by_account: HashMap<Pubkey, AccountRecv>,
    lag: Option<StreamingStats>,
}

pub type Extractor = Arc<dyn Fn(&[u8]) -> Option<u64> + Send + Sync>;

pub type ExpectedWrites = HashMap<Pubkey, Vec<u64>>;

pub struct SubscriberPool {
    states: Vec<Arc<Mutex<ConnState>>>,
    threads: Vec<std::thread::JoinHandle<()>>,
    shutdown: watch::Sender<bool>,
}

impl SubscriberPool {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        url: &str,
        accounts: &[Pubkey],
        connections: usize,
        threads: usize,
        produced: Arc<ProducedLedger>,
        expected: Arc<ExpectedWrites>,
        extractor: Extractor,
        lag_threshold: Duration,
    ) -> Self {
        let states: Vec<Arc<Mutex<ConnState>>> = (0..connections)
            .map(|_| Arc::new(Mutex::new(ConnState::default())))
            .collect();
        let (shutdown, _) = watch::channel(false);
        let thread_count = threads.clamp(1, connections.max(1));
        let accounts: Arc<Vec<Pubkey>> = Arc::new(accounts.to_vec());
        let threshold_micros = lag_threshold.as_micros() as u64;

        let mut handles = Vec::with_capacity(thread_count);
        for thread_index in 0..thread_count {
            let my_states: Vec<Arc<Mutex<ConnState>>> = states
                .iter()
                .enumerate()
                .filter(|(conn_index, _)| {
                    conn_index % thread_count == thread_index
                })
                .map(|(_, state)| state.clone())
                .collect();
            let url = url.to_owned();
            let accounts = accounts.clone();
            let produced = produced.clone();
            let expected = expected.clone();
            let extractor = extractor.clone();
            let shutdown_rx = shutdown.subscribe();
            handles.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("subscriber runtime build is infallible");
                let local = tokio::task::LocalSet::new();
                runtime.block_on(local.run_until(async move {
                    let conn_tasks: Vec<_> = my_states
                        .into_iter()
                        .map(|state| {
                            tokio::task::spawn_local(run_connection(
                                url.clone(),
                                accounts.clone(),
                                state,
                                produced.clone(),
                                expected.clone(),
                                extractor.clone(),
                                shutdown_rx.clone(),
                                threshold_micros,
                            ))
                        })
                        .collect();
                    for task in conn_tasks {
                        let _ = task.await;
                    }
                }));
            }));
        }
        Self {
            states,
            threads: handles,
            shutdown,
        }
    }

    pub fn first_error(&self) -> Option<String> {
        self.states
            .iter()
            .enumerate()
            .find_map(|(conn_index, state)| {
                state
                    .lock()
                    .unwrap()
                    .error
                    .as_ref()
                    .map(|error| format!("conn {conn_index}: {error}"))
            })
    }

    pub async fn await_subscribed(
        &self,
        per_connection: usize,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(error) = self.first_error() {
                return Err(format!("subscriber pool: {error}").into());
            }
            let ready = self.states.iter().all(|state| {
                state.lock().unwrap().ready_subs >= per_connection
            });
            if ready {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for pool subscriptions ({timeout:?})"
                )
                .into());
            }
            tokio::time::sleep(AWAIT_POLL).await;
        }
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
                            .map(|recv| (recv.count as usize) < write_ids.len())
                            .unwrap_or(true)
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
                                .map(|recv| recv.has(write_ids.len() - 1))
                                .unwrap_or(false)
                    })
                    .count()
            })
            .sum()
    }

    pub async fn await_final(
        &self,
        expected: &ExpectedWrites,
        timeout: Duration,
    ) -> usize {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let missing = self.missing_final(expected);
            if missing == 0 || tokio::time::Instant::now() >= deadline {
                return missing;
            }
            tokio::time::sleep(AWAIT_POLL).await;
        }
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
                    .unwrap_or_else(|_| {
                        panic!("subscriber thread still holds a conn state")
                    })
                    .into_inner()
                    .unwrap();
                ConnReport {
                    received: state.received,
                    over_threshold: state.over_threshold,
                    lag: state.lag.unwrap_or_default(),
                    by_account: state.by_account,
                    error: state.error,
                }
            })
            .collect()
    }
}

struct PoolHandler {
    state: Arc<Mutex<ConnState>>,
    produced: Arc<ProducedLedger>,
    expected: Arc<ExpectedWrites>,
    extractor: Extractor,
    threshold_micros: u64,
    account_by_req: HashMap<u64, Pubkey>,
    account_by_subid: HashMap<u64, Pubkey>,
    local_lag: StreamingStats,
}

impl FrameHandler for PoolHandler {
    fn on_reply(&mut self, id: u64, result: Option<&LazyValue<'_>>) {
        if let (Some(account), Some(subid)) =
            (self.account_by_req.remove(&id), conn::reply_u64(result))
        {
            self.account_by_subid.insert(subid, account);
        }
        self.state.lock().unwrap().ready_subs += 1;
    }

    fn on_notification(
        &mut self,
        method: &str,
        subscription: u64,
        payload: &LazyValue<'_>,
    ) {
        if method != "accountNotification" {
            return;
        }
        let Some(data) = account_update_data(payload) else {
            return;
        };
        let Some(id) = (self.extractor)(&data) else {
            return;
        };
        let Some(lag_micros) = self.produced.lag_micros(id) else {
            return;
        };
        let Some(account) = self.account_by_subid.get(&subscription).copied()
        else {
            return;
        };
        let Some(write_ids) = self.expected.get(&account) else {
            return;
        };
        let Ok(index) = write_ids.binary_search(&id) else {
            return;
        };
        {
            let mut state = self.state.lock().unwrap();
            let recv = state.by_account.entry(account).or_default();
            if !recv.mark(index, write_ids.len()) {
                return;
            }
            state.received += 1;
            if lag_micros > self.threshold_micros {
                state.over_threshold += 1;
            }
        }
        self.local_lag.push(lag_micros.min(u32::MAX as u64) as u32);
    }

    fn on_closed(&mut self, reason: &CloseReason) {
        self.state.lock().unwrap().error = Some(reason.to_string());
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_connection(
    url: String,
    accounts: Arc<Vec<Pubkey>>,
    state: Arc<Mutex<ConnState>>,
    produced: Arc<ProducedLedger>,
    expected: Arc<ExpectedWrites>,
    extractor: Extractor,
    mut shutdown: watch::Receiver<bool>,
    threshold_micros: u64,
) {
    let socket = match conn::connect(&url).await {
        Ok(socket) => socket,
        Err(error) => {
            state.lock().unwrap().error = Some(error.to_string());
            return;
        }
    };
    let (requester, mut stream) = conn::split(socket);
    let mut handler = PoolHandler {
        state: state.clone(),
        produced,
        expected,
        extractor,
        threshold_micros,
        account_by_req: HashMap::new(),
        account_by_subid: HashMap::new(),
        local_lag: StreamingStats::new(),
    };

    for account in accounts.iter() {
        let req_id = requester.mint();
        handler.account_by_req.insert(req_id, *account);
        if let Err(error) = requester
            .send(req_id, "accountSubscribe", &conn::account_params(account))
            .await
        {
            state.lock().unwrap().error = Some(error.to_string());
            return;
        }
    }

    tokio::select! {
        _ = shutdown.changed() => {}
        _ = conn::drive(&mut stream, &mut handler) => {}
    }
    state.lock().unwrap().lag = Some(std::mem::take(&mut handler.local_lag));
}
