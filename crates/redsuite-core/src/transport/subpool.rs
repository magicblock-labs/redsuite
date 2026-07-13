use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use json::Deserialize;
use pubkey::Pubkey;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    stats::{ObservationsStats, StreamingStats},
    Result,
};

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

#[derive(Debug, Clone, Copy, Default)]
pub struct AccountRecv {
    pub count: u64,
    pub max_id: u64,
}

#[derive(Debug)]
pub struct ConnReport {
    pub received: u64,
    pub over_threshold: u64,
    pub lag: ObservationsStats,
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
    lag_final: Option<ObservationsStats>,
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

    pub fn missing_final(&self, final_ids: &HashMap<Pubkey, u64>) -> usize {
        self.states
            .iter()
            .map(|state| {
                let state = state.lock().unwrap();
                final_ids
                    .iter()
                    .filter(|(account, final_id)| {
                        state
                            .by_account
                            .get(account)
                            .map(|recv| recv.max_id < **final_id)
                            .unwrap_or(true)
                    })
                    .count()
            })
            .sum()
    }

    pub async fn await_final(
        &self,
        final_ids: &HashMap<Pubkey, u64>,
        timeout: Duration,
    ) -> usize {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let missing = self.missing_final(final_ids);
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
                    lag: state.lag_final.unwrap_or_default(),
                    by_account: state.by_account,
                    error: state.error,
                }
            })
            .collect()
    }
}

#[derive(Deserialize)]
struct Frame {
    method: Option<String>,
    id: Option<u64>,
    result: Option<u64>,
    error: Option<json::Value>,
    params: Option<NotificationParams>,
}

#[derive(Deserialize)]
struct NotificationParams {
    subscription: u64,
    result: NotificationResult,
}

#[derive(Deserialize)]
struct NotificationResult {
    value: AccountValue,
}

#[derive(Deserialize)]
struct AccountValue {
    data: (String, String),
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
    let fail = |message: String| {
        state.lock().unwrap().error = Some(message);
    };
    let socket = match connect_async(&url).await {
        Ok((socket, _)) => socket,
        Err(error) => {
            fail(format!("{url}: {error}"));
            return;
        }
    };
    let (mut sink, mut stream) = socket.split();

    let mut account_by_req: HashMap<u64, Pubkey> = HashMap::new();
    for (account_index, account) in accounts.iter().enumerate() {
        let req_id = account_index as u64 + 1;
        account_by_req.insert(req_id, *account);
        let message = format!(
            r#"{{"jsonrpc":"2.0","id":{req_id},"method":"accountSubscribe","params":["{account}",{{"encoding":"base64","commitment":"confirmed"}}]}}"#
        );
        if let Err(error) = sink.send(Message::Text(message.into())).await {
            fail(format!("accountSubscribe send: {error}"));
            return;
        }
    }

    let mut account_by_subid: HashMap<u64, Pubkey> = HashMap::new();
    let mut local_lag = StreamingStats::new();
    let mut stopped = false;
    while !stopped {
        tokio::select! {
            _ = shutdown.changed() => stopped = true,
            incoming = stream.next() => {
                let text = match incoming {
                    Some(Ok(Message::Text(text))) => text,
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        fail("connection closed".to_owned());
                        break;
                    }
                    Some(Ok(_)) => continue,
                };
                let Ok(frame) = json::from_str::<Frame>(&text) else {
                    continue;
                };
                if let Some(error) = frame.error {
                    fail(error.to_string());
                    break;
                }
                match (frame.method.as_deref(), frame.params, frame.id) {
                    (Some("accountNotification"), Some(params), _) => {
                        let Ok(data) = base64::engine::general_purpose::STANDARD
                            .decode(&params.result.value.data.0)
                        else {
                            continue;
                        };
                        let Some(id) = extractor(&data) else {
                            continue;
                        };
                        let Some(lag_micros) = produced.lag_micros(id) else {
                            continue;
                        };
                        let Some(account) =
                            account_by_subid.get(&params.subscription).copied()
                        else {
                            continue;
                        };
                        let expected_here = expected
                            .get(&account)
                            .map(|write_ids| {
                                write_ids.binary_search(&id).is_ok()
                            })
                            .unwrap_or(false);
                        if !expected_here {
                            continue;
                        }
                        {
                            let mut state = state.lock().unwrap();
                            let recv =
                                state.by_account.entry(account).or_default();
                            if id <= recv.max_id {
                                continue;
                            }
                            recv.count += 1;
                            recv.max_id = id;
                            state.received += 1;
                            if lag_micros > threshold_micros {
                                state.over_threshold += 1;
                            }
                        }
                        local_lag.push(lag_micros.min(u32::MAX as u64) as u32);
                    }
                    (None, _, Some(req_id)) => {
                        let mut state = state.lock().unwrap();
                        if let (Some(account), Some(subid)) =
                            (account_by_req.remove(&req_id), frame.result)
                        {
                            account_by_subid.insert(subid, account);
                        }
                        state.ready_subs += 1;
                    }
                    _ => {}
                }
            }
        }
    }
    state.lock().unwrap().lag_final = Some(local_lag.finalize(false));
}
