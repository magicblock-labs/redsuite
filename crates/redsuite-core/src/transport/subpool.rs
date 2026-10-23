use std::{
    collections::HashMap,
    ops::ControlFlow::{self, Continue},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context};
use futures_util::future::join_all;
use json::LazyValue;
use pubkey::Pubkey;
use tokio::{
    sync::{mpsc, watch},
    time,
};
use url::Url;

use super::{
    clock::SendClock,
    socket::{self, CloseReason},
    subscription::{self, Router, SubscriptionState, Topic},
    NETWORK_TIMEOUT,
};
use crate::{
    runner::{spawn_workers, Workers},
    stats::StreamingStats,
    Result,
};

#[derive(Default)]
struct AccountRecv {
    count: usize,
    seen: Vec<u64>,
}

impl AccountRecv {
    fn mark(&mut self, index: usize, capacity: usize) -> bool {
        if self.seen.is_empty() {
            self.seen = vec![0; capacity.div_ceil(64)];
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
}

#[derive(Debug, Default)]
pub struct ConnReport {
    pub received: u64,
    pub over_threshold: u64,
    pub lag: StreamingStats,
    pub missing_final: usize,
    pub incomplete: usize,
    pub error: Option<String>,
}

pub type Extractor = Arc<dyn Fn(&[u8]) -> Option<u64> + Send + Sync>;
pub type ExpectedWrites = HashMap<Pubkey, Vec<u64>>;

enum Event {
    Ready,
    Complete,
    Failed(String),
}

pub struct SubscriberPool {
    workers: Workers<Vec<ConnReport>>,
    events: mpsc::UnboundedReceiver<Event>,
    connections: usize,
    ready: usize,
    complete: usize,
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
        // Each connection sends at most one readiness, completion, and failure event.
        let (events, receiver) = mpsc::unbounded_channel();
        let subscriptions = Arc::new(Subscriptions {
            url: url.clone(),
            accounts: accounts.to_vec(),
            clock,
            expected,
            extractor,
            threshold_micros: lag_threshold.as_micros() as u64,
            events,
        });
        let workers = spawn_workers(threads.clamp(1, connections.max(1)), move |worker| {
            let subscriptions = subscriptions.clone();
            async move {
                let connections = (worker.index..connections)
                    .step_by(worker.threads)
                    .map(|index| subscriptions.connect(index, worker.stop_signal()));
                Ok(join_all(connections).await)
            }
        });
        Self {
            workers,
            events: receiver,
            connections,
            ready: 0,
            complete: 0,
        }
    }

    async fn wait(&mut self, final_writes: bool) -> Result<()> {
        while (if final_writes { self.complete } else { self.ready }) < self.connections {
            match self.events.recv().await.context("subscriber workers stopped")? {
                Event::Ready => self.ready += 1,
                Event::Complete => self.complete += 1,
                Event::Failed(error) => return Err(anyhow!("subscriber pool: {error}")),
            }
        }
        Ok(())
    }

    pub async fn await_subscribed(&mut self) -> Result<()> {
        time::timeout(NETWORK_TIMEOUT, self.wait(false))
            .await
            .context("pool subscriptions")?
    }

    pub async fn await_final(&mut self) {
        // Final reports retain missing writes and failures even when the wait times out.
        let _ = time::timeout(NETWORK_TIMEOUT, self.wait(true)).await;
    }

    pub async fn finalize(self) -> Result<Vec<ConnReport>> {
        Ok(self.workers.join().await?.into_iter().flatten().collect())
    }
}

struct PoolObserver {
    subscriptions: Arc<Subscriptions>,
    index: usize,
    by_account: HashMap<Pubkey, AccountRecv>,
    report: ConnReport,
}

impl SubscriptionState for PoolObserver {
    type Key = Pubkey;

    fn on_subscribed(&mut self, count: usize) {
        if count == self.subscriptions.accounts.len() {
            let _ = self.subscriptions.events.send(Event::Ready);
        }
    }

    fn on_close(&mut self, reason: &CloseReason) {
        self.fail(reason.to_string());
    }
}

impl PoolObserver {
    fn fail(&mut self, error: String) {
        let _ = self
            .subscriptions
            .events
            .send(Event::Failed(format!("conn {}: {error}", self.index)));
        self.report.error = Some(error);
    }

    fn observe(&mut self, account: Pubkey, payload: &LazyValue<'_>) -> ControlFlow<()> {
        let Some(data) = subscription::account_data(payload) else {
            return Continue(());
        };
        let Some(id) = (self.subscriptions.extractor)(&data) else {
            return Continue(());
        };
        let Some(lag_micros) = self.subscriptions.clock.elapsed_micros(id) else {
            return Continue(());
        };
        let Some(write_ids) = self.subscriptions.expected.get(&account) else {
            return Continue(());
        };
        let Ok(index) = write_ids.binary_search(&id) else {
            return Continue(());
        };
        let recv = self.by_account.entry(account).or_default();
        if !recv.mark(index, write_ids.len()) {
            return Continue(());
        }
        if recv.count == write_ids.len() {
            self.report.incomplete -= 1;
        }
        if index == write_ids.len() - 1 {
            self.report.missing_final -= 1;
            if self.report.missing_final == 0 {
                let _ = self.subscriptions.events.send(Event::Complete);
            }
        }
        self.report.received += 1;
        self.report.over_threshold +=
            u64::from(u64::from(lag_micros) > self.subscriptions.threshold_micros);
        self.report.lag.push(lag_micros);
        Continue(())
    }
}

struct Subscriptions {
    url: Url,
    accounts: Vec<Pubkey>,
    clock: Arc<SendClock>,
    expected: Arc<ExpectedWrites>,
    extractor: Extractor,
    threshold_micros: u64,
    events: mpsc::UnboundedSender<Event>,
}

impl Subscriptions {
    async fn connect(
        self: &Arc<Self>,
        index: usize,
        mut shutdown: watch::Receiver<bool>,
    ) -> ConnReport {
        let report = ConnReport {
            missing_final: self.expected.values().filter(|ids| !ids.is_empty()).count(),
            incomplete: self.expected.len(),
            ..ConnReport::default()
        };
        if report.missing_final == 0 {
            let _ = self.events.send(Event::Complete);
        }
        let handler = PoolObserver {
            subscriptions: self.clone(),
            index,
            by_account: HashMap::new(),
            report,
        };
        let mut router = Router::new(handler);
        let result: Result<()> = tokio::select! {
            _ = shutdown.wait_for(|stop| *stop) => Ok(()),
            result = async {
                let mut socket = socket::connect(&self.url).await?;
                for (index, account) in self.accounts.iter().enumerate() {
                    let id = index as u64 + 1;
                    router.register(id, *account);
                    socket::send(&mut socket, id, Topic::Account.subscribe(),
                        &subscription::account_params(account)).await?;
                }
                let reason = socket::drive(&mut socket, |frame| router.dispatch(frame, &mut PoolObserver::observe)).await;
                router.close(&reason);
                Ok(())
            } => result,
        };
        if let Err(error) = result {
            router.state.fail(format!("{error:#}"));
        }
        router.state.report
    }
}
