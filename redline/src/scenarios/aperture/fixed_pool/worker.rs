use std::{
    cell::RefCell,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use futures_util::{
    future::{self, try_join_all},
    stream, StreamExt, TryStreamExt,
};
use pubkey::Pubkey;
use redsuite_core::{
    prep,
    programs::REDLINE,
    random::{self, Rng},
    runner::{execute_for_raw, Pacing, SendTransaction},
    transport::observations::{ObservationClose, ObservationSocket},
    Api, Result,
};
use signature::Signature;
use tokio::{
    sync::{mpsc, watch, Semaphore},
    task::JoinSet,
    time::{self, MissedTickBehavior},
};
use url::Url;

use super::{
    observations::{Clock, Count, Latency, Sample, Tracker, EXPIRY},
    Profile, SEED, WRITABLE,
};

const RPC_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const STATUS_BATCH: usize = 256;
const STATUS_PARALLELISM: usize = 8;

type SharedTracker = Rc<RefCell<Tracker>>;
type SocketSlot = Rc<RefCell<Option<Rc<ObservationSocket>>>>;

#[derive(Clone)]
pub(super) struct Config {
    pub(super) profile: Profile,
    pub(super) rpc: Url,
    pub(super) ws: Url,
    pub(super) accounts: Arc<Vec<Pubkey>>,
    pub(super) payers: Arc<Vec<[u8; 64]>>,
    pub(super) stop: watch::Receiver<bool>,
}

pub(super) struct Outcome {
    pub(super) result: Result<()>,
    pub(super) samples: Vec<Sample>,
}

pub(super) async fn run(
    index: usize,
    config: Config,
    ready: mpsc::Sender<bool>,
    mut start: watch::Receiver<Option<Clock>>,
    worker_stop: watch::Receiver<bool>,
) -> Result<Option<Outcome>> {
    let profile = config.profile;
    let pool_size = profile.accounts / profile.workers;
    let accounts: Rc<[Pubkey]> = config.accounts[index * pool_size..(index + 1) * pool_size].into();
    let senders = prep::worker_senders(
        &config.rpc,
        &config.ws,
        &config.payers,
        index,
        profile.workers,
    );
    let api = Api::new(config.rpc.clone(), config.ws.clone());
    let tracker = Rc::new(RefCell::new(Tracker::default()));
    let routes = (0..profile.sockets)
        .map(|socket| {
            accounts
                .iter()
                .copied()
                .enumerate()
                .skip(socket)
                .step_by(profile.sockets)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let initialize = async {
        tokio::try_join!(
            try_join_all(
                routes.iter().map(|accounts| account_socket(&config.ws, accounts, &tracker))
            ),
            try_join_all((0..profile.sockets).map(|_| connect(&config.ws, &tracker))),
        )
    };
    let initialized = tokio::select! {
        _ = start.changed() => return Ok(None),
        result = initialize => result.with_context(|| format!("initializing benchmark worker {index}")),
    };
    let _ = ready.send(initialized.is_ok()).await;
    drop(ready);
    let (accounts_ws, signature_ws) = initialized?;
    let mut tasks = JoinSet::new();
    for (socket, route) in accounts_ws.into_iter().zip(routes) {
        tasks.spawn_local(restore_accounts(
            config.ws.clone(),
            socket,
            route,
            tracker.clone(),
        ));
    }
    let signatures: Rc<Vec<SocketSlot>> = Rc::new(
        signature_ws
            .into_iter()
            .map(|socket| Rc::new(RefCell::new(Some(socket))))
            .collect(),
    );
    for (socket_index, socket) in signatures.iter().cloned().enumerate() {
        tasks.spawn_local(restore_signatures(
            config.ws.clone(),
            socket,
            socket_index,
            profile.sockets,
            tracker.clone(),
        ));
    }
    tasks.spawn_local(poll_status(
        config.rpc.clone(),
        config.ws.clone(),
        tracker.clone(),
    ));
    tasks.spawn_local(expire(tracker.clone(), signatures.clone()));
    let Ok(clock) = start.wait_for(Option::is_some).await else {
        return Ok(None);
    };
    let clock = clock.expect("the coordinator published a measurement clock");
    tracker.borrow_mut().data.clock = Some(clock);

    let request_slots = Arc::new(Semaphore::new(profile.http_concurrency));
    let mut rng = random::rng(SEED, &["benchmark"], index as u64);
    let mut indices: Vec<_> = (0..pool_size).collect();
    let stop = worker_stop.clone();
    let requests = execute_for_raw(
        Pacing::PerSecond(profile.rate_per_worker),
        profile.concurrency,
        clock.end,
        worker_stop,
        |id| {
            let account = accounts[rng.gen_range(0..pool_size)];
            let targets = rng.gen_ratio(4, 5).then(|| {
                let mut mask = 0u128;
                let targets: [Pubkey; WRITABLE] = std::array::from_fn(|position| {
                    let other = rng.gen_range(position..pool_size);
                    indices.swap(position, other);
                    let selected = indices[position];
                    mask |= 1 << selected;
                    accounts[selected]
                });
                (targets, mask)
            });
            let sender = senders[rng.gen_range(0..senders.len())].clone();
            let socket = signatures[id as usize % profile.sockets].clone();
            let tracker = tracker.clone();
            let api = api.clone();
            let request_slots = request_slots.clone();
            let stop = stop.clone();
            let requested_stop = config.stop.clone();
            async move {
                let transaction = if let Some((targets, mask)) = targets {
                    let transaction =
                        time::timeout(RPC_TIMEOUT, sender.prepare(&[REDLINE.write(id, &targets)]))
                            .await
                            .context("preparing benchmark transaction timed out")??;
                    let signature = transaction.signatures[0];
                    let connection = subscribe_signature(&socket, id, &signature, &tracker).await?;
                    Some((transaction, signature, connection, mask))
                } else {
                    None
                };
                let permit =
                    request_slots.acquire_owned().await.context("benchmark HTTP pool closed")?;
                Ok(SendTransaction(move |sent: Instant| async move {
                    let _permit = permit;
                    if sent >= clock.end || *stop.borrow() || *requested_stop.borrow() {
                        if let Some((_, _, connection, _)) = transaction {
                            unsubscribe_signature(connection, id).await;
                        }
                        return Ok(());
                    }
                    if let Some((transaction, signature, connection, mask)) = transaction {
                        let current = socket.borrow().as_ref().is_some_and(|current| {
                            Rc::ptr_eq(current, &connection) && connection.is_open()
                        });
                        if !current {
                            tracker.borrow_mut().data.count(Count::WriteSkipped, sent, sent, 1);
                            return Err(anyhow!("signature connection closed before submission"));
                        }
                        tracker.borrow_mut().submitted(id, signature, sent, mask);
                        let result =
                            time::timeout(RPC_TIMEOUT, sender.submit_prepared(&transaction))
                                .await
                                .context("sendTransaction timed out")
                                .flatten();
                        tracker.borrow_mut().http_done(id, Instant::now(), result.is_ok());
                        result.map(|_| ())
                    } else {
                        let result = time::timeout(RPC_TIMEOUT, api.get_account(&account))
                            .await
                            .context("getAccountInfo timed out")
                            .flatten()
                            .and_then(|account| account.context("benchmark account was missing"));
                        let ok = result.is_ok();
                        tracker.borrow_mut().data.record(
                            sent,
                            Instant::now(),
                            Some(if ok { Count::Read } else { Count::ReadError }),
                            ok.then_some(Latency::Read),
                        );
                        result.map(|_| ())
                    }
                }))
            }
        },
    )
    .await;

    let drain = time::timeout(EXPIRY + RPC_TIMEOUT, async {
        while tracker.borrow().pending() {
            time::sleep(POLL_INTERVAL).await;
        }
    })
    .await;
    tasks.shutdown().await;
    let result = requests.and_then(|_| drain.context("benchmark observations did not drain"));
    let samples = std::mem::take(&mut tracker.borrow_mut().data.samples);
    Ok(Some(Outcome { result, samples }))
}

async fn subscribe_signature(
    slot: &SocketSlot,
    id: u64,
    signature: &Signature,
    tracker: &SharedTracker,
) -> Result<Rc<ObservationSocket>> {
    let started = Instant::now();
    let connection = slot.borrow().clone();
    let Some(connection) = connection else {
        tracker.borrow_mut().data.count(Count::WriteSkipped, started, started, 1);
        return Err(anyhow!("signature connection is reconnecting"));
    };
    let ack = time::timeout(RPC_TIMEOUT, connection.signature(id, signature)).await;
    let now = Instant::now();
    if !matches!(ack, Ok(Ok(()))) {
        let data = &mut tracker.borrow_mut().data;
        data.count(Count::AckError, started, now, 1);
        data.count(Count::WriteSkipped, started, now, 1);
        tokio::task::spawn_local(unsubscribe_signature(connection, id));
        return Err(anyhow!("signature subscription ACK failed: {ack:?}"));
    }
    tracker.borrow_mut().data.record(started, now, None, Some(Latency::Ack));
    Ok(connection)
}

async fn connect(ws: &Url, tracker: &SharedTracker) -> Result<Rc<ObservationSocket>> {
    let tracker = tracker.clone();
    let socket = time::timeout(
        RPC_TIMEOUT,
        ObservationSocket::connect(ws, move |event| {
            tracker.borrow_mut().observe(event, Instant::now());
        }),
    )
    .await
    .context("benchmark WebSocket connect timed out")??;
    Ok(Rc::new(socket))
}

async fn account_socket(
    ws: &Url,
    accounts: &[(usize, Pubkey)],
    tracker: &SharedTracker,
) -> Result<Rc<ObservationSocket>> {
    let socket = connect(ws, tracker).await?;
    time::timeout(
        RPC_TIMEOUT,
        try_join_all(accounts.iter().map(|(index, account)| socket.account(*index, account))),
    )
    .await
    .context("benchmark account subscription timed out")??;
    Ok(socket)
}

async fn restore_accounts(
    ws: Url,
    mut socket: Rc<ObservationSocket>,
    accounts: Vec<(usize, Pubkey)>,
    tracker: SharedTracker,
) {
    loop {
        disconnected(&tracker, socket.closed().await);
        socket = reconnect(async || account_socket(&ws, &accounts, &tracker).await).await;
        event(&tracker, Count::Reconnect);
    }
}

async fn restore_signatures(
    ws: Url,
    slot: SocketSlot,
    index: usize,
    sockets: usize,
    tracker: SharedTracker,
) {
    let mut socket = slot.borrow().clone().expect("initial signature socket");
    let mut cleanup = JoinSet::new();
    loop {
        let reason = socket.closed().await;
        slot.borrow_mut().take();
        disconnected(&tracker, reason);
        let (replacement, restored) =
            reconnect(async || signature_socket(&ws, index, sockets, &tracker).await).await;
        socket = replacement;
        while cleanup.try_join_next().is_some() {}
        for id in restored {
            if !tracker.borrow().signature_pending(id) {
                cleanup.spawn_local(unsubscribe_signature(socket.clone(), id));
            }
        }
        slot.borrow_mut().replace(socket.clone());
        event(&tracker, Count::Reconnect);
    }
}

async fn reconnect<T>(mut connect: impl AsyncFnMut() -> Result<T>) -> T {
    loop {
        if let Ok(socket) = connect().await {
            return socket;
        }
        time::sleep(POLL_INTERVAL).await;
    }
}

async fn signature_socket(
    ws: &Url,
    index: usize,
    sockets: usize,
    tracker: &SharedTracker,
) -> Result<(Rc<ObservationSocket>, Vec<u64>)> {
    let socket = connect(ws, tracker).await?;
    let pending = tracker.borrow().restore(index, sockets);
    let connection = &socket;
    let restored = stream::iter(pending)
        .map(|(id, signature)| async move {
            if !tracker.borrow().signature_pending(id) {
                return Ok(None);
            }
            time::timeout(RPC_TIMEOUT, connection.signature(id, &signature))
                .await
                .context("signature restore ACK timed out")??;
            Ok::<_, anyhow::Error>(Some(id))
        })
        .buffer_unordered(128)
        .try_filter_map(future::ok)
        .try_collect()
        .await?;
    Ok((socket, restored))
}

fn disconnected(tracker: &SharedTracker, reason: ObservationClose) {
    event(tracker, Count::Disconnect);
    match reason {
        ObservationClose::GoingAway => event(tracker, Count::GoingAway),
        ObservationClose::Transport => event(tracker, Count::TransportDisconnected),
        ObservationClose::Other => {}
    }
}

fn event(tracker: &SharedTracker, counter: Count) {
    let now = Instant::now();
    tracker.borrow_mut().data.count(counter, now, now, 1);
}

async fn poll_status(rpc: Url, ws: Url, tracker: SharedTracker) {
    let api = Api::new(rpc, ws);
    let mut interval = time::interval(POLL_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let pending = tracker.borrow().statuses(Instant::now());
        stream::iter(pending.chunks(STATUS_BATCH))
            .for_each_concurrent(STATUS_PARALLELISM, |batch| {
                let api = &api;
                let tracker = &tracker;
                async move {
                    let signatures: Vec<_> =
                        batch.iter().map(|(_, signature)| *signature).collect();
                    let results = match time::timeout(
                        RPC_TIMEOUT,
                        api.get_signature_statuses_with_history(&signatures),
                    )
                    .await
                    {
                        Ok(Ok(results)) if results.len() == batch.len() => results,
                        _ => {
                            event(tracker, Count::StatusRpcError);
                            return;
                        }
                    };
                    let now = Instant::now();
                    let mut tracker = tracker.borrow_mut();
                    for ((id, _), result) in batch.iter().zip(results) {
                        if let Some(status) = result {
                            tracker.status(*id, status.err.is_none(), now);
                        }
                    }
                }
            })
            .await;
    }
}

async fn expire(tracker: SharedTracker, slots: Rc<Vec<SocketSlot>>) {
    let mut interval = time::interval(POLL_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut cleanup = JoinSet::new();
    loop {
        interval.tick().await;
        while cleanup.try_join_next().is_some() {}
        for id in tracker.borrow_mut().reap(Instant::now()) {
            let socket = slots[id as usize % slots.len()].borrow().clone();
            if let Some(socket) = socket {
                cleanup.spawn_local(unsubscribe_signature(socket, id));
            }
        }
    }
}

async fn unsubscribe_signature(socket: Rc<ObservationSocket>, id: u64) {
    let _ = time::timeout(RPC_TIMEOUT, socket.unsubscribe_signature(id)).await;
}
