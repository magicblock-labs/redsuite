use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    time::{Duration, Instant},
};

use base64::Engine;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use json::Deserialize;
use pubkey::Pubkey;
use signature::Signature;
use tokio::{net::TcpStream, sync::oneshot};
use tokio_tungstenite::{
    connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};

use crate::{
    stats::{ObservationsStats, StreamingStats},
    Result,
};

const AWAIT_POLL: Duration = Duration::from_millis(20);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

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

#[derive(Debug)]
pub struct UpdateOutcome {
    pub lag: ObservationsStats,
    pub observed: usize,
    pub superseded: usize,
}

#[derive(Default)]
struct Shared {
    ready_subs: usize,
    error: Option<String>,
    subs_by_req: HashMap<u64, Pubkey>,
    account_by_subid: HashMap<u64, Pubkey>,
    pending: HashMap<u64, (Pubkey, Instant)>,
    // closed-loop per-id wake-ups, fired the moment an id settles
    waiters: HashMap<u64, oneshot::Sender<()>>,
    lag: StreamingStats,
    observed: usize,
    superseded: usize,
}

impl Shared {
    fn settle_waiter(&mut self, id: u64) {
        if let Some(tx) = self.waiters.remove(&id) {
            let _ = tx.send(());
        }
    }

    fn fail(&mut self, msg: impl Into<String>) {
        self.error = Some(msg.into());
        // dropping the senders errors every parked waiter
        self.waiters.clear();
    }
}

pub struct AccountUpdates {
    // tokio Mutex, not RefCell: subscribes hold the sink across an await
    sink: tokio::sync::Mutex<SplitSink<Socket, Message>>,
    shared: Rc<RefCell<Shared>>,
    reader: tokio::task::JoinHandle<()>,
    next_req_id: Cell<u64>,
}

impl AccountUpdates {
    pub async fn connect(
        url: &str,
        extractor: impl Fn(&[u8]) -> Option<u64> + 'static,
    ) -> Result<Self> {
        let (socket, _) = connect_async(url)
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        let (sink, stream) = socket.split();
        let shared = Rc::new(RefCell::new(Shared::default()));
        let reader = tokio::task::spawn_local(read_loop(
            stream,
            shared.clone(),
            extractor,
        ));
        Ok(Self {
            sink: tokio::sync::Mutex::new(sink),
            shared,
            reader,
            next_req_id: Cell::new(1),
        })
    }

    pub async fn account_subscribe(&self, pk: &Pubkey) -> Result<()> {
        let id = self.next_req_id.replace(self.next_req_id.get() + 1);
        self.shared.borrow_mut().subs_by_req.insert(id, *pk);
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"accountSubscribe","params":["{pk}",{{"encoding":"base64","commitment":"confirmed"}}]}}"#
        );
        self.sink
            .lock()
            .await
            .send(Message::Text(msg.into()))
            .await
            .map_err(|e| format!("accountSubscribe send: {e}"))?;
        Ok(())
    }

    pub async fn await_subscribed(
        &self,
        count: usize,
        timeout: Duration,
    ) -> Result<()> {
        self.await_shared(timeout, "subscription acks", |shared| {
            shared.ready_subs >= count
        })
        .await
    }

    pub fn track(&self, id: u64, account: Pubkey) {
        self.shared
            .borrow_mut()
            .pending
            .insert(id, (account, Instant::now()));
    }

    pub async fn await_observed(
        &self,
        count: usize,
        timeout: Duration,
    ) -> Result<()> {
        self.await_shared(timeout, "account updates", |shared| {
            shared.observed >= count
        })
        .await
    }

    // Writes coalesce per slot, so completion is "nothing pending" rather
    // than "every id observed": a newer id for the same account settles the
    // older ones as superseded.
    pub async fn await_settled(&self, timeout: Duration) -> Result<()> {
        self.await_shared(timeout, "pending account updates", |shared| {
            shared.pending.is_empty()
        })
        .await
    }

    // Event-driven single-id wait (closed loop) — resolves when the id is
    // observed or superseded; immediate if it already settled or was never
    // tracked. Wrap in a timeout at the call site.
    pub async fn await_id(&self, id: u64) -> Result<()> {
        let rx = {
            let mut shared = self.shared.borrow_mut();
            if let Some(err) = &shared.error {
                return Err(format!("ws stream: {err}").into());
            }
            if !shared.pending.contains_key(&id) {
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            shared.waiters.insert(id, tx);
            rx
        };
        rx.await
            .map_err(|_| "ws stream failed while awaiting update".into())
    }

    pub fn finalize(&self) -> UpdateOutcome {
        self.reader.abort();
        let mut shared = self.shared.borrow_mut();
        UpdateOutcome {
            lag: std::mem::take(&mut shared.lag).finalize(false),
            observed: shared.observed,
            superseded: shared.superseded,
        }
    }

    async fn await_shared(
        &self,
        timeout: Duration,
        what: &str,
        done: impl Fn(&Shared) -> bool,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            {
                let shared = self.shared.borrow();
                if let Some(err) = &shared.error {
                    return Err(format!("ws stream: {err}").into());
                }
                if done(&shared) {
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for {what} ({timeout:?})"
                )
                .into());
            }
            tokio::time::sleep(AWAIT_POLL).await;
        }
    }
}

async fn read_loop(
    mut stream: SplitStream<Socket>,
    shared: Rc<RefCell<Shared>>,
    extractor: impl Fn(&[u8]) -> Option<u64>,
) {
    while let Some(msg) = stream.next().await {
        let text = match msg {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) | Err(_) => {
                shared.borrow_mut().fail("connection closed");
                return;
            }
            Ok(_) => continue,
        };
        let Ok(frame) = json::from_str::<Frame>(&text) else {
            continue;
        };
        if let Some(error) = frame.error {
            shared.borrow_mut().fail(error.to_string());
            return;
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
                let mut shared = shared.borrow_mut();
                if let Some((_, sent)) = shared.pending.remove(&id) {
                    shared.lag.push(sent.elapsed().as_micros() as u32);
                    shared.observed += 1;
                    shared.settle_waiter(id);
                }
                if let Some(account) =
                    shared.account_by_subid.get(&params.subscription).copied()
                {
                    let settled: Vec<u64> = shared
                        .pending
                        .iter()
                        .filter(|(&pending_id, (acc, _))| {
                            *acc == account && pending_id < id
                        })
                        .map(|(&pending_id, _)| pending_id)
                        .collect();
                    for pending_id in settled {
                        shared.pending.remove(&pending_id);
                        shared.superseded += 1;
                        shared.settle_waiter(pending_id);
                    }
                }
            }
            (None, _, Some(req_id)) => {
                let mut shared = shared.borrow_mut();
                if let (Some(account), Some(subid)) =
                    (shared.subs_by_req.remove(&req_id), frame.result)
                {
                    shared.account_by_subid.insert(subid, account);
                }
                shared.ready_subs += 1;
            }
            _ => {}
        }
    }
    shared.borrow_mut().fail("stream ended");
}

// ---- signature confirmations -------------------------------------------

#[derive(Deserialize)]
struct SigFrame {
    method: Option<String>,
    id: Option<u64>,
    result: Option<u64>,
    error: Option<json::Value>,
    params: Option<SigParams>,
}

#[derive(Deserialize)]
struct SigParams {
    subscription: u64,
    result: SigResult,
}

#[derive(Deserialize)]
struct SigResult {
    value: SigValue,
}

#[derive(Deserialize)]
struct SigValue {
    err: Option<json::Value>,
}

#[derive(Debug)]
pub struct SignatureOutcome {
    pub latency: ObservationsStats,
    pub confirmed: usize,
    // notification carried an on-chain error
    pub failed: usize,
    pub first_failure: Option<String>,
    // still pending when finalized — always report against the timeout used
    pub unconfirmed: usize,
}

#[derive(Default)]
struct SigShared {
    error: Option<String>,
    id_by_req: HashMap<u64, u64>,
    id_by_subid: HashMap<u64, u64>,
    pending: HashMap<u64, Instant>,
    waiters: HashMap<u64, oneshot::Sender<()>>,
    latency: StreamingStats,
    confirmed: usize,
    failed: usize,
    first_failure: Option<String>,
}

impl SigShared {
    fn settle_waiter(&mut self, id: u64) {
        if let Some(tx) = self.waiters.remove(&id) {
            let _ = tx.send(());
        }
    }

    fn fail(&mut self, msg: impl Into<String>) {
        self.error = Some(msg.into());
        self.waiters.clear();
    }
}

// signatureSubscribe is one-shot: one notification at the commitment level,
// then the server auto-unsubscribes. Subscribe BEFORE delivering the tx
// (`TxSender::prepare` exposes the signature) — a post-delivery subscribe
// races the confirmation and can miss it.
pub struct SignatureConfirmations {
    // tokio Mutex, not RefCell: subscribes come from concurrent tasks and
    // hold the sink across an await
    sink: tokio::sync::Mutex<SplitSink<Socket, Message>>,
    shared: Rc<RefCell<SigShared>>,
    reader: tokio::task::JoinHandle<()>,
    next_req_id: Cell<u64>,
}

impl SignatureConfirmations {
    pub async fn connect(url: &str) -> Result<Self> {
        let (socket, _) = connect_async(url)
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        let (sink, stream) = socket.split();
        let shared = Rc::new(RefCell::new(SigShared::default()));
        let reader =
            tokio::task::spawn_local(sig_read_loop(stream, shared.clone()));
        Ok(Self {
            sink: tokio::sync::Mutex::new(sink),
            shared,
            reader,
            next_req_id: Cell::new(1),
        })
    }

    // The latency clock starts here, before delivery — signature latency is
    // send-start → inclusion, the same origin as delivery and account lag.
    pub async fn subscribe(&self, id: u64, sig: &Signature) -> Result<()> {
        let req = self.next_req_id.replace(self.next_req_id.get() + 1);
        {
            let mut shared = self.shared.borrow_mut();
            if let Some(err) = &shared.error {
                return Err(format!("ws stream: {err}").into());
            }
            shared.id_by_req.insert(req, id);
            shared.pending.insert(id, Instant::now());
        }
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":{req},"method":"signatureSubscribe","params":["{sig}",{{"commitment":"confirmed"}}]}}"#
        );
        self.sink
            .lock()
            .await
            .send(Message::Text(msg.into()))
            .await
            .map_err(|e| format!("signatureSubscribe send: {e}"))?;
        Ok(())
    }

    pub async fn await_id(&self, id: u64) -> Result<()> {
        let rx = {
            let mut shared = self.shared.borrow_mut();
            if let Some(err) = &shared.error {
                return Err(format!("ws stream: {err}").into());
            }
            if !shared.pending.contains_key(&id) {
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            shared.waiters.insert(id, tx);
            rx
        };
        rx.await
            .map_err(|_| "ws stream failed while awaiting signature".into())
    }

    pub async fn await_all(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            {
                let shared = self.shared.borrow();
                if let Some(err) = &shared.error {
                    return Err(format!("ws stream: {err}").into());
                }
                if shared.pending.is_empty() {
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for signature confirmations ({timeout:?})"
                )
                .into());
            }
            tokio::time::sleep(AWAIT_POLL).await;
        }
    }

    pub fn finalize(&self) -> SignatureOutcome {
        self.reader.abort();
        let mut shared = self.shared.borrow_mut();
        SignatureOutcome {
            latency: std::mem::take(&mut shared.latency).finalize(false),
            confirmed: shared.confirmed,
            failed: shared.failed,
            first_failure: shared.first_failure.take(),
            unconfirmed: shared.pending.len(),
        }
    }
}

async fn sig_read_loop(
    mut stream: SplitStream<Socket>,
    shared: Rc<RefCell<SigShared>>,
) {
    while let Some(msg) = stream.next().await {
        let text = match msg {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) | Err(_) => {
                shared.borrow_mut().fail("connection closed");
                return;
            }
            Ok(_) => continue,
        };
        let Ok(frame) = json::from_str::<SigFrame>(&text) else {
            continue;
        };
        if let Some(error) = frame.error {
            shared.borrow_mut().fail(error.to_string());
            return;
        }
        match (frame.method.as_deref(), frame.params, frame.id) {
            (Some("signatureNotification"), Some(params), _) => {
                let mut shared = shared.borrow_mut();
                let Some(id) = shared.id_by_subid.remove(&params.subscription)
                else {
                    continue;
                };
                let Some(sent) = shared.pending.remove(&id) else {
                    continue;
                };
                match params.result.value.err {
                    None => {
                        shared.latency.push(sent.elapsed().as_micros() as u32);
                        shared.confirmed += 1;
                    }
                    Some(err) => {
                        shared.failed += 1;
                        shared
                            .first_failure
                            .get_or_insert(format!("id {id}: {err}"));
                    }
                }
                shared.settle_waiter(id);
            }
            (None, _, Some(req_id)) => {
                let mut shared = shared.borrow_mut();
                if let (Some(id), Some(subid)) =
                    (shared.id_by_req.remove(&req_id), frame.result)
                {
                    shared.id_by_subid.insert(subid, id);
                }
            }
            _ => {}
        }
    }
    shared.borrow_mut().fail("stream ended");
}
