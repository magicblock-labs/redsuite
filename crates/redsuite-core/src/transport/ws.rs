use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    time::{Duration, Instant},
};

use base64::Engine;
use json::{Deserialize, LazyValue};
use pubkey::Pubkey;
use signature::Signature;
use tokio::sync::oneshot;

use super::conn::{self, CloseReason, FrameHandler, Reader, Requester};
use crate::{
    stats::{ObservationsStats, StreamingStats},
    Result,
};

const AWAIT_POLL: Duration = Duration::from_millis(20);

#[derive(Deserialize)]
struct AccountPayload {
    value: AccountValue,
}

#[derive(Deserialize)]
struct AccountValue {
    data: (String, String),
}

pub(super) fn account_update_data(payload: &LazyValue<'_>) -> Option<Vec<u8>> {
    let payload =
        json::from_str::<AccountPayload>(payload.as_raw_str()).ok()?;
    base64::engine::general_purpose::STANDARD
        .decode(&payload.value.data.0)
        .ok()
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

struct AccountHandler<E> {
    shared: Rc<RefCell<Shared>>,
    extractor: E,
}

impl<E: Fn(&[u8]) -> Option<u64>> FrameHandler for AccountHandler<E> {
    fn on_reply(&mut self, id: u64, result: Option<&LazyValue<'_>>) {
        let mut shared = self.shared.borrow_mut();
        if let (Some(account), Some(subid)) =
            (shared.subs_by_req.remove(&id), conn::reply_u64(result))
        {
            shared.account_by_subid.insert(subid, account);
        }
        shared.ready_subs += 1;
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
        let mut shared = self.shared.borrow_mut();
        if let Some((_, sent)) = shared.pending.remove(&id) {
            shared.lag.push(sent.elapsed().as_micros() as u32);
            shared.observed += 1;
            shared.settle_waiter(id);
        }
        if let Some(account) =
            shared.account_by_subid.get(&subscription).copied()
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

    fn on_closed(&mut self, reason: &CloseReason) {
        self.shared.borrow_mut().fail(reason.to_string());
    }
}

pub struct AccountUpdates {
    requester: Requester,
    shared: Rc<RefCell<Shared>>,
    reader: Reader,
}

impl AccountUpdates {
    pub async fn connect(
        url: &str,
        extractor: impl Fn(&[u8]) -> Option<u64> + 'static,
    ) -> Result<Self> {
        let (requester, stream) = conn::split(conn::connect(url).await?);
        let shared = Rc::new(RefCell::new(Shared::default()));
        let reader = Reader::spawn(
            stream,
            AccountHandler {
                shared: shared.clone(),
                extractor,
            },
        );
        Ok(Self {
            requester,
            shared,
            reader,
        })
    }

    pub async fn account_subscribe(&self, account: &Pubkey) -> Result<()> {
        let id = self.requester.mint();
        self.shared.borrow_mut().subs_by_req.insert(id, *account);
        self.requester
            .send(id, "accountSubscribe", &conn::account_params(account))
            .await
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
        self.reader.stop();
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

// ---- signature confirmations -------------------------------------------

#[derive(Deserialize)]
struct SigPayload {
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

struct SigHandler {
    shared: Rc<RefCell<SigShared>>,
}

impl FrameHandler for SigHandler {
    fn on_reply(&mut self, id: u64, result: Option<&LazyValue<'_>>) {
        let mut shared = self.shared.borrow_mut();
        if let (Some(tracked), Some(subid)) =
            (shared.id_by_req.remove(&id), conn::reply_u64(result))
        {
            shared.id_by_subid.insert(subid, tracked);
        }
    }

    fn on_notification(
        &mut self,
        method: &str,
        subscription: u64,
        payload: &LazyValue<'_>,
    ) {
        if method != "signatureNotification" {
            return;
        }
        let Ok(result) = json::from_str::<SigPayload>(payload.as_raw_str())
        else {
            return;
        };
        let mut shared = self.shared.borrow_mut();
        let Some(id) = shared.id_by_subid.remove(&subscription) else {
            return;
        };
        let Some(sent) = shared.pending.remove(&id) else {
            return;
        };
        match result.value.err {
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

    fn on_closed(&mut self, reason: &CloseReason) {
        self.shared.borrow_mut().fail(reason.to_string());
    }
}

// signatureSubscribe is one-shot: one notification at the commitment level,
// then the server auto-unsubscribes. Subscribe BEFORE delivering the tx
// (`TxSender::prepare` exposes the signature) — a post-delivery subscribe
// races the confirmation and can miss it.
pub struct SignatureConfirmations {
    requester: Requester,
    shared: Rc<RefCell<SigShared>>,
    reader: Reader,
}

impl SignatureConfirmations {
    pub async fn connect(url: &str) -> Result<Self> {
        let (requester, stream) = conn::split(conn::connect(url).await?);
        let shared = Rc::new(RefCell::new(SigShared::default()));
        let reader = Reader::spawn(
            stream,
            SigHandler {
                shared: shared.clone(),
            },
        );
        Ok(Self {
            requester,
            shared,
            reader,
        })
    }

    // The latency clock starts here, before delivery — signature latency is
    // send-start → inclusion, the same origin as delivery and account lag.
    pub async fn subscribe(
        &self,
        id: u64,
        signature: &Signature,
    ) -> Result<()> {
        let req = self.requester.mint();
        {
            let mut shared = self.shared.borrow_mut();
            if let Some(err) = &shared.error {
                return Err(format!("ws stream: {err}").into());
            }
            shared.id_by_req.insert(req, id);
            shared.pending.insert(id, Instant::now());
        }
        self.requester
            .send(
                req,
                "signatureSubscribe",
                &conn::signature_params(&signature.to_string()),
            )
            .await
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
        self.reader.stop();
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
