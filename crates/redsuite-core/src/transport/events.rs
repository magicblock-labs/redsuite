use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};

use json::LazyValue;
use pubkey::Pubkey;

use super::conn::{self, CloseReason, FrameHandler, Reader, Requester};
use crate::Result;

const AWAIT_POLL: Duration = Duration::from_millis(20);

#[derive(Default)]
struct Shared {
    error: Option<String>,
    ready_subs: usize,
    key_by_subid: HashMap<u64, u64>,
    events: HashMap<u64, Vec<json::Value>>,
    malformed: usize,
}

impl Shared {
    fn fail(&mut self, msg: impl Into<String>) {
        self.error = Some(msg.into());
    }
}

struct EventHandler {
    shared: Rc<RefCell<Shared>>,
}

impl FrameHandler for EventHandler {
    fn on_reply(&mut self, id: u64, result: Option<&LazyValue<'_>>) {
        let mut shared = self.shared.borrow_mut();
        if let Some(subid) = conn::reply_u64(result) {
            shared.key_by_subid.insert(subid, id);
        }
        shared.ready_subs += 1;
    }

    fn on_notification(
        &mut self,
        _method: &str,
        subscription: u64,
        payload: &LazyValue<'_>,
    ) {
        let mut shared = self.shared.borrow_mut();
        let Some(key) = shared.key_by_subid.get(&subscription).copied() else {
            return;
        };
        let Ok(event) = json::from_str::<json::Value>(payload.as_raw_str())
        else {
            shared.malformed += 1;
            return;
        };
        shared.events.entry(key).or_default().push(event);
    }

    fn on_malformed(&mut self, _text: &str) {
        self.shared.borrow_mut().malformed += 1;
    }

    fn on_closed(&mut self, reason: &CloseReason) {
        self.shared.borrow_mut().fail(reason.to_string());
    }
}

pub struct EventSubscriptions {
    requester: Requester,
    shared: Rc<RefCell<Shared>>,
    reader: Reader,
}

impl EventSubscriptions {
    pub async fn connect(url: &str) -> Result<Self> {
        let (requester, stream) = conn::split(conn::connect(url).await?);
        let shared = Rc::new(RefCell::new(Shared::default()));
        let reader = Reader::spawn(
            stream,
            EventHandler {
                shared: shared.clone(),
            },
        );
        Ok(Self {
            requester,
            shared,
            reader,
        })
    }

    pub async fn slot_subscribe(&self) -> Result<u64> {
        self.subscribe("slotSubscribe", "[]").await
    }

    pub async fn account_subscribe(&self, account: &Pubkey) -> Result<u64> {
        self.subscribe("accountSubscribe", &conn::account_params(account))
            .await
    }

    pub async fn logs_subscribe_all(&self) -> Result<u64> {
        self.subscribe("logsSubscribe", conn::logs_all_params())
            .await
    }

    pub async fn logs_subscribe_mentions(
        &self,
        account: &Pubkey,
    ) -> Result<u64> {
        self.subscribe("logsSubscribe", &conn::logs_mentions_params(account))
            .await
    }

    pub async fn program_subscribe(&self, program: &Pubkey) -> Result<u64> {
        self.subscribe("programSubscribe", &conn::program_params(program))
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

    pub async fn await_events(
        &self,
        key: u64,
        count: usize,
        timeout: Duration,
    ) -> Result<()> {
        self.await_shared(timeout, "notifications", |shared| {
            shared.events.get(&key).map_or(0, Vec::len) >= count
        })
        .await
    }

    pub fn events(&self, key: u64) -> Vec<json::Value> {
        self.shared
            .borrow()
            .events
            .get(&key)
            .cloned()
            .unwrap_or_default()
    }

    pub fn malformed_frames(&self) -> usize {
        self.shared.borrow().malformed
    }

    pub fn finalize(&self) {
        self.reader.stop();
    }

    async fn subscribe(&self, method: &str, params: &str) -> Result<u64> {
        let key = self.requester.mint();
        self.requester.send(key, method, params).await?;
        Ok(key)
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
