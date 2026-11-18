use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use redsuite_core::{programs::CellHeader, transport::observations::Notification};
use signature::Signature;

pub(super) const EXPIRY: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
pub(super) struct Clock {
    pub(super) begin: Instant,
    pub(super) end: Instant,
    pub(super) unix: f64,
}

impl Clock {
    pub(super) fn new(warmup: Duration, measured: Duration) -> Self {
        let begin = Instant::now() + warmup;
        let unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
            + warmup.as_secs_f64();
        Self {
            begin,
            end: begin + measured,
            unix,
        }
    }

    pub(super) fn measured(self, at: Instant) -> bool {
        self.begin <= at && at < self.end
    }
}

#[derive(Clone, Copy)]
pub(super) enum Count {
    Submitted,
    Accepted,
    SendError,
    Read,
    ReadError,
    Signature,
    SignatureError,
    SignatureExpired,
    Status,
    StatusError,
    StatusExpired,
    StatusRpcError,
    AccountExpected,
    Account,
    AccountExpired,
    AccountDuplicate,
    AckError,
    WriteSkipped,
    Disconnect,
    GoingAway,
    TransportDisconnected,
    Reconnect,
    Length,
}

const COUNTERS: [&str; Count::Length as usize] = [
    "transactions submitted",
    "transactions accepted",
    "transaction submission errors",
    "account reads",
    "account read errors",
    "signature confirmations",
    "signature execution errors",
    "signature timeouts",
    "status confirmations",
    "status execution errors",
    "status unobserved",
    "status RPC errors",
    "account updates expected",
    "account updates received",
    "account update timeouts",
    "duplicate account updates",
    "signature ACK errors",
    "writes skipped by signature gate",
    "WebSocket disconnects",
    "WebSocket Going Away closes",
    "WebSocket transport errors",
    "WebSocket reconnects",
];

#[derive(Clone, Copy)]
pub(super) enum Latency {
    Ack,
    Send,
    Read,
    Signature,
    Status,
    Account,
    Length,
}

const LATENCIES: [&str; Latency::Length as usize] = [
    "signature subscription ACK",
    "transaction response",
    "account read response",
    "signature notification",
    "independent status observation",
    "account notification",
];

#[derive(Default)]
struct Timing {
    count: u64,
    sum_us: u128,
    max_us: u128,
}

#[derive(Default)]
pub(super) struct Sample {
    counts: [u64; Count::Length as usize],
    times: [Timing; Latency::Length as usize],
}

impl Sample {
    pub(super) fn merge(&mut self, other: &Self) {
        for (total, value) in self.counts.iter_mut().zip(&other.counts) {
            *total += value;
        }
        for (total, value) in self.times.iter_mut().zip(&other.times) {
            total.count += value.count;
            total.sum_us += value.sum_us;
            total.max_us = total.max_us.max(value.max_us);
        }
    }

    pub(super) fn counters(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        COUNTERS.into_iter().zip(self.counts)
    }

    pub(super) fn timings(&self) -> impl Iterator<Item = (&'static str, u64, f64, f64)> + '_ {
        LATENCIES
            .into_iter()
            .zip(&self.times)
            .filter(|(_, timing)| timing.count > 0)
            .map(|(name, timing)| {
                (
                    name,
                    timing.count,
                    timing.sum_us as f64 / timing.count as f64,
                    timing.max_us as f64,
                )
            })
    }

    pub(super) fn count(&self, counter: Count) -> u64 {
        self.counts[counter as usize]
    }
}

#[derive(Default)]
pub(super) struct Observations {
    pub(super) clock: Option<Clock>,
    pub(super) samples: Vec<Sample>,
}

impl Observations {
    fn sample(&mut self, started: Instant, at: Instant) -> Option<&mut Sample> {
        let clock = self.clock.filter(|clock| clock.measured(started))?;
        let second = at.duration_since(clock.begin).as_secs() as usize;
        if second >= self.samples.len() {
            self.samples.resize_with(second + 1, Sample::default);
        }
        Some(&mut self.samples[second])
    }

    pub(super) fn count(&mut self, counter: Count, started: Instant, at: Instant, n: u64) {
        if n != 0 {
            if let Some(sample) = self.sample(started, at) {
                sample.counts[counter as usize] += n;
            }
        }
    }

    pub(super) fn record(
        &mut self,
        started: Instant,
        at: Instant,
        counter: Option<Count>,
        latency: Option<Latency>,
    ) {
        let Some(sample) = self.sample(started, at) else { return };
        if let Some(counter) = counter {
            sample.counts[counter as usize] += 1;
        }
        if let Some(metric) = latency {
            let us = at.duration_since(started).as_micros();
            let timing = &mut sample.times[metric as usize];
            timing.count += 1;
            timing.sum_us += us;
            timing.max_us = timing.max_us.max(us);
        }
    }
}

struct Pending {
    signature: Signature,
    signature_pending: bool,
    status_pending: bool,
    sent: Instant,
    http_done: Option<Instant>,
    expected: u128,
    seen: u128,
}

impl Pending {
    fn notification_expired(&self, now: Instant) -> bool {
        self.http_done.is_some_and(|done| now >= done + EXPIRY)
    }

    fn status_expired(&self, now: Instant) -> bool {
        now >= self.sent + EXPIRY
    }
}

#[derive(Default)]
pub(super) struct Tracker {
    pending: HashMap<u64, Pending>,
    pub(super) data: Observations,
}

impl Tracker {
    pub(super) fn submitted(
        &mut self,
        id: u64,
        signature: Signature,
        sent: Instant,
        expected: u128,
    ) {
        self.pending.insert(
            id,
            Pending {
                signature,
                signature_pending: true,
                status_pending: true,
                sent,
                http_done: None,
                expected,
                seen: 0,
            },
        );
        if let Some(sample) = self.data.sample(sent, sent) {
            sample.counts[Count::Submitted as usize] += 1;
            sample.counts[Count::AccountExpected as usize] += u64::from(expected.count_ones());
        }
    }

    pub(super) fn http_done(&mut self, id: u64, now: Instant, ok: bool) {
        let tx = self
            .pending
            .get_mut(&id)
            .expect("submitted transaction has a pending HTTP request");
        tx.http_done = Some(now);
        self.data.record(
            tx.sent,
            now,
            Some(if ok { Count::Accepted } else { Count::SendError }),
            ok.then_some(Latency::Send),
        );
    }

    pub(super) fn observe(&mut self, notification: Notification, now: Instant) {
        match notification {
            Notification::Account { index, data } => {
                let Some(tx) =
                    CellHeader::written_id(&data).and_then(|id| self.pending.get_mut(&id))
                else {
                    return;
                };
                let bit = 1u128 << index;
                if tx.notification_expired(now) || tx.expected & bit == 0 {
                    return;
                }
                let duplicate = tx.seen & bit != 0;
                tx.seen |= bit;
                self.data.record(
                    tx.sent,
                    now,
                    Some(if duplicate { Count::AccountDuplicate } else { Count::Account }),
                    (!duplicate).then_some(Latency::Account),
                );
            }
            Notification::Signature { id, succeeded } => {
                let Some(tx) = self.pending.get_mut(&id) else { return };
                if !tx.signature_pending || tx.notification_expired(now) {
                    return;
                }
                tx.signature_pending = false;
                self.data.record(
                    tx.sent,
                    now,
                    Some(if succeeded { Count::Signature } else { Count::SignatureError }),
                    succeeded.then_some(Latency::Signature),
                );
            }
        }
    }

    pub(super) fn status(&mut self, id: u64, succeeded: bool, now: Instant) {
        let Some(tx) = self.pending.get_mut(&id) else { return };
        if !tx.status_pending || tx.status_expired(now) {
            return;
        }
        tx.status_pending = false;
        self.data.record(
            tx.sent,
            now,
            Some(if succeeded { Count::Status } else { Count::StatusError }),
            Some(Latency::Status),
        );
    }

    pub(super) fn restore(&self, socket: usize, connections: usize) -> Vec<(u64, Signature)> {
        self.pending
            .iter()
            .filter_map(|(&id, tx)| {
                (id as usize % connections == socket && tx.signature_pending)
                    .then_some((id, tx.signature))
            })
            .collect()
    }

    pub(super) fn signature_pending(&self, id: u64) -> bool {
        self.pending.get(&id).is_some_and(|tx| tx.signature_pending)
    }

    pub(super) fn statuses(&self, now: Instant) -> Vec<(u64, Signature)> {
        self.pending
            .iter()
            .filter_map(|(&id, tx)| {
                (tx.status_pending && !tx.status_expired(now)).then_some((id, tx.signature))
            })
            .collect()
    }

    pub(super) fn reap(&mut self, now: Instant) -> Vec<u64> {
        let mut unsubscribe = Vec::new();
        self.pending.retain(|&id, tx| {
            if tx.http_done.is_none() {
                return true;
            }
            if tx.status_pending && tx.status_expired(now) {
                tx.status_pending = false;
                self.data.count(Count::StatusExpired, tx.sent, now, 1);
            }
            if tx.notification_expired(now) {
                if tx.signature_pending {
                    unsubscribe.push(id);
                    self.data.count(Count::SignatureExpired, tx.sent, now, 1);
                }
                self.data.count(
                    Count::AccountExpired,
                    tx.sent,
                    now,
                    u64::from((tx.expected & !tx.seen).count_ones()),
                );
                return false;
            }
            tx.status_pending || tx.signature_pending || tx.expected != tx.seen
        });
        unsubscribe
    }

    pub(super) fn pending(&self) -> bool {
        !self.pending.is_empty()
    }
}
