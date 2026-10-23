use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use crate::runner::Delivery;

pub struct SendClock {
    epoch: Instant,
    first_id: u64,
    sent_micros: Vec<AtomicU64>,
}

impl SendClock {
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

    pub fn stamp(&self, id: u64, at: Instant) {
        let slot = self
            .slot_of(id)
            .unwrap_or_else(|| panic!("id {id} outside the send clock range"));
        let stamp = at.duration_since(self.epoch).as_micros() as u64 + 1;
        slot.store(stamp, Ordering::Release);
    }

    pub fn start(&self, id: u64) -> Instant {
        let now = Instant::now();
        self.stamp(id, now);
        now
    }

    pub fn elapsed_micros(&self, id: u64) -> Option<u32> {
        let stamp = self.slot_of(id)?.load(Ordering::Acquire);
        if stamp == 0 {
            return None;
        }
        let now = self.epoch.elapsed().as_micros() as u64;
        Some(now.saturating_sub(stamp - 1).min(u64::from(u32::MAX)) as u32)
    }

    pub fn stamped<D: Delivery>(self: &Arc<Self>, id: u64, inner: D) -> Stamped<D> {
        Stamped::new(Some(self.clone()), id, inner)
    }
}

pub struct Stamped<D> {
    clock: Option<Arc<SendClock>>,
    id: u64,
    inner: D,
}

impl<D: Delivery> Stamped<D> {
    pub fn new(clock: Option<Arc<SendClock>>, id: u64, inner: D) -> Self {
        Self { clock, id, inner }
    }
}

impl<D: Delivery> Delivery for Stamped<D> {
    const ORIGIN: &'static str = D::ORIGIN;
    type Fut = D::Fut;

    fn deliver(self, sent: Instant) -> D::Fut {
        if let Some(clock) = &self.clock {
            clock.stamp(self.id, sent);
        }
        self.inner.deliver(sent)
    }
}
