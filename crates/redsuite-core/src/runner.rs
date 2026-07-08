use std::{cell::RefCell, future::Future, rc::Rc, time::Instant};

use crate::{
    stats::{ObservationsStats, StreamingStats},
    transport::rate::RateManager,
    Result,
};

pub struct RunConfig {
    pub iterations: u64,
    pub rate: u32,
    pub concurrency: usize,
}

#[derive(Debug)]
pub struct RunOutcome {
    pub delivered: u64,
    // iterations that errored at either stage (delivery or sync)
    pub failed: u64,
    pub first_error: Option<String>,
    pub delivery: ObservationsStats,
    // closed loop only: send-start → all confirmations for the id
    pub sync: Option<ObservationsStats>,
    pub rps: ObservationsStats,
    pub wall: std::time::Duration,
}

impl RunOutcome {
    pub fn achieved_rps(&self) -> f64 {
        self.delivered as f64 / self.wall.as_secs_f64()
    }
}

#[derive(Default)]
struct Tally {
    delivered: u64,
    failed: u64,
    first_error: Option<String>,
    delivery: StreamingStats,
    sync: StreamingStats,
}

pub async fn drive<F, Fut>(cfg: RunConfig, mut request: F) -> RunOutcome
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
{
    let mut rate = RateManager::new(cfg.concurrency, cfg.rate);
    let tally = Rc::new(RefCell::new(Tally::default()));
    let started = Instant::now();

    let mut handles = Vec::with_capacity(cfg.iterations as usize);
    for id in 1..=cfg.iterations {
        let permit = rate.tick().await;
        let fut = request(id);
        let tally = tally.clone();
        handles.push(tokio::task::spawn_local(async move {
            let sent = Instant::now();
            let result = fut.await;
            let mut tally = tally.borrow_mut();
            match result {
                Ok(()) => {
                    tally.delivery.push(sent.elapsed().as_micros() as u32);
                    tally.delivered += 1;
                }
                Err(e) => {
                    tally.failed += 1;
                    tally.first_error.get_or_insert(e.to_string());
                }
            }
            drop(permit);
        }));
    }
    let rps = rate.stats();
    for handle in handles {
        let _ = handle.await;
    }

    let tally = Rc::try_unwrap(tally)
        .unwrap_or_else(|_| panic!("drive tasks still hold the tally"))
        .into_inner();
    RunOutcome {
        delivered: tally.delivered,
        failed: tally.failed,
        first_error: tally.first_error,
        delivery: tally.delivery.finalize(false),
        sync: None,
        rps,
        wall: started.elapsed(),
    }
}

pub async fn drive_closed<F, Fut, S, SFut>(
    cfg: RunConfig,
    mut request: F,
    mut sync: S,
) -> RunOutcome
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>> + 'static,
    S: FnMut(u64) -> SFut,
    SFut: Future<Output = Result<()>> + 'static,
{
    let mut rate = RateManager::new(cfg.concurrency, cfg.rate);
    let tally = Rc::new(RefCell::new(Tally::default()));
    let started = Instant::now();

    let mut handles = Vec::with_capacity(cfg.iterations as usize);
    for id in 1..=cfg.iterations {
        let permit = rate.tick().await;
        let fut = request(id);
        let sync_fut = sync(id);
        let tally = tally.clone();
        handles.push(tokio::task::spawn_local(async move {
            let sent = Instant::now();
            let delivered = match fut.await {
                Ok(()) => {
                    let mut tally = tally.borrow_mut();
                    tally.delivery.push(sent.elapsed().as_micros() as u32);
                    tally.delivered += 1;
                    true
                }
                Err(e) => {
                    let mut tally = tally.borrow_mut();
                    tally.failed += 1;
                    tally.first_error.get_or_insert(e.to_string());
                    false
                }
            };
            if delivered {
                match sync_fut.await {
                    Ok(()) => tally
                        .borrow_mut()
                        .sync
                        .push(sent.elapsed().as_micros() as u32),
                    Err(e) => {
                        let mut tally = tally.borrow_mut();
                        tally.failed += 1;
                        tally.first_error.get_or_insert(format!("sync: {e}"));
                    }
                }
            }
            drop(permit);
        }));
    }
    let rps = rate.stats();
    for handle in handles {
        let _ = handle.await;
    }

    let tally = Rc::try_unwrap(tally)
        .unwrap_or_else(|_| panic!("drive tasks still hold the tally"))
        .into_inner();
    RunOutcome {
        delivered: tally.delivered,
        failed: tally.failed,
        first_error: tally.first_error,
        delivery: tally.delivery.finalize(false),
        sync: Some(tally.sync.finalize(false)),
        rps,
        wall: started.elapsed(),
    }
}
