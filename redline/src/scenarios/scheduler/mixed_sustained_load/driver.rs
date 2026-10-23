use super::{build_ixs, id_for, lane_of, mode_for, MIX_PERIOD};
use pubkey::Pubkey;
use redsuite_core::{
    prep,
    runner::{
        execute_for_raw, merge_outcomes, spawn_workers, split_budget, split_iterations, Delivery,
        Pacing, RawRunOutcome, RunOutcome, SendTransaction,
    },
    Result,
};
use std::{rc::Rc, sync::Arc, time::Instant};
use tokio::sync::{watch, Mutex};
use url::Url;

/// Worker-owned contiguous lanes; each worker advances its own round counter.
#[derive(Clone, Copy)]
struct LaneSpan {
    lo: u64,
    len: u64,
}

impl LaneSpan {
    fn job_id(&self, job: u64, lanes: u64) -> u64 {
        id_for(job / self.len, self.lo + job % self.len, lanes)
    }
}

pub(super) struct Executed {
    pub(super) outcome: RunOutcome,
    pub(super) last_ids: Vec<Option<u64>>,
    pub(super) counts: [u64; 3],
}

pub(super) struct ExecuteConfig {
    pub(super) lanes: u64,
    pub(super) until: (Instant, watch::Receiver<bool>),
    pub(super) read_span: usize,
    pub(super) hash_iters: u32,
    pub(super) rate: u32,
    pub(super) threads: usize,
    pub(super) concurrency: usize,
}

pub(super) async fn execute(
    er_rpc_url: Url,
    er_ws_url: Url,
    config: ExecuteConfig,
    pool: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
) -> Result<Executed> {
    let threads = config.threads.max(1);
    let spans: Vec<LaneSpan> = split_iterations(config.lanes, threads)
        .into_iter()
        .map(|(lo, len)| LaneSpan { lo, len })
        .collect();
    let rates = Pacing::PerSecond(config.rate).partition(spans.len())?;
    let lanes = config.lanes;
    let until = config.until;
    let concurrency = split_budget(config.concurrency as u64, spans.len());
    let read_span = config.read_span;
    let hash_iters = config.hash_iters;
    let outcomes = spawn_workers(spans.len(), move |worker| {
        let index = worker.index;
        let span = spans[index];
        let rate = rates[index];
        let concurrency = concurrency[index] as usize;
        let (deadline, stop) = until.clone();
        let er_rpc_url = er_rpc_url.clone();
        let er_ws_url = er_ws_url.clone();
        let pool = pool.clone();
        let payer_bytes = payer_bytes.clone();
        async move {
            let senders =
                prep::worker_senders(&er_rpc_url, &er_ws_url, &payer_bytes, index, threads);
            let locks: Rc<Vec<Arc<Mutex<()>>>> =
                Rc::new((0..span.len).map(|_| Arc::new(Mutex::new(()))).collect());
            let request = |iteration| {
                let id = span.job_id(iteration - 1, lanes);
                let lane_index = (lane_of(id, lanes) - span.lo) as usize;
                let ixs = build_ixs(id, &pool, read_span, hash_iters);
                let sender = senders[(id as usize) % senders.len()].clone();
                let locks = locks.clone();
                async move {
                    let lane = locks[lane_index].clone().lock_owned().await;
                    let send = sender.staged(ixs).await?;
                    Ok(SendTransaction(move |sent: Instant| async move {
                        let _lane = lane;
                        send.deliver(sent).await
                    }))
                }
            };
            let outcome = execute_for_raw(rate, concurrency, deadline, stop, request).await?;
            Ok((span, outcome))
        }
    })
    .join()
    .await?;
    Ok(summarize(lanes, outcomes))
}

// Timed workers stop at different rounds, so their IDs are not one global prefix.
// Count admitted IDs (including failed sends); delivery checks reject failures.
fn summarize(lanes: u64, outcomes: Vec<(LaneSpan, RawRunOutcome)>) -> Executed {
    let mut last_ids = vec![None; lanes as usize];
    let mut counts = [0; 3];
    for (span, outcome) in &outcomes {
        let admitted = outcome.delivered + outcome.failed;
        for offset in 0..span.len {
            let lane = span.lo + offset;
            let rounds = admitted / span.len + u64::from(offset < admitted % span.len);
            if rounds > 0 {
                last_ids[lane as usize] = Some(id_for(rounds - 1, lane, lanes));
            }
            // Coprime lane counts visit every mix slot once per period.
            for round in 0..MIX_PERIOD {
                let count = rounds / MIX_PERIOD + u64::from(round < rounds % MIX_PERIOD);
                counts[mode_for(id_for(round, lane, lanes)) as usize] += count;
            }
        }
    }
    Executed {
        outcome: merge_outcomes(outcomes.into_iter().map(|(_, outcome)| outcome)),
        last_ids,
        counts,
    }
}
