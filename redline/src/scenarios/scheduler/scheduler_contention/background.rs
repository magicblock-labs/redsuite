use super::{BUSY_EXECUTORS, WARM_IN_TIMEOUT};
use anyhow::{ensure, Context};
use pubkey::Pubkey;
use redsuite_core::{
    prep,
    redline::{causal::independent_ixs, check_ok},
    runner::{
        execute_until_raw, merge_outcomes, spawn_workers, split_budget, Pacing, RawRunOutcome,
        RunOutcome, Worker, Workers, SEND_ORIGIN,
    },
    transport::{clock::SendClock, ws::SignatureConfirmations},
    wait, ErClient, ErCtx, Result, TxSender,
};
use std::{
    cell::{Cell, RefCell},
    mem,
    rc::Rc,
    sync::Arc,
    time::Instant,
};
use tokio::sync::watch;
use url::Url;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum LoadMode {
    Bounded { lanes: usize },
    Open { concurrency: usize },
}

impl LoadMode {
    pub(super) fn label(self) -> &'static str {
        match self {
            LoadMode::Bounded { .. } => "contention",
            LoadMode::Open { .. } => "backpressure",
        }
    }

    pub(super) fn shape(self) -> String {
        match self {
            LoadMode::Bounded { lanes } => format!(
                "{lanes} lanes, one unconfirmed high-cu tx each (ready queue \
                 deep, sequencer intake never throttled)"
            ),
            LoadMode::Open { concurrency } => format!(
                "open loop at concurrency {concurrency} (sequencer channel \
                 full, pending-work bound engaged)"
            ),
        }
    }
}

pub(super) fn spawn_load(
    er_rpc_url: Url,
    er_ws_url: Url,
    mode: LoadMode,
    threads: usize,
    accounts: Arc<Vec<Pubkey>>,
    payer_bytes: Arc<Vec<[u8; 64]>>,
    iters: u32,
) -> Workers<RawRunOutcome> {
    let threads = threads.clamp(1, accounts.len().max(1));
    spawn_workers(threads, move |worker| {
        let er_rpc_url = er_rpc_url.clone();
        let er_ws_url = er_ws_url.clone();
        let accounts = accounts.clone();
        let payer_bytes = payer_bytes.clone();
        async move {
            let client = ErClient::new(er_rpc_url, er_ws_url.clone());
            let (lane_limit, concurrency) = match mode {
                LoadMode::Bounded { lanes } => (lanes, 0),
                LoadMode::Open { concurrency } => (
                    accounts.len(),
                    split_budget(concurrency as u64, worker.threads)[worker.index] as usize,
                ),
            };
            let mut lanes = Vec::new();
            for index in (worker.index..lane_limit.min(accounts.len())).step_by(worker.threads) {
                let payer = prep::payer_from_bytes(&payer_bytes[index]);
                lanes.push((accounts[index], client.sender(Rc::new(payer))));
            }
            if lanes.is_empty() {
                return Ok(RawRunOutcome::default());
            }
            match mode {
                LoadMode::Open { .. } if concurrency == 0 => Ok(RawRunOutcome::default()),
                LoadMode::Open { .. } => open_load(lanes, iters, concurrency, &worker).await,
                LoadMode::Bounded { .. } => {
                    BoundedLoad::run(&er_ws_url, lanes, iters, worker.stop_signal()).await
                }
            }
        }
    })
}

async fn open_load(
    lanes: Vec<(Pubkey, TxSender)>,
    iters: u32,
    concurrency: usize,
    worker: &Worker,
) -> Result<RawRunOutcome> {
    execute_until_raw(Pacing::Unlimited, concurrency, worker.stop_signal(), |id| {
        let (account, sender) = &lanes[((id - 1) as usize) % lanes.len()];
        let ixs = independent_ixs(id, *account, iters);
        let sender = sender.clone();
        sender.staged(ixs)
    })
    .await
}

struct BoundedLoad {
    sigs: SignatureConfirmations,
    tally: RefCell<RawRunOutcome>,
    next_id: Cell<u64>,
    stop: watch::Receiver<bool>,
    iters: u32,
}

impl BoundedLoad {
    async fn run(
        ws_url: &Url,
        lanes: Vec<(Pubkey, TxSender)>,
        iters: u32,
        stop: watch::Receiver<bool>,
    ) -> Result<RawRunOutcome> {
        let load = Rc::new(Self {
            sigs: SignatureConfirmations::connect(ws_url, Arc::new(SendClock::new(1, 0))).await?,
            tally: RefCell::new(RawRunOutcome::default()),
            next_id: Cell::new(0),
            stop,
            iters,
        });
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(lanes.len());
        for (account, sender) in lanes {
            tasks.push(tokio::task::spawn_local(
                load.clone().run_lane(account, sender),
            ));
        }
        for task in tasks {
            task.await.context("independent lane panicked")?;
        }
        let confirmations = load.sigs.finalize();
        let tally = mem::take(&mut *load.tally.borrow_mut());
        Ok(RawRunOutcome {
            failed: tally.failed + confirmations.failed as u64,
            first_error: tally.first_error.or(confirmations.first_failure),
            origin: SEND_ORIGIN,
            wall: started.elapsed(),
            ..tally
        })
    }

    async fn run_lane(self: Rc<Self>, account: Pubkey, sender: TxSender) {
        while !*self.stop.borrow() {
            let result = self.deliver(account, &sender).await;
            let mut tally = self.tally.borrow_mut();
            match result {
                Ok(()) => tally.delivered += 1,
                Err(error) => {
                    tally.failed += 1;
                    tally.first_error.get_or_insert(format!("{error:#}"));
                    break;
                }
            }
        }
    }

    async fn deliver(&self, account: Pubkey, sender: &TxSender) -> Result<()> {
        let id = self.next_id.get() + 1;
        self.next_id.set(id);
        let tx = sender.prepare(&independent_ixs(id, account, self.iters)).await?;
        self.sigs.subscribe(id, &tx.signatures[0]).await?;
        let sent = Instant::now();
        sender.submit_prepared(&tx).await?;
        self.tally.borrow_mut().delivery.push(sent.elapsed().as_micros() as u32);
        self.sigs.await_id(id).await?;
        Ok(())
    }
}

pub(super) async fn join_load(handles: Workers<RawRunOutcome>) -> Result<RunOutcome> {
    Ok(merge_outcomes(handles.join().await?))
}

pub(super) async fn await_load_reaching_executors(er: &ErCtx) -> Result<()> {
    let reached = wait::retry(
        "the independent load reaches the executors",
        WARM_IN_TIMEOUT,
        async || {
            let metrics = er.scrape_metrics().await?;
            let busy = metrics.get(BUSY_EXECUTORS).context("busy executors metric missing")?;
            ensure!(busy >= 1.0);
            Ok(())
        },
    )
    .await;
    check_ok(reached, "independent load did not reach the executors");
    er.scrape_metrics().await?;
    Ok(())
}
