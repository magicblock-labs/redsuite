use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    cells::OwnedCell, programs::REDSHIFT, topology::RestartConfig, Api, BaseCtx, ChainCtx, ErCtx,
    Result, Scenario,
};
use signer::Signer;
use tokio::{task::JoinHandle, time};

const INITIAL_ACCOUNT_SPACE: u32 = 10 * 1024;
const ACCOUNTS: usize = 8;
const GROW_BYTES: u32 = 32;
const GROW_VARIANTS: usize = 64;
const PAYER_LAMPORTS: u64 = 10_000_000_000;
const GROW_INTERVAL: Duration = Duration::from_millis(40);
const CHURN_BEFORE_RESTART: Duration = Duration::from_secs(6);
const CHURN_AFTER_RESTART: Duration = Duration::from_secs(12);

/// Repeated snapshot reads whose task result retains any failure witnessed during resize churn.
struct Readers {
    stop: Rc<Cell<bool>>,
    task: JoinHandle<Result<()>>,
}

impl Readers {
    fn start(api: Api, addresses: Vec<Pubkey>) -> Self {
        let stop = Rc::new(Cell::new(false));
        let signal = stop.clone();
        let task = tokio::task::spawn_local(async move {
            let mut reads = 0;
            while !signal.get() {
                let accounts = api
                    .get_multiple_accounts(&addresses)
                    .await
                    .with_context(|| format!("snapshot account reads; completed={reads}"))?;
                ensure!(
                    accounts.iter().all(Option::is_some),
                    "snapshot account missing; completed={reads}"
                );
                reads += 1;
            }
            Ok(())
        });
        Self { stop, task }
    }

    /// Stops between reads and joins the task so background failures cannot be discarded.
    async fn stop(self, phase: &str) -> Result<()> {
        self.stop.set(true);
        self.task
            .await
            .with_context(|| format!("reader task {phase}"))?
            .with_context(|| format!("snapshot reads {phase}"))
    }
}

/// Checks account reads and final sizes while resize traffic spans a restart.
/// Readers run alongside growth on each side of the restart, not while the RPC server is down.
pub struct AccountReadsDuringResize {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
}

impl Scenario for AccountReadsDuringResize {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        Ok(Self { base, er, payer })
    }

    /// Carries expected sizes across restart and resumes concurrent
    /// reads against recovered accounts.
    async fn run(self) -> Result<()> {
        let mut sizes = [INITIAL_ACCOUNT_SPACE; ACCOUNTS];
        let er = &self.er;
        er.await_program_clone(&redshift_interface::id()).await?;
        let accounts = self
            .base
            .cells(REDSHIFT, &self.payer)
            .create_owned_sized(er.identity(), ACCOUNTS, INITIAL_ACCOUNT_SPACE)
            .await?;
        let addresses: Vec<_> = accounts.iter().map(|account| account.address).collect();
        er.await_clones(&addresses, INITIAL_ACCOUNT_SPACE as usize).await?;
        let sponsor = self.base.funded_payer(crate::PAYER_LAMPORTS).await?;
        self.base.delegate_payer(&sponsor, &self.payer, er.identity()).await?;

        let readers = Readers::start(er.api().clone(), addresses.clone());
        self.churn(
            &accounts,
            &mut sizes,
            CHURN_BEFORE_RESTART,
            "before restart",
        )
        .await?;
        readers.stop("before restart").await?;

        let timing = self.er.restart(RestartConfig::default()).await?;
        ensure!(timing.exit_code == Some(0));

        let readers = Readers::start(er.api().clone(), addresses.clone());
        self.churn(&accounts, &mut sizes, CHURN_AFTER_RESTART, "after restart").await?;
        readers.stop("after restart").await?;

        let observed: Vec<u32> = er
            .accounts(&addresses)
            .await?
            .into_iter()
            .map(|account| account.map_or(0, |a| a.data.len() as u32))
            .collect();
        ensure!(observed == sizes);

        Ok(())
    }
}

impl AccountReadsDuringResize {
    /// Grows accounts round-robin, updating expected sizes only after confirmed execution.
    async fn churn(
        &self,
        accounts: &[OwnedCell],
        sizes: &mut [u32],
        duration: Duration,
        phase: &str,
    ) -> Result<()> {
        let started = Instant::now();
        let mut grows = 0usize;
        while started.elapsed() < duration {
            let index = grows % accounts.len();
            let bytes = GROW_BYTES + ((grows / accounts.len()) % GROW_VARIANTS) as u32;
            let space = sizes[index] + bytes;
            let resize = REDSHIFT.resize(self.payer.pubkey(), accounts[index].address, space);
            self.er.submit_and_confirm(&self.payer, &[resize]).await.with_context(|| {
                format!("resizing account {index} {phase}; completed grows={grows}")
            })?;
            sizes[index] = space;
            grows += 1;
            time::sleep(GROW_INTERVAL).await;
        }
        Ok(())
    }
}
