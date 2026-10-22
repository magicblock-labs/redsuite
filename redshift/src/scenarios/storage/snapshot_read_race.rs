use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    cells::OwnedCell,
    programs::REDSHIFT,
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    Api, BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use signer::Signer;
use tokio::{task::JoinHandle, time};

use crate::LARGE_ACCOUNT_SPACE;

const LABEL: &str = "snapshot-read-race";
const SUPERBLOCK_SLOTS: u64 = 10;
const ACCOUNTS: usize = 8;
const GROW_BYTES: u32 = 32;
const GROW_VARIANTS: usize = 64;
const PAYER_LAMPORTS: u64 = 10_000_000_000;
const GROW_INTERVAL: Duration = Duration::from_millis(40);
const CHURN_BEFORE_RESTART: Duration = Duration::from_secs(6);
const CHURN_AFTER_RESTART: Duration = Duration::from_secs(12);

struct Readers {
    stop: Rc<Cell<bool>>,
    task: JoinHandle<Result<()>>,
}

struct AccountReader {
    api: Api,
    addresses: Vec<Pubkey>,
    stop: Rc<Cell<bool>>,
}

impl AccountReader {
    async fn run(self) -> Result<()> {
        let mut reads = 0;
        while !self.stop.get() {
            self.read()
                .await
                .with_context(|| format!("snapshot account reads; completed={reads}"))?;
            reads += 1;
        }
        Ok(())
    }

    async fn read(&self) -> Result<()> {
        let accounts = self.api.get_multiple_accounts(&self.addresses).await?;
        ensure!(accounts.iter().all(Option::is_some));
        Ok(())
    }
}

impl Readers {
    fn start(api: Api, addresses: Vec<Pubkey>) -> Self {
        let stop = Rc::new(Cell::new(false));
        let reader = AccountReader {
            api,
            addresses,
            stop: stop.clone(),
        };
        let task = tokio::task::spawn_local(reader.run());
        Self { stop, task }
    }

    async fn stop(self, phase: &str) -> Result<()> {
        self.stop.set(true);
        self.task
            .await
            .with_context(|| format!("reader task {phase}"))?
            .with_context(|| format!("snapshot reads {phase}"))
    }
}

pub struct SnapshotReadRace {
    base: Rc<BaseCtx>,
    private: PrivateEr,
    payer: Keypair,
}

impl PrivateErScenario for SnapshotReadRace {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let private = topology::private_er(
            &base,
            ErOptions {
                label: LABEL.to_owned(),
                settings: Box::new(|params| {
                    params.engine.blockstore.superblock = SUPERBLOCK_SLOTS;
                }),
                ..Default::default()
            },
        )
        .await?;

        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        Ok(Self { base, private, payer })
    }

    async fn run(mut self) -> Result<()> {
        let mut sizes = vec![LARGE_ACCOUNT_SPACE; ACCOUNTS];
        let er = self.private.ctx();
        er.await_program_clone(&redshift_interface::id()).await?;
        let accounts = self
            .base
            .cells(REDSHIFT, &self.payer)
            .create_owned_sized(er.identity(), ACCOUNTS, LARGE_ACCOUNT_SPACE)
            .await?;
        let addresses: Vec<_> = accounts.iter().map(|account| account.address).collect();
        er.await_clones(&addresses, LARGE_ACCOUNT_SPACE as usize).await?;
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

        let timing = self.private.restart(RestartConfig::default()).await?;
        ensure!(timing.exit_code == Some(0));

        let er = self.private.ctx();
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
        self.private.finish().await?;
        Ok(())
    }
}

impl SnapshotReadRace {
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
            self.private
                .ctx()
                .submit_and_confirm(&self.payer, &[resize])
                .await
                .with_context(|| {
                    format!("resizing account {index} {phase}; completed grows={grows}")
                })?;
            sizes[index] = space;
            grows += 1;
            time::sleep(GROW_INTERVAL).await;
        }
        Ok(())
    }
}
