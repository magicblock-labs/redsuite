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
    Api, BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
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
const CLONE_TIMEOUT: Duration = Duration::from_secs(20);

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
        let missing: Vec<_> = accounts
            .iter()
            .enumerate()
            .filter_map(|(index, account)| account.is_none().then_some(index))
            .collect();
        let _: () = ensure!(
            missing.is_empty(),
            "missing snapshot account indices: {missing:?}"
        );
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

async fn churn(
    er: &ErCtx,
    payer: &Keypair,
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
        let resize = REDSHIFT.resize(payer.pubkey(), accounts[index].address, space);
        er.submit_and_confirm(payer, &[resize]).await.with_context(|| {
            format!("resizing account {index} {phase}; completed grows={grows}")
        })?;
        sizes[index] = space;
        grows += 1;
        time::sleep(GROW_INTERVAL).await;
    }
    Ok(())
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

    async fn run(self) -> Result<()> {
        let Self { base, mut private, payer } = self;
        let mut sizes = vec![LARGE_ACCOUNT_SPACE; ACCOUNTS];
        let er = private.ctx();
        er.await_program_clone(&redshift_interface::id(), CLONE_TIMEOUT).await?;
        let accounts = base
            .cells(REDSHIFT, &payer)
            .create_owned_sized(er.identity(), ACCOUNTS, LARGE_ACCOUNT_SPACE)
            .await?;
        let addresses: Vec<_> = accounts.iter().map(|account| account.address).collect();
        er.await_clones(&addresses, LARGE_ACCOUNT_SPACE as usize, CLONE_TIMEOUT).await?;
        let sponsor = base.funded_payer(crate::PAYER_LAMPORTS).await?;
        base.delegate_payer(&sponsor, &payer, er.identity()).await?;

        let readers = Readers::start(er.api().clone(), addresses.clone());
        churn(
            er,
            &payer,
            &accounts,
            &mut sizes,
            CHURN_BEFORE_RESTART,
            "before restart",
        )
        .await?;
        readers.stop("before restart").await?;

        let timing = private.restart(RestartConfig::default()).await?;
        ensure!(timing.exit_code == Some(0));

        let er = private.ctx();
        let readers = Readers::start(er.api().clone(), addresses.clone());
        churn(
            er,
            &payer,
            &accounts,
            &mut sizes,
            CHURN_AFTER_RESTART,
            "after restart",
        )
        .await?;
        readers.stop("after restart").await?;

        let observed: Vec<u32> = er
            .accounts(&addresses)
            .await?
            .into_iter()
            .map(|account| account.map_or(0, |a| a.data.len() as u32))
            .collect();
        ensure!(observed == sizes);
        private.finish().await?;
        Ok(())
    }
}
