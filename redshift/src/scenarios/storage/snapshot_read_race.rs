use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    cells::OwnedCell,
    check, check_eq,
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
    task: JoinHandle<(u64, Option<String>)>,
}

impl Readers {
    fn start(api: Api, addresses: Vec<Pubkey>) -> Self {
        let stop = Rc::new(Cell::new(false));
        let flag = stop.clone();
        let task = tokio::task::spawn_local(async move {
            let mut reads = 0u64;
            while !flag.get() {
                match api.get_multiple_accounts(&addresses).await {
                    Ok(accounts) if accounts.iter().all(Option::is_some) => {
                        reads += 1;
                    }
                    Ok(accounts) => {
                        let missing = addresses
                            .iter()
                            .zip(accounts.iter())
                            .filter(|(_, a)| a.is_none())
                            .map(|(pk, _)| pk.to_string())
                            .collect::<Vec<_>>()
                            .join(", ");
                        return (reads, Some(format!("missing account(s): {missing}")));
                    }
                    Err(error) => return (reads, Some(error.to_string())),
                }
            }
            (reads, None)
        });
        Self { stop, task }
    }

    async fn stop(self, phase: &str) -> Result<()> {
        self.stop.set(true);
        let (reads, failure) =
            self.task.await.map_err(|error| format!("reader task {phase}: {error}"))?;
        check!(
            failure.is_none(),
            "{phase}: account reads must keep succeeding while the er seals \
             superblocks; {reads} reads then: {}",
            failure.unwrap_or_default()
        )?;
        Ok(())
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
        let resize = REDSHIFT.resize(
            payer.pubkey(),
            REDSHIFT.primary(accounts[index].authority.pubkey()),
            space,
        );
        let submitted = er.submit_and_confirm(payer, &[resize]).await;
        check!(
            submitted.is_ok(),
            "{phase}: resizing account {index} must succeed after {grows} grows: {}",
            submitted.err().map(|e| e.to_string()).unwrap_or_default()
        )?;
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
        check_eq!(
            timing.exit_code,
            Some(0),
            "the er must stop cleanly on SIGTERM before the relaunch"
        )?;

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
        check_eq!(
            observed,
            sizes,
            "every account must retain its resized length"
        )?;
        private.finish().await?;
        Ok(())
    }
}
