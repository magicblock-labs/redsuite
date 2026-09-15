use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::schedulecommit::{build, ORDER_BOOK_INIT_SIZE};
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, dlp, prep, system, topology,
    topology::{ErOptions, RestartConfig},
    Api, BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport,
};
use signer::Signer;

const LABEL: &str = "snapshot-read-race";
const SUPERBLOCK_SLOTS: u64 = 10;
const BOOKS: usize = 8;
const GROW_BYTES: u64 = 32;
const GROW_VARIANTS: u64 = 64;
const PAYER_LAMPORTS: u64 = 10_000_000_000;
const GROW_INTERVAL: Duration = Duration::from_millis(40);
const CHURN_BEFORE_RESTART: Duration = Duration::from_secs(6);
const CHURN_AFTER_RESTART: Duration = Duration::from_secs(12);
const CLONE_TIMEOUT: Duration = Duration::from_secs(20);
const RESIZES: &str = "engine_accountsdb_persisted_resizes";
const COMPACTIONS: &str = "engine_accountsdb_persisted_compactions";

pub struct SnapshotReadRace;

struct Book {
    manager: Keypair,
    address: Pubkey,
}

struct Readers {
    stop: Rc<Cell<bool>>,
    task: tokio::task::JoinHandle<(u64, Option<String>)>,
}

impl Readers {
    fn start(api: Api, addresses: Vec<Pubkey>) -> Self {
        let stop = Rc::new(Cell::new(false));
        let flag = stop.clone();
        let task = tokio::task::spawn_local(async move {
            let mut reads = 0u64;
            while !flag.get() {
                match api.get_multiple_accounts(&addresses).await {
                    Ok(_) => reads += 1,
                    Err(error) => return (reads, Some(error.to_string())),
                }
            }
            (reads, None)
        });
        Self { stop, task }
    }

    async fn stop(self, phase: &str) -> Result<u64> {
        self.stop.set(true);
        let (reads, failure) = self
            .task
            .await
            .map_err(|error| format!("reader task {phase}: {error}"))?;
        check!(
            failure.is_none(),
            "{phase}: account reads must keep succeeding while the er seals \
             superblocks; {reads} reads then: {}",
            failure.unwrap_or_default()
        )?;
        Ok(reads)
    }
}

async fn metric(er: &ErCtx, name: &str) -> Result<u64> {
    let metrics = er.scrape_metrics().await?;
    Ok(metrics.get(name).unwrap_or(0.0) as u64)
}

async fn delegate_payer(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
) -> Result<()> {
    let sponsor = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let ixs = [
        system::assign(&payer.pubkey(), &dlp::dlp_id()),
        dlp::delegate_account(
            &sponsor.pubkey(),
            &payer.pubkey(),
            &er.identity(),
        ),
    ];
    base.submit_and_confirm_with(&sponsor, &[payer], &ixs)
        .await?;
    Ok(())
}

async fn delegate_books(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
) -> Result<Vec<Book>> {
    let mut books = Vec::with_capacity(BOOKS);
    for _ in 0..BOOKS {
        let manager = Keypair::new();
        let (init, address) =
            build::init_order_book(payer.pubkey(), manager.pubkey());
        let delegate = build::delegate_order_book(
            payer.pubkey(),
            manager.pubkey(),
            prep::COMMIT_FREQUENCY_MS,
            Some(er.identity()),
        );
        base.submit_and_confirm_with(payer, &[&manager], &[init, delegate])
            .await?;
        books.push(Book { manager, address });
    }
    for book in &books {
        let address = book.address;
        check::poll(
            &format!("the er clones the delegated order book {address}"),
            CLONE_TIMEOUT,
            || async {
                matches!(
                    er.account(&address).await,
                    Ok(Some(clone)) if clone.data.len() == ORDER_BOOK_INIT_SIZE
                )
            },
        )
        .await?;
    }
    Ok(books)
}

async fn churn(
    er: &ErCtx,
    payer: &Keypair,
    books: &[Book],
    sizes: &mut [u64],
    duration: Duration,
    phase: &str,
) -> Result<u64> {
    let started = Instant::now();
    let mut grows = 0u64;
    while started.elapsed() < duration {
        let index = grows as usize % books.len();
        let book = &books[index];
        let bytes = GROW_BYTES + (grows / books.len() as u64) % GROW_VARIANTS;
        let grow = build::grow_order_book(
            payer.pubkey(),
            book.manager.pubkey(),
            bytes,
        );
        let submitted = er.submit_and_confirm(payer, &[grow]).await;
        check!(
            submitted.is_ok(),
            "{phase}: growing order book {} must succeed after {grows} \
             grows: {}",
            book.address,
            submitted.err().map(|e| e.to_string()).unwrap_or_default()
        )?;
        sizes[index] += bytes;
        grows += 1;
        tokio::time::sleep(GROW_INTERVAL).await;
    }
    Ok(grows)
}

#[async_trait(?Send)]
impl PrivateErScenario for SnapshotReadRace {
    fn name(&self) -> &str {
        "redshift/snapshot_read_race"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let mut private = topology::private_er(
            base,
            ErOptions {
                label: LABEL.to_owned(),
                env: vec![(
                    "MBV_ENGINE__BLOCKSTORE__SUPERBLOCK".to_owned(),
                    SUPERBLOCK_SLOTS.to_string(),
                )],
                request_timeout: None,
                base_endpoints: None,
            },
        )
        .await?;

        let payer = prep::funded_payer(base, PAYER_LAMPORTS).await?;
        let books;
        let mut sizes = vec![ORDER_BOOK_INIT_SIZE as u64; BOOKS];
        let reads_before;
        let grows_before;
        {
            let er = private.ctx();
            check::poll(
                "the er clones the redshift program as executable",
                CLONE_TIMEOUT,
                || async {
                    matches!(
                        er.account(&redshift_interface::id()).await,
                        Ok(Some(clone)) if clone.executable
                    )
                },
            )
            .await?;
            books = delegate_books(base, er, &payer).await?;
            delegate_payer(base, er, &payer).await?;
            let addresses: Vec<Pubkey> =
                books.iter().map(|book| book.address).collect();

            let readers = Readers::start(er.api().clone(), addresses.clone());
            grows_before = churn(
                er,
                &payer,
                &books,
                &mut sizes,
                CHURN_BEFORE_RESTART,
                "before restart",
            )
            .await?;
            reads_before = readers.stop("before restart").await?;
        }

        let timing = private.restart(RestartConfig::default()).await?;
        check_eq!(
            timing.exit_code,
            Some(0),
            "the er must stop cleanly on SIGTERM before the relaunch"
        )?;

        let er = private.ctx();
        let addresses: Vec<Pubkey> =
            books.iter().map(|book| book.address).collect();
        let readers = Readers::start(er.api().clone(), addresses.clone());
        let grows_after = churn(
            er,
            &payer,
            &books,
            &mut sizes,
            CHURN_AFTER_RESTART,
            "after restart",
        )
        .await?;
        let reads_after = readers.stop("after restart").await?;

        let observed: Vec<u64> = er
            .accounts(&addresses)
            .await?
            .into_iter()
            .map(|account| account.map_or(0, |a| a.data.len() as u64))
            .collect();
        check_eq!(
            observed,
            sizes,
            "every order book must reflect all of its grows"
        )?;
        let resizes = metric(er, RESIZES).await?;
        let compactions = metric(er, COMPACTIONS).await?;
        private.finish().await?;

        Ok(ScenarioReport::ok(self.name())
            .setting("superblock slots", SUPERBLOCK_SLOTS)
            .setting("order books", BOOKS)
            .setting("grow bytes", GROW_BYTES)
            .metric("grows before restart", Unit::Count, grows_before as f64)
            .metric("grows after restart", Unit::Count, grows_after as f64)
            .metric("reads before restart", Unit::Count, reads_before as f64)
            .metric("reads after restart", Unit::Count, reads_after as f64)
            .metric("storage resizes", Unit::Count, resizes as f64)
            .metric("storage compactions", Unit::Count, compactions as f64)
            .metric(
                "restart startup ms",
                Unit::Millis,
                timing.startup.as_secs_f64() * 1e3,
            ))
    }
}
