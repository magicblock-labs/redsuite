use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::schedulecommit::{build, MainAccount};
use redsuite_core::{
    check, check_eq, prep, topology,
    topology::{ErOptions, PrivateEr, RestartConfig},
    BaseCtx, ChainCtx, DynError, ErCtx, PrivateErScenario, Result,
};
use signature::Signature;
use signer::Signer;
use tokio::time;
use tracing::debug;

use crate::PAYER_LAMPORTS;

const LABEL: &str = "ledger-retention";
const SUPERBLOCK_SLOTS: u64 = 40;
const LEDGER_SIZE_LIMIT_BYTES: u64 = 1;
const RETENTION_EVENTS: u64 = 3;
const RETENTION_TIMEOUT: Duration = Duration::from_secs(180);
const TX_INTERVAL: Duration = Duration::from_millis(200);
const PROGRAM_CLONE_TIMEOUT: Duration = Duration::from_secs(20);
const TX_LEDGER_TIMEOUT: Duration = Duration::from_secs(10);
const SIGNATURE_WINDOW: usize = 1_000;
const BLOCK_SETTLE_SLOTS: u64 = 2;
const TRUNCATIONS: &str = r#"engine_ledger_operation_duration_micros_count{op="truncate"}"#;
const SUPERBLOCKS: &str = "engine_ledger_superblocks";

#[derive(Clone)]
struct Sent {
    signature: Signature,
    slot: u64,
}

struct HistoryView {
    pruned: Vec<Sent>,
    retained: Vec<Sent>,
}

impl HistoryView {
    fn max_pruned_slot(&self) -> Option<u64> {
        self.pruned.iter().map(|sent| sent.slot).max()
    }

    fn min_retained_slot(&self) -> Option<u64> {
        self.retained.iter().map(|sent| sent.slot).min()
    }
}

async fn truncations(er: &ErCtx) -> Result<u64> {
    let metrics = er.scrape_metrics().await?;
    Ok(metrics.get(TRUNCATIONS).unwrap_or(0.0) as u64)
}

async fn superblocks(er: &ErCtx) -> Result<u64> {
    let metrics = er.scrape_metrics().await?;
    Ok(metrics.get(SUPERBLOCKS).unwrap_or(0.0) as u64)
}

async fn delegate_counter(base: &BaseCtx, er: &ErCtx, payer: &Keypair) -> Result<Pubkey> {
    let payer_chain = base.funded_payer(PAYER_LAMPORTS).await?;
    let counter = base
        .schedulecommit(payer)
        .init_counter(er.identity(), prep::COMMIT_FREQUENCY_MS)
        .await?;
    base.delegate_payer(&payer_chain, payer, er.identity()).await?;
    Ok(counter)
}

async fn seed_history(er: &ErCtx, counter: &Pubkey, sent: &mut Vec<Sent>) -> Result<()> {
    let listed = check::poll_for(
        "the er records the clone of the delegated counter",
        PROGRAM_CLONE_TIMEOUT,
        || async {
            check!(
                er.account(counter).await?.is_some(),
                "counter {counter} not cloned"
            )?;
            let signatures = er.api().get_signatures_for_address(counter, SIGNATURE_WINDOW).await?;
            check!(!signatures.is_empty(), "counter {counter} has no history")?;
            Ok::<_, DynError>(signatures)
        },
    )
    .await?;
    for signature in listed.iter().rev() {
        let signature = *signature;
        let tx = er
            .api()
            .get_transaction(&signature)
            .await?
            .ok_or_else(|| format!("listed transaction {signature} is missing on the er"))?;
        sent.push(Sent { signature, slot: tx.slot });
    }
    Ok(())
}

fn add_count(adds: u64) -> u8 {
    (adds % 255 + 1) as u8
}

async fn send_add(er: &ErCtx, payer: &Keypair, count: u8, sent: &mut Vec<Sent>) -> Result<()> {
    let signature = er.submit_and_confirm(payer, &[build::add(payer.pubkey(), count)]).await?;
    let tx = er.api().await_transaction(&signature, TX_LEDGER_TIMEOUT).await?;
    check!(
        tx.err.is_none(),
        "add {signature} must succeed on the er, got {:?}",
        tx.err
    )?;
    sent.push(Sent { signature, slot: tx.slot });
    Ok(())
}

async fn observe(er: &ErCtx, sent: &[Sent]) -> Result<HistoryView> {
    let mut view = HistoryView {
        pruned: Vec::new(),
        retained: Vec::new(),
    };
    for entry in sent {
        match er.api().get_transaction(&entry.signature).await? {
            Some(tx) => {
                check_eq!(
                    tx.slot,
                    entry.slot,
                    "retained transaction {} must keep its slot",
                    entry.signature
                )?;
                view.retained.push(entry.clone());
            }
            None => view.pruned.push(entry.clone()),
        }
    }
    Ok(view)
}

async fn counter_value(er: &ErCtx, counter: &Pubkey) -> Result<u64> {
    let account = er.account(counter).await?.ok_or("the delegated counter is missing on the er")?;
    Ok(MainAccount::try_decode(&account.data)?.count)
}

async fn verify(
    er: &ErCtx,
    label: &str,
    view: &HistoryView,
    previous_pruned: usize,
    counter: &Pubkey,
    expected_count: u64,
) -> Result<()> {
    check!(
        view.pruned.len() >= previous_pruned,
        "{label}: pruned history must not come back — {} pruned before, {} \
         now",
        previous_pruned,
        view.pruned.len()
    )?;
    if let (Some(max_pruned), Some(min_retained)) =
        (view.max_pruned_slot(), view.min_retained_slot())
    {
        check!(
            max_pruned < min_retained,
            "{label}: retention must remove only the oldest history — a \
             pruned transaction sits in slot {max_pruned} while slot \
             {min_retained} is still retained"
        )?;
    }

    let tip = er.api().get_slot().await?;
    for entry in &view.pruned {
        let block = er.api().get_block(entry.slot).await?;
        check!(
            block.is_none(),
            "{label}: slot {} holds pruned transaction {} but getBlock still \
             returns it",
            entry.slot,
            entry.signature
        )?;
    }
    for entry in &view.retained {
        if entry.slot + BLOCK_SETTLE_SLOTS > tip {
            continue;
        }
        let block = er.api().get_block(entry.slot).await?;
        check!(
            block.is_some(),
            "{label}: slot {} holds retained transaction {} but getBlock \
             returns nothing",
            entry.slot,
            entry.signature
        )?;
    }

    let listed = er.api().get_signatures_for_address(counter, SIGNATURE_WINDOW).await?;
    let expected: Vec<Signature> =
        view.retained.iter().rev().map(|entry| entry.signature).collect();
    check_eq!(
        listed,
        expected,
        "{label}: getSignaturesForAddress must list exactly the retained \
         transactions, newest first"
    )?;

    check_eq!(
        counter_value(er, counter).await?,
        expected_count,
        "{label}: the delegated counter must hold every add"
    )?;
    Ok(())
}

pub struct LedgerRetention {
    base: Rc<BaseCtx>,
    private: PrivateEr,
    payer: Keypair,
}

impl PrivateErScenario for LedgerRetention {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let private = topology::private_er(
            &base,
            ErOptions {
                label: LABEL.to_owned(),
                settings: Box::new(|params| {
                    params.engine.blockstore.superblock = SUPERBLOCK_SLOTS;
                    params.engine.ledger.size_limit = LEDGER_SIZE_LIMIT_BYTES;
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
        let mut sent: Vec<Sent> = Vec::new();
        let mut adds: u64 = 0;
        let mut total: u64 = 0;
        let mut events: u64 = 0;
        let mut previous_pruned = 0usize;
        let mut first_pruning_event: Option<u64> = None;
        let counter;
        let before;
        {
            let er = private.ctx();
            er.await_program_clone(&redshift_interface::id(), PROGRAM_CLONE_TIMEOUT).await?;
            counter = delegate_counter(&base, er, &payer).await?;
            seed_history(er, &counter, &mut sent).await?;

            let baseline = truncations(er).await?;
            let started = Instant::now();
            loop {
                let count = add_count(adds);
                send_add(er, &payer, count, &mut sent).await?;
                adds += 1;
                total += u64::from(count);

                let observed = truncations(er).await? - baseline;
                if observed > events {
                    events = observed;
                    let view = observe(er, &sent).await?;
                    verify(
                        er,
                        &format!("retention event {events}"),
                        &view,
                        previous_pruned,
                        &counter,
                        total,
                    )
                    .await?;
                    if view.pruned.len() > previous_pruned && first_pruning_event.is_none() {
                        first_pruning_event = Some(events);
                    }
                    previous_pruned = view.pruned.len();
                    let superblocks = superblocks(er).await?;
                    debug!(
                        pruned = view.pruned.len(),
                        retained = view.retained.len(),
                        superblocks,
                        "retention advanced",
                    );
                }
                if events >= RETENTION_EVENTS && first_pruning_event.is_some() {
                    break;
                }
                check!(
                    started.elapsed() < RETENTION_TIMEOUT,
                    "retention must purge {RETENTION_EVENTS} superblocks and \
                     reach this scenario's history within \
                     {RETENTION_TIMEOUT:?}; saw {events} events and {} pruned \
                     transactions",
                    previous_pruned
                )?;
                time::sleep(TX_INTERVAL).await;
            }

            before = observe(er, &sent).await?;
            verify(
                er,
                "before restart",
                &before,
                previous_pruned,
                &counter,
                total,
            )
            .await?;
            check!(
                !before.retained.is_empty(),
                "some of this scenario's history must still be retained \
                 before the restart"
            )?;
        }

        let timing = private.restart(RestartConfig::default()).await?;
        check_eq!(
            timing.exit_code,
            Some(0),
            "the er must stop cleanly on SIGTERM before the relaunch"
        )?;
        check!(
            !timing.needed_sigkill,
            "the graceful stop must not escalate to SIGKILL"
        )?;

        let er = private.ctx();
        let after = observe(er, &sent).await?;
        check!(
            after.pruned.len() >= before.pruned.len(),
            "a restart must not resurrect pruned history — {} pruned before, \
             {} after",
            before.pruned.len(),
            after.pruned.len()
        )?;
        verify(
            er,
            "after restart",
            &after,
            before.pruned.len(),
            &counter,
            total,
        )
        .await?;
        check!(
            !after.retained.is_empty(),
            "the restart must preserve the retained history"
        )?;

        let count = add_count(adds);
        send_add(er, &payer, count, &mut sent).await?;
        total += u64::from(count);
        let final_view = observe(er, &sent).await?;
        verify(
            er,
            "after restart add",
            &final_view,
            after.pruned.len(),
            &counter,
            total,
        )
        .await?;
        private.finish().await?;
        Ok(())
    }
}
