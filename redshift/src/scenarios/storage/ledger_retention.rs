use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::flexi::{build, FlexiCounter};
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, dlp, prep, system, topology,
    topology::{ErOptions, RestartConfig},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport,
};
use signature::Signature;
use signer::Signer;

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
const TRUNCATIONS: &str =
    r#"engine_ledger_operation_duration_micros_count{op="truncate"}"#;
const SUPERBLOCKS: &str = "engine_ledger_superblocks";

pub struct LedgerRetention;

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

async fn await_program_clone(er: &ErCtx, program: &Pubkey) -> Result<()> {
    check::poll(
        &format!("the er clones the program {program} as executable"),
        PROGRAM_CLONE_TIMEOUT,
        || async {
            matches!(er.account(program).await, Ok(Some(clone)) if clone.executable)
        },
    )
    .await?;
    Ok(())
}

async fn delegate_counter(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
) -> Result<Pubkey> {
    let payer_chain = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let (init, counter) = build::init_counter(payer.pubkey(), LABEL);
    base.submit_and_confirm(payer, &[init]).await?;
    base.submit_and_confirm(
        payer,
        &[build::delegate_counter(
            payer.pubkey(),
            prep::COMMIT_FREQUENCY_MS,
            Some(er.identity()),
        )],
    )
    .await?;
    let delegate_payer = [
        system::assign(&payer.pubkey(), &dlp::dlp_id()),
        dlp::delegate_account(
            &payer_chain.pubkey(),
            &payer.pubkey(),
            &er.identity(),
        ),
    ];
    base.submit_and_confirm_with(&payer_chain, &[payer], &delegate_payer)
        .await?;
    Ok(counter)
}

async fn send_add(
    er: &ErCtx,
    payer: &Keypair,
    sent: &mut Vec<Sent>,
) -> Result<()> {
    let signature = er
        .submit_and_confirm(payer, &[build::add(payer.pubkey(), 1)])
        .await?;
    let tx = er
        .api()
        .await_transaction(&signature, TX_LEDGER_TIMEOUT)
        .await?;
    check!(
        tx.err.is_none(),
        "add {signature} must succeed on the er, got {:?}",
        tx.err
    )?;
    sent.push(Sent {
        signature,
        slot: tx.slot,
    });
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
    let account = er
        .account(counter)
        .await?
        .ok_or("the delegated counter is missing on the er")?;
    Ok(FlexiCounter::try_decode(&account.data)?.count)
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

    let listed = er
        .api()
        .get_signatures_for_address(counter, SIGNATURE_WINDOW)
        .await?;
    let expected: Vec<String> = view
        .retained
        .iter()
        .rev()
        .map(|entry| entry.signature.to_string())
        .collect();
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

#[async_trait(?Send)]
impl PrivateErScenario for LedgerRetention {
    fn name(&self) -> &str {
        "redshift/ledger_retention"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let mut private = topology::private_er(
            base,
            ErOptions {
                label: LABEL.to_owned(),
                env: vec![
                    (
                        "MBV_ENGINE__BLOCKSTORE__SUPERBLOCK".to_owned(),
                        SUPERBLOCK_SLOTS.to_string(),
                    ),
                    (
                        "MBV_ENGINE__LEDGER__SIZE_LIMIT".to_owned(),
                        LEDGER_SIZE_LIMIT_BYTES.to_string(),
                    ),
                ],
                request_timeout: None,
            },
        )
        .await?;

        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let mut sent: Vec<Sent> = Vec::new();
        let mut adds: u64 = 0;
        let mut events: u64 = 0;
        let mut previous_pruned = 0usize;
        let mut first_pruning_event: Option<u64> = None;
        let counter;
        let before;
        {
            let er = private.ctx();
            await_program_clone(er, &redshift_interface::id()).await?;
            counter = delegate_counter(base, er, &payer).await?;

            let baseline = truncations(er).await?;
            let started = Instant::now();
            loop {
                send_add(er, &payer, &mut sent).await?;
                adds += 1;

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
                        adds,
                    )
                    .await?;
                    if view.pruned.len() > previous_pruned
                        && first_pruning_event.is_none()
                    {
                        first_pruning_event = Some(events);
                    }
                    previous_pruned = view.pruned.len();
                    eprintln!(
                        "[redsuite] {}: retention event {events}: {} pruned, \
                         {} retained, {} adds, superblocks allocated {}",
                        self.name(),
                        view.pruned.len(),
                        view.retained.len(),
                        adds,
                        superblocks(er).await?,
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
                tokio::time::sleep(TX_INTERVAL).await;
            }

            before = observe(er, &sent).await?;
            verify(
                er,
                "before restart",
                &before,
                previous_pruned,
                &counter,
                adds,
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
            adds,
        )
        .await?;
        check!(
            !after.retained.is_empty(),
            "the restart must preserve the retained history"
        )?;

        send_add(er, &payer, &mut sent).await?;
        adds += 1;
        let final_view = observe(er, &sent).await?;
        verify(
            er,
            "after restart add",
            &final_view,
            after.pruned.len(),
            &counter,
            adds,
        )
        .await?;
        let superblocks_allocated = superblocks(er).await?;
        private.finish().await?;

        Ok(ScenarioReport::ok(self.name())
            .setting("superblock slots", SUPERBLOCK_SLOTS)
            .setting("ledger size limit bytes", LEDGER_SIZE_LIMIT_BYTES)
            .setting("counter", counter)
            .metric("retention events", Unit::Count, events as f64)
            .metric(
                "first event pruning scenario history",
                Unit::Count,
                first_pruning_event.unwrap_or(0) as f64,
            )
            .metric("transactions sent", Unit::Count, sent.len() as f64)
            .metric(
                "pruned transactions",
                Unit::Count,
                final_view.pruned.len() as f64,
            )
            .metric(
                "retained transactions",
                Unit::Count,
                final_view.retained.len() as f64,
            )
            .metric(
                "superblocks allocated",
                Unit::Count,
                superblocks_allocated as f64,
            )
            .metric(
                "restart shutdown ms",
                Unit::Millis,
                timing.shutdown.as_secs_f64() * 1e3,
            )
            .metric(
                "restart startup ms",
                Unit::Millis,
                timing.startup.as_secs_f64() * 1e3,
            ))
    }
}
