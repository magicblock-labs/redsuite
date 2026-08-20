pub mod ledger_restore_basics;
pub mod ledger_restore_chain;

use std::time::Duration;

use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::flexi::build as flexi;
use redsuite_core::{
    check, dlp, prep, system, topology, BaseCtx, ChainCtx, ErCtx, Result,
};
use signer::Signer;

const SOL: u64 = 1_000_000_000;
const PERSIST_SLOTS: u64 = 11;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const SLOT_TIMEOUT: Duration = Duration::from_secs(30);
const CLONE_TIMEOUT: Duration = Duration::from_secs(20);

async fn advance_slots(er: &ErCtx, count: u64) -> Result<u64> {
    let target = er.api().get_slot().await? + count;
    check::poll(
        &format!("the er reaches slot {target}"),
        SLOT_TIMEOUT,
        || async {
            matches!(er.api().get_slot().await, Ok(slot) if slot >= target)
        },
    )
    .await?;
    er.api().get_slot().await
}

async fn boot_writer(
    base: &BaseCtx,
    label: &str,
) -> Result<topology::PrivateEr> {
    let er = topology::private_er(
        base,
        topology::ErOptions {
            label: label.to_owned(),
            ..Default::default()
        },
    )
    .await?;
    er.wait_ready(READY_TIMEOUT).await?;
    Ok(er)
}

async fn boot_reader(
    base: &BaseCtx,
    label: &str,
    lifecycle: &str,
    reset: bool,
    env: Vec<(String, String)>,
) -> Result<topology::PrivateEr> {
    let er = topology::private_er(
        base,
        topology::ErOptions {
            label: label.to_owned(),
            lifecycle: lifecycle.to_owned(),
            keep_storage: true,
            reset,
            env,
            ..Default::default()
        },
    )
    .await?;
    er.wait_ready(READY_TIMEOUT).await?;
    Ok(er)
}

async fn await_er_balance(
    er: &ErCtx,
    account: &Pubkey,
    expected: u64,
) -> Result<()> {
    check::poll(
        &format!("the er balance of {account} reaches {expected}"),
        CLONE_TIMEOUT,
        || async {
            matches!(
                er.api().get_balance(account).await,
                Ok(balance) if balance == expected
            )
        },
    )
    .await?;
    Ok(())
}

struct CounterActor {
    payer: Keypair,
    counter: Pubkey,
}

async fn counter_actor(
    base: &BaseCtx,
    er: &ErCtx,
    funder: &Keypair,
    label: &str,
) -> Result<CounterActor> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let (init, counter) = flexi::init_counter(payer.pubkey(), label);
    base.send(&payer, &[init]).await?;
    base.send(
        &payer,
        &[flexi::delegate_counter(
            payer.pubkey(),
            prep::COMMIT_FREQUENCY_MS,
            Some(er.identity()),
        )],
    )
    .await?;
    let setup = [
        system::assign(&payer.pubkey(), &dlp::dlp_id()),
        dlp::delegate_account(
            &funder.pubkey(),
            &payer.pubkey(),
            &er.identity(),
        ),
    ];
    base.send_with(funder, &[&payer], &setup).await?;
    check::poll(
        "the er clones the delegated counter",
        CLONE_TIMEOUT,
        || async {
            matches!(
                er.account(&counter).await,
                Ok(Some(clone)) if !clone.data.is_empty()
            )
        },
    )
    .await?;
    Ok(CounterActor { payer, counter })
}
