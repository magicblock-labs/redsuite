use std::time::Duration;

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_program::flexi::{build as flexi, FlexiCounter};
use redsuite_core::{
    assert::poll_until, dlp, prep, receipt, system, topology, BaseCtx,
    ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport,
};
use signer::Signer;

const SOL: u64 = 1_000_000_000;
const PERSIST_SLOTS: u64 = 11;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const SLOT_TIMEOUT: Duration = Duration::from_secs(30);
const CLONE_TIMEOUT: Duration = Duration::from_secs(20);
const RESTORE_TIMEOUT: Duration = Duration::from_secs(45);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);
const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;
const CRANK_TASK_ID: i64 = 108;
const LABEL: &str = "counter of payer";

pub struct LedgerRestoreChain;

async fn advance_slots(er: &ErCtx, count: u64) -> Result<u64> {
    let target = er.api().get_slot().await? + count;
    poll_until(SLOT_TIMEOUT, || async {
        matches!(er.api().get_slot().await, Ok(slot) if slot >= target)
    })
    .await;
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
) -> Result<topology::PrivateEr> {
    let er = topology::private_er(
        base,
        topology::ErOptions {
            label: label.to_owned(),
            keep_storage: true,
            reset: false,
            ..Default::default()
        },
    )
    .await?;
    er.wait_ready(READY_TIMEOUT).await?;
    Ok(er)
}

async fn delegated_wallet(
    base: &BaseCtx,
    funder: &Keypair,
    validator: Pubkey,
    lamports: u64,
) -> Result<Keypair> {
    let wallet = Keypair::new();
    base.airdrop(&wallet.pubkey(), lamports).await?;
    let setup = [
        system::assign(&wallet.pubkey(), &dlp::dlp_id()),
        dlp::delegate_account(&funder.pubkey(), &wallet.pubkey(), &validator),
    ];
    base.send_with(funder, &[&wallet], &setup).await?;
    Ok(wallet)
}

async fn await_er_balance(
    er: &ErCtx,
    account: &Pubkey,
    expected: u64,
) -> Result<()> {
    poll_until(CLONE_TIMEOUT, || async {
        matches!(
            er.api().get_balance(account).await,
            Ok(balance) if balance == expected
        )
    })
    .await;
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
) -> Result<CounterActor> {
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let (init, counter) = flexi::init_counter(payer.pubkey(), LABEL);
    base.send(&payer, &[init]).await?;
    base.send(
        &payer,
        &[flexi::delegate_counter(
            payer.pubkey(),
            COMMIT_FREQUENCY_MS,
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
    poll_until(CLONE_TIMEOUT, || async {
        matches!(
            er.account(&counter).await,
            Ok(Some(clone)) if !clone.data.is_empty()
        )
    })
    .await;
    Ok(CounterActor { payer, counter })
}

fn expected_counter(count: u64, updates: u64) -> FlexiCounter {
    FlexiCounter {
        count,
        updates,
        label: LABEL.to_owned(),
    }
}

async fn read_counter(er: &ErCtx, counter: &Pubkey) -> Result<FlexiCounter> {
    let account = er
        .account(counter)
        .await?
        .ok_or("the counter account is missing")?;
    Ok(FlexiCounter::try_decode(&account.data)?)
}

async fn await_counter_state(
    er: &ErCtx,
    counter: &Pubkey,
    expected: &FlexiCounter,
) -> Result<()> {
    poll_until(RESTORE_TIMEOUT, || async {
        matches!(
            read_counter(er, counter).await,
            Ok(state) if &state == expected
        )
    })
    .await;
    Ok(())
}

async fn chain_signature_count(
    base: &BaseCtx,
    counter: &Pubkey,
) -> Result<usize> {
    Ok(base
        .api()
        .get_signatures_for_address(counter, 20)
        .await?
        .len())
}

#[async_trait(?Send)]
impl PrivateErScenario for LedgerRestoreChain {
    fn name(&self) -> &str {
        "redshift/ledger_restore_chain"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let funder = prep::funded_payer(base, 10 * SOL).await?;
        let mut report = ScenarioReport::ok(self.name());

        // delegated account [06] — an ephemeral restore replays the write to
        // a delegated counter.
        {
            let mut writer = boot_writer(base, "restore-delegated").await?;
            let actor = counter_actor(base, writer.ctx(), &funder).await?;
            writer
                .ctx()
                .send(&actor.payer, &[flexi::add(actor.payer.pubkey(), 3)])
                .await?;
            assert_eq!(
                read_counter(writer.ctx(), &actor.counter).await?,
                expected_counter(3, 1),
                "the written counter state"
            );
            advance_slots(writer.ctx(), PERSIST_SLOTS).await?;
            writer.stop(true).await?;
            drop(writer);

            let reader = boot_reader(base, "restore-delegated").await?;
            await_counter_state(
                reader.ctx(),
                &actor.counter,
                &expected_counter(3, 1),
            )
            .await?;
        }

        // delegated + committed [07] — the committed state survives the
        // restore on both sides, and replay does not send the commit again.
        {
            let mut writer = boot_writer(base, "restore-commit").await?;
            let actor = counter_actor(base, writer.ctx(), &funder).await?;
            let er = writer.ctx();
            er.send(&actor.payer, &[flexi::add(actor.payer.pubkey(), 3)])
                .await?;
            er.send(&actor.payer, &[flexi::mul(actor.payer.pubkey(), 2)])
                .await?;
            assert_eq!(
                read_counter(er, &actor.counter).await?,
                expected_counter(6, 2),
                "the pre-commit counter state"
            );
            advance_slots(er, PERSIST_SLOTS).await?;

            let vault = dlp::magic_fee_vault_pda(&er.identity());
            assert!(
                er.account(&vault).await?.is_some(),
                "the magic fee vault must be on the er"
            );
            let signature = er
                .send(
                    &actor.payer,
                    &[flexi::add_and_schedule_commit(
                        actor.payer.pubkey(),
                        4,
                        false,
                        Some(vault),
                    )],
                )
                .await?;
            let commit_receipt = receipt::fetch_commit_receipt(
                er.api(),
                &signature,
                RECEIPT_TIMEOUT,
            )
            .await?;
            if let Some(message) = &commit_receipt.error_message {
                return Err(format!("the commit failed: {message}").into());
            }
            receipt::confirm_base_signatures(
                base.api(),
                &commit_receipt,
                BASE_CONFIRM_TIMEOUT,
            )
            .await?;
            assert_eq!(
                read_counter(er, &actor.counter).await?,
                expected_counter(10, 3),
                "the ephem counter state after the commit"
            );
            poll_until(RESTORE_TIMEOUT, || async {
                matches!(
                    base.account(&actor.counter).await,
                    Ok(Some(acc))
                        if FlexiCounter::try_decode(&acc.data).ok()
                            == Some(expected_counter(10, 3))
                )
            })
            .await;
            let written_signatures =
                chain_signature_count(base, &actor.counter).await?;
            advance_slots(er, PERSIST_SLOTS).await?;
            writer.stop(true).await?;
            drop(writer);

            let reader = boot_reader(base, "restore-commit").await?;
            await_counter_state(
                reader.ctx(),
                &actor.counter,
                &expected_counter(10, 3),
            )
            .await?;
            let on_base = base
                .account(&actor.counter)
                .await?
                .ok_or("the counter is not on base after the restore")?;
            assert_eq!(
                FlexiCounter::try_decode(&on_base.data)?,
                expected_counter(10, 3),
                "the chain counter state after the restore"
            );
            advance_slots(reader.ctx(), 3).await?;
            let restored_signatures =
                chain_signature_count(base, &actor.counter).await?;
            assert_eq!(
                restored_signatures, written_signatures,
                "the replay must not send the commit to base again"
            );
            report =
                report.setting("commit chain signatures", written_signatures);
        }

        // account flush [12] — a restore that hydrates from flushed account
        // state still lands both transfers.
        {
            let mut writer = boot_writer(base, "restore-flush").await?;
            let identity = writer.ctx().identity();
            advance_slots(writer.ctx(), 1).await?;
            let sender =
                delegated_wallet(base, &funder, identity, 1_111_111).await?;
            let receiver =
                delegated_wallet(base, &funder, identity, 1_000_000).await?;
            let er = writer.ctx();
            er.send(
                &sender,
                &[system::transfer(&sender.pubkey(), &receiver.pubkey(), 111)],
            )
            .await?;
            await_er_balance(er, &receiver.pubkey(), 1_000_111).await?;
            advance_slots(er, 3).await?;
            er.send(
                &sender,
                &[system::transfer(
                    &sender.pubkey(),
                    &receiver.pubkey(),
                    111_000,
                )],
            )
            .await?;
            await_er_balance(er, &receiver.pubkey(), 1_111_111).await?;
            advance_slots(er, PERSIST_SLOTS).await?;
            writer.stop(true).await?;
            drop(writer);

            let reader = boot_reader(base, "restore-flush").await?;
            await_er_balance(reader.ctx(), &receiver.pubkey(), 1_111_111)
                .await?;
        }

        // crank persistence [16] — a scheduled task survives the restore and
        // keeps cranking.
        {
            let mut writer = boot_writer(base, "restore-crank").await?;
            let actor = counter_actor(base, writer.ctx(), &funder).await?;
            writer
                .ctx()
                .send(
                    &actor.payer,
                    &[flexi::schedule_counter_task(
                        actor.payer.pubkey(),
                        CRANK_TASK_ID,
                        50,
                        1000,
                        false,
                        false,
                    )],
                )
                .await?;
            advance_slots(writer.ctx(), 3).await?;
            let written =
                read_counter(writer.ctx(), &actor.counter).await?.count;
            assert!(
                written > 0,
                "the crank must move the counter before the kill"
            );
            advance_slots(writer.ctx(), PERSIST_SLOTS).await?;
            writer.stop(true).await?;
            drop(writer);

            let reader = boot_reader(base, "restore-crank").await?;
            let er = reader.ctx();
            poll_until(RESTORE_TIMEOUT, || async {
                matches!(
                    read_counter(er, &actor.counter).await,
                    Ok(state) if state.count >= written
                )
            })
            .await;
            let restored = read_counter(er, &actor.counter).await?.count;
            poll_until(RESTORE_TIMEOUT, || async {
                matches!(
                    read_counter(er, &actor.counter).await,
                    Ok(state) if state.count > restored
                )
            })
            .await;
            report = report
                .setting("crank count at kill", written)
                .setting("crank count at restore", restored);
        }

        Ok(report.setting("persist slots", PERSIST_SLOTS))
    }
}
