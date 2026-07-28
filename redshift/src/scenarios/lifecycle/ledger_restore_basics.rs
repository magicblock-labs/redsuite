use std::time::Duration;

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_program::flexi::{build as flexi, FlexiCounter};
use redsuite_core::{
    assert::poll_until, dlp, prep, system, topology, BaseCtx, ChainCtx, ErCtx,
    Result, Scenario, ScenarioReport,
};
use signature::Signature;
use signer::Signer;

const RENT_EXEMPT: u64 = 890_880;
const SOL: u64 = 1_000_000_000;
const PERSIST_SLOTS: u64 = 11;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const SLOT_TIMEOUT: Duration = Duration::from_secs(30);
const CLONE_TIMEOUT: Duration = Duration::from_secs(20);
const TX_TIMEOUT: Duration = Duration::from_secs(20);
const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;
const LABEL_ONE: &str = "counter of payer 1";
const LABEL_TWO: &str = "counter of payer 2";

pub struct LedgerRestoreBasics;

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

async fn transfer(
    er: &ErCtx,
    from: &Keypair,
    to: &Pubkey,
    lamports: u64,
) -> Result<Signature> {
    er.send(from, &[system::transfer(&from.pubkey(), to, lamports)])
        .await
}

async fn block_time(er: &ErCtx, signature: &Signature) -> Result<i64> {
    let tx = er.api().await_transaction(signature, TX_TIMEOUT).await?;
    tx.block_time
        .ok_or_else(|| "the transaction carries no block time".into())
}

async fn assert_restored_tx(er: &ErCtx, signature: &Signature) -> Result<()> {
    let tx = er.api().await_transaction(signature, TX_TIMEOUT).await?;
    assert!(
        tx.err.is_none(),
        "a restored transaction must keep its ok status"
    );
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

async fn assert_counter(
    er: &ErCtx,
    counter: &Pubkey,
    expected: &FlexiCounter,
    side: &str,
) -> Result<()> {
    let account = er
        .account(counter)
        .await?
        .ok_or("the counter account is missing")?;
    assert_eq!(
        &FlexiCounter::try_decode(&account.data)?,
        expected,
        "the {side} counter state"
    );
    Ok(())
}

#[async_trait(?Send)]
impl Scenario for LedgerRestoreBasics {
    fn name(&self) -> &str {
        "redshift/ledger_restore_basics"
    }

    async fn run(&self, base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        let funder = prep::funded_payer(base, 10 * SOL).await?;
        let mut report = ScenarioReport::ok(self.name());

        // empty — a ledger with no transactions restores into a healthy
        // validator.
        {
            let mut writer = boot_writer(base, "restore-empty").await?;
            advance_slots(writer.ctx(), PERSIST_SLOTS).await?;
            writer.stop(true).await?;
            drop(writer);
            let reader =
                boot_reader(base, "restore-empty", "ephemeral", false, vec![])
                    .await?;
            let slot = reader.ctx().api().get_slot().await?;
            report = report.setting("empty restore slot", slot);
        }

        // transfers — a chain of order-dependent transfers, split over the
        // same slot and separate slots, restores with exact balances, intact
        // signature statuses, and the original block time.
        {
            let mut writer = boot_writer(base, "restore-transfers").await?;
            advance_slots(writer.ctx(), 1).await?;
            let identity = writer.ctx().identity();
            let mut wallets = Vec::new();
            for index in 0..5u64 {
                let lamports =
                    RENT_EXEMPT + if index == 0 { 5 * SOL } else { 0 };
                wallets.push(
                    delegated_wallet(base, &funder, identity, lamports).await?,
                );
            }
            let er = writer.ctx();
            let mut signatures = Vec::new();
            signatures.push(
                transfer(er, &wallets[0], &wallets[1].pubkey(), 4 * SOL)
                    .await?,
            );
            signatures.push(
                transfer(er, &wallets[1], &wallets[2].pubkey(), 3 * SOL)
                    .await?,
            );
            advance_slots(er, 1).await?;
            signatures.push(
                transfer(er, &wallets[2], &wallets[3].pubkey(), 2 * SOL)
                    .await?,
            );
            advance_slots(er, 1).await?;
            signatures.push(
                transfer(er, &wallets[3], &wallets[4].pubkey(), SOL).await?,
            );
            for wallet in &wallets {
                await_er_balance(er, &wallet.pubkey(), RENT_EXEMPT + SOL)
                    .await?;
            }
            advance_slots(er, PERSIST_SLOTS).await?;
            let written_time = block_time(er, &signatures[3]).await?;
            writer.stop(true).await?;
            drop(writer);

            let reader = boot_reader(
                base,
                "restore-transfers",
                "offline",
                false,
                vec![],
            )
            .await?;
            let er = reader.ctx();
            for wallet in &wallets {
                let balance = er.api().get_balance(&wallet.pubkey()).await?;
                assert_eq!(
                    balance,
                    RENT_EXEMPT + SOL,
                    "a restored wallet balance"
                );
            }
            for signature in &signatures {
                assert_restored_tx(er, signature).await?;
            }
            let restored_time = block_time(er, &signatures[3]).await?;
            assert_eq!(
                restored_time, written_time,
                "the restored block time must match the written block time"
            );
            report = report.setting("restored block time", restored_time);
        }

        // counters — order-sensitive add/mul sequences on two counters
        // restore to the exact end state on an offline validator.
        {
            let mut writer = boot_writer(base, "restore-counter").await?;
            let one =
                counter_actor(base, writer.ctx(), &funder, LABEL_ONE).await?;
            let two =
                counter_actor(base, writer.ctx(), &funder, LABEL_TWO).await?;
            let er = writer.ctx();
            er.send(&one.payer, &[flexi::add(one.payer.pubkey(), 5)])
                .await?;
            er.send(&one.payer, &[flexi::mul(one.payer.pubkey(), 2)])
                .await?;
            advance_slots(er, 1).await?;
            er.send(&two.payer, &[flexi::add(two.payer.pubkey(), 9)])
                .await?;
            advance_slots(er, 1).await?;
            er.send(&one.payer, &[flexi::add(one.payer.pubkey(), 3)])
                .await?;
            advance_slots(er, 1).await?;
            er.send(&two.payer, &[flexi::mul(two.payer.pubkey(), 3)])
                .await?;
            let expected_one = FlexiCounter {
                count: 13,
                updates: 3,
                label: LABEL_ONE.to_owned(),
            };
            let expected_two = FlexiCounter {
                count: 27,
                updates: 2,
                label: LABEL_TWO.to_owned(),
            };
            assert_counter(er, &one.counter, &expected_one, "written").await?;
            assert_counter(er, &two.counter, &expected_two, "written").await?;
            advance_slots(er, PERSIST_SLOTS).await?;
            writer.stop(true).await?;
            drop(writer);

            let reader =
                boot_reader(base, "restore-counter", "offline", false, vec![])
                    .await?;
            assert_counter(
                reader.ctx(),
                &one.counter,
                &expected_one,
                "restored",
            )
            .await?;
            assert_counter(
                reader.ctx(),
                &two.counter,
                &expected_two,
                "restored",
            )
            .await?;
        }

        // resume strategies — a replay restore keeps the transfer and its
        // transaction; a reset restore clones fresh state from base and drops
        // the ledger history.
        for reset in [false, true] {
            let label = if reset {
                "restore-reset"
            } else {
                "restore-replay"
            };
            let mut writer = boot_writer(base, label).await?;
            let identity = writer.ctx().identity();
            advance_slots(writer.ctx(), 1).await?;
            let sender =
                delegated_wallet(base, &funder, identity, 1_111_111).await?;
            await_er_balance(writer.ctx(), &sender.pubkey(), 1_111_111).await?;
            advance_slots(writer.ctx(), 3).await?;
            let receiver =
                delegated_wallet(base, &funder, identity, 1_000_000).await?;
            await_er_balance(writer.ctx(), &receiver.pubkey(), 1_000_000)
                .await?;
            let signature =
                transfer(writer.ctx(), &sender, &receiver.pubkey(), 100)
                    .await?;
            await_er_balance(writer.ctx(), &receiver.pubkey(), 1_000_100)
                .await?;
            await_er_balance(writer.ctx(), &sender.pubkey(), 1_111_011).await?;
            let saved_slot = advance_slots(writer.ctx(), PERSIST_SLOTS).await?;
            writer.stop(true).await?;
            drop(writer);

            let env = if reset {
                vec![
                    ("MBV_ACCOUNTSDB__RESET".to_owned(), "true".to_owned()),
                    ("MBV_TASK_SCHEDULER__RESET".to_owned(), "true".to_owned()),
                ]
            } else {
                vec![]
            };
            let reader =
                boot_reader(base, label, "ephemeral", reset, env).await?;
            let er = reader.ctx();
            if !reset {
                let restored_slot = er.api().get_slot().await?;
                assert!(
                    restored_slot >= saved_slot,
                    "a replay restore must resume at or past the written \
                     slot: {restored_slot} < {saved_slot}"
                );
            }
            let expected = if reset { 1_111_111 } else { 1_111_011 };
            await_er_balance(er, &sender.pubkey(), expected).await?;
            let restored_tx = er.api().get_transaction(&signature).await?;
            assert_eq!(
                restored_tx.is_none(),
                reset,
                "a reset restore must drop the transaction and a replay \
                 restore must keep it"
            );
            report =
                report.setting(format!("{label} sender lamports"), expected);
        }

        // NOTE: the upstream new-validator-authority restore (test 14) is not
        // ported. A fresh identity needs a validator-fees-vault on base, and
        // only the dlp admin can create it — the mainnet-cloned dlp keeps its
        // real authority, so no local key qualifies. See the incr-28 identity
        // rework in redshift.md.

        Ok(report.setting("persist slots", PERSIST_SLOTS))
    }
}
