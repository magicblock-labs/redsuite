use std::time::Duration;

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::flexi::{build as flexi, FlexiCounter};
use redsuite_core::{
    check, check_eq, dlp, prep, system, BaseCtx, ChainCtx, ErCtx,
    PrivateErScenario, Result, ScenarioReport,
};
use signature::Signature;
use signer::Signer;

use super::{
    advance_slots, await_er_balance, boot_reader, boot_writer, counter_actor,
    CLONE_TIMEOUT, PERSIST_SLOTS, SOL,
};

const RENT_EXEMPT: u64 = 890_880;
const TX_TIMEOUT: Duration = Duration::from_secs(20);
const LABEL_ONE: &str = "counter of payer 1";
const LABEL_TWO: &str = "counter of payer 2";
const LABEL_FRESH: &str = "counter of the fresh authority";

pub struct LedgerRestoreBasics;

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
    check!(
        tx.err.is_none(),
        "a restored transaction must keep its ok status"
    )?;
    Ok(())
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
    check_eq!(
        &FlexiCounter::try_decode(&account.data)?,
        expected,
        "the {side} counter state"
    )?;
    Ok(())
}

#[async_trait(?Send)]
impl PrivateErScenario for LedgerRestoreBasics {
    fn name(&self) -> &str {
        "redshift/ledger_restore_basics"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
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
                    prep::delegated_payer(base, &funder, identity, lamports)
                        .await?,
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
                check_eq!(
                    balance,
                    RENT_EXEMPT + SOL,
                    "a restored wallet balance"
                )?;
            }
            for signature in &signatures {
                assert_restored_tx(er, signature).await?;
            }
            let restored_time = block_time(er, &signatures[3]).await?;
            check_eq!(
                restored_time,
                written_time,
                "the restored block time must match the written block time"
            )?;
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
                prep::delegated_payer(base, &funder, identity, 1_111_111)
                    .await?;
            await_er_balance(writer.ctx(), &sender.pubkey(), 1_111_111).await?;
            advance_slots(writer.ctx(), 3).await?;
            let receiver =
                prep::delegated_payer(base, &funder, identity, 1_000_000)
                    .await?;
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
                check!(
                    restored_slot >= saved_slot,
                    "a replay restore must resume at or past the written \
                     slot: {restored_slot} < {saved_slot}"
                )?;
            }
            let expected = if reset { 1_111_111 } else { 1_111_011 };
            await_er_balance(er, &sender.pubkey(), expected).await?;
            let restored_tx = er.api().get_transaction(&signature).await?;
            check_eq!(
                restored_tx.is_none(),
                reset,
                "a reset restore must drop the transaction and a replay \
                 restore must keep it"
            )?;
            report =
                report.setting(format!("{label} sender lamports"), expected);
        }

        // new authority — the full write -> kill -> restore cycle works for
        // an identity that has never validated before (upstream 14; the
        // pool-injected fees vault replaces upstream's genesis accounts). The
        // reader boots against the live base like upstream's does — a cloned
        // program does NOT survive an offline restore (probed 2026-07-29:
        // the account vanishes without a remote to re-clone from), so the
        // program assert is presence + byte equality, not ledger provenance.
        {
            let mut writer =
                boot_writer(base, "restore-fresh-authority").await?;
            let identity = writer.ctx().identity();
            let vault = dlp::validator_fees_vault_pda(&identity);
            let vault_on_base = base
                .account(&vault)
                .await?
                .ok_or("the fresh identity has no fees vault on base")?;
            check_eq!(
                vault_on_base.owner,
                dlp::dlp_id(),
                "the pool-injected fees vault must be dlp-owned"
            )?;
            let actor =
                counter_actor(base, writer.ctx(), &funder, LABEL_FRESH).await?;
            let er = writer.ctx();
            er.send(&actor.payer, &[flexi::add(actor.payer.pubkey(), 7)])
                .await?;
            let expected = FlexiCounter {
                count: 7,
                updates: 1,
                label: LABEL_FRESH.to_owned(),
            };
            assert_counter(er, &actor.counter, &expected, "written").await?;
            let program = er
                .account(&redshift_interface::id())
                .await?
                .ok_or("the redshift program did not clone into the er")?;
            check!(
                program.executable,
                "the cloned program must present as executable"
            )?;
            advance_slots(er, PERSIST_SLOTS).await?;
            writer.stop(true).await?;
            drop(writer);

            let reader = boot_reader(
                base,
                "restore-fresh-authority",
                "ephemeral",
                false,
                vec![],
            )
            .await?;
            let er = reader.ctx();
            assert_counter(er, &actor.counter, &expected, "restored").await?;
            check::poll(
                "the reader re-clones the redshift program",
                CLONE_TIMEOUT,
                || async {
                    matches!(
                        er.account(&redshift_interface::id()).await,
                        Ok(Some(_))
                    )
                },
            )
            .await?;
            let restored_program = er
                .account(&redshift_interface::id())
                .await?
                .ok_or("the cloned program vanished in the restore")?;
            check!(
                restored_program.executable,
                "the restored program must stay executable"
            )?;
            check_eq!(
                restored_program.owner,
                program.owner,
                "the restored program owner"
            )?;
            check_eq!(
                restored_program.data.len(),
                program.data.len(),
                "the restored program size"
            )?;
            // the first 48 bytes are the LoaderV4State header, whose deploy
            // slot is re-stamped on every clone
            check_eq!(
                restored_program.data[48..],
                program.data[48..],
                "the restored program bytecode"
            )?;
            report = report
                .setting("fresh authority", identity)
                .setting(
                    "fresh authority vault lamports",
                    vault_on_base.lamports,
                )
                .setting("restored program owner", restored_program.owner);
        }

        Ok(report.setting("persist slots", PERSIST_SLOTS))
    }
}
