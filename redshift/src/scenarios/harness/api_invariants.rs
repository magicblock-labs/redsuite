use std::{
    rc::Rc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use keypair::Keypair;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, mdp, prep, system, BaseCtx, ChainCtx, CheckError, ErCtx,
    Result, Scenario, ScenarioReport,
};
use signer::Signer;

const CLOCK_ITERATIONS: usize = 10;
const TRANSFER_LAMPORTS: u64 = 1_000_000;
const DELEGATED_LAMPORTS: u64 = 1_000_000_000;
const LEDGER_SETTLE_SLOTS: u64 = 10;
const LEDGER_SETTLE_TIMEOUT: Duration = Duration::from_secs(10);
const SLOT_POLL: Duration = Duration::from_millis(50);

pub struct ApiInvariants;

#[async_trait(?Send)]
impl Scenario for ApiInvariants {
    fn name(&self) -> &str {
        "redshift/api_invariants"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let from = prep::delegated_payer(
            base,
            &funder,
            er.identity(),
            DELEGATED_LAMPORTS,
        )
        .await?;
        let to = prep::delegated_payer(
            base,
            &funder,
            er.identity(),
            DELEGATED_LAMPORTS,
        )
        .await?;
        let from_pubkey = from.pubkey();
        let to_pubkey = to.pubkey();
        let sender = er.sender(Rc::new(from));

        for iteration in 0..CLOCK_ITERATIONS {
            let signature = sender
                .submit_fresh(&[system::transfer(
                    &from_pubkey,
                    &to_pubkey,
                    TRANSFER_LAMPORTS,
                )])
                .await?;
            let confirmed = er
                .api()
                .await_transaction(&signature, Duration::from_secs(5))
                .await?;
            check!(
                confirmed.err.is_none(),
                "iteration {iteration}: ER transfer failed: {:?}",
                confirmed.err
            )?;
            let tx_slot = confirmed.slot;

            let settle_deadline =
                tokio::time::Instant::now() + LEDGER_SETTLE_TIMEOUT;
            while er.api().get_slot().await? < tx_slot + LEDGER_SETTLE_SLOTS {
                check!(
                    tokio::time::Instant::now() < settle_deadline,
                    "iteration {iteration}: ER slot never reached {} within \
                     {LEDGER_SETTLE_TIMEOUT:?}",
                    tx_slot + LEDGER_SETTLE_SLOTS
                )?;
                tokio::time::sleep(SLOT_POLL).await;
            }

            let ledger_timestamp =
                er.api().get_block_time(tx_slot).await?.ok_or_else(|| {
                    CheckError::new(format!(
                        "iteration {iteration}: getBlockTime(slot {tx_slot}) \
                         returned null"
                    ))
                })?;
            let block =
                er.api().get_block(tx_slot).await?.ok_or_else(|| {
                    CheckError::new(format!(
                        "iteration {iteration}: getBlock(slot {tx_slot}) \
                         returned null"
                    ))
                })?;
            let settled_tx = er
                .api()
                .get_transaction(&signature)
                .await?
                .ok_or("settled transaction vanished from the ledger")?;

            check_eq!(
                block.block_time,
                Some(ledger_timestamp),
                "iteration {iteration}: getBlock.blockTime diverges from \
                 getBlockTime for slot {tx_slot}"
            )?;
            check_eq!(
                settled_tx.block_time,
                Some(ledger_timestamp),
                "iteration {iteration}: getTransaction.blockTime diverges \
                 from getBlockTime for slot {tx_slot}"
            )?;
            check!(
                ledger_timestamp > 0,
                "iteration {iteration}: timestamp should be positive"
            )?;
            let now_unix_seconds = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before unix epoch")
                .as_secs() as i64;
            check!(
                ledger_timestamp <= now_unix_seconds,
                "iteration {iteration}: timestamp {ledger_timestamp} is in \
                 the future (now {now_unix_seconds})"
            )?;
        }

        let validator = Keypair::new();
        base.airdrop(&validator.pubkey(), DELEGATED_LAMPORTS)
            .await?;
        let record = mdp::DomainRecord {
            identity: validator.pubkey(),
            status: mdp::STATUS_ACTIVE,
            block_time_ms: 101,
            base_fee: 102,
            features: [0u8; 32],
            load_average: 222,
            country_code: *b"BOL",
            addr: "1.1.1.0:1010".to_owned(),
        };
        let record_pda = mdp::record_pda(&validator.pubkey());

        base.submit_and_confirm(&validator, &[mdp::register(&record)])
            .await?;
        let registered = base
            .account(&record_pda)
            .await?
            .ok_or("domain record missing after register")?;
        check_eq!(
            registered.owner,
            mdp::mdp_id(),
            "domain record not owned by mdp"
        )?;
        check_eq!(
            registered.data,
            record.encode(),
            "registered record bytes diverge from the submitted record"
        )?;

        let mut mutated = record.clone();
        mutated.status = mdp::STATUS_DRAINING;
        mutated.base_fee = 0;
        mutated.addr = "this.is.very.long.string.to.test.sync".to_owned();
        base.submit_and_confirm(&validator, &[mdp::sync(&mutated)])
            .await?;
        let synced = base
            .account(&record_pda)
            .await?
            .ok_or("domain record missing after sync")?;
        check_eq!(
            synced.data,
            mutated.encode(),
            "synced record bytes diverge from the mutated record"
        )?;
        check!(
            synced.data.len() > registered.data.len(),
            "sync with a longer addr should have grown the record account"
        )?;

        base.submit_and_confirm(
            &validator,
            &[mdp::unregister(&validator.pubkey())],
        )
        .await?;
        let unregistered = base.account(&record_pda).await?;
        check!(
            unregistered.is_none(),
            "domain record still present after unregister"
        )?;

        Ok(ScenarioReport::ok(self.name())
            .setting("clock iterations", CLOCK_ITERATIONS)
            .setting("ledger settle slots", LEDGER_SETTLE_SLOTS)
            .setting(
                "record bytes",
                format!("{} -> {}", registered.data.len(), synced.data.len()),
            )
            .metric("transfers", Unit::Count, CLOCK_ITERATIONS as f64))
    }
}
