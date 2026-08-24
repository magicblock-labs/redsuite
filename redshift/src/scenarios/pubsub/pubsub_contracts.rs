use std::{rc::Rc, time::Duration};

use async_trait::async_trait;
use json::{JsonContainerTrait, JsonValueTrait};
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, prep, system,
    transport::{events::EventSubscriptions, wsraw::RawWs},
    BaseCtx, ChainCtx, CheckError, ErCtx, Result, Scenario, ScenarioReport,
    TxSender,
};
use signer::Signer;

const ACCOUNT_LAMPORTS: u64 = 1_000_000_000;
const TRANSFER_LAMPORTS: u64 = 10_000;
const SEQUENTIAL_TRANSFERS: u64 = 10;
const LOGS_TRANSFERS: usize = 5;
const EVENT_TIMEOUT: Duration = Duration::from_secs(10);
const EVENT_POLL: Duration = Duration::from_millis(20);
const RAW_FIRST_TIMEOUT: Duration = Duration::from_secs(3);
const RAW_DRAIN_TIMEOUT: Duration = Duration::from_millis(300);
const RAW_SILENCE_TIMEOUT: Duration = Duration::from_millis(700);

pub struct PubsubContracts;

async fn await_event_where(
    events: &EventSubscriptions,
    key: u64,
    what: &str,
    matches: impl Fn(&json::Value) -> bool,
) -> Result<json::Value> {
    let deadline = tokio::time::Instant::now() + EVENT_TIMEOUT;
    loop {
        if let Some(event) =
            events.events(key).into_iter().find(|event| matches(event))
        {
            return Ok(event);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(CheckError::new(format!(
                "timed out waiting for {what}"
            ))
            .actual(format!("{:?}", events.events(key)))
            .context("waited", format!("{EVENT_TIMEOUT:?}"))
            .into());
        }
        tokio::time::sleep(EVENT_POLL).await;
    }
}

fn event_lamports(event: &json::Value) -> Option<u64> {
    event.get("value").get("lamports").as_u64()
}

fn event_signature(event: &json::Value) -> Option<String> {
    event
        .get("value")
        .get("signature")
        .as_str()
        .map(str::to_owned)
}

const BLOCKHASH_RENEW: Duration = Duration::from_millis(100);

async fn transfer(
    sender: &TxSender,
    from: &Pubkey,
    to: &Pubkey,
    lamports: u64,
) -> Result<String> {
    tokio::time::sleep(BLOCKHASH_RENEW).await;
    let signature = sender
        .send_fresh(&[system::transfer(from, to, lamports)])
        .await?;
    Ok(signature.to_string())
}

#[async_trait(?Send)]
impl Scenario for PubsubContracts {
    fn name(&self) -> &str {
        "redshift/pubsub_contracts"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let account1 = prep::delegated_payer(
            base,
            &funder,
            er.identity(),
            ACCOUNT_LAMPORTS,
        )
        .await?;
        let account2 = prep::delegated_payer(
            base,
            &funder,
            er.identity(),
            ACCOUNT_LAMPORTS,
        )
        .await?;
        let account1_pubkey = account1.pubkey();
        let account2_pubkey = account2.pubkey();
        let sender = er.sender(Rc::new(account1));
        let system_program = system::system_id();

        let events = EventSubscriptions::connect(er.ws_url()).await?;
        let account1_key = events.account_subscribe(&account1_pubkey).await?;
        let account2_key = events.account_subscribe(&account2_pubkey).await?;
        events.await_subscribed(2, Duration::from_secs(5)).await?;

        let mut sent = 0u64;
        transfer(
            &sender,
            &account1_pubkey,
            &account2_pubkey,
            TRANSFER_LAMPORTS,
        )
        .await?;
        sent += 1;
        await_event_where(&events, account1_key, "account 1 debit", |event| {
            event_lamports(event) == Some(ACCOUNT_LAMPORTS - TRANSFER_LAMPORTS)
        })
        .await?;
        await_event_where(&events, account2_key, "account 2 credit", |event| {
            event_lamports(event) == Some(ACCOUNT_LAMPORTS + TRANSFER_LAMPORTS)
        })
        .await?;
        for event in events.events(account1_key) {
            let lamports = event_lamports(&event);
            check!(
                lamports == Some(ACCOUNT_LAMPORTS)
                    || lamports == Some(ACCOUNT_LAMPORTS - TRANSFER_LAMPORTS),
                "unexpected account 1 lamports in {event:?}"
            )?;
        }
        for event in events.events(account2_key) {
            let lamports = event_lamports(&event);
            check!(
                lamports == Some(ACCOUNT_LAMPORTS)
                    || lamports == Some(ACCOUNT_LAMPORTS + TRANSFER_LAMPORTS),
                "unexpected account 2 lamports in {event:?}"
            )?;
        }

        while sent < SEQUENTIAL_TRANSFERS {
            transfer(
                &sender,
                &account1_pubkey,
                &account2_pubkey,
                TRANSFER_LAMPORTS,
            )
            .await?;
            sent += 1;
            let expected = ACCOUNT_LAMPORTS - sent * TRANSFER_LAMPORTS;
            await_event_where(
                &events,
                account1_key,
                "sequential account 1 update",
                |event| event_lamports(event) == Some(expected),
            )
            .await?;
        }
        let drained =
            ACCOUNT_LAMPORTS - SEQUENTIAL_TRANSFERS * TRANSFER_LAMPORTS;

        let mut previous = ACCOUNT_LAMPORTS;
        for event in events.events(account1_key) {
            let lamports = event_lamports(&event).ok_or_else(|| {
                CheckError::new(format!(
                    "account 1 notification without lamports: {event:?}"
                ))
            })?;
            check!(
                lamports == previous
                    || previous.checked_sub(TRANSFER_LAMPORTS)
                        == Some(lamports),
                "out-of-order account 1 notification {lamports}: neither the \
                 previous {previous} nor one step down (a replaying validator \
                 that resent a stale balance would fail here)"
            )?;
            previous = lamports;
        }
        check_eq!(
            previous,
            drained,
            "the last account 1 notification must show the fully drained balance"
        )?;

        let logs_all_key = events.logs_subscribe_all().await?;
        let mentions1_key =
            events.logs_subscribe_mentions(&account1_pubkey).await?;
        let mentions2_key =
            events.logs_subscribe_mentions(&account2_pubkey).await?;
        let program_key = events.program_subscribe(&system_program).await?;
        events.await_subscribed(6, Duration::from_secs(5)).await?;

        let mut logs_signatures = Vec::with_capacity(LOGS_TRANSFERS);
        for _ in 0..LOGS_TRANSFERS {
            logs_signatures.push(
                transfer(
                    &sender,
                    &account1_pubkey,
                    &account2_pubkey,
                    TRANSFER_LAMPORTS,
                )
                .await?,
            );
            sent += 1;
        }
        for logs_signature in &logs_signatures {
            for (key, what) in [
                (logs_all_key, "logs(all) entry"),
                (mentions1_key, "logs mentions(account1) entry"),
                (mentions2_key, "logs mentions(account2) entry"),
            ] {
                let event = await_event_where(&events, key, what, |event| {
                    event_signature(event).as_deref()
                        == Some(logs_signature.as_str())
                })
                .await?;
                check!(
                    event.get("value").get("err").is_null(),
                    "{what} carries an error: {event:?}"
                )?;
                let log_count = event
                    .get("value")
                    .get("logs")
                    .as_array()
                    .map_or(0, |logs| logs.len());
                check!(log_count > 0, "{what} has no log lines: {event:?}")?;
            }
        }
        let expected1 = ACCOUNT_LAMPORTS - sent * TRANSFER_LAMPORTS;
        let expected2 = ACCOUNT_LAMPORTS + sent * TRANSFER_LAMPORTS;
        await_event_where(&events, program_key, "program update 1", |event| {
            event.get("value").get("pubkey").as_str()
                == Some(account1_pubkey.to_string().as_str())
                && event.get("value").get("account").get("lamports").as_u64()
                    == Some(expected1)
        })
        .await?;
        await_event_where(&events, program_key, "program update 2", |event| {
            event.get("value").get("pubkey").as_str()
                == Some(account2_pubkey.to_string().as_str())
                && event.get("value").get("account").get("lamports").as_u64()
                    == Some(expected2)
        })
        .await?;

        let confirmations =
            redsuite_core::transport::ws::SignatureConfirmations::connect(
                er.ws_url(),
            )
            .await?;
        let immediate_tx = sender
            .prepare(&[system::transfer(
                &account1_pubkey,
                &account2_pubkey,
                7_777,
            )])
            .await?;
        let immediate_sig = immediate_tx.signatures[0];
        confirmations.subscribe(1, &immediate_sig).await?;
        sender.deliver(&immediate_tx).await?;
        tokio::time::timeout(EVENT_TIMEOUT, confirmations.await_id(1))
            .await
            .map_err(|_| {
                CheckError::new("signature notification never fired")
            })??;

        let delayed_signature = sender
            .send_fresh(&[system::transfer(
                &account1_pubkey,
                &account2_pubkey,
                8_888,
            )])
            .await?;
        er.api()
            .await_transaction(&delayed_signature, Duration::from_secs(5))
            .await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        confirmations.subscribe(2, &delayed_signature).await?;
        tokio::time::timeout(EVENT_TIMEOUT, confirmations.await_id(2))
            .await
            .map_err(|_| {
                CheckError::new(
                    "delayed signature subscription never fired for an \
                     already-confirmed signature",
                )
            })??;
        let confirmation_outcome = confirmations.finalize();
        check_eq!(
            confirmation_outcome.confirmed,
            2,
            "expected both signature notifications: {confirmation_outcome:?}"
        )?;
        check_eq!(
            confirmation_outcome.failed,
            0,
            "signature notifications carried errors: {confirmation_outcome:?}"
        )?;

        let slot_key = events.slot_subscribe().await?;
        events.await_subscribed(7, Duration::from_secs(5)).await?;
        events.await_events(slot_key, 10, EVENT_TIMEOUT).await?;
        let slot_events = events.events(slot_key);
        let mut last_slot = 0u64;
        for event in &slot_events {
            let slot = event.get("slot").as_u64().ok_or_else(|| {
                CheckError::new(format!(
                    "slot event without a slot field: {event:?}"
                ))
            })?;
            check!(
                slot > last_slot,
                "slot sequence not strictly increasing: {slot} after \
                 {last_slot}"
            )?;
            last_slot = slot;
        }
        events.finalize();

        let mut raw = RawWs::connect(er.ws_url()).await?;

        let account_sub = raw.account_subscribe(&account1_pubkey).await?;
        transfer(
            &sender,
            &account1_pubkey,
            &account2_pubkey,
            TRANSFER_LAMPORTS,
        )
        .await?;
        check!(
            raw.next_notification(RAW_FIRST_TIMEOUT).await?.is_some(),
            "no account notification before unsubscribe"
        )?;
        check!(
            raw.account_unsubscribe(account_sub).await?,
            "the account unsubscribe must return true"
        )?;
        while raw.next_notification(RAW_DRAIN_TIMEOUT).await?.is_some() {}
        let after_account_unsub = sender
            .send_fresh(&[system::transfer(
                &account1_pubkey,
                &account2_pubkey,
                TRANSFER_LAMPORTS,
            )])
            .await?;
        er.api()
            .await_transaction(&after_account_unsub, Duration::from_secs(5))
            .await?;
        check!(
            raw.next_notification(RAW_SILENCE_TIMEOUT).await?.is_none(),
            "account notifications continued after unsubscribe"
        )?;

        let logs_sub = raw.logs_subscribe_mentions(&account1_pubkey).await?;
        transfer(
            &sender,
            &account1_pubkey,
            &account2_pubkey,
            TRANSFER_LAMPORTS,
        )
        .await?;
        check!(
            raw.next_notification(RAW_FIRST_TIMEOUT).await?.is_some(),
            "no logs notification before unsubscribe"
        )?;
        check!(
            raw.logs_unsubscribe(logs_sub).await?,
            "the logs unsubscribe must return true"
        )?;
        while raw.next_notification(RAW_DRAIN_TIMEOUT).await?.is_some() {}
        let after_logs_unsub = sender
            .send_fresh(&[system::transfer(
                &account1_pubkey,
                &account2_pubkey,
                TRANSFER_LAMPORTS,
            )])
            .await?;
        er.api()
            .await_transaction(&after_logs_unsub, Duration::from_secs(5))
            .await?;
        check!(
            raw.next_notification(RAW_SILENCE_TIMEOUT).await?.is_none(),
            "logs notifications continued after unsubscribe"
        )?;

        let program_sub = raw.program_subscribe(&system_program).await?;
        transfer(
            &sender,
            &account1_pubkey,
            &account2_pubkey,
            TRANSFER_LAMPORTS,
        )
        .await?;
        check!(
            raw.next_notification(RAW_FIRST_TIMEOUT).await?.is_some(),
            "no program notification before unsubscribe"
        )?;
        check!(
            raw.program_unsubscribe(program_sub).await?,
            "the program unsubscribe must return true"
        )?;
        while raw.next_notification(RAW_DRAIN_TIMEOUT).await?.is_some() {}
        let after_program_unsub = sender
            .send_fresh(&[system::transfer(
                &account1_pubkey,
                &account2_pubkey,
                TRANSFER_LAMPORTS,
            )])
            .await?;
        er.api()
            .await_transaction(&after_program_unsub, Duration::from_secs(5))
            .await?;
        check!(
            raw.next_notification(RAW_SILENCE_TIMEOUT).await?.is_none(),
            "program notifications continued after unsubscribe"
        )?;

        let slot_sub = raw.slot_subscribe().await?;
        check!(
            raw.next_notification(RAW_FIRST_TIMEOUT).await?.is_some(),
            "no slot notification before unsubscribe"
        )?;
        check!(
            raw.slot_unsubscribe(slot_sub).await?,
            "the slot unsubscribe must return true"
        )?;
        while raw.next_notification(RAW_DRAIN_TIMEOUT).await?.is_some() {}
        check!(
            raw.next_notification(RAW_SILENCE_TIMEOUT).await?.is_none(),
            "slot notifications continued after unsubscribe"
        )?;

        let unsent = sender
            .prepare(&[system::transfer(
                &account1_pubkey,
                &account2_pubkey,
                9_999,
            )])
            .await?;
        let unsent_signature = unsent.signatures[0].to_string();
        let signature_sub = raw.signature_subscribe(&unsent_signature).await?;
        raw.signature_unsubscribe(signature_sub).await?;
        raw.close().await?;

        Ok(ScenarioReport::ok(self.name())
            .setting("transfer lamports", TRANSFER_LAMPORTS)
            .setting("sequential transfers", SEQUENTIAL_TRANSFERS)
            .metric("account 1 drained to", Unit::Count, drained as f64)
            .metric("slot events", Unit::Count, slot_events.len() as f64))
    }
}
