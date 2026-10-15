use std::{
    collections::BTreeMap,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures_util::future::join_all;
use json::{JsonContainerTrait, JsonValueTrait};
use pubkey::Pubkey;
use redsuite_core::redline::Accounts;
use redsuite_core::{
    check, check_eq, prep,
    rpc_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_client::GetConfirmedSignaturesForAddress2Config,
    },
    rpc_client_api::config::{
        RpcBlockConfig, RpcSendTransactionConfig, RpcSimulateTransactionConfig,
        RpcTransactionConfig,
    },
    system,
    transport::{
        clock::SendClock, events::EventSubscriptions,
        ws::SignatureConfirmations, wsraw::RawWs,
    },
    BaseCtx, ChainCtx, CheckError, ErCtx, Result, Scenario, TxSender,
};
use serde::Deserialize;
use signature::Signature;
use signer::Signer;
use solana_commitment_config::CommitmentConfig;
use solana_transaction_status_client_types::{
    TransactionConfirmationStatus, TransactionDetails, UiTransactionEncoding,
};
use transaction::Transaction;

use crate::program::instruction::build;

const WRITES: usize = 256;
const PAYERS: usize = 32;
const PAYER_LAMPORTS: u64 = 2_000_000_000;
const FIRST_ID: u64 = 1_000;
const ACCOUNTS_PER_CALL: usize = 100;
const HISTORY_LIMIT: usize = 16;
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const BURST_DEADLINE: Duration = Duration::from_secs(20);
const CONFIRM_DEADLINE: Duration = Duration::from_secs(30);
const PUBLICATION_TIMEOUT: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(50);
const LEDGER_SETTLE_SLOTS: u64 = 10;
const LEDGER_SETTLE_TIMEOUT: Duration = Duration::from_secs(10);

const ACCOUNT_LAMPORTS: u64 = 1_000_000_000;
const TRANSFER_LAMPORTS: u64 = 10_000;
const SEQUENTIAL_TRANSFERS: u64 = 10;
const LOGS_TRANSFERS: usize = 5;
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(5);
const EVENT_TIMEOUT: Duration = Duration::from_secs(10);
const EVENT_POLL: Duration = Duration::from_millis(20);
const RAW_FIRST_TIMEOUT: Duration = Duration::from_secs(3);
const RAW_DRAIN_TIMEOUT: Duration = Duration::from_millis(300);
const RAW_SILENCE_TIMEOUT: Duration = Duration::from_millis(700);

pub struct RpcLifecycle;

struct Write {
    pda: Pubkey,
    id: u64,
    transaction: Transaction,
    signature: Signature,
}

#[derive(Deserialize)]
struct WithContext<T> {
    value: T,
}

#[derive(Deserialize)]
struct RawBlockhash {
    blockhash: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawDelegationStatus {
    is_delegated: bool,
}

fn confirmed() -> CommitmentConfig {
    CommitmentConfig::confirmed()
}

fn transaction_config() -> RpcTransactionConfig {
    RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        commitment: Some(confirmed()),
        max_supported_transaction_version: Some(0),
    }
}

fn block_config() -> RpcBlockConfig {
    RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        transaction_details: Some(TransactionDetails::Signatures),
        rewards: Some(false),
        commitment: Some(confirmed()),
        max_supported_transaction_version: Some(0),
    }
}

fn is_confirmed(status: &Option<TransactionConfirmationStatus>) -> bool {
    matches!(
        status,
        Some(TransactionConfirmationStatus::Confirmed)
            | Some(TransactionConfirmationStatus::Finalized)
    )
}

async fn await_clones(client: &RpcClient, pdas: &[Pubkey]) -> Result<()> {
    let deadline = Instant::now() + CLONE_TIMEOUT;
    loop {
        let mut all_present = true;
        for chunk in pdas.chunks(ACCOUNTS_PER_CALL) {
            let accounts = client
                .get_multiple_accounts_with_commitment(chunk, confirmed())
                .await?
                .value;
            all_present &= accounts.iter().all(|account| {
                account.as_ref().is_some_and(|account| {
                    account.data.len() == crate::ACCOUNT_SPACE as usize
                })
            });
        }
        if all_present {
            return Ok(());
        }
        check!(
            Instant::now() < deadline,
            "the ER did not clone all {} delegated accounts within \
             {CLONE_TIMEOUT:?}",
            pdas.len()
        )?;
        tokio::time::sleep(POLL).await;
    }
}

async fn await_statuses(
    client: &RpcClient,
    signatures: &[Signature],
) -> Result<Vec<u64>> {
    let deadline = Instant::now() + CONFIRM_DEADLINE;
    loop {
        let statuses = client.get_signature_statuses(signatures).await?.value;
        check_eq!(
            statuses.len(),
            signatures.len(),
            "getSignatureStatuses must answer one entry per signature"
        )?;
        let mut slots = Vec::with_capacity(signatures.len());
        for (signature, status) in signatures.iter().zip(&statuses) {
            let Some(status) = status else {
                break;
            };
            check!(
                status.err.is_none(),
                "write {signature} failed on the ER: {:?}",
                status.err
            )?;
            if !is_confirmed(&status.confirmation_status) {
                break;
            }
            slots.push(status.slot);
        }
        if slots.len() == signatures.len() {
            return Ok(slots);
        }
        check!(
            Instant::now() < deadline,
            "only {} of {} writes reached confirmed status within \
             {CONFIRM_DEADLINE:?}",
            slots.len(),
            signatures.len()
        )?;
        tokio::time::sleep(POLL).await;
    }
}

async fn await_publication(client: &RpcClient, slot: u64) -> Result<()> {
    let deadline = Instant::now() + PUBLICATION_TIMEOUT;
    loop {
        if client
            .get_block_with_config(slot, block_config())
            .await
            .is_ok()
        {
            return Ok(());
        }
        check!(
            Instant::now() < deadline,
            "getBlock({slot}) was not published within {PUBLICATION_TIMEOUT:?}"
        )?;
        tokio::time::sleep(POLL).await;
    }
}

async fn burst(
    scenario: &str,
    base: &BaseCtx,
    er: &ErCtx,
    client: &RpcClient,
) -> Result<()> {
    let payers = prep::funded_payers(base, PAYERS, PAYER_LAMPORTS).await?;
    let pdas = Accounts::new(crate::ACCOUNT_SPACE, er.identity())
        .init_batched(base, &payers, WRITES, true)
        .await?;
    let per_payer = WRITES.div_ceil(PAYERS);
    await_clones(client, &pdas).await?;
    let blockhash = client.get_latest_blockhash().await?;
    check!(
        client.is_blockhash_valid(&blockhash, confirmed()).await?,
        "getLatestBlockhash returned {blockhash} but isBlockhashValid \
         rejects it"
    )?;
    let pda_strings: Vec<String> =
        pdas.iter().map(ToString::to_string).collect();
    let for_accounts: WithContext<RawBlockhash> = er
        .api()
        .call("getBlockhashForAccounts", &(pda_strings,))
        .await?;
    let for_accounts: hash::Hash = for_accounts.value.blockhash.parse()?;
    check!(
        client
            .is_blockhash_valid(&for_accounts, confirmed())
            .await?,
        "getBlockhashForAccounts returned {for_accounts} but \
         isBlockhashValid rejects it"
    )?;

    let writes: Vec<Write> = pdas
        .iter()
        .enumerate()
        .map(|(index, pda)| {
            let id = FIRST_ID + index as u64;
            let payer = &payers[index / per_payer];
            let transaction = Transaction::new_signed_with_payer(
                &[build::simple_byte_set(id, &[*pda])],
                Some(&payer.pubkey()),
                &[payer],
                blockhash,
            );
            let signature = transaction.signatures[0];
            Write {
                pda: *pda,
                id,
                transaction,
                signature,
            }
        })
        .collect();
    let signatures: Vec<Signature> =
        writes.iter().map(|write| write.signature).collect();
    let probe = &writes[0];
    client
        .get_fee_for_message(&probe.transaction.message)
        .await?;
    let simulation = client
        .simulate_transaction_with_config(
            &probe.transaction,
            RpcSimulateTransactionConfig {
                sig_verify: true,
                commitment: Some(confirmed()),
                ..RpcSimulateTransactionConfig::default()
            },
        )
        .await?
        .value;
    check!(
        simulation.err.is_none(),
        "simulateTransaction rejected a valid write: {:?}",
        simulation.err
    )?;
    let after_simulation = client
        .get_account_with_commitment(&probe.pda, confirmed())
        .await?
        .value
        .ok_or("the probe account vanished after simulation")?;
    check_eq!(
        crate::written_id(&after_simulation.data),
        Some(0),
        "simulateTransaction must not mutate the probe account"
    )?;
    let after_simulation = client
        .get_signature_statuses(&[probe.signature])
        .await?
        .value;
    check!(
        after_simulation.first().is_some_and(Option::is_none),
        "simulateTransaction must not record a signature status"
    )?;

    let send_config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: Some(confirmed().commitment),
        ..RpcSendTransactionConfig::default()
    };
    let sends = join_all(writes.iter().map(|write| {
        client.send_transaction_with_config(&write.transaction, send_config)
    }));
    let sends =
        tokio::time::timeout(BURST_DEADLINE, sends)
            .await
            .map_err(|_| {
                format!(
                    "the burst of {WRITES} sends exceeded {BURST_DEADLINE:?}"
                )
            })?;
    let mut failures = 0u64;
    for (write, sent) in writes.iter().zip(sends) {
        match sent {
            Ok(signature) => check_eq!(
                signature,
                write.signature,
                "sendTransaction must echo the locally computed signature"
            )?,
            Err(error) => {
                failures += 1;
                eprintln!(
                    "[redsuite] {scenario}: send of {} failed: {error}",
                    write.signature
                );
            }
        }
    }
    check_eq!(
        failures,
        0,
        "every write must be accepted by sendTransaction"
    )?;

    let slots = await_statuses(client, &signatures).await?;
    let min_slot = *slots.iter().min().ok_or("no slots recorded")?;
    let max_slot = *slots.iter().max().ok_or("no slots recorded")?;
    await_publication(client, max_slot).await?;

    let mut by_slot: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for (index, slot) in slots.iter().enumerate() {
        by_slot.entry(*slot).or_default().push(index);
    }

    let mut block_times = Vec::with_capacity(writes.len());
    for (write, slot) in writes.iter().zip(&slots) {
        let transaction = client
            .get_transaction_with_config(&write.signature, transaction_config())
            .await?;
        check_eq!(
            transaction.slot,
            *slot,
            "getTransaction slot must match getSignatureStatuses for {}",
            write.signature
        )?;
        let meta = transaction
            .transaction
            .meta
            .ok_or("getTransaction returned no meta")?;
        check!(
            meta.err.is_none(),
            "getTransaction reports a failed write {}: {:?}",
            write.signature,
            meta.err
        )?;
        block_times.push(
            transaction
                .block_time
                .ok_or("getTransaction returned no blockTime")?,
        );
    }

    for (slot, indices) in &by_slot {
        let block = client.get_block_with_config(*slot, block_config()).await?;
        let block_time = client.get_block_time(*slot).await?;
        check_eq!(
            block.block_time,
            Some(block_time),
            "getBlock.blockTime must match getBlockTime for slot {slot}"
        )?;
        let listed =
            block.signatures.ok_or("getBlock returned no signatures")?;
        for &index in indices {
            let signature = writes[index].signature.to_string();
            check!(
                listed.contains(&signature),
                "getBlock({slot}) must list {signature}"
            )?;
            check_eq!(
                block_times[index],
                block_time,
                "getTransaction.blockTime must match getBlockTime for slot \
                 {slot}"
            )?;
        }
    }
    let blocks = client
        .get_blocks_with_commitment(min_slot, Some(max_slot), confirmed())
        .await?;
    check!(
        blocks.windows(2).all(|pair| pair[0] < pair[1]),
        "getBlocks must return strictly ascending slots"
    )?;
    for slot in by_slot.keys() {
        check!(
            blocks.contains(slot),
            "getBlocks({min_slot}, {max_slot}) must include slot {slot}"
        )?;
    }
    let limited = client
        .get_blocks_with_limit_and_commitment(
            min_slot,
            (max_slot - min_slot + 1) as usize,
            confirmed(),
        )
        .await?;
    check_eq!(
        limited,
        blocks,
        "getBlocksWithLimit must agree with getBlocks over the same range"
    )?;
    let slot_before = client.get_slot_with_commitment(confirmed()).await?;
    let height = client.get_block_height_with_commitment(confirmed()).await?;
    let slot_after = client.get_slot_with_commitment(confirmed()).await?;
    check!(
        slot_before <= height && height <= slot_after,
        "getBlockHeight ({height}) must sit between consecutive getSlot \
         reads ({slot_before}, {slot_after})"
    )?;
    check!(
        slot_before >= max_slot,
        "getSlot ({slot_before}) must not precede the last write slot \
         {max_slot}"
    )?;

    for (chunk_index, chunk) in pdas.chunks(ACCOUNTS_PER_CALL).enumerate() {
        let batched = client
            .get_multiple_accounts_with_commitment(chunk, confirmed())
            .await?
            .value;
        for (offset, (pda, batched)) in chunk.iter().zip(batched).enumerate() {
            let write = &writes[chunk_index * ACCOUNTS_PER_CALL + offset];
            let batched = batched
                .ok_or("getMultipleAccounts dropped a written account")?;
            let single = client
                .get_account_with_commitment(pda, confirmed())
                .await?
                .value
                .ok_or("getAccountInfo dropped a written account")?;
            let balance = client
                .get_balance_with_commitment(pda, confirmed())
                .await?
                .value;
            check_eq!(
                crate::written_id(&single.data),
                Some(write.id),
                "getAccountInfo must show the written id for {pda}"
            )?;
            check_eq!(
                batched.data,
                single.data,
                "getMultipleAccounts must agree with getAccountInfo for {pda}"
            )?;
            check_eq!(
                batched.lamports,
                single.lamports,
                "getMultipleAccounts lamports must agree with getAccountInfo \
                 for {pda}"
            )?;
            check_eq!(
                balance,
                single.lamports,
                "getBalance must agree with getAccountInfo for {pda}"
            )?;
        }
    }

    for (write, slot) in writes.iter().zip(&slots) {
        let history = client
            .get_signatures_for_address_with_config(
                &write.pda,
                GetConfirmedSignaturesForAddress2Config {
                    limit: Some(HISTORY_LIMIT),
                    commitment: Some(confirmed()),
                    ..GetConfirmedSignaturesForAddress2Config::default()
                },
            )
            .await?;
        let entry = history
            .iter()
            .find(|entry| entry.signature == write.signature.to_string())
            .ok_or_else(|| {
                format!(
                    "getSignaturesForAddress({}) does not list {}",
                    write.pda, write.signature
                )
            })?;
        check_eq!(
            entry.slot,
            *slot,
            "getSignaturesForAddress slot must match the status slot for {}",
            write.signature
        )?;
        check!(
            entry.err.is_none(),
            "getSignaturesForAddress reports a failed write {}",
            write.signature
        )?;
    }
    let delegated: RawDelegationStatus = er
        .api()
        .call("getDelegationStatus", &(probe.pda.to_string(),))
        .await?;
    check!(
        delegated.is_delegated,
        "getDelegationStatus must report the written pda as delegated"
    )?;
    let undelegated: RawDelegationStatus = er
        .api()
        .call("getDelegationStatus", &(payers[0].pubkey().to_string(),))
        .await?;
    check!(
        !undelegated.is_delegated,
        "getDelegationStatus must report a plain payer as not delegated"
    )?;
    Ok(())
}

async fn transfer(
    er: &ErCtx,
    sender: &TxSender,
    to: &Pubkey,
    lamports: u64,
) -> Result<(Signature, u64)> {
    crate::phase("http transfer", async {
        let signature = sender
            .submit_fresh(&[system::transfer(
                &sender.payer().pubkey(),
                to,
                lamports,
            )])
            .await?;
        let confirmed = er
            .api()
            .await_transaction(&signature, TRANSFER_TIMEOUT)
            .await?;
        check!(
            confirmed.err.is_none(),
            "transfer {signature} failed: {:?}",
            confirmed.err
        )?;
        Ok((signature, confirmed.slot))
    })
    .await
}

async fn timestamps(
    er: &ErCtx,
    executions: Vec<(Signature, u64)>,
) -> Result<()> {
    let last_slot = executions.last().map_or(0, |(_, slot)| *slot);
    let settled = check::poll_until(LEDGER_SETTLE_TIMEOUT, POLL, async || {
        Ok::<_, redsuite_core::DynError>(
            (er.api().get_slot().await? >= last_slot + LEDGER_SETTLE_SLOTS)
                .then_some(()),
        )
    })
    .await?;
    check!(
        settled.is_some(),
        "the slot never reached {} within {LEDGER_SETTLE_TIMEOUT:?}",
        last_slot + LEDGER_SETTLE_SLOTS
    )?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    for (signature, slot) in &executions {
        let time = er.api().get_block_time(*slot).await?.ok_or_else(|| {
            CheckError::new(format!(
                "getBlockTime({slot}) returned null for {signature}"
            ))
        })?;
        let block = er.api().get_block(*slot).await?.ok_or_else(|| {
            CheckError::new(format!(
                "getBlock({slot}) returned null for {signature}"
            ))
        })?;
        let transaction = er
            .api()
            .get_transaction(signature)
            .await?
            .ok_or_else(|| format!("settled {signature} vanished"))?;
        check_eq!(
            (block.block_time, transaction.block_time),
            (Some(time), Some(time)),
            "block and transaction {signature} agree with getBlockTime({slot})"
        )?;
        check!(
            time > 0 && time <= now,
            "timestamp {time} of {signature} is positive and not in the \
             future (now {now})"
        )?;
    }
    Ok(())
}

async fn await_event_where(
    events: &EventSubscriptions,
    key: u64,
    what: &str,
    matches: impl Fn(&json::Value) -> bool,
) -> Result<json::Value> {
    check::poll_until(EVENT_TIMEOUT, EVENT_POLL, async || {
        Ok::<_, CheckError>(
            events.events(key).into_iter().find(|event| matches(event)),
        )
    })
    .await?
    .ok_or_else(|| {
        CheckError::new(format!("timed out waiting for {what}"))
            .actual(format!("{:?}", events.events(key)))
            .context("waited", format!("{EVENT_TIMEOUT:?}"))
            .into()
    })
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

async fn wallet_stream(
    base: &BaseCtx,
    er: &ErCtx,
    events: &EventSubscriptions,
) -> Result<()> {
    let system_program = system::system_id();
    let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let account1 =
        prep::delegated_payer(base, &funder, er.identity(), ACCOUNT_LAMPORTS)
            .await?;
    let account2 =
        prep::delegated_payer(base, &funder, er.identity(), ACCOUNT_LAMPORTS)
            .await?
            .pubkey();
    let account1_pubkey = account1.pubkey();
    let sender = er.sender(Rc::new(account1));
    let account1_key = events.account_subscribe(&account1_pubkey).await?;
    let account2_key = events.account_subscribe(&account2).await?;
    events.await_subscribed(3, Duration::from_secs(5)).await?;
    let mut sent = 0u64;

    let executions = crate::phase("ws account stream", async {
        let mut executions = Vec::new();
        while sent < SEQUENTIAL_TRANSFERS {
            executions.push(
                transfer(er, &sender, &account2, TRANSFER_LAMPORTS)
                    .await?,
            );
            sent += 1;
            let expected = ACCOUNT_LAMPORTS - sent * TRANSFER_LAMPORTS;
            await_event_where(
                events,
                account1_key,
                "sequential account 1 update",
                |event| event_lamports(event) == Some(expected),
            )
            .await?;
            if sent == 1 {
                await_event_where(
                    events,
                    account2_key,
                    "account 2 credit",
                    |event| {
                        event_lamports(event)
                            == Some(ACCOUNT_LAMPORTS + TRANSFER_LAMPORTS)
                    },
                )
                .await?;
                for event in events.events(account2_key) {
                    let lamports = event_lamports(&event);
                    check!(
                        lamports == Some(ACCOUNT_LAMPORTS)
                            || lamports
                                == Some(
                                    ACCOUNT_LAMPORTS + TRANSFER_LAMPORTS
                                ),
                        "unexpected account 2 lamports in {event:?}"
                    )?;
                }
            }
        }
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
            ACCOUNT_LAMPORTS - SEQUENTIAL_TRANSFERS * TRANSFER_LAMPORTS,
            "the last account 1 notification must show the fully drained balance"
        )?;
        Ok(executions)
    })
    .await?;

    let notifications = async {
        crate::phase("ws logs and program notifications", async {
            let mentions1_key =
                events.logs_subscribe_mentions(&account1_pubkey).await?;
            let mentions2_key =
                events.logs_subscribe_mentions(&account2).await?;
            let program_key = events.program_subscribe(&system_program).await?;
            events.await_subscribed(6, Duration::from_secs(5)).await?;
            let mut logs_signatures = Vec::with_capacity(LOGS_TRANSFERS);
            for _ in 0..LOGS_TRANSFERS {
                let (signature, _) =
                    transfer(er, &sender, &account2, TRANSFER_LAMPORTS).await?;
                logs_signatures.push(signature.to_string());
                sent += 1;
            }
            for logs_signature in &logs_signatures {
                for (key, what) in [
                    (mentions1_key, "logs mentions(account1) entry"),
                    (mentions2_key, "logs mentions(account2) entry"),
                ] {
                    let event = await_event_where(events, key, what, |event| {
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
                    check!(
                        log_count > 0,
                        "{what} has no log lines: {event:?}"
                    )?;
                }
            }
            let expected1 = ACCOUNT_LAMPORTS - sent * TRANSFER_LAMPORTS;
            let expected2 = ACCOUNT_LAMPORTS + sent * TRANSFER_LAMPORTS;
            for (what, account, expected) in [
                ("program update 1", account1_pubkey, expected1),
                ("program update 2", account2, expected2),
            ] {
                await_event_where(events, program_key, what, |event| {
                    event.get("value").get("pubkey").as_str()
                        == Some(account.to_string().as_str())
                        && event
                            .get("value")
                            .get("account")
                            .get("lamports")
                            .as_u64()
                            == Some(expected)
                })
                .await?;
            }
            Ok(())
        })
        .await?;

        crate::phase("ws signature notifications", async {
            let confirmations = SignatureConfirmations::connect(
                er.ws_url(),
                Arc::new(SendClock::new(1, 2)),
            )
            .await?;
            let immediate_tx = sender
                .prepare(&[system::transfer(
                    &account1_pubkey,
                    &account2,
                    7_777,
                )])
                .await?;
            confirmations
                .subscribe(1, &immediate_tx.signatures[0])
                .await?;
            crate::phase(
                "http transfer",
                sender.submit_prepared(&immediate_tx),
            )
            .await?;
            tokio::time::timeout(EVENT_TIMEOUT, confirmations.await_id(1))
                .await
                .map_err(|_| {
                    CheckError::new("signature notification never fired")
                })??;
            let (delayed_signature, _) =
                transfer(er, &sender, &account2, 8_888).await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
            confirmations.subscribe(2, &delayed_signature).await?;
            tokio::time::timeout(EVENT_TIMEOUT, confirmations.await_id(2))
                .await
                .map_err(|_| {
                    CheckError::new(
                        "delayed signature subscription never fired for \
                         an already-confirmed signature",
                    )
                })??;
            let outcome = confirmations.finalize();
            check_eq!(
                outcome.confirmed,
                2,
                "expected both signature notifications: {outcome:?}"
            )?;
            check_eq!(
                outcome.failed,
                0,
                "signature notifications carried errors: {outcome:?}"
            )?;
            Ok(())
        })
        .await?;

        crate::phase("ws unsubscribe silence", async {
            let mut raw = RawWs::connect(er.ws_url()).await?;
            for case in ["account", "logs", "program"] {
                let subscription = match case {
                    "account" => {
                        raw.account_subscribe(&account1_pubkey).await?
                    }
                    "logs" => {
                        raw.logs_subscribe_mentions(&account1_pubkey).await?
                    }
                    _ => raw.program_subscribe(&system_program).await?,
                };
                transfer(er, &sender, &account2, TRANSFER_LAMPORTS).await?;
                check!(
                    raw.next_notification(RAW_FIRST_TIMEOUT).await?.is_some(),
                    "no {case} notification before unsubscribe"
                )?;
                let removed = match case {
                    "account" => raw.account_unsubscribe(subscription).await?,
                    "logs" => raw.logs_unsubscribe(subscription).await?,
                    _ => raw.program_unsubscribe(subscription).await?,
                };
                check!(removed, "the {case} unsubscribe must return true")?;
                while raw.next_notification(RAW_DRAIN_TIMEOUT).await?.is_some()
                {
                }
                let (signature, _) =
                    transfer(er, &sender, &account2, TRANSFER_LAMPORTS).await?;
                check!(
                    raw.next_notification(RAW_SILENCE_TIMEOUT).await?.is_none(),
                    "{case} notifications continued after unsubscribe \
                     ({signature})"
                )?;
            }
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
            let refusal = match raw.logs_subscribe_all().await {
                Err(error) => error.to_string(),
                Ok(subid) => format!("accepted as subscription {subid}"),
            };
            check!(
                refusal.contains("-32602")
                    && refusal.contains("'all' filter is not supported"),
                "logsSubscribe(all) must be refused with invalid params \
                 naming the filter, got: {refusal}"
            )?;
            let unsent = sender
                .prepare(&[system::transfer(
                    &account1_pubkey,
                    &account2,
                    9_999,
                )])
                .await?;
            let signature_sub = raw
                .signature_subscribe(&unsent.signatures[0].to_string())
                .await?;
            raw.signature_unsubscribe(signature_sub).await?;
            check_eq!(
                raw.malformed_frames(),
                0,
                "the raw socket carried frames that do not parse as \
                 JSON-RPC"
            )?;
            raw.close().await
        })
        .await
    };
    tokio::try_join!(
        crate::phase("http timestamps", timestamps(er, executions)),
        notifications,
    )?;
    Ok(())
}

async fn slot_stream(events: &EventSubscriptions, slot_key: u64) -> Result<()> {
    events.await_events(slot_key, 10, EVENT_TIMEOUT).await?;
    let mut last_slot = 0u64;
    for event in events.events(slot_key) {
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
    Ok(())
}

#[async_trait(?Send)]
impl Scenario for RpcLifecycle {
    fn name(&self) -> &str {
        "redshift/rpc_lifecycle"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<()> {
        let client = RpcClient::new_with_commitment(
            er.api().url().to_owned(),
            confirmed(),
        );
        let events = EventSubscriptions::connect(er.ws_url()).await?;
        let slot_key = events.slot_subscribe().await?;
        tokio::try_join!(
            crate::phase("http burst", burst(self.name(), base, er, &client)),
            wallet_stream(base, er, &events),
            crate::phase("ws slot stream", slot_stream(&events, slot_key)),
        )?;
        check_eq!(
            events.malformed_frames(),
            0,
            "the event stream carried frames that do not parse as JSON-RPC"
        )?;
        events.finalize();
        Ok(())
    }
}
