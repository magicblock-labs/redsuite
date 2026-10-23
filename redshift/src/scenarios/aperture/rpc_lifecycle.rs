use std::{
    collections::BTreeMap,
    rc::Rc,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{ensure, Context};
use futures_util::future::join_all;
use hash::Hash;
use pubkey::Pubkey;
use redsuite_core::{
    api::{encode, transaction_meta},
    programs::{CellHeader, REDLINE},
    redline::Accounts,
    rpc_types::{
        config::{RpcBlockConfig, RpcSignaturesForAddressConfig, RpcTransactionConfig},
        request::RpcRequest,
        response::{
            Response, RpcBlockhash, RpcConfirmedTransactionStatusWithSignature, RpcKeyedAccount,
            RpcLogsResponse, SlotInfo,
        },
    },
    system,
    transport::{
        clock::SendClock, events::EventSubscriptions, ws::SignatureConfirmations, wsraw::RawWs,
    },
    Api, BaseCtx, ChainCtx, ErCtx, Result, Scenario, TxSender,
};
use serde::Deserialize;
use signature::Signature;
use signer::Signer;
use solana_account_decoder_client_types::UiAccount;
use solana_commitment_config::CommitmentConfig;
use solana_transaction_status_client_types::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionDetails, UiConfirmedBlock,
    UiTransactionEncoding,
};
use transaction::Transaction;

use crate::ACCOUNT_SPACE;

const WRITES: usize = 256;
const PAYERS: usize = 32;
const PAYER_LAMPORTS: u64 = 2_000_000_000;
const FIRST_ID: u64 = 1_000;
const ACCOUNTS_PER_CALL: usize = 100;
const HISTORY_LIMIT: usize = 16;
const BURST_DEADLINE: Duration = Duration::from_secs(20);
const LEDGER_SETTLE_SLOTS: u64 = 10;

const ACCOUNT_LAMPORTS: u64 = 1_000_000_000;
const TRANSFER_LAMPORTS: u64 = 10_000;
const SEQUENTIAL_TRANSFERS: u64 = 10;
const LOGS_TRANSFERS: usize = 5;
const RAW_FIRST_TIMEOUT: Duration = Duration::from_secs(3);
const RAW_DRAIN_TIMEOUT: Duration = Duration::from_millis(300);
const RAW_SILENCE_TIMEOUT: Duration = Duration::from_millis(700);

struct Write {
    address: Pubkey,
    id: u64,
    transaction: Transaction,
    signature: Signature,
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

async fn await_clones(client: &Api, pdas: &[Pubkey]) -> Result<()> {
    client
        .await_accounts(pdas, "delegated account clones", async || {
            for chunk in pdas.chunks(ACCOUNTS_PER_CALL) {
                let accounts = client.get_multiple_accounts(chunk).await?;
                ensure!(accounts.iter().all(|account| account
                    .as_ref()
                    .is_some_and(|account| account.data.len() == ACCOUNT_SPACE as usize)));
            }
            Ok(())
        })
        .await
}

async fn await_statuses(client: &Api, signatures: &[Signature]) -> Result<Vec<u64>> {
    futures_util::future::try_join_all(
        signatures.iter().map(|signature| client.confirm(signature)),
    )
    .await?;
    client
        .await_query("confirmed signature statuses", async || {
            let statuses = client.get_signature_statuses(signatures).await?;
            statuses
                .into_iter()
                .map(|status| {
                    let status =
                        status.context("confirmed signature missing from getSignatureStatuses")?;
                    ensure!(status.err == None);
                    ensure!(status.satisfies_commitment(confirmed()));
                    Ok(status.slot)
                })
                .collect()
        })
        .await
}

async fn await_publication(client: &Api, slot: u64) -> Result<()> {
    client
        .await_query("block publication", async || {
            client
                .call::<UiConfirmedBlock>(RpcRequest::GetBlock, &(slot, block_config()))
                .await?;
            Ok(())
        })
        .await
}

async fn burst(base: &BaseCtx, er: &ErCtx, client: &Api) -> Result<()> {
    let payers = base.funded_payers(PAYERS, PAYER_LAMPORTS).await?;
    let pdas = Accounts::new(ACCOUNT_SPACE, er.identity())
        .init_batched(base, &payers, WRITES, true)
        .await?;
    let per_payer = WRITES.div_ceil(PAYERS);
    await_clones(client, &pdas).await?;
    let blockhash = client.get_latest_blockhash().await?;
    ensure!(client.is_blockhash_valid(&blockhash).await?);
    let pda_strings: Vec<String> = pdas.iter().map(ToString::to_string).collect();
    let for_accounts: Response<RpcBlockhash> = er
        .api()
        .call(
            RpcRequest::Custom {
                method: "getBlockhashForAccounts",
            },
            &(pda_strings,),
        )
        .await?;
    let for_accounts: Hash = for_accounts.value.blockhash.parse()?;
    ensure!(client.is_blockhash_valid(&for_accounts).await?);

    let writes: Vec<Write> = pdas
        .iter()
        .enumerate()
        .map(|(index, pda)| {
            let id = FIRST_ID + index as u64;
            let payer = &payers[index / per_payer];
            let transaction = Transaction::new_signed_with_payer(
                &[REDLINE.write(id, &[*pda])],
                Some(&payer.pubkey()),
                &[payer],
                blockhash,
            );
            let signature = transaction.signatures[0];
            Write {
                address: *pda,
                id,
                transaction,
                signature,
            }
        })
        .collect();
    let signatures: Vec<Signature> = writes.iter().map(|write| write.signature).collect();
    let probe = &writes[0];
    let message = encode(&probe.transaction.message)?;
    let _fee: Option<u64> =
        client.call_value(RpcRequest::GetFeeForMessage, &(message, confirmed())).await?;
    let simulation = client.simulate_transaction(&probe.transaction).await?;
    ensure!(simulation.err == None);
    let after_simulation = client
        .get_account(&probe.address)
        .await?
        .context("the probe account vanished after simulation")?;
    ensure!(CellHeader::written_id(&after_simulation.data) == Some(0));
    let after_simulation = client.get_signature_status(&probe.signature).await?;
    ensure!(after_simulation.is_none());

    let sends = join_all(writes.iter().map(|write| client.send_transaction(&write.transaction)));
    let sends = tokio::time::timeout(BURST_DEADLINE, sends)
        .await
        .with_context(|| format!("the burst of {WRITES} sends exceeded {BURST_DEADLINE:?}"))?;
    for sent in sends {
        sent?;
    }

    let slots = await_statuses(client, &signatures).await?;
    let min_slot = *slots.iter().min().context("no slots recorded")?;
    let max_slot = *slots.iter().max().context("no slots recorded")?;
    await_publication(client, max_slot).await?;

    let mut by_slot: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for (index, slot) in slots.iter().enumerate() {
        by_slot.entry(*slot).or_default().push(index);
    }

    let mut block_times = Vec::with_capacity(writes.len());
    for (write, slot) in writes.iter().zip(&slots) {
        let transaction: EncodedConfirmedTransactionWithStatusMeta = client
            .call(
                RpcRequest::GetTransaction,
                &(write.signature.to_string(), transaction_config()),
            )
            .await?;
        ensure!(transaction.slot == *slot);
        let meta = transaction.transaction.meta.context("getTransaction returned no meta")?;
        ensure!(meta.err == None);
        block_times.push(transaction.block_time.context("getTransaction returned no blockTime")?);
    }

    for (slot, indices) in &by_slot {
        let block: UiConfirmedBlock =
            client.call(RpcRequest::GetBlock, &(*slot, block_config())).await?;
        let block_time = client.call::<i64>(RpcRequest::GetBlockTime, &(*slot,)).await?;
        ensure!(block.block_time == Some(block_time));
        let listed = block.signatures.context("getBlock returned no signatures")?;
        for &index in indices {
            let signature = writes[index].signature.to_string();
            ensure!(listed.contains(&signature));
            ensure!(block_times[index] == block_time);
        }
    }
    let blocks: Vec<u64> = client
        .call(
            RpcRequest::GetBlocks,
            &(min_slot, Some(max_slot), confirmed()),
        )
        .await?;
    ensure!(blocks.windows(2).all(|pair| pair[0] < pair[1]));
    for slot in by_slot.keys() {
        ensure!(blocks.contains(slot));
    }
    let limited: Vec<u64> = client
        .call(
            RpcRequest::GetBlocksWithLimit,
            &(min_slot, (max_slot - min_slot + 1) as usize, confirmed()),
        )
        .await?;
    ensure!(limited == blocks);
    let slot_before = client.get_slot().await?;
    let height: u64 = client.call(RpcRequest::GetBlockHeight, &(confirmed(),)).await?;
    let slot_after = client.get_slot().await?;
    ensure!(slot_before <= height && height <= slot_after);
    ensure!(slot_before >= max_slot);

    for (chunk_index, chunk) in pdas.chunks(ACCOUNTS_PER_CALL).enumerate() {
        let batched = client.get_multiple_accounts(chunk).await?;
        for (offset, (pda, batched)) in chunk.iter().zip(batched).enumerate() {
            let write = &writes[chunk_index * ACCOUNTS_PER_CALL + offset];
            let batched = batched.context("getMultipleAccounts dropped a written account")?;
            let single = client
                .get_account(pda)
                .await?
                .context("getAccountInfo dropped a written account")?;
            let balance = client.get_balance(pda).await?;
            ensure!(CellHeader::written_id(&single.data) == Some(write.id));
            ensure!(batched.data == single.data);
            ensure!(batched.lamports == single.lamports);
            ensure!(balance == single.lamports);
        }
    }

    for (write, slot) in writes.iter().zip(&slots) {
        let config = RpcSignaturesForAddressConfig {
            limit: Some(HISTORY_LIMIT),
            commitment: Some(confirmed()),
            ..RpcSignaturesForAddressConfig::default()
        };
        let history: Vec<RpcConfirmedTransactionStatusWithSignature> = client
            .call(
                RpcRequest::GetSignaturesForAddress,
                &(write.address.to_string(), config),
            )
            .await?;
        let entry = history
            .iter()
            .find(|entry| entry.signature == write.signature.to_string())
            .with_context(|| {
                format!(
                    "getSignaturesForAddress({}) does not list {}",
                    write.address, write.signature
                )
            })?;
        ensure!(entry.slot == *slot);
        ensure!(entry.err == None);
    }
    let delegated: RawDelegationStatus = er
        .api()
        .call(
            RpcRequest::Custom { method: "getDelegationStatus" },
            &(probe.address.to_string(),),
        )
        .await?;
    ensure!(delegated.is_delegated);
    let undelegated: RawDelegationStatus = er
        .api()
        .call(
            RpcRequest::Custom { method: "getDelegationStatus" },
            &(payers[0].pubkey().to_string(),),
        )
        .await?;
    ensure!(!undelegated.is_delegated);
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
            .submit_fresh(&[system::transfer(&sender.payer().pubkey(), to, lamports)])
            .await?;
        let confirmed = er.api().await_transaction(&signature).await?;
        let meta = transaction_meta(&confirmed)?;
        ensure!(meta.err == None);
        Ok((signature, confirmed.slot))
    })
    .await
}

async fn timestamps(er: &ErCtx, executions: Vec<(Signature, u64)>) -> Result<()> {
    let last_slot = executions.last().map_or(0, |(_, slot)| *slot);
    er.api().await_slot(last_slot + LEDGER_SETTLE_SLOTS).await?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    for (signature, slot) in &executions {
        let time = er.api().get_block_time(*slot).await?.context("settled block time missing")?;
        let block = er.api().get_block(*slot).await?.context("settled block missing")?;
        let transaction = er
            .api()
            .get_transaction(signature)
            .await?
            .context("settled transaction missing")?;
        ensure!((block.block_time, transaction.block_time) == (Some(time), Some(time)));
        ensure!(time > 0 && time <= now);
    }
    Ok(())
}

async fn await_event_where<T: serde::de::DeserializeOwned>(
    events: &EventSubscriptions,
    key: u64,
    what: &str,
    matches: impl Fn(&T) -> bool,
) -> Result<T> {
    events.await_event(key, matches).await.with_context(|| what.to_owned())
}

async fn wallet_stream(base: &BaseCtx, er: &ErCtx, events: &EventSubscriptions) -> Result<()> {
    let system_program = system::system_id();
    let funder = base.funded_payer(crate::PAYER_LAMPORTS).await?;
    let account1 = base.delegated_payer(&funder, er.identity(), ACCOUNT_LAMPORTS).await?;
    let account2 = base.delegated_payer(&funder, er.identity(), ACCOUNT_LAMPORTS).await?.pubkey();
    let account1_pubkey = account1.pubkey();
    let sender = er.sender(Rc::new(account1));
    let account1_key = events.account_subscribe(&account1_pubkey).await?;
    let account2_key = events.account_subscribe(&account2).await?;
    events.await_subscribed(3).await?;
    let mut sent = 0u64;

    let executions = crate::phase("ws account stream", async {
        let mut executions = Vec::new();
        while sent < SEQUENTIAL_TRANSFERS {
            executions.push(transfer(er, &sender, &account2, TRANSFER_LAMPORTS).await?);
            sent += 1;
            let expected = ACCOUNT_LAMPORTS - sent * TRANSFER_LAMPORTS;
            await_event_where(
                events,
                account1_key,
                "sequential account 1 update",
                |event: &Response<UiAccount>| event.value.lamports == expected,
            )
            .await?;
            if sent == 1 {
                await_event_where(
                    events,
                    account2_key,
                    "account 2 credit",
                    |event: &Response<UiAccount>| {
                        event.value.lamports == ACCOUNT_LAMPORTS + TRANSFER_LAMPORTS
                    },
                )
                .await?;
                for event in events.events::<Response<UiAccount>>(account2_key)? {
                    let lamports = event.value.lamports;
                    ensure!(
                        lamports == ACCOUNT_LAMPORTS
                            || lamports == ACCOUNT_LAMPORTS + TRANSFER_LAMPORTS
                    );
                }
            }
        }
        let mut previous = ACCOUNT_LAMPORTS;
        for event in events.events::<Response<UiAccount>>(account1_key)? {
            let lamports = event.value.lamports;
            ensure!(
                lamports == previous || previous.checked_sub(TRANSFER_LAMPORTS) == Some(lamports)
            );
            previous = lamports;
        }
        ensure!(previous == (ACCOUNT_LAMPORTS - SEQUENTIAL_TRANSFERS * TRANSFER_LAMPORTS));
        Ok(executions)
    })
    .await?;

    let notifications = async {
        crate::phase("ws logs and program notifications", async {
            let mentions1_key = events.logs_subscribe_mentions(&account1_pubkey).await?;
            let mentions2_key = events.logs_subscribe_mentions(&account2).await?;
            let program_key = events.program_subscribe(&system_program).await?;
            events.await_subscribed(6).await?;
            let mut logs_signatures = Vec::with_capacity(LOGS_TRANSFERS);
            for _ in 0..LOGS_TRANSFERS {
                let (signature, _) = transfer(er, &sender, &account2, TRANSFER_LAMPORTS).await?;
                logs_signatures.push(signature.to_string());
                sent += 1;
            }
            for logs_signature in &logs_signatures {
                for (key, what) in [
                    (mentions1_key, "logs mentions(account1) entry"),
                    (mentions2_key, "logs mentions(account2) entry"),
                ] {
                    let event = await_event_where(
                        events,
                        key,
                        what,
                        |event: &Response<RpcLogsResponse>| {
                            event.value.signature == *logs_signature
                        },
                    )
                    .await?;
                    ensure!(event.value.err == None);
                    let log_count = event.value.logs.len();
                    ensure!(log_count > 0);
                }
            }
            let expected1 = ACCOUNT_LAMPORTS - sent * TRANSFER_LAMPORTS;
            let expected2 = ACCOUNT_LAMPORTS + sent * TRANSFER_LAMPORTS;
            for (what, account, expected) in [
                ("program update 1", account1_pubkey, expected1),
                ("program update 2", account2, expected2),
            ] {
                await_event_where(
                    events,
                    program_key,
                    what,
                    |event: &Response<RpcKeyedAccount>| {
                        event.value.pubkey == account.to_string()
                            && event.value.account.lamports == expected
                    },
                )
                .await?;
            }
            Ok(())
        })
        .await?;

        crate::phase("ws signature notifications", async {
            let confirmations =
                SignatureConfirmations::connect(er.ws_url(), Arc::new(SendClock::new(1, 2)))
                    .await?;
            let immediate_tx =
                sender.prepare(&[system::transfer(&account1_pubkey, &account2, 7_777)]).await?;
            confirmations.subscribe(1, &immediate_tx.signatures[0]).await?;
            crate::phase("http transfer", sender.submit_prepared(&immediate_tx)).await?;
            confirmations.await_id(1).await.context("signature notification never fired")?;
            let (delayed_signature, _) = transfer(er, &sender, &account2, 8_888).await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
            confirmations.subscribe(2, &delayed_signature).await?;
            confirmations.await_id(2).await.context(
                "delayed signature subscription never fired for an already-confirmed signature",
            )?;
            let outcome = confirmations.finalize();
            ensure!(outcome.confirmed == 2);
            ensure!(outcome.failed == 0);
            Ok(())
        })
        .await?;

        crate::phase("ws unsubscribe silence", async {
            let mut raw = RawWs::connect(er.ws_url()).await?;
            for case in ["account", "logs", "program"] {
                let subscription = match case {
                    "account" => raw.account_subscribe(&account1_pubkey).await?,
                    "logs" => raw.logs_subscribe_mentions(&account1_pubkey).await?,
                    _ => raw.program_subscribe(&system_program).await?,
                };
                transfer(er, &sender, &account2, TRANSFER_LAMPORTS).await?;
                ensure!(raw.next_notification(RAW_FIRST_TIMEOUT).await?.is_some());
                let removed = match case {
                    "account" => raw.account_unsubscribe(subscription).await?,
                    "logs" => raw.logs_unsubscribe(subscription).await?,
                    _ => raw.program_unsubscribe(subscription).await?,
                };
                ensure!(removed);
                while raw.next_notification(RAW_DRAIN_TIMEOUT).await?.is_some() {}
                transfer(er, &sender, &account2, TRANSFER_LAMPORTS).await?;
                ensure!(raw.next_notification(RAW_SILENCE_TIMEOUT).await?.is_none());
            }
            let slot_sub = raw.slot_subscribe().await?;
            ensure!(raw.next_notification(RAW_FIRST_TIMEOUT).await?.is_some());
            ensure!(raw.slot_unsubscribe(slot_sub).await?);
            while raw.next_notification(RAW_DRAIN_TIMEOUT).await?.is_some() {}
            ensure!(raw.next_notification(RAW_SILENCE_TIMEOUT).await?.is_none());
            let refusal = match raw.logs_subscribe_all().await {
                Err(error) => format!("{error:#}"),
                Ok(_) => "subscription accepted".to_owned(),
            };
            ensure!(
                refusal.contains("-32602") && refusal.contains("'all' filter is not supported")
            );
            let unsent =
                sender.prepare(&[system::transfer(&account1_pubkey, &account2, 9_999)]).await?;
            let signature_sub = raw.signature_subscribe(&unsent.signatures[0]).await?;
            raw.signature_unsubscribe(signature_sub).await?;
            ensure!(raw.malformed_frames() == 0);
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
    events.await_events(slot_key, 10).await?;
    let mut last_slot = 0u64;
    for event in events.events::<SlotInfo>(slot_key)? {
        let slot = event.slot;
        ensure!(slot > last_slot);
        last_slot = slot;
    }
    Ok(())
}

pub struct RpcLifecycle {
    base: Rc<BaseCtx>,
    er: ErCtx,
    client: Api,
    events: EventSubscriptions,
    slot_key: u64,
}

impl Scenario for RpcLifecycle {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let client = er.api().clone();
        let events = EventSubscriptions::connect(er.ws_url()).await?;
        let slot_key = events.slot_subscribe().await?;
        Ok(Self {
            base,
            er,
            client,
            events,
            slot_key,
        })
    }

    async fn run(self) -> Result<()> {
        let Self {
            base,
            er,
            client,
            events,
            slot_key,
        } = self;
        tokio::try_join!(
            crate::phase("http burst", burst(&base, &er, &client)),
            wallet_stream(&base, &er, &events),
            crate::phase("ws slot stream", slot_stream(&events, slot_key)),
        )?;
        ensure!(events.malformed_frames() == 0);
        events.finalize();
        Ok(())
    }
}
