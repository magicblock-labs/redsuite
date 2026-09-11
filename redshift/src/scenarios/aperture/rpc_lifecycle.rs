use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures_util::future::join_all;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, prep,
    report::Unit,
    rpc_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_client::GetConfirmedSignaturesForAddress2Config,
    },
    rpc_client_api::config::{
        RpcBlockConfig, RpcSendTransactionConfig, RpcSimulateTransactionConfig,
        RpcTransactionConfig,
    },
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
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

#[async_trait(?Send)]
impl Scenario for RpcLifecycle {
    fn name(&self) -> &str {
        "redshift/rpc_lifecycle"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let client = RpcClient::new_with_commitment(
            er.api().url().to_owned(),
            confirmed(),
        );

        let payers = prep::funded_payers(base, PAYERS, PAYER_LAMPORTS).await?;
        let pdas = prep::init_delegated_accounts_batched(
            base,
            &payers,
            WRITES,
            crate::ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        let per_payer = WRITES.div_ceil(PAYERS);
        await_clones(&client, &pdas).await?;
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
        let fee = client
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
        let burst_started = Instant::now();
        let sends = join_all(writes.iter().map(|write| {
            client.send_transaction_with_config(&write.transaction, send_config)
        }));
        let sends = tokio::time::timeout(BURST_DEADLINE, sends).await.map_err(
            |_| {
                format!(
                    "the burst of {WRITES} sends exceeded {BURST_DEADLINE:?}"
                )
            },
        )?;
        let burst_elapsed = burst_started.elapsed();
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
                        "[redsuite] {}: send of {} failed: {error}",
                        self.name(),
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

        let slots = await_statuses(&client, &signatures).await?;
        let min_slot = *slots.iter().min().ok_or("no slots recorded")?;
        let max_slot = *slots.iter().max().ok_or("no slots recorded")?;
        await_publication(&client, max_slot).await?;

        let mut by_slot: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
        for (index, slot) in slots.iter().enumerate() {
            by_slot.entry(*slot).or_default().push(index);
        }

        let mut block_times = Vec::with_capacity(writes.len());
        for (write, slot) in writes.iter().zip(&slots) {
            let transaction = client
                .get_transaction_with_config(
                    &write.signature,
                    transaction_config(),
                )
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
            let block =
                client.get_block_with_config(*slot, block_config()).await?;
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
                    "getTransaction.blockTime must match getBlockTime for \
                     slot {slot}"
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
        let height =
            client.get_block_height_with_commitment(confirmed()).await?;
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
            for (offset, (pda, batched)) in
                chunk.iter().zip(batched).enumerate()
            {
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

        Ok(ScenarioReport::ok(self.name())
            .setting("writes", WRITES)
            .setting("payers", PAYERS)
            .setting("fee lamports", fee)
            .setting("first slot", min_slot)
            .setting("last slot", max_slot)
            .metric(
                "burst elapsed ms",
                Unit::Millis,
                burst_elapsed.as_secs_f64() * 1e3,
            )
            .metric("burst failures", Unit::Count, failures as f64)
            .metric("distinct slots", Unit::Count, by_slot.len() as f64))
    }
}
