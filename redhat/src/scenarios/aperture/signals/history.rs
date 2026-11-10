use super::{
    channels::Channels, finish, fixture, BaseCtx, ChainCtx, ErCtx, Result, Scenario, TIMEOUT,
};
use anyhow::{ensure, Context};
use instruction::{AccountMeta, Instruction};
use message::Message;
use redsuite_core::{
    api::transaction_meta,
    random,
    rpc_types::{config::RpcBlockConfig, request::RpcRequest, response::TransactionError},
    transaction_types::TransactionDetails,
    wait,
};
use serde_json::Value;
use signer::Signer;
use std::{
    fs,
    path::{Path, PathBuf},
    rc::Rc,
};
use tracing::debug;

signal_scenario!(LedgerIndexSignals, ledger_index);

async fn ledger_index(base: Rc<BaseCtx>, er: ErCtx) -> Result<()> {
    let mut fixture = fixture(base, er, "ledger_index_signals").await?;
    let mut rng = random::rng(
        fixture.base.config().seed,
        &["redhat", "ledger_index_signals", "program"],
        0,
    );
    let mut channels = Channels::open(&fixture).await?;
    channels.control(&mut fixture, 12_000, &[]).await?;
    let missing_program = random::pubkey(&mut rng);
    ensure!(fixture.base.account(&missing_program).await?.is_none());
    let (instructions, error) = fixture.failed_write(12_001, 17, 7);
    let cases = [
        ("failed execution", instructions, error),
        (
            "loading failure",
            vec![Instruction {
                program_id: missing_program,
                accounts: vec![AccountMeta::new_readonly(fixture.keys[1], false)],
                data: vec![],
            }],
            TransactionError::InvalidProgramForExecution,
        ),
    ];
    let mut findings = Vec::new();
    for (index, (label, instructions, error)) in cases.into_iter().enumerate() {
        let (tx, probe, slot) =
            channels.submit_failure(&mut fixture, &instructions, &error).await?;
        match fixture.er.api().get_transaction(&tx.signature).await? {
            Some(record) => {
                let meta = transaction_meta(&record)?;
                ensure!(record.slot == slot && meta.err == tx.error && meta.fee == tx.fee);
                if label == "loading failure" {
                    ensure!(
                        meta.pre_balances.is_empty() && meta.post_balances.is_empty(),
                        "loading-failure probe unexpectedly produced resolved execution metadata"
                    );
                }
            }
            None => findings.push(format!(
                "{label}/getTransaction: ledger signature {} has no addressable transaction record",
                tx.signature
            )),
        }
        for key in Message::new(&instructions, Some(&fixture.payer.pubkey())).account_keys {
            let signatures = fixture.er.api().get_signatures_for_address(&key, 100).await?;
            let count = signatures.iter().filter(|signature| **signature == tx.signature).count();
            if count != 1 {
                findings.push(format!(
                    "{label}/address index: ledger signature {} appears {count} times for {key}",
                    tx.signature
                ));
            }
        }
        let observed = channels
            .control_with_probe(&mut fixture, 12_100 + index as u64, &[], Some(&probe))
            .await?;
        for channel in ["signature", "failures"] {
            if !observed.contains(channel) {
                findings.push(format!(
                    "{label}/{channel}: ledger failure {} has no diagnostic",
                    tx.signature
                ));
            }
        }
    }
    channels.control(&mut fixture, 12_200, &[]).await?;
    finish(findings)
}

fn superblocks(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir()
            && entry.file_name().to_str().is_some_and(|name| name.starts_with("superblock-"))
        {
            paths.push(entry.path());
        }
    }
    ensure!(
        !paths.is_empty(),
        "no owned superblocks to witness retention"
    );
    Ok(paths)
}

fn retention_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    ["prun", "cleaned", "retention", "minimum ledger", "first available block"]
        .iter()
        .any(|needle| text.contains(needle))
}

signal_scenario!(PrunedReadSignals, pruned_reads);

async fn pruned_reads(base: Rc<BaseCtx>, er: ErCtx) -> Result<()> {
    let mut fixture = fixture(base, er, "pruned_read_signals").await?;
    let mut channels = Channels::open(&fixture).await?;
    channels.control(&mut fixture, 13_000, &[]).await?;
    let signature = *fixture
        .er
        .api()
        .get_signatures_for_address(&fixture.keys[1], 1)
        .await?
        .first()
        .context("retention landmark has no address index")?;
    let landmark = fixture
        .er
        .api()
        .get_transaction(&signature)
        .await?
        .context("retention landmark missing")?;
    ensure!(transaction_meta(&landmark)?.err.is_none());
    let before = fixture.snapshot(&[]).await?;
    let directories = superblocks(&fixture.er.storage_dir().await?)?;
    debug!(slot = landmark.slot, %signature, ?directories, "pruned-read baseline");
    wait::retry(
        "landmark superblocks physically pruned",
        TIMEOUT * directories.len() as u32,
        async || {
            ensure!(
                directories.iter().all(|path| !path.exists()),
                "landmark superblocks still retained"
            );
            Ok(())
        },
    )
    .await?;
    fixture.unchanged(&before).await?;
    let latest = fixture.er.api().get_slot().await?;
    let mut findings = Vec::new();
    let mut advertised = false;
    for method in [RpcRequest::GetFirstAvailableBlock, RpcRequest::MinimumLedgerSlot] {
        let boundary: u64 = fixture.er.api().call(method, &Vec::<u8>::new()).await?;
        let valid = boundary > landmark.slot && boundary <= latest;
        advertised |= valid;
        if !valid {
            findings.push(format!(
                "validator#1888: {method}/retention boundary: advertises {boundary} after known slot {} was pruned",
                landmark.slot
            ));
        }
    }
    let config = RpcBlockConfig {
        transaction_details: Some(TransactionDetails::None),
        rewards: Some(false),
        max_supported_transaction_version: Some(1),
        ..Default::default()
    };
    let block: std::result::Result<Value, _> = fixture
        .er
        .api()
        .try_call(RpcRequest::GetBlock, &(landmark.slot, config))
        .await?;
    let block_time: std::result::Result<Value, _> =
        fixture.er.api().try_call(RpcRequest::GetBlockTime, &(landmark.slot,)).await?;
    for (channel, result) in [("getBlock", block), ("getBlockTime", block_time)] {
        match result {
            Err(error) if retention_error(&error.error.message) => {},
            Ok(value) if !value.is_null() => findings.push(format!("{channel}: served a record below the witnessed pruned boundary")),
            result if !advertised => findings.push(format!("validator#1888: {channel}: silently omitted known pruned slot {} without a retention diagnostic: {result:?}", landmark.slot)),
            Err(error) => findings.push(format!("{channel}: rejected known pruned slot {} without a retention diagnostic: {error}", landmark.slot)),
            Ok(_) => {},
        }
    }
    let slots: Vec<u64> =
        fixture.er.api().call(RpcRequest::GetBlocks, &(landmark.slot, latest)).await?;
    if slots.contains(&landmark.slot) {
        findings.push(format!(
            "validator#1888: getBlocks: still advertises known pruned slot {}",
            landmark.slot
        ));
    }
    if !advertised {
        findings.push("validator#1888: getBlocks/retention boundary: omitted a previously published slot with no advertised retention limit".into());
    }
    ensure!(
        fixture.er.api().get_transaction(&signature).await?.is_none(),
        "physical pruning did not remove the landmark lookup"
    );
    channels.control(&mut fixture, 13_001, &[]).await?;
    finish(findings)
}
