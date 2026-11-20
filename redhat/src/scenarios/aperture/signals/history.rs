use super::{channels::Channels, finish, fixture, BaseCtx, ChainCtx, ErCtx, Result, Scenario};
use anyhow::ensure;
use instruction::{AccountMeta, Instruction};
use message::Message;
use redsuite_core::{api::transaction_meta, random, rpc_types::response::TransactionError};
use signer::Signer;
use std::rc::Rc;

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
