use std::{collections::HashSet, mem};

use anyhow::{anyhow, ensure, Context};
use pubkey::Pubkey;
use signature::Signature;
use solana_rpc_client_types::response::{Response, RpcLogsResponse};
use solana_transaction_status_client_types::UiTransactionStatusMeta;
use tokio::time;
use tracing::warn;

use crate::{
    api::{custom_error_code, transaction_logs, transaction_meta, Api},
    transport::{events::Subscription, NETWORK_TIMEOUT},
    BaseCtx, ChainCtx, ErCtx, Result,
};

pub const INTENT_FAILED_CODE: u32 = 0x7461_636F;

pub const COMMIT_LIMIT_ERR: u32 = 0xA000_0000;

pub const INTENT_TOO_LARGE_ERR: u32 = 0xA000_0002;

pub const SPONSORED_COMMIT_LIMIT: u64 = 10;

const LOG_MARKER: &str = "ScheduledCommitSent ";

const SCHEDULE_MARKER: &str = "Scheduled commit with ID: ";

const RECEIPT_HISTORY_LIMIT: usize = 64;

#[derive(Debug)]
pub struct CommitReceipt {
    pub signature: Signature,
    pub commit_id: Option<u64>,
    pub payer: Option<Pubkey>,
    pub included: Vec<Pubkey>,
    pub excluded: Vec<Pubkey>,
    pub base_signatures: Vec<Signature>,
    pub requested_undelegation: bool,
    // set when the intent failed; the receipt tx then errs INTENT_FAILED_CODE
    pub error_message: Option<String>,
    pub receipt_err_code: Option<u32>,
}

/// Live scheduling and receipt observations armed before submitting a commit.
pub struct CommitWatch {
    scheduling: Subscription<Response<RpcLogsResponse>>,
    receipts: Subscription<Response<RpcLogsResponse>>,
    deadline: time::Instant,
}

impl CommitWatch {
    pub async fn new(er: &ErCtx, payer: &Pubkey) -> Result<Self> {
        let deadline = time::Instant::now() + NETWORK_TIMEOUT;
        let events = er.api().events().await?;
        let scheduling = events.logs(payer).await?;
        let receipts = events.logs(&er.identity()).await?;
        Ok(Self { scheduling, receipts, deadline })
    }

    pub async fn check(
        mut self,
        base: &BaseCtx,
        signature: &Signature,
        expected: &[Pubkey],
        undelegate: bool,
    ) -> Result<CommitReceipt> {
        time::timeout_at(self.deadline, async {
            let scheduling = self.scheduling.for_signature(signature).await?;
            ensure!(
                scheduling.err.is_none(),
                "scheduling failed: {:?}",
                scheduling.err
            );
            let receipt_signature = receipt_signature_in_logs(&scheduling.logs)
                .context("missing ScheduledCommitSent signature")?;
            let receipt = self.receipts.for_signature(&receipt_signature).await?;
            let receipt = parse_logs(
                receipt_signature,
                &receipt.logs,
                receipt.err.and_then(custom_error_code),
            );
            receipt.verify(base, expected, undelegate).await?;
            Ok(receipt)
        })
        .await
        .with_context(|| {
            format!("commit {signature}: scheduling or receipt notification missing")
        })?
    }
}

impl ErCtx {
    /// Checks the standard commit receipt and confirms settlement. Duplicate rejection
    /// is returned unchanged; callers must corroborate it against base state.
    pub async fn check_commit(
        &self,
        base: &BaseCtx,
        signature: &Signature,
        expected: &[Pubkey],
        expect_undelegation: bool,
    ) -> Result<CommitReceipt> {
        let receipt = fetch_commit_receipt(self.api(), signature).await?;
        receipt.verify(base, expected, expect_undelegation).await?;
        Ok(receipt)
    }
}

impl CommitReceipt {
    async fn verify(&self, base: &BaseCtx, expected: &[Pubkey], undelegate: bool) -> Result<()> {
        if let Some(message) = &self.error_message {
            if self.failure_is_duplicate_rejection() {
                warn_duplicate_rejection(message);
                return Ok(());
            }
            return Err(anyhow!("commit intent failed: {message}"));
        }
        self.check_accounts(expected, undelegate)?;
        self.confirm(base.api()).await
    }
    pub fn check_included(&self, expected: &[Pubkey]) -> Result<()> {
        let mut included = self.included.clone();
        included.sort();
        let mut expected = expected.to_vec();
        expected.sort();
        ensure!(included == expected);
        Ok(())
    }

    pub fn check_accounts(&self, expected: &[Pubkey], expect_undelegation: bool) -> Result<()> {
        self.check_included(expected)?;
        ensure!(self.excluded.as_slice() == []);
        ensure!(self.requested_undelegation == expect_undelegation);
        Ok(())
    }
    /// Requires success and base signatures, without confirming those transactions.
    pub fn check_success(&self, phase: &str) -> Result<()> {
        ensure!(
            self.succeeded(),
            "{phase}: settlement failed: {:?} (code {:?})",
            self.error_message,
            self.receipt_err_code
        );
        ensure!(
            !self.base_signatures.is_empty(),
            "{phase}: missing base transactions"
        );
        Ok(())
    }

    pub async fn confirm(&self, base: &Api) -> Result<()> {
        for chain_signature in &self.base_signatures {
            base.confirm(chain_signature).await.context("base settlement confirmation")?;
        }
        Ok(())
    }

    pub fn succeeded(&self) -> bool {
        self.error_message.is_none() && self.receipt_err_code.is_none()
    }

    // The >= 0.14.10 committor retries an unconfirmed base tx with a fresh
    // blockhash
    pub fn failure_is_duplicate_rejection(&self) -> bool {
        const DUPLICATE_REJECTIONS: [&str; 4] =
            ["InvalidAccountOwner", "Custom(11)", "Custom(12)", "invalid Commit id"];
        self.error_message.as_deref().is_some_and(|message| {
            DUPLICATE_REJECTIONS.iter().any(|needle| message.contains(needle))
        })
    }
}

pub fn warn_duplicate_rejection(error: &str) {
    warn!(error = %format_args!("{error:#}"), "intent rejected as duplicate; verifying base state");
}

// Builtin ic_msg lines arrive bare; tolerate a BPF-style prefix anyway.
fn strip_program_prefix(line: &str) -> &str {
    line.strip_prefix("Program log: ").unwrap_or(line)
}

// From the SCHEDULING tx's logs: the receipt signature announced at CPI time.
pub fn receipt_signature_in_logs(logs: &[String]) -> Option<Signature> {
    logs.iter().find_map(|line| {
        let rest = strip_program_prefix(line).strip_prefix("ScheduledCommitSent signature: ")?;
        rest.trim().parse().ok()
    })
}

pub fn intent_id_in_logs(logs: &[String]) -> Option<u64> {
    logs.iter().find_map(|line| {
        strip_program_prefix(line).strip_prefix(SCHEDULE_MARKER)?.trim().parse().ok()
    })
}

// `[pk, pk]`, joined with ", "
fn pubkey_list(list: &str) -> Vec<Pubkey> {
    list.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .filter_map(|token| token.trim().parse().ok())
        .collect()
}

pub fn parse_receipt(signature: Signature, receipt_tx: &UiTransactionStatusMeta) -> CommitReceipt {
    parse_logs(
        signature,
        transaction_logs(receipt_tx),
        receipt_tx.err.clone().and_then(custom_error_code),
    )
}

fn parse_logs(
    signature: Signature,
    logs: &[String],
    receipt_err_code: Option<u32>,
) -> CommitReceipt {
    let mut receipt = CommitReceipt {
        signature,
        commit_id: None,
        payer: None,
        included: Vec::new(),
        excluded: Vec::new(),
        base_signatures: Vec::new(),
        requested_undelegation: false,
        error_message: None,
        receipt_err_code,
    };
    for line in logs {
        let Some(rest) = strip_program_prefix(line).strip_prefix(LOG_MARKER) else {
            continue;
        };
        if let Some(id_part) = rest.strip_prefix("id: ") {
            receipt.commit_id =
                id_part.split(',').next().and_then(|token| token.trim().parse().ok());
        } else if let Some(payer) = rest.strip_prefix("payer: ") {
            receipt.payer = payer.trim().parse().ok();
        } else if let Some(list) = rest.strip_prefix("included: ") {
            receipt.included = pubkey_list(list);
        } else if let Some(list) = rest.strip_prefix("excluded: ") {
            receipt.excluded = pubkey_list(list);
        } else if let Some(indexed) = rest.strip_prefix("signature[") {
            let chain_signature =
                indexed.split_once("]: ").and_then(|(_, sig_text)| sig_text.trim().parse().ok());
            if let Some(chain_signature) = chain_signature {
                receipt.base_signatures.push(chain_signature);
            }
        } else if rest.trim() == "requested undelegation" {
            receipt.requested_undelegation = true;
        } else if let Some(message) = rest.strip_prefix("error message: ") {
            receipt.error_message = Some(message.to_owned());
        }
    }
    receipt
}

pub async fn scheduled_receipt_signature(
    er: &Api,
    commit_signature: &Signature,
) -> Result<Signature> {
    let commit_tx = er.await_transaction(commit_signature).await?;
    let meta = transaction_meta(&commit_tx)?;
    ensure!(meta.err == None);
    receipt_signature_in_logs(transaction_logs(meta))
        .ok_or_else(|| anyhow!("missing ScheduledCommitSent signature"))
}

pub async fn fetch_commit_receipt(er: &Api, commit_signature: &Signature) -> Result<CommitReceipt> {
    let receipt_signature = scheduled_receipt_signature(er, commit_signature).await?;
    let receipt_tx = er.await_transaction(&receipt_signature).await?;
    Ok(parse_receipt(
        receipt_signature,
        transaction_meta(&receipt_tx)?,
    ))
}

pub async fn scheduled_intent_id(er: &Api, commit_signature: &Signature) -> Result<u64> {
    let commit_tx = er.await_transaction(commit_signature).await?;
    intent_id_in_logs(transaction_logs(transaction_meta(&commit_tx)?))
        .ok_or_else(|| anyhow!("missing scheduled intent ID"))
}

pub async fn fetch_commit_receipt_by_intent(
    er: &Api,
    validator: &Pubkey,
    intent_id: u64,
) -> Result<CommitReceipt> {
    let mut last_fetch_error = None;
    let result = time::timeout(NETWORK_TIMEOUT, async {
        let events = er.events().await?;
        let mut logs = events.logs(validator).await?;
        let mut slots = events.slots().await?;
        let mut pending = er.get_signatures_for_address(validator, RECEIPT_HISTORY_LIMIT).await?;
        let mut seen: HashSet<_> = pending.iter().copied().collect();
        loop {
            for signature in mem::take(&mut pending) {
                match er.get_transaction(&signature).await {
                    Ok(Some(transaction)) => {
                        let receipt = parse_receipt(signature, transaction_meta(&transaction)?);
                        if receipt.commit_id == Some(intent_id) {
                            return Ok(receipt);
                        }
                    }
                    Ok(None) => pending.push(signature),
                    Err(error) => {
                        last_fetch_error = Some(error);
                        pending.push(signature);
                    }
                }
            }
            tokio::select! {
                update = logs.next() => {
                    let signature = update?.value.signature.parse()?;
                    if seen.insert(signature) { pending.push(signature); }
                }
                update = slots.next() => { update?; }
            }
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(timeout_error) => Err(last_fetch_error.unwrap_or_else(|| timeout_error.into()))
            .context("commit receipt not published"),
    }
}
