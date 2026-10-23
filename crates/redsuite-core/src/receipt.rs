use std::{collections::HashSet, time::Duration};

use pubkey::Pubkey;
use signature::Signature;
use tokio::{time, time::Instant};
use tracing::warn;

use crate::{
    api::{custom_error_code, Api, TransactionInfo},
    check,
    check::CheckError,
    check_eq, BaseCtx, ChainCtx, ErCtx, Result,
};

pub const INTENT_FAILED_CODE: u32 = 0x7461_636F;

pub const COMMIT_LIMIT_ERR: u32 = 0xA000_0000;

pub const INTENT_TOO_LARGE_ERR: u32 = 0xA000_0002;

pub const SPONSORED_COMMIT_LIMIT: u64 = 10;

const LOG_MARKER: &str = "ScheduledCommitSent ";

const SCHEDULE_MARKER: &str = "Scheduled commit with ID: ";

const RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);

const SCHEDULE_FETCH_TIMEOUT: Duration = Duration::from_secs(20);

const RECEIPT_HISTORY_LIMIT: usize = 64;

const RECEIPT_POLL: Duration = Duration::from_millis(500);

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
        let receipt = fetch_commit_receipt(self.api(), signature, RECEIPT_TIMEOUT).await?;
        if let Some(message) = &receipt.error_message {
            if receipt.failure_is_duplicate_rejection() {
                warn_duplicate_rejection(message);
                return Ok(receipt);
            }
            return Err(CheckError::new("the commit intent succeeds").actual(message).into());
        }
        let mut included = receipt.included.clone();
        included.sort();
        let mut expected = expected.to_vec();
        expected.sort();
        check_eq!(
            included,
            expected,
            "the receipt must list exactly the committed accounts"
        )?;
        check!(
            receipt.excluded.is_empty(),
            "the receipt must exclude no accounts"
        )?;
        check_eq!(
            receipt.requested_undelegation,
            expect_undelegation,
            "the receipt undelegation flag must match the commit type"
        )?;
        receipt.confirm(base.api(), BASE_CONFIRM_TIMEOUT).await?;
        Ok(receipt)
    }
}

impl CommitReceipt {
    /// Requires success and base signatures, without confirming those transactions.
    pub fn check_success(&self, phase: &str) -> Result<()> {
        check!(self.succeeded(), "{phase}: settlement failed: {self:?}")?;
        check!(
            !self.base_signatures.is_empty(),
            "{phase}: a settled receipt names its base transactions"
        )?;
        Ok(())
    }

    pub async fn confirm(&self, base: &Api, timeout: Duration) -> Result<()> {
        for chain_signature in &self.base_signatures {
            let base_tx =
                base.await_transaction(chain_signature, timeout).await.map_err(|error| {
                    CheckError::new(format!("base commit tx {chain_signature} lands on base"))
                        .caused_by(error)
                })?;
            if let Some(err) = &base_tx.err {
                return Err(CheckError::new(format!(
                    "base commit tx {chain_signature} succeeds on base"
                ))
                .actual(format!("failed on-chain: {err:?}"))
                .into());
            }
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
    warn!(%error, "intent rejected as duplicate; verifying base state");
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

pub fn parse_receipt(signature: Signature, receipt_tx: &TransactionInfo) -> CommitReceipt {
    let mut receipt = CommitReceipt {
        signature,
        commit_id: None,
        payer: None,
        included: Vec::new(),
        excluded: Vec::new(),
        base_signatures: Vec::new(),
        requested_undelegation: false,
        error_message: None,
        receipt_err_code: receipt_tx.err.as_ref().and_then(custom_error_code),
    };
    for line in &receipt_tx.logs {
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
    let commit_tx = er.await_transaction(commit_signature, SCHEDULE_FETCH_TIMEOUT).await?;
    if let Some(err) = &commit_tx.err {
        return Err(
            CheckError::new(format!("commit tx {commit_signature} succeeds on the ER"))
                .actual(format!("failed on-chain: {err:?}"))
                .into(),
        );
    }
    receipt_signature_in_logs(&commit_tx.logs).ok_or_else(|| {
        CheckError::new(format!(
            "commit tx {commit_signature} logs carry no \
             ScheduledCommitSent signature — was a commit actually \
             scheduled?"
        ))
        .into()
    })
}

pub async fn fetch_commit_receipt(
    er: &Api,
    commit_signature: &Signature,
    timeout: Duration,
) -> Result<CommitReceipt> {
    let receipt_signature = scheduled_receipt_signature(er, commit_signature).await?;
    let receipt_tx = er.await_transaction(&receipt_signature, timeout).await?;
    Ok(parse_receipt(receipt_signature, &receipt_tx))
}

pub async fn scheduled_intent_id(er: &Api, commit_signature: &Signature) -> Result<u64> {
    let commit_tx = er.await_transaction(commit_signature, SCHEDULE_FETCH_TIMEOUT).await?;
    intent_id_in_logs(&commit_tx.logs).ok_or_else(|| {
        CheckError::new(format!(
            "commit tx {commit_signature} logs carry no scheduled intent id"
        ))
        .into()
    })
}

pub async fn fetch_commit_receipt_by_intent(
    er: &Api,
    validator: &Pubkey,
    intent_id: u64,
    timeout: Duration,
) -> Result<CommitReceipt> {
    let deadline = Instant::now() + timeout;
    let mut seen = HashSet::new();
    let mut last_fetch_error = None;
    loop {
        let history = er.get_signatures_for_address(validator, RECEIPT_HISTORY_LIMIT).await?;
        for signature in history {
            if !seen.insert(signature) {
                continue;
            }
            let tx = match er.get_transaction(&signature).await {
                Ok(Some(tx)) => tx,
                Ok(None) => continue,
                Err(error) => {
                    last_fetch_error = Some(error.to_string());
                    continue;
                }
            };
            let receipt = parse_receipt(signature, &tx);
            if receipt.commit_id == Some(intent_id) {
                return Ok(receipt);
            }
        }
        if Instant::now() >= deadline {
            let mut message =
                format!("no commit receipt for intent {intent_id} within {timeout:?}");
            if let Some(error) = &last_fetch_error {
                message.push_str(&format!(
                    "; last getTransaction error while scanning receipts: \
                     {error}"
                ));
            }
            return Err(message.into());
        }
        time::sleep(RECEIPT_POLL).await;
    }
}
