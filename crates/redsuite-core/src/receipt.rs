use std::{collections::HashSet, time::Duration};

use pubkey::Pubkey;
use signature::Signature;

use crate::{
    api::{custom_error_code, Api, TransactionInfo},
    Result,
};

const MAGIC_PROGRAM: &str = "Magic11111111111111111111111111111111111111";

const SCHEDULE_ID_MARKER: &str = "Scheduled commit with ID: ";

const RECEIPT_SCAN_LIMIT: usize = 64;

const RECEIPT_POLL: Duration = Duration::from_millis(200);

pub const INTENT_FAILED_CODE: u32 = 0x7461_636F;

pub const COMMIT_LIMIT_ERR: u32 = 0xA000_0000;

pub const INTENT_TOO_LARGE_ERR: u32 = 0xA000_0002;

pub const SPONSORED_COMMIT_LIMIT: u64 = 10;

const LOG_MARKER: &str = "ScheduledCommitSent ";

const SCHEDULE_FETCH_TIMEOUT: Duration = Duration::from_secs(20);

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

impl CommitReceipt {
    pub fn succeeded(&self) -> bool {
        self.error_message.is_none() && self.receipt_err_code.is_none()
    }
}

// Builtin ic_msg lines arrive bare; tolerate a BPF-style prefix anyway.
fn strip_program_prefix(line: &str) -> &str {
    line.strip_prefix("Program log: ").unwrap_or(line)
}

// From the SCHEDULING tx's logs: the receipt signature announced at CPI time.
pub fn receipt_signature_in_logs(logs: &[String]) -> Option<Signature> {
    logs.iter().find_map(|line| {
        let rest = strip_program_prefix(line)
            .strip_prefix("ScheduledCommitSent signature: ")?;
        rest.trim().parse().ok()
    })
}

pub fn commit_id_in_logs(logs: &[String]) -> Option<u64> {
    logs.iter().find_map(|line| {
        let rest =
            strip_program_prefix(line).strip_prefix(SCHEDULE_ID_MARKER)?;
        rest.trim().parse().ok()
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

pub fn parse_receipt(
    signature: Signature,
    receipt_tx: &TransactionInfo,
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
        receipt_err_code: receipt_tx.err.as_ref().and_then(custom_error_code),
    };
    for line in &receipt_tx.logs {
        let Some(rest) = strip_program_prefix(line).strip_prefix(LOG_MARKER)
        else {
            continue;
        };
        if let Some(id_part) = rest.strip_prefix("id: ") {
            receipt.commit_id = id_part
                .split(',')
                .next()
                .and_then(|token| token.trim().parse().ok());
        } else if let Some(payer) = rest.strip_prefix("payer: ") {
            receipt.payer = payer.trim().parse().ok();
        } else if let Some(list) = rest.strip_prefix("included: ") {
            receipt.included = pubkey_list(list);
        } else if let Some(list) = rest.strip_prefix("excluded: ") {
            receipt.excluded = pubkey_list(list);
        } else if let Some(indexed) = rest.strip_prefix("signature[") {
            let chain_signature = indexed
                .split_once("]: ")
                .and_then(|(_, sig_text)| sig_text.trim().parse().ok());
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

pub async fn fetch_commit_receipt(
    er: &Api,
    commit_signature: &Signature,
    timeout: Duration,
) -> Result<CommitReceipt> {
    let commit_tx = er
        .await_transaction(commit_signature, SCHEDULE_FETCH_TIMEOUT)
        .await?;
    if let Some(err) = &commit_tx.err {
        return Err(format!(
            "commit tx {commit_signature} failed on-chain: {err:?}"
        )
        .into());
    }
    if let Some(receipt_signature) = receipt_signature_in_logs(&commit_tx.logs)
    {
        let receipt_tx =
            er.await_transaction(&receipt_signature, timeout).await?;
        return Ok(parse_receipt(receipt_signature, &receipt_tx));
    }
    let commit_id = commit_id_in_logs(&commit_tx.logs).ok_or_else(|| {
        format!(
            "commit tx {commit_signature} logs carry neither a \
                 ScheduledCommitSent signature nor a scheduled commit id — \
                 was a commit actually scheduled?"
        )
    })?;
    await_receipt_by_id(er, commit_id, timeout).await
}

// One pass over recent Magic-program transactions; `inspected` skips
// signatures already fetched by an earlier pass.
pub async fn find_receipt_by_id(
    er: &Api,
    commit_id: u64,
    inspected: &mut HashSet<String>,
) -> Result<Option<CommitReceipt>> {
    let magic_program: Pubkey = MAGIC_PROGRAM.parse()?;
    let candidates = er
        .get_signatures_for_address(&magic_program, RECEIPT_SCAN_LIMIT)
        .await?;
    for signature_text in candidates {
        if !inspected.insert(signature_text.clone()) {
            continue;
        }
        let Ok(signature) = signature_text.parse::<Signature>() else {
            continue;
        };
        let Some(tx) = er.get_transaction(&signature).await? else {
            continue;
        };
        let receipt = parse_receipt(signature, &tx);
        if receipt.commit_id == Some(commit_id) {
            return Ok(Some(receipt));
        }
    }
    Ok(None)
}

async fn await_receipt_by_id(
    er: &Api,
    commit_id: u64,
    timeout: Duration,
) -> Result<CommitReceipt> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut inspected = HashSet::new();
    loop {
        if let Some(receipt) =
            find_receipt_by_id(er, commit_id, &mut inspected).await?
        {
            return Ok(receipt);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "no ScheduledCommitSent receipt for commit id {commit_id} \
                 within {timeout:?}"
            )
            .into());
        }
        tokio::time::sleep(RECEIPT_POLL).await;
    }
}

pub async fn confirm_base_signatures(
    base: &Api,
    receipt: &CommitReceipt,
    timeout: Duration,
) -> Result<()> {
    for chain_signature in &receipt.base_signatures {
        let base_tx = base
            .await_transaction(chain_signature, timeout)
            .await
            .map_err(|e| format!("base commit tx {chain_signature}: {e}"))?;
        if let Some(err) = &base_tx.err {
            return Err(format!(
                "base commit tx {chain_signature} failed on-chain: {err:?}"
            )
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::from([byte; 32])
    }

    fn sig(byte: u8) -> Signature {
        Signature::from([byte; 64])
    }

    #[test]
    fn schedule_log_names_the_receipt() {
        let logs = vec![
            "Program 3JnJ727jWEmPVU8qfXwtH63sCNDX7nMgsLbg8qy8aaPX invoke [1]"
                .to_owned(),
            format!("ScheduledCommitSent signature: {}", sig(9)),
            // the indexed form must NOT match the schedule-time extractor
            format!("ScheduledCommitSent signature[0]: {}", sig(1)),
        ];
        assert_eq!(receipt_signature_in_logs(&logs), Some(sig(9)));
        assert_eq!(receipt_signature_in_logs(&[]), None);
    }

    #[test]
    fn schedule_log_names_the_intent_id() {
        let logs = vec![
            "Program log: Scheduled commit with ID: 42".to_owned(),
            "Scheduled commit with ID: 7".to_owned(),
        ];
        assert_eq!(commit_id_in_logs(&logs), Some(42));
        assert_eq!(commit_id_in_logs(&[]), None);
    }

    #[test]
    fn receipt_parses_the_v0_13_line_formats() {
        let tx = TransactionInfo {
            slot: 42,
            block_time: None,
            err: None,
            logs: vec![
                format!(
                    "ScheduledCommitSent id: 7, slot: 1337, blockhash: {}",
                    pk(3)
                ),
                format!("ScheduledCommitSent payer: {}", pk(1)),
                format!("ScheduledCommitSent included: [{}, {}]", pk(4), pk(5)),
                "ScheduledCommitSent excluded: []".to_owned(),
                format!("ScheduledCommitSent signature[0]: {}", sig(6)),
                format!("ScheduledCommitSent signature[1]: {}", sig(7)),
            ],
        };
        let receipt = parse_receipt(sig(9), &tx);
        assert_eq!(receipt.signature, sig(9));
        assert_eq!(receipt.commit_id, Some(7));
        assert_eq!(receipt.payer, Some(pk(1)));
        assert_eq!(receipt.included, vec![pk(4), pk(5)]);
        assert!(receipt.excluded.is_empty());
        assert_eq!(receipt.base_signatures, vec![sig(6), sig(7)]);
        assert!(!receipt.requested_undelegation);
        assert!(receipt.succeeded());
    }

    #[test]
    fn failed_intent_receipt_carries_the_error() {
        let err: json::Value = json::from_str(&format!(
            r#"{{"InstructionError":[0,{{"Custom":{INTENT_FAILED_CODE}}}]}}"#,
        ))
        .unwrap();
        let tx = TransactionInfo {
            slot: 43,
            block_time: None,
            err: Some(err),
            logs: vec![
                format!("ScheduledCommitSent payer: {}", pk(1)),
                format!("ScheduledCommitSent included: [{}]", pk(4)),
                "ScheduledCommitSent requested undelegation".to_owned(),
                "ScheduledCommitSent error message: FailedToFitError: \
                 too many accounts"
                    .to_owned(),
            ],
        };
        let receipt = parse_receipt(sig(9), &tx);
        assert!(!receipt.succeeded());
        assert!(receipt
            .error_message
            .as_deref()
            .unwrap()
            .starts_with("FailedToFitError"));
        assert!(receipt.requested_undelegation);
        assert_eq!(receipt.receipt_err_code, Some(INTENT_FAILED_CODE));
        assert!(receipt.base_signatures.is_empty());
    }

    #[test]
    fn bpf_style_prefixes_are_tolerated() {
        let tx = TransactionInfo {
            slot: 44,
            block_time: None,
            err: None,
            logs: vec![format!(
                "Program log: ScheduledCommitSent included: [{}]",
                pk(2)
            )],
        };
        assert_eq!(parse_receipt(sig(1), &tx).included, vec![pk(2)]);
    }
}
