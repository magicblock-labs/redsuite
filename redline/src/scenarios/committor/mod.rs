use anyhow::anyhow;
pub mod commit_throughput_ceiling;
pub mod commit_width_envelope;

use std::{fmt::Display, time::Duration};

use redsuite_core::{
    receipt::{self, CommitReceipt},
    Api, Result,
};
use signature::Signature;

async fn settled_receipt(
    api: &Api,
    signature: &Signature,
    timeout: Duration,
    what: impl Display,
) -> Result<CommitReceipt> {
    let receipt = receipt::fetch_commit_receipt(api, signature, timeout).await?;
    match &receipt.error_message {
        Some(message) => Err(anyhow!("{what}: {message}")),
        None => Ok(receipt),
    }
}
