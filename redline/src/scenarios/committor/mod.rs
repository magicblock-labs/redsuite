use anyhow::ensure;
pub mod commit_throughput_ceiling;
pub mod commit_width_envelope;

use redsuite_core::{
    receipt::{self, CommitReceipt},
    Api, Result,
};
use signature::Signature;

async fn settled_receipt(api: &Api, signature: &Signature) -> Result<CommitReceipt> {
    let receipt = receipt::fetch_commit_receipt(api, signature).await?;
    ensure!(receipt.error_message == None);
    Ok(receipt)
}
