use std::result;

use anyhow::{ensure, Context};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::future::join_all;
use hash::Hash;
use instruction::AccountMeta;
use keypair::Keypair;
use pubkey::Pubkey;
use signature::Signature;
use signer::Signer;
use solana_instruction_error::InstructionError;

use crate::{
    api::{transaction_meta, RpcError},
    programs::{CellHeader, REDSHIFT},
    rpc_types::{
        config::{RpcBlockConfig, RpcSendTransactionConfig, RpcTransactionConfig},
        request::RpcRequest,
        response::{Response, TransactionError},
    },
    transaction_types::{
        EncodedConfirmedTransactionWithStatusMeta, TransactionDetails, UiConfirmedBlock,
        UiTransactionEncoding, UiTransactionError,
    },
    transport::wsraw::RawWs,
    ChainCtx, ErCtx, Result,
};

pub struct Signed {
    pub encoded: String,
    pub signature: Signature,
    pub blockhash: Hash,
    pub fee: u64,
    pub error: Option<UiTransactionError>,
}

impl Signed {
    pub async fn from_versioned(
        er: &ErCtx,
        payer: &Keypair,
        message: message::VersionedMessage,
        error: Option<UiTransactionError>,
    ) -> Result<Self> {
        let transaction = transaction::versioned::VersionedTransaction::try_new(message, &[payer])?;
        let encoded_message = STANDARD.encode(wincode::serialize(&transaction.message)?);
        let quoted: result::Result<Response<Option<u64>>, RpcError> =
            er.api().try_call(RpcRequest::GetFeeForMessage, &(encoded_message,)).await?;
        let fee = match quoted {
            Ok(response) => response.value.context("missing fee quote")?,
            Err(error) => {
                ensure!(
                    matches!(transaction.message, message::VersionedMessage::V0(_))
                        && error.error.message.contains("lookup tables is disabled"),
                    "{error}"
                );
                0
            }
        };
        Ok(Self {
            encoded: STANDARD.encode(wincode::serialize(&transaction)?),
            signature: transaction.signatures[0],
            blockhash: *transaction.message.recent_blockhash(),
            fee,
            error,
        })
    }

    pub async fn new(
        er: &ErCtx,
        payer: &Keypair,
        owner: Pubkey,
        count: u8,
        fail: bool,
        cold: Option<Pubkey>,
    ) -> Result<Self> {
        let mut instructions =
            vec![REDSHIFT.batch().add(&[REDSHIFT.primary(owner)], i64::from(count)).build()];
        if let Some(cold) = cold {
            instructions[0].accounts.push(AccountMeta::new_readonly(cold, false));
        }
        if fail {
            instructions
                .push(REDSHIFT.batch().writable(&[REDSHIFT.primary(owner)]).fail(0).build());
        }
        let blockhash = er.api().get_latest_blockhash().await?;
        let message = transaction::Message::new_with_blockhash(
            &instructions,
            Some(&payer.pubkey()),
            &blockhash,
        );
        Self::from_versioned(
            er,
            payer,
            message::VersionedMessage::Legacy(message),
            fail.then(|| TransactionError::InstructionError(1, InstructionError::Custom(0)).into()),
        )
        .await
    }

    pub async fn send(&self, er: &ErCtx) -> Result<result::Result<String, RpcError>> {
        self.send_config(er, true).await
    }

    pub async fn send_config(
        &self,
        er: &ErCtx,
        skip_preflight: bool,
    ) -> Result<result::Result<String, RpcError>> {
        let config = RpcSendTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            skip_preflight,
            max_retries: Some(0),
            ..RpcSendTransactionConfig::default()
        };
        er.api().try_call(RpcRequest::SendTransaction, &(&self.encoded, config)).await
    }

    pub async fn valid(&self, er: &ErCtx) -> Result<bool> {
        er.api()
            .call_value(RpcRequest::IsBlockhashValid, &(self.blockhash.to_string(),))
            .await
    }

    pub async fn copies(&self, er: &ErCtx, count: usize) -> Result<()> {
        for result in join_all((0..count).map(|_| self.send(er))).await {
            if let Err(error) = result? {
                ensure!(error.error.code.code() == -32003);
            }
        }
        Ok(())
    }

    pub async fn outcome(&self, er: &ErCtx) -> Result<u64> {
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            max_supported_transaction_version: Some(1),
            ..RpcTransactionConfig::default()
        };
        let tx = er
            .api()
            .await_query("transaction published", async || {
                er.api()
                    .call::<Option<EncodedConfirmedTransactionWithStatusMeta>>(
                        RpcRequest::GetTransaction,
                        &(self.signature.to_string(), config),
                    )
                    .await?
                    .context("transaction missing")
            })
            .await?;
        let meta = transaction_meta(&tx)?;
        ensure!(meta.err == self.error);
        ensure!(meta.fee == self.fee);
        Ok(tx.slot)
    }
}

pub async fn subscribe(er: &ErCtx, tx: &Signed) -> Result<(RawWs, u64)> {
    let mut ws = RawWs::connect(er.ws_url()).await?;
    let id = ws.signature_subscribe(&tx.signature).await?;
    Ok((ws, id))
}

pub fn decode_state(accounts: &[Option<account::Account>]) -> Result<(u64, CellHeader)> {
    let payer = accounts[0].as_ref().context("payer missing")?;
    let cell = accounts[1].as_ref().context("cell missing")?;
    Ok((payer.lamports, CellHeader::try_decode(&cell.data)?))
}

pub async fn audit(er: &ErCtx, start: u64, expected: &[(&Signed, usize)]) -> Result<u64> {
    let signatures: Vec<_> = expected
        .iter()
        .map(|(transaction, count)| (&transaction.signature, *count))
        .collect();
    audit_signatures(er, start, &signatures).await
}

pub async fn audit_signatures(
    er: &ErCtx,
    start: u64,
    expected: &[(&Signature, usize)],
) -> Result<u64> {
    let end = er.api().get_slot().await? + 2;
    er.api().await_slot(end).await?;
    er.api()
        .await_query("block is published", async || {
            ensure!(er.api().get_block(end).await?.is_some());
            Ok(())
        })
        .await?;
    let blocks: Vec<u64> = er.api().call(RpcRequest::GetBlocks, &(start, end)).await?;
    let mut counts: Vec<_> = expected
        .iter()
        .map(|(signature, count)| (signature.to_string(), *count, 0))
        .collect();
    for slot in blocks {
        let config = RpcBlockConfig {
            transaction_details: Some(TransactionDetails::Signatures),
            rewards: Some(false),
            max_supported_transaction_version: Some(1),
            ..RpcBlockConfig::default()
        };
        let block: UiConfirmedBlock = er.api().call(RpcRequest::GetBlock, &(slot, config)).await?;
        let signatures = block.signatures.context("block signatures missing")?;
        for (signature, _, count) in &mut counts {
            *count +=
                signatures.iter().filter(|actual| actual.as_str() == signature.as_str()).count();
        }
    }
    for (signature, expected, count) in counts {
        ensure!(
            count == expected,
            "{signature}: expected {expected} inclusions, got {count}"
        );
    }
    Ok(end + 1)
}
