use std::{rc::Rc, result};

use account::Account;
use anyhow::{anyhow, ensure, Context};
use pubkey::Pubkey;
use signature::Signature;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client_types::response::{Response, RpcSignatureResult};
use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;
use tokio::time;
use transaction::Transaction;

use super::{decode_account, Api, ConfirmTimeout, TxError};
use crate::{
    transport::{
        events::{EventSubscriptions, Subscription},
        NETWORK_TIMEOUT,
    },
    Result,
};

impl Api {
    pub async fn events(&self) -> Result<Rc<EventSubscriptions>> {
        let mut events = self.events.lock().await;
        if let Some(events) = events.as_ref().filter(|events| !events.is_closed()) {
            return Ok(events.clone());
        }
        let connection = Rc::new(EventSubscriptions::connect(&self.ws_url).await?);
        *events = Some(connection.clone());
        Ok(connection)
    }

    pub async fn await_account(
        &self,
        address: &Pubkey,
        ready: impl Fn(&Account) -> Result<()>,
    ) -> Result<Account> {
        time::timeout(NETWORK_TIMEOUT, async {
            let events = self.events().await?;
            let mut subscription = events.account(address).await?;
            let mut account = self.get_account(address).await?;
            loop {
                let error = match account.take() {
                    Some(account) => match ready(&account) {
                        Ok(()) => return Ok(account),
                        Err(error) => error,
                    },
                    None => anyhow!("account missing"),
                };
                let update = subscription.next().await.map_err(|cause| cause.context(error))?;
                account = Some(decode_account(update.value)?);
            }
        })
        .await
        .context("account not ready")?
    }

    pub async fn await_accounts<T>(
        &self,
        addresses: &[Pubkey],
        what: &str,
        observe: impl AsyncFnMut() -> Result<T>,
    ) -> Result<T> {
        time::timeout(NETWORK_TIMEOUT, async {
            let events = self.events().await?;
            let mut subscriptions = Vec::with_capacity(addresses.len());
            for address in addresses {
                subscriptions.push(events.account(address).await?);
            }
            Subscription::query_many(&mut subscriptions, what, observe).await
        })
        .await
        .with_context(|| format!("{what} after {NETWORK_TIMEOUT:?}"))?
    }

    pub async fn await_query<T>(
        &self,
        what: &str,
        observe: impl AsyncFnMut() -> Result<T>,
    ) -> Result<T> {
        time::timeout(NETWORK_TIMEOUT, async {
            let events = self.events().await?;
            events.slots().await?.query(what, observe).await
        })
        .await
        .with_context(|| format!("{what} after {NETWORK_TIMEOUT:?}"))?
    }

    pub async fn await_program_accounts<T>(
        &self,
        program: &Pubkey,
        ready: impl Fn(Vec<(Pubkey, Account)>) -> Result<T>,
    ) -> Result<T> {
        time::timeout(NETWORK_TIMEOUT, async {
            let events = self.events().await?;
            let mut subscription = events.program(program).await?;
            subscription
                .query("program accounts", async || {
                    ready(self.get_program_accounts(program).await?)
                })
                .await
        })
        .await
        .context("program accounts not ready")?
    }

    pub async fn await_slot(&self, slot: u64) -> Result<u64> {
        // Slot notifications are processed; preserve the confirmed HTTP observation.
        self.await_query("confirmed slot", async || {
            let current = self.get_slot().await?;
            ensure!(current >= slot);
            Ok(current)
        })
        .await
    }

    pub async fn confirm(&self, signature: &Signature) -> Result<()> {
        self.try_confirm(signature).await?.map_err(Into::into)
    }

    pub async fn try_confirm(&self, signature: &Signature) -> Result<result::Result<(), TxError>> {
        time::timeout(NETWORK_TIMEOUT, async {
            let events = self.events().await?;
            let mut subscription =
                events.signature(signature, CommitmentConfig::confirmed()).await?;
            if let Some(status) = self.get_signature_status(signature).await? {
                if let Some(err) = status.err {
                    return Ok(Err(TxError { err }));
                }
                if status.satisfies_commitment(CommitmentConfig::confirmed()) {
                    return Ok(Ok(()));
                }
            }
            Self::signature_outcome(&mut subscription).await
        })
        .await
        .map_err(|_| ConfirmTimeout)?
    }

    async fn signature_outcome(
        subscription: &mut Subscription<Response<RpcSignatureResult>>,
    ) -> Result<result::Result<(), TxError>> {
        loop {
            let update = subscription.next().await?;
            if let RpcSignatureResult::ProcessedSignature(result) = update.value {
                let outcome = match result.err {
                    None => Ok(()),
                    Some(err) => Err(TxError { err: err.into() }),
                };
                return Ok(outcome);
            }
        }
    }

    pub async fn send_and_confirm(&self, transaction: &Transaction) -> Result<Signature> {
        let signature = transaction.signatures[0];
        time::timeout(NETWORK_TIMEOUT, async {
            let events = self.events().await?;
            let mut subscription =
                events.signature(&signature, CommitmentConfig::confirmed()).await?;
            self.send_transaction(transaction).await?;
            Self::signature_outcome(&mut subscription).await??;
            Ok(signature)
        })
        .await
        .map_err(|_| ConfirmTimeout)?
    }

    pub async fn await_transaction(
        &self,
        signature: &Signature,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta> {
        time::timeout(NETWORK_TIMEOUT, async {
            let events = self.events().await?;
            let commitment = CommitmentConfig::confirmed();
            let mut confirmation = events.signature(signature, commitment).await?;
            let mut slots = events.slots().await?;
            if let Some(transaction) = self.get_transaction(signature).await? {
                return Ok(transaction);
            }
            let status = self.get_signature_status(signature).await?;
            if !status.is_some_and(|status| {
                status.err.is_some() || status.satisfies_commitment(commitment)
            }) {
                let _ = Self::signature_outcome(&mut confirmation).await?;
            }
            loop {
                if let Some(transaction) = self.get_transaction(signature).await? {
                    return Ok(transaction);
                }
                slots.next().await?;
            }
        })
        .await
        .context("transaction not published")?
    }
}
