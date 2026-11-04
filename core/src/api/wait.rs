use std::{ops::ControlFlow, rc::Rc, result};

use account::Account;
use anyhow::{anyhow, ensure, Context, Error};
use futures_util::{stream, StreamExt, TryStreamExt};
use pubkey::Pubkey;
use signature::Signature;
use solana_account_decoder_client_types::UiAccount;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client_types::response::{Response, RpcSignatureResult};
use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;
use tokio::time;
use transaction::Transaction;

use super::{decode_account, Api, ConfirmTimeout, SendError, TxError};
use crate::{
    transport::{
        events::{EventSubscriptions, Subscription},
        NETWORK_TIMEOUT,
    },
    wait, Result,
};

/// Acknowledged account subscriptions and their latest observed images, in request order.
pub struct AccountWatch {
    addresses: Vec<Pubkey>,
    subscriptions: Vec<Subscription<Response<UiAccount>>>,
    accounts: Vec<Option<Account>>,
    seen: Vec<bool>,
    deadline: time::Instant,
}

impl AccountWatch {
    /// Seed an existing-state observation without refetching on subsequent updates.
    pub fn seed(&mut self, accounts: Vec<Option<Account>>) -> Result<()> {
        ensure!(
            accounts.len() == self.addresses.len(),
            "account snapshot length mismatch"
        );
        self.accounts = accounts;
        self.seen.fill(true);
        Ok(())
    }

    pub async fn until<T>(
        &mut self,
        what: &str,
        ready: impl Fn(&[Option<Account>]) -> Result<T>,
    ) -> Result<T> {
        loop {
            let error = if self.seen.contains(&false) {
                anyhow!("account notifications missing")
            } else {
                match ready(&self.accounts) {
                    Ok(value) => return Ok(value),
                    Err(error) => error,
                }
            };
            let (index, update) = time::timeout_at(
                self.deadline,
                Subscription::next_from(&mut self.subscriptions),
            )
            .await
            .with_context(|| format!("{what}: {error:#}; accounts={:?}", self.addresses))??;
            let account = decode_account(update.value)?;
            // Account subscriptions report closure as the empty system account.
            self.accounts[index] = (account != Account::default()).then_some(account);
            self.seen[index] = true;
        }
    }
}

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
        self.await_accounts(&[*address], "account not ready", |accounts| {
            let account = accounts[0].as_ref().context("account missing")?;
            ready(account)?;
            Ok(account.clone())
        })
        .await
    }

    /// Subscribes before reading the initial snapshot so updates cannot fall into a setup gap.
    /// Read/decode errors are fatal; pure readiness-check failures wait for account notifications.
    pub async fn await_accounts<T>(
        &self,
        addresses: &[Pubkey],
        what: &str,
        ready: impl Fn(&[Option<Account>]) -> Result<T>,
    ) -> Result<T> {
        let mut watch = self.watch_accounts(addresses).await?;
        let accounts = time::timeout_at(watch.deadline, async {
            let mut accounts = Vec::with_capacity(addresses.len());
            for chunk in addresses.chunks(100) {
                accounts.extend(self.get_multiple_accounts(chunk).await?);
            }
            Result::Ok(accounts)
        })
        .await
        .with_context(|| format!("{what}: initial snapshot deadline elapsed"))??;
        watch.seed(accounts)?;
        watch.until(what, ready).await
    }

    pub async fn watch_accounts(&self, addresses: &[Pubkey]) -> Result<AccountWatch> {
        let events = self.events().await?;
        let deadline = time::Instant::now() + NETWORK_TIMEOUT;
        let subscriptions = stream::iter(addresses)
            .map(async |address| {
                time::timeout_at(deadline, events.account(address))
                    .await
                    .with_context(|| events.diagnostics())
                    .flatten()
                    .with_context(|| {
                        format!(
                            "accountSubscribe not acknowledged: address={address}, malformed={}",
                            events.malformed_frames()
                        )
                    })
            })
            .buffered(16)
            .try_collect()
            .await?;
        Ok(AccountWatch {
            addresses: addresses.to_vec(),
            subscriptions,
            accounts: vec![None; addresses.len()],
            seen: vec![false; addresses.len()],
            deadline,
        })
    }

    /// Probes immediately after subscription setup and again after each observed slot.
    /// `Err` fails immediately, `Break(value)` completes, and `Continue(reason)` waits
    /// for another slot while preserving the pending reason for timeout diagnostics.
    /// Subscription setup and all probes share one network deadline.
    pub async fn await_query<T>(
        &self,
        what: &str,
        observe: impl AsyncFnMut() -> Result<ControlFlow<T, Error>>,
    ) -> Result<T> {
        let deadline = time::Instant::now() + NETWORK_TIMEOUT;
        let mut slots = time::timeout_at(deadline, async {
            let events = self.events().await?;
            events.slots().await
        })
        .await
        .with_context(|| format!("{what}: slot subscription after {NETWORK_TIMEOUT:?}"))??;
        slots.query_until(deadline, what, observe).await
    }

    pub async fn await_slot(&self, slot: u64) -> Result<u64> {
        // Slot notifications are processed; preserve the confirmed HTTP observation.
        self.await_query(&format!("confirmed slot >= {slot}"), async || {
            let current = self.get_slot().await?;
            Ok(wait::when(current >= slot, current))
        })
        .await
    }

    pub async fn confirm(&self, signature: &Signature) -> Result<()> {
        self.try_confirm(signature).await?.map_err(Into::into)
    }

    /// Registers notifications before consulting HTTP status, covering already-executed signatures.
    /// Execution failure is an inner error; transport failure or unknown outcome is an outer error.
    pub async fn try_confirm(&self, signature: &Signature) -> Result<result::Result<(), TxError>> {
        time::timeout(NETWORK_TIMEOUT, async {
            let events = self.events().await?;
            let mut subscription =
                events.signature(signature, CommitmentConfig::confirmed()).await?;
            if let Some(Response {
                value: RpcSignatureResult::ProcessedSignature(result),
                ..
            }) = subscription.try_next()?
            {
                return Ok(result.err.map_or(Ok(()), |err| Err(TxError { err: err.into() })));
            }
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

    /// Ignores received-signature notifications; completes only on a processed signature result.
    async fn signature_outcome(
        subscription: &mut Subscription<Response<RpcSignatureResult>>,
    ) -> Result<result::Result<(), TxError>> {
        loop {
            let update = subscription.next().await?;
            if let RpcSignatureResult::ProcessedSignature(result) = update.value {
                return Ok(result.err.map_or(Ok(()), |err| Err(TxError { err: err.into() })));
            }
        }
    }

    pub async fn send_and_confirm(&self, transaction: &Transaction) -> Result<Signature> {
        self.try_send_and_confirm(transaction).await?.map_err(Into::into)
    }

    /// Separates RPC rejection and execution failure from transport and confirmation failures.
    pub async fn try_send_and_confirm(
        &self,
        transaction: &Transaction,
    ) -> Result<result::Result<Signature, SendError>> {
        let signature = transaction.signatures[0];
        time::timeout(NETWORK_TIMEOUT, async {
            let events = self.events().await?;
            let mut subscription =
                events.signature(&signature, CommitmentConfig::confirmed()).await?;
            if let Err(error) = self.try_send_transaction(transaction).await? {
                return Ok(Err(error.into()));
            }
            let outcome = Self::signature_outcome(&mut subscription).await?;
            Ok(outcome.map(|()| signature).map_err(Into::into))
        })
        .await
        .map_err(|_| ConfirmTimeout)?
    }

    pub async fn await_transaction(
        &self,
        signature: &Signature,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta> {
        self.await_query(&format!("transaction {signature} published"), async || {
            let transaction = self.get_transaction(signature).await?;
            Ok(wait::check(
                transaction.context("transaction not published"),
            ))
        })
        .await
    }
}
