use std::{future::Future, rc::Rc, sync::Arc};

use anyhow::{ensure, Context};
use futures_util::future::join_all;
use keypair::Keypair;
use pubkey::Pubkey;
use signer::Signer;
use solana_rpc_client_types::request::RpcRequest;
use url::Url;

use crate::{
    context::{BaseCtx, ChainCtx, ErCtx},
    dlp, system,
    transport::NETWORK_TIMEOUT,
    wait, ErClient, Result, TxSender,
};

pub const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;

const PREP_CONCURRENCY: usize = 32;

pub(crate) async fn bounded<T, F, Fut>(count: usize, what: &str, make: F) -> Result<Vec<T>>
where
    F: Fn(usize) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    use futures_util::{StreamExt, TryFutureExt, TryStreamExt};
    futures_util::stream::iter(0..count)
        .map(|index| {
            make(index)
                .map_err(move |error| error.context(format!("preparing {what} {index} of {count}")))
        })
        .buffered(PREP_CONCURRENCY)
        .try_collect()
        .await
}

pub fn payer_from_bytes(bytes: &[u8]) -> Keypair {
    Keypair::try_from(bytes).expect("payer bytes round-trip")
}

// Each worker owns its client/cache and the same round-robin payer partition.
pub fn worker_senders(
    rpc_url: &Url,
    ws_url: &Url,
    payers: &[[u8; 64]],
    worker: usize,
    threads: usize,
) -> Vec<TxSender> {
    let client = ErClient::new(rpc_url.clone(), ws_url.clone());
    payers
        .iter()
        .skip(worker)
        .step_by(threads)
        .map(|bytes| client.sender(Rc::new(payer_from_bytes(bytes))))
        .collect()
}

pub fn payer_bytes(payers: &[Keypair]) -> Arc<Vec<[u8; 64]>> {
    Arc::new(payers.iter().map(Keypair::to_bytes).collect())
}

impl BaseCtx {
    pub async fn funded_payer(&self, lamports: u64) -> Result<Keypair> {
        let payer = Keypair::new();
        self.airdrop(&payer.pubkey(), lamports).await.context("funding payer")?;
        Ok(payer)
    }

    pub async fn funded_payers(&self, count: usize, lamports: u64) -> Result<Vec<Keypair>> {
        bounded(count, "payer", |_| self.funded_payer(lamports)).await
    }

    pub async fn delegated_payer(
        &self,
        funder: &Keypair,
        validator: Pubkey,
        lamports: u64,
    ) -> Result<Keypair> {
        let delegatee = self.funded_payer(lamports).await?;
        self.delegate_payer(funder, &delegatee, validator).await?;
        Ok(delegatee)
    }

    pub async fn delegate_payer(
        &self,
        funder: &Keypair,
        delegatee: &Keypair,
        validator: Pubkey,
    ) -> Result<()> {
        let delegatee_pubkey = delegatee.pubkey();
        let delegate_setup = [
            system::assign(&delegatee_pubkey, &dlp::dlp_id()),
            dlp::delegate_account(&funder.pubkey(), &delegatee_pubkey, &validator),
        ];
        self.submit_and_confirm_with(funder, &[delegatee], &delegate_setup).await?;
        let on_chain = self
            .account(&delegatee_pubkey)
            .await?
            .context("delegated payer account missing after delegation")?;
        ensure!(on_chain.owner == dlp::dlp_id());
        Ok(())
    }
}

impl ErCtx {
    pub async fn await_delegated(&self, account: &Pubkey, what: &str) -> Result<()> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Status {
            is_delegated: bool,
        }
        wait::retry(what, NETWORK_TIMEOUT, async || {
            let status: Status = self
                .api()
                .call(
                    RpcRequest::Custom { method: "getDelegationStatus" },
                    &(account.to_string(),),
                )
                .await?;
            ensure!(status.is_delegated);
            Ok(())
        })
        .await
    }

    pub async fn await_clones(&self, pdas: &[Pubkey], space: usize) -> Result<()> {
        for pda in pdas {
            self.await_account(pda, |account| {
                ensure!(account.data.len() == space);
                Ok(())
            })
            .await?;
        }
        Ok(())
    }

    pub async fn touch(&self, accounts: &[Pubkey]) {
        for window in accounts.chunks(16) {
            join_all(window.iter().map(|account| self.account(account))).await;
        }
    }

    pub async fn prewarm(&self, accounts: &[Pubkey], space: usize) -> Result<()> {
        self.touch(accounts).await;
        self.await_clones(accounts, space).await
    }

    pub async fn await_cloned_payers(&self, payers: &[Pubkey]) -> Result<()> {
        bounded(payers.len(), "cloned payer", |index| {
            let payer = payers[index];
            async move {
                self.await_account(&payer, |account| {
                    ensure!(account.lamports > 0);
                    Ok(())
                })
                .await?;
                Ok(())
            }
        })
        .await?;
        Ok(())
    }

    pub async fn await_program_clone(&self, program: &Pubkey) -> Result<()> {
        self.await_account(program, |account| {
            ensure!(account.executable);
            Ok(())
        })
        .await?;
        Ok(())
    }
}
