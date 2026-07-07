//! The black-box facade scenarios drive: one context per chain in the topology.

use std::time::Duration;

use account::Account;
use async_trait::async_trait;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use signature::Signature;

use crate::{
    api::{self, Api, Metrics},
    Result,
};

const AIRDROP_TIMEOUT: Duration = Duration::from_secs(20);
const AIRDROP_POLL: Duration = Duration::from_millis(200);

#[async_trait(?Send)]
pub trait ChainCtx {
    fn api(&self) -> &Api;
    async fn send(
        &self,
        payer: &Keypair,
        ixs: &[Instruction],
    ) -> Result<Signature>;
    async fn account(&self, pk: &Pubkey) -> Result<Option<Account>>;
    async fn airdrop(&self, pk: &Pubkey, lamports: u64) -> Result<()>;
}

pub struct BaseCtx {
    api: Api,
    ws_url: String,
}

impl BaseCtx {
    pub(crate) fn new(rpc_url: String, ws_url: String) -> Self {
        Self {
            api: Api::new(rpc_url),
            ws_url,
        }
    }

    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }
}

pub struct ErCtx {
    api: Api,
    ws_url: String,
    metrics_url: String,
    identity: Pubkey,
}

impl ErCtx {
    pub(crate) fn new(
        rpc_url: String,
        ws_url: String,
        metrics_url: String,
        identity: Pubkey,
    ) -> Self {
        Self {
            api: Api::new(rpc_url),
            ws_url,
            metrics_url,
            identity,
        }
    }

    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }

    pub fn identity(&self) -> Pubkey {
        self.identity
    }

    pub async fn scrape_metrics(&self) -> Result<Metrics> {
        api::scrape_metrics(&self.metrics_url).await
    }
}

async fn airdrop_and_confirm(
    api: &Api,
    pk: &Pubkey,
    lamports: u64,
) -> Result<()> {
    let before = api.get_balance(pk).await.unwrap_or(0);
    let deadline = tokio::time::Instant::now() + AIRDROP_TIMEOUT;
    let mut requested = false;
    loop {
        if !requested {
            // The faucet can lag the RPC service right after boot — retry
            // the request itself, not just the balance poll.
            requested = api.request_airdrop(pk, lamports).await.is_ok();
        }
        if requested
            && api.get_balance(pk).await.unwrap_or(0) >= before + lamports
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "airdrop of {lamports} lamports to {pk} not confirmed"
            )
            .into());
        }
        tokio::time::sleep(AIRDROP_POLL).await;
    }
}

#[async_trait(?Send)]
impl ChainCtx for BaseCtx {
    fn api(&self) -> &Api {
        &self.api
    }
    async fn send(
        &self,
        _payer: &Keypair,
        _ixs: &[Instruction],
    ) -> Result<Signature> {
        Err("ChainCtx::send lands with the redline transport port".into())
    }
    async fn account(&self, pk: &Pubkey) -> Result<Option<Account>> {
        self.api.get_account(pk).await
    }
    async fn airdrop(&self, pk: &Pubkey, lamports: u64) -> Result<()> {
        airdrop_and_confirm(&self.api, pk, lamports).await
    }
}

#[async_trait(?Send)]
impl ChainCtx for ErCtx {
    fn api(&self) -> &Api {
        &self.api
    }
    async fn send(
        &self,
        _payer: &Keypair,
        _ixs: &[Instruction],
    ) -> Result<Signature> {
        Err("ChainCtx::send lands with the redline transport port".into())
    }
    async fn account(&self, pk: &Pubkey) -> Result<Option<Account>> {
        self.api.get_account(pk).await
    }
    async fn airdrop(&self, _pk: &Pubkey, _lamports: u64) -> Result<()> {
        Err("the ER has no faucet — airdrop on the base; clone-on-access pulls it in".into())
    }
}
