use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use account::Account;
use async_trait::async_trait;
use hash::Hash;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use signature::Signature;
use signer::Signer;
use transaction::Transaction;

use crate::{
    api::{self, Api, Metrics},
    profile::ExecutionConfig,
    resources::Resources,
    Result,
};

const AIRDROP_TIMEOUT: Duration = Duration::from_secs(60);
const AIRDROP_POLL: Duration = Duration::from_millis(200);
// Same 20s budget as test-integration's 40x500ms convention, but polled at
// the ER's block cadence so confirm latency reflects the chain, not the poll.
const CONFIRM_ATTEMPTS: u32 = 400;
const CONFIRM_POLL: Duration = Duration::from_millis(50);
// The ER produces 50ms blocks, so its blockhash window is much shorter than
// the base chain's.
const BASE_BLOCKHASH_TTL: Duration = Duration::from_secs(20);
const ER_BLOCKHASH_TTL: Duration = Duration::from_secs(2);

#[async_trait(?Send)]
pub trait ChainCtx {
    fn api(&self) -> &Api;
    async fn send(
        &self,
        payer: &Keypair,
        ixs: &[Instruction],
    ) -> Result<Signature>;
    async fn send_with(
        &self,
        payer: &Keypair,
        cosigners: &[&Keypair],
        ixs: &[Instruction],
    ) -> Result<Signature>;
    async fn account(&self, pk: &Pubkey) -> Result<Option<Account>>;
    async fn accounts(&self, pks: &[Pubkey]) -> Result<Vec<Option<Account>>> {
        self.api().get_multiple_accounts(pks).await
    }
    async fn airdrop(&self, pk: &Pubkey, lamports: u64) -> Result<()>;
}

pub(crate) struct BlockhashCache {
    ttl: Duration,
    cached: RefCell<Option<(Hash, Instant)>>,
}

impl BlockhashCache {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            cached: RefCell::new(None),
        }
    }

    async fn get(&self, api: &Api) -> Result<Hash> {
        if let Some((hash, at)) = *self.cached.borrow() {
            if at.elapsed() < self.ttl {
                return Ok(hash);
            }
        }
        let hash = api.get_latest_blockhash().await?;
        *self.cached.borrow_mut() = Some((hash, Instant::now()));
        Ok(hash)
    }
}

async fn send_and_confirm(
    api: &Api,
    blockhash: &BlockhashCache,
    payer: &Keypair,
    cosigners: &[&Keypair],
    ixs: &[Instruction],
) -> Result<Signature> {
    let hash = blockhash.get(api).await?;
    let mut signers = vec![payer];
    signers.extend_from_slice(cosigners);
    let tx = Transaction::new_signed_with_payer(
        ixs,
        Some(&payer.pubkey()),
        &signers,
        hash,
    );
    let sig = api.send_transaction(&tx).await?;
    for _ in 0..CONFIRM_ATTEMPTS {
        if api.signature_confirmed(&sig).await? {
            return Ok(sig);
        }
        tokio::time::sleep(CONFIRM_POLL).await;
    }
    Err(format!("transaction {sig} not confirmed within {CONFIRM_ATTEMPTS}x{CONFIRM_POLL:?}").into())
}

#[derive(Clone)]
pub struct TxSender {
    api: Api,
    blockhash: Rc<BlockhashCache>,
    payer: Rc<Keypair>,
}

impl TxSender {
    pub fn payer(&self) -> &Keypair {
        &self.payer
    }

    // Sign without delivering — the signature is known before the wire send,
    // so confirmation subscriptions can be registered race-free.
    pub async fn prepare(&self, ixs: &[Instruction]) -> Result<Transaction> {
        let hash = self.blockhash.get(&self.api).await?;
        Ok(Transaction::new_signed_with_payer(
            ixs,
            Some(&self.payer.pubkey()),
            &[&*self.payer],
            hash,
        ))
    }

    pub async fn deliver(&self, tx: &Transaction) -> Result<Signature> {
        self.api.send_transaction(tx).await
    }

    pub async fn send(&self, ixs: &[Instruction]) -> Result<Signature> {
        self.deliver(&self.prepare(ixs).await?).await
    }

    pub async fn send_fresh(&self, ixs: &[Instruction]) -> Result<Signature> {
        let hash = self.api.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            ixs,
            Some(&self.payer.pubkey()),
            &[&*self.payer],
            hash,
        );
        self.api.send_transaction(&tx).await
    }
}

pub struct ErClient {
    api: Api,
    blockhash: Rc<BlockhashCache>,
}

impl ErClient {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            api: Api::new(rpc_url),
            blockhash: Rc::new(BlockhashCache::new(ER_BLOCKHASH_TTL)),
        }
    }

    pub fn api(&self) -> &Api {
        &self.api
    }

    pub fn sender(&self, payer: Rc<Keypair>) -> TxSender {
        TxSender {
            api: self.api.clone(),
            blockhash: self.blockhash.clone(),
            payer,
        }
    }
}

pub struct BaseCtx {
    api: Api,
    ws_url: String,
    blockhash: Rc<BlockhashCache>,
    resources: Rc<Resources>,
    config: ExecutionConfig,
}

impl BaseCtx {
    pub(crate) fn new(
        rpc_url: String,
        ws_url: String,
        config: ExecutionConfig,
    ) -> Self {
        Self {
            api: Api::new(rpc_url),
            ws_url,
            blockhash: Rc::new(BlockhashCache::new(BASE_BLOCKHASH_TTL)),
            resources: Rc::new(Resources::default()),
            config,
        }
    }

    pub(crate) fn resources(&self) -> Rc<Resources> {
        self.resources.clone()
    }

    pub fn config(&self) -> ExecutionConfig {
        self.config
    }

    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }

    pub fn sender(&self, payer: Rc<Keypair>) -> TxSender {
        TxSender {
            api: self.api.clone(),
            blockhash: self.blockhash.clone(),
            payer,
        }
    }
}

pub struct ErCtx {
    api: Api,
    ws_url: String,
    metrics_url: String,
    identity: Pubkey,
    blockhash: Rc<BlockhashCache>,
}

impl ErCtx {
    pub(crate) fn new(
        rpc_url: String,
        ws_url: String,
        metrics_url: String,
        identity: Pubkey,
    ) -> Self {
        Self::new_with_timeout(rpc_url, ws_url, metrics_url, identity, None)
    }

    pub(crate) fn new_with_timeout(
        rpc_url: String,
        ws_url: String,
        metrics_url: String,
        identity: Pubkey,
        request_timeout: Option<Duration>,
    ) -> Self {
        let api = match request_timeout {
            Some(timeout) => Api::with_timeout(rpc_url, timeout),
            None => Api::new(rpc_url),
        };
        Self {
            api,
            ws_url,
            metrics_url,
            identity,
            blockhash: Rc::new(BlockhashCache::new(ER_BLOCKHASH_TTL)),
        }
    }

    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }

    pub fn sender(&self, payer: Rc<Keypair>) -> TxSender {
        TxSender {
            api: self.api.clone(),
            blockhash: self.blockhash.clone(),
            payer,
        }
    }

    pub fn identity(&self) -> Pubkey {
        self.identity
    }

    pub fn metrics_url(&self) -> &str {
        &self.metrics_url
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
        payer: &Keypair,
        ixs: &[Instruction],
    ) -> Result<Signature> {
        send_and_confirm(&self.api, &self.blockhash, payer, &[], ixs).await
    }
    async fn send_with(
        &self,
        payer: &Keypair,
        cosigners: &[&Keypair],
        ixs: &[Instruction],
    ) -> Result<Signature> {
        send_and_confirm(&self.api, &self.blockhash, payer, cosigners, ixs)
            .await
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
        payer: &Keypair,
        ixs: &[Instruction],
    ) -> Result<Signature> {
        send_and_confirm(&self.api, &self.blockhash, payer, &[], ixs).await
    }
    async fn send_with(
        &self,
        payer: &Keypair,
        cosigners: &[&Keypair],
        ixs: &[Instruction],
    ) -> Result<Signature> {
        send_and_confirm(&self.api, &self.blockhash, payer, cosigners, ixs)
            .await
    }
    async fn account(&self, pk: &Pubkey) -> Result<Option<Account>> {
        self.api.get_account(pk).await
    }
    async fn airdrop(&self, _pk: &Pubkey, _lamports: u64) -> Result<()> {
        Err("the ER has no faucet — airdrop on the base; clone-on-access pulls it in".into())
    }
}
