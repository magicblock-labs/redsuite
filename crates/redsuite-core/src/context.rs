use std::{
    cell::RefCell,
    collections::HashMap,
    future::Future,
    rc::{Rc, Weak},
    time::{Duration, Instant},
};

use account::Account;
use anyhow::{ensure, Context};
use hash::Hash;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use signature::Signature;
use signer::Signer;
use tokio::{sync::Mutex, time};
use transaction::Transaction;
use url::Url;

use crate::{
    api::{Api, Metrics, MetricsCollector},
    cells,
    netfault::BaseProxies,
    profile::ExecutionConfig,
    report::{self, ScenarioReport},
    resources::Resources,
    runner::{Delivery, SendTransaction},
    topology::{PrivateEr, RestartConfig, RestartOutcome},
    transport::NETWORK_TIMEOUT,
    wait, Result,
};

// The ER produces 50ms blocks, so its blockhash window is much shorter than
// the base chain's.
const BASE_BLOCKHASH_TTL: Duration = Duration::from_secs(20);
const ER_BLOCKHASH_TTL: Duration = Duration::from_secs(2);

pub trait ChainCtx: Sized {
    fn api(&self) -> &Api;
    fn cells<'a>(
        &'a self,
        program: fixture::Program,
        payer: &'a Keypair,
    ) -> cells::Program<'a, Self> {
        cells::Program { chain: self, payer, program }
    }
    fn submit_and_confirm(
        &self,
        payer: &Keypair,
        ixs: &[Instruction],
    ) -> impl Future<Output = Result<Signature>> {
        self.submit_and_confirm_with(payer, &[], ixs)
    }
    fn submit_and_confirm_with(
        &self,
        payer: &Keypair,
        cosigners: &[&Keypair],
        ixs: &[Instruction],
    ) -> impl Future<Output = Result<Signature>>;
    fn await_account(
        &self,
        address: &Pubkey,
        ready: impl Fn(&Account) -> Result<()>,
    ) -> impl Future<Output = Result<Account>> {
        self.api().await_account(address, ready)
    }
    fn account(&self, pk: &Pubkey) -> impl Future<Output = Result<Option<Account>>> {
        self.api().get_account(pk)
    }
    fn accounts(&self, pks: &[Pubkey]) -> impl Future<Output = Result<Vec<Option<Account>>>> {
        self.api().get_multiple_accounts(pks)
    }
}

pub(crate) struct BlockhashCache {
    ttl: Duration,
    cached: RefCell<Option<(Hash, Instant)>>,
}

impl BlockhashCache {
    fn new(ttl: Duration) -> Self {
        Self { ttl, cached: RefCell::new(None) }
    }

    fn reset(&self) {
        self.cached.borrow_mut().take();
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

async fn submit_and_confirm(
    api: &Api,
    blockhash: &BlockhashCache,
    payer: &Keypair,
    cosigners: &[&Keypair],
    ixs: &[Instruction],
) -> Result<Signature> {
    let hash = blockhash.get(api).await?;
    let mut signers = vec![payer];
    signers.extend_from_slice(cosigners);
    let tx = Transaction::new_signed_with_payer(ixs, Some(&payer.pubkey()), &signers, hash);
    api.send_and_confirm(&tx).await
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

    pub async fn submit_prepared(&self, tx: &Transaction) -> Result<Signature> {
        self.api.send_transaction(tx).await
    }

    pub fn deliver(&self, tx: Transaction) -> impl Delivery {
        let api = self.api.clone();
        SendTransaction(
            move |_: Instant| async move { api.send_transaction(&tx).await.map(|_| ()) },
        )
    }

    pub async fn staged(self, ixs: impl AsRef<[Instruction]>) -> Result<impl Delivery> {
        Ok(self.deliver(self.prepare(ixs.as_ref()).await?))
    }

    pub async fn submit(&self, ixs: &[Instruction]) -> Result<Signature> {
        self.submit_prepared(&self.prepare(ixs).await?).await
    }

    pub async fn submit_fresh(&self, ixs: &[Instruction]) -> Result<Signature> {
        let hash = self.api.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            ixs,
            Some(&self.payer.pubkey()),
            &[&*self.payer],
            hash,
        );
        self.api.send_transaction(&tx).await
    }

    pub async fn submit_and_confirm(&self, ixs: &[Instruction]) -> Result<Signature> {
        let transaction = self.prepare(ixs).await?;
        self.api.send_and_confirm(&transaction).await
    }
}

pub struct ErClient {
    api: Api,
    blockhash: Rc<BlockhashCache>,
}

impl ErClient {
    pub fn new(rpc_url: Url, ws_url: Url) -> Self {
        Self {
            api: Api::new(rpc_url, ws_url),
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
    pub(crate) scenario: Option<String>,
    api: Api,
    grpc: Option<(Url, String)>,
    blockhash: Rc<BlockhashCache>,
    resources: Rc<Resources>,
    config: ExecutionConfig,
}

impl BaseCtx {
    pub async fn airdrop(&self, pk: &Pubkey, lamports: u64) -> Result<()> {
        airdrop_and_confirm(&self.api, pk, lamports).await
    }
    pub(crate) fn new(
        rpc_url: Url,
        ws_url: Url,
        grpc: Option<(Url, String)>,
        config: ExecutionConfig,
    ) -> Self {
        Self {
            scenario: None,
            api: Api::new(rpc_url, ws_url),
            grpc,
            blockhash: Rc::new(BlockhashCache::new(BASE_BLOCKHASH_TTL)),
            resources: Rc::new(Resources::default()),
            config,
        }
    }

    pub(crate) fn resources(&self) -> Rc<Resources> {
        self.resources.clone()
    }

    /// Persists a benchmark cell under this run without making it fatal on I/O failure.
    pub fn report_cell(&self, label: &str, report: &ScenarioReport) -> Result<()> {
        let name = self.scenario.as_deref().context("cell report outside a scenario run")?;
        report::persist_cell(name, label, report);
        Ok(())
    }

    pub fn reclaim(&self, owner: &str) {
        self.resources.reclaim(owner);
    }

    pub fn grpc(&self) -> Option<(&Url, &str)> {
        self.grpc.as_ref().map(|(url, plugin)| (url, plugin.as_str()))
    }

    pub fn config(&self) -> ExecutionConfig {
        self.config
    }

    pub fn ws_url(&self) -> &Url {
        self.api.ws_url()
    }

    pub fn sender(&self, payer: Rc<Keypair>) -> TxSender {
        TxSender {
            api: self.api.clone(),
            blockhash: self.blockhash.clone(),
            payer,
        }
    }
}

#[derive(Clone)]
pub struct ErCtx {
    api: Api,
    metrics: MetricsCollector,
    identity: Pubkey,
    blockhash: Rc<BlockhashCache>,
    pub(crate) pid: u32,
    pub(crate) proxies: Option<Rc<BaseProxies>>,
    pub(crate) owner: Option<Weak<Mutex<PrivateEr>>>,
}

impl ErCtx {
    /// Read only resident accounts; unlike account reads, this never triggers cloning.
    pub async fn local_accounts(
        &self,
        owners: &[Pubkey],
        keys: &[Pubkey],
    ) -> Result<Vec<Option<Account>>> {
        let mut accounts = HashMap::new();
        for owner in owners {
            accounts.extend(self.api.get_program_accounts(owner).await?);
        }
        Ok(keys.iter().map(|key| accounts.get(key).cloned()).collect())
    }

    pub(crate) fn new(
        rpc_url: Url,
        ws_url: Url,
        metrics_url: Url,
        identity: Pubkey,
        pid: u32,
    ) -> Self {
        Self {
            api: Api::new(rpc_url, ws_url),
            metrics: MetricsCollector::new(metrics_url),
            identity,
            blockhash: Rc::new(BlockhashCache::new(ER_BLOCKHASH_TTL)),
            pid,
            proxies: None,
            owner: None,
        }
    }

    pub fn proxies(&self) -> Option<&BaseProxies> {
        self.proxies.as_deref()
    }

    pub async fn restart(&self, config: RestartConfig) -> Result<RestartOutcome> {
        self.owner
            .as_ref()
            .and_then(Weak::upgrade)
            .context("only a group ER is restartable through its context")?
            .lock()
            .await
            .restart(config)
            .await
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn ws_url(&self) -> &Url {
        self.api.ws_url()
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

    pub(crate) fn reset_blockhash(&self) {
        self.blockhash.reset();
    }

    pub fn metrics(&self) -> &MetricsCollector {
        &self.metrics
    }

    pub async fn scrape_metrics(&self) -> Result<Metrics> {
        self.metrics.scrape().await
    }
}

async fn airdrop_and_confirm(api: &Api, pk: &Pubkey, lamports: u64) -> Result<()> {
    time::timeout(NETWORK_TIMEOUT, async {
        let before = api.get_balance(pk).await.unwrap_or(0);
        let signature = wait::retry("airdrop accepted", NETWORK_TIMEOUT, async || {
            api.request_airdrop(pk, lamports).await
        })
        .await?;
        let signature = signature.parse()?;
        api.confirm(&signature).await?;
        api.await_account(pk, |account| {
            ensure!(account.lamports >= before + lamports);
            Ok(())
        })
        .await?;
        Ok(())
    })
    .await
    .context("airdrop not confirmed")?
}

impl ChainCtx for BaseCtx {
    fn api(&self) -> &Api {
        &self.api
    }
    fn submit_and_confirm_with(
        &self,
        payer: &Keypair,
        cosigners: &[&Keypair],
        ixs: &[Instruction],
    ) -> impl Future<Output = Result<Signature>> {
        submit_and_confirm(&self.api, &self.blockhash, payer, cosigners, ixs)
    }
}

impl ChainCtx for ErCtx {
    fn api(&self) -> &Api {
        &self.api
    }
    fn submit_and_confirm_with(
        &self,
        payer: &Keypair,
        cosigners: &[&Keypair],
        ixs: &[Instruction],
    ) -> impl Future<Output = Result<Signature>> {
        submit_and_confirm(&self.api, &self.blockhash, payer, cosigners, ixs)
    }
}
