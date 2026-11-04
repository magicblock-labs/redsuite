mod transaction;
use transaction::{try_submit_and_confirm, BlockhashCache};
pub use transaction::{ErClient, TxSender};

use std::{
    collections::BTreeMap,
    future::Future,
    path::PathBuf,
    rc::{Rc, Weak},
    result,
    time::Duration,
};

use account::Account;
use anyhow::{Context, Error};
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use signature::Signature;
use tokio::{sync::Mutex, time};
use url::Url;

use crate::{
    api::{Api, Metrics, MetricsCollector, SendError},
    cells,
    config::{ErEndpoints, ExecutionConfig},
    netfault::{Faults, Proxy},
    report::{self, ScenarioReport},
    resources::Resources,
    rpc_types::{request::RpcRequest, response::RpcIdentity},
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
    ) -> impl Future<Output = Result<Signature>> {
        async move {
            self.try_submit_and_confirm_with(payer, cosigners, ixs)
                .await?
                .map_err(Into::into)
        }
    }
    /// Keeps RPC/execution refusals typed so negative scenarios cannot mistake
    /// transport failure for success.
    fn try_submit_and_confirm(
        &self,
        payer: &Keypair,
        ixs: &[Instruction],
    ) -> impl Future<Output = Result<result::Result<Signature, SendError>>> {
        self.try_submit_and_confirm_with(payer, &[], ixs)
    }
    /// Typed submission with explicit cosigners; blockhash, transport, and
    /// timeout errors stay outer.
    fn try_submit_and_confirm_with(
        &self,
        payer: &Keypair,
        cosigners: &[&Keypair],
        ixs: &[Instruction],
    ) -> impl Future<Output = Result<result::Result<Signature, SendError>>>;
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
        if let Some(environment) = &self.config.environment {
            use signer::{EncodableKey, Signer};
            let payer = Keypair::read_from_file(&environment.payer)
                .map_err(|error| Error::msg(format!("reading setup payer: {error}")))?;
            self.submit_and_confirm(
                &payer,
                &[crate::system::transfer(&payer.pubkey(), pk, lamports)],
            )
            .await
            .map(|_| ())
        } else {
            airdrop_and_confirm(&self.api, pk, lamports).await
        }
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
        self.config.clone()
    }

    pub fn ws_url(&self) -> &Url {
        self.api.ws_url()
    }

    pub fn sender(&self, payer: Rc<Keypair>) -> TxSender {
        TxSender::new(self.api.clone(), self.blockhash.clone(), payer)
    }
}

#[derive(Clone)]
pub struct ErCtx {
    api: Api,
    metrics: MetricsCollector,
    identity: Pubkey,
    blockhash: Rc<BlockhashCache>,
    pub(crate) pid: u32,
    pub(crate) proxy: Option<Rc<Proxy>>,
    faults: Option<Rc<Faults>>,
    pub(crate) owner: Option<Weak<Mutex<PrivateEr>>>,
}

impl ErCtx {
    pub(crate) fn for_scenario(mut self) -> Result<Self> {
        self.api = self.api.scoped();
        self.faults = self.proxy.as_ref().map(|proxy| proxy.scope().map(Rc::new)).transpose()?;
        Ok(self)
    }

    /// Read only resident accounts; unlike account reads, this never triggers cloning.
    pub async fn local_accounts(
        &self,
        owners: &[Pubkey],
        keys: &[Pubkey],
    ) -> Result<Vec<Option<Account>>> {
        let accounts = self.local_program_accounts(owners).await?;
        Ok(keys.iter().map(|key| accounts.get(key).cloned()).collect())
    }

    /// Read resident accounts whose owners are in [`crate::topology::local_account_owners`].
    ///
    /// Accounts with other owners return `None` even when resident. Use
    /// [`Self::local_accounts`] with explicit owners to include them.
    pub async fn local_images(&self, keys: &[Pubkey]) -> Result<Vec<Option<Account>>> {
        self.local_accounts(&crate::topology::local_account_owners(), keys).await
    }

    pub async fn local_program_accounts(
        &self,
        owners: &[Pubkey],
    ) -> Result<BTreeMap<Pubkey, Account>> {
        let mut accounts = BTreeMap::new();
        for owner in owners {
            accounts.extend(self.api.get_program_accounts(owner).await?);
        }
        Ok(accounts)
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
            proxy: None,
            faults: None,
            owner: None,
        }
    }

    pub fn faults(&self) -> Result<Rc<Faults>> {
        self.faults.clone().context("scenario requires a fault proxy")
    }

    pub(crate) async fn reset_faults(&self) -> Result<()> {
        match &self.faults {
            Some(faults) => faults.reset().await,
            None => Ok(()),
        }
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
        self.owner
            .as_ref()
            .and_then(Weak::upgrade)
            .and_then(|owner| owner.try_lock().ok().map(|er| er.pid()))
            .unwrap_or(self.pid)
    }

    /// Attached validators have no local process identity.
    pub fn local_pid(&self) -> Option<u32> {
        let pid = self.pid();
        (pid != 0).then_some(pid)
    }

    pub async fn storage_dir(&self) -> Result<PathBuf> {
        let owner = self
            .owner
            .as_ref()
            .and_then(Weak::upgrade)
            .context("attached ER storage is not locally owned")?;
        let path = owner.lock().await.storage_dir().to_owned();
        Ok(path)
    }

    pub async fn attach(endpoints: &ErEndpoints) -> Result<Self> {
        let api = Api::new(endpoints.rpc.clone(), endpoints.ws.clone());
        let identity: RpcIdentity = api.call(RpcRequest::GetIdentity, &[0u8; 0]).await?;
        Ok(Self::new(
            endpoints.rpc.clone(),
            endpoints.ws.clone(),
            endpoints.metrics.clone(),
            identity.identity.parse()?,
            0,
        ))
    }

    pub fn ws_url(&self) -> &Url {
        self.api.ws_url()
    }

    pub fn sender(&self, payer: Rc<Keypair>) -> TxSender {
        TxSender::new(self.api.clone(), self.blockhash.clone(), payer)
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
    let deadline = time::Instant::now() + NETWORK_TIMEOUT;
    let signature: Signature = time::timeout_at(
        deadline,
        wait::retry("airdrop accepted", NETWORK_TIMEOUT, async || {
            api.request_airdrop(pk, lamports).await
        }),
    )
    .await
    .unwrap_or_else(|error| Err(error.into()))
    .with_context(|| format!("airdrop request failed: address={pk}, lamports={lamports}"))?
    .parse()?;

    // A shared recipient may spend the credit before its balance is observed.
    time::timeout_at(deadline, api.confirm(&signature))
        .await
        .unwrap_or_else(|error| Err(error.into()))
        .with_context(|| {
            format!(
                "airdrop not confirmed: address={pk}, lamports={lamports}, signature={signature}"
            )
        })
}

impl ChainCtx for BaseCtx {
    fn api(&self) -> &Api {
        &self.api
    }
    fn try_submit_and_confirm_with(
        &self,
        payer: &Keypair,
        cosigners: &[&Keypair],
        ixs: &[Instruction],
    ) -> impl Future<Output = Result<result::Result<Signature, SendError>>> {
        try_submit_and_confirm(&self.api, &self.blockhash, payer, cosigners, ixs)
    }
}

impl ChainCtx for ErCtx {
    fn api(&self) -> &Api {
        &self.api
    }
    fn try_submit_and_confirm_with(
        &self,
        payer: &Keypair,
        cosigners: &[&Keypair],
        ixs: &[Instruction],
    ) -> impl Future<Output = Result<result::Result<Signature, SendError>>> {
        try_submit_and_confirm(&self.api, &self.blockhash, payer, cosigners, ixs)
    }
}
