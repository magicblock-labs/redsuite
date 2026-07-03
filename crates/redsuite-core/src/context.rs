//! The black-box facade scenarios drive: one context per chain in the topology.

use account::Account;
use async_trait::async_trait;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use signature::Signature;

use crate::{
    api::{Api, Metrics},
    Result,
};

#[async_trait(?Send)]
pub trait ChainCtx {
    fn api(&self) -> &Api;
    async fn send(&self, payer: &Keypair, ixs: &[Instruction]) -> Result<Signature>;
    async fn account(&self, pk: &Pubkey) -> Result<Option<Account>>;
    async fn airdrop(&self, pk: &Pubkey, lamports: u64) -> Result<()>;
}

pub struct BaseCtx {}

pub struct ErCtx {}

impl ErCtx {
    /// ER validator identity — the `Delegate` authority.
    pub fn identity(&self) -> Pubkey {
        todo!()
    }

    /// GET the Prometheus `/metrics` endpoint (`mbv_*`).
    pub async fn scrape_metrics(&self) -> Result<Metrics> {
        todo!()
    }
}

#[async_trait(?Send)]
impl ChainCtx for BaseCtx {
    fn api(&self) -> &Api {
        todo!()
    }
    async fn send(&self, _payer: &Keypair, _ixs: &[Instruction]) -> Result<Signature> {
        todo!()
    }
    async fn account(&self, _pk: &Pubkey) -> Result<Option<Account>> {
        todo!()
    }
    async fn airdrop(&self, _pk: &Pubkey, _lamports: u64) -> Result<()> {
        todo!()
    }
}

#[async_trait(?Send)]
impl ChainCtx for ErCtx {
    fn api(&self) -> &Api {
        todo!()
    }
    async fn send(&self, _payer: &Keypair, _ixs: &[Instruction]) -> Result<Signature> {
        todo!()
    }
    async fn account(&self, _pk: &Pubkey) -> Result<Option<Account>> {
        todo!()
    }
    async fn airdrop(&self, _pk: &Pubkey, _lamports: u64) -> Result<()> {
        todo!()
    }
}
