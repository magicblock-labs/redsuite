use super::ER_BLOCKHASH_TTL;
use crate::{
    api::Api,
    runner::{Delivery, SendTransaction},
    Result,
};
use hash::Hash;
use instruction::Instruction;
use keypair::Keypair;
use signature::Signature;
use signer::Signer;
use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};
use transaction::Transaction;
use url::Url;

pub(super) struct BlockhashCache {
    ttl: Duration,
    cached: RefCell<Option<(Hash, Instant)>>,
}

impl BlockhashCache {
    pub(super) fn new(ttl: Duration) -> Self {
        Self { ttl, cached: RefCell::new(None) }
    }

    pub(super) fn reset(&self) {
        self.cached.borrow_mut().take();
    }

    async fn get(&self, api: &Api) -> Result<Hash> {
        if let Some((hash, _)) = self.cached.borrow().filter(|(_, at)| at.elapsed() < self.ttl) {
            return Ok(hash);
        }
        let hash = api.get_latest_blockhash().await?;
        *self.cached.borrow_mut() = Some((hash, Instant::now()));
        Ok(hash)
    }
}

pub(super) async fn submit_and_confirm(
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
    pub(super) fn new(api: Api, blockhash: Rc<BlockhashCache>, payer: Rc<Keypair>) -> Self {
        Self { api, blockhash, payer }
    }

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
        TxSender::new(self.api.clone(), self.blockhash.clone(), payer)
    }
}
