use std::{ops::Range, slice, sync::Arc};

use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redline_interface::{instruction::build, layout};
use signature::Signature;
use signer::Signer;
use url::Url;

use crate::{
    prep,
    runner::{execute_threaded, RunOutcome, ThreadRunConfig},
    transport::clock::{SendClock, Stamped},
    ChainCtx, CheckError, Result,
};

pub mod causal;

pub struct Program<'a, C> {
    pub(crate) chain: &'a C,
    pub(crate) payer: &'a Keypair,
    pub(crate) id: Pubkey,
}

impl<C: ChainCtx> Program<'_, C> {
    pub fn at(mut self, id: Pubkey) -> Self {
        self.id = id;
        self
    }

    fn accounts(&self, space: u32, authority: Pubkey) -> Accounts {
        Accounts {
            program_id: self.id,
            space,
            authority,
        }
    }

    pub async fn init(&self, space: u32, authority: Pubkey, seed: u8) -> Result<Pubkey> {
        self.accounts(space, authority).init(self.chain, self.payer, seed, false).await
    }

    pub async fn init_delegated(&self, space: u32, authority: Pubkey, seed: u8) -> Result<Pubkey> {
        self.accounts(space, authority).init(self.chain, self.payer, seed, true).await
    }

    pub async fn write(&self, value: u64, keys: &[Pubkey]) -> Result<Signature> {
        self.chain
            .submit_and_confirm(
                self.payer,
                &[build::simple_byte_set_at(self.id, value, keys)],
            )
            .await
    }

    pub async fn init_many(
        &self,
        space: u32,
        authority: Pubkey,
        count: usize,
    ) -> Result<Vec<Pubkey>> {
        self.accounts(space, authority)
            .init_batched(self.chain, slice::from_ref(self.payer), count, false)
            .await
    }

    pub async fn commit(&self, id: u64, keys: &[Pubkey]) -> Result<Signature> {
        let mut ix = build::commit_accounts(id, self.payer.pubkey(), keys);
        ix.program_id = self.id;
        self.chain.submit_and_confirm(self.payer, &[ix]).await
    }

    pub async fn commit_and_undelegate(&self, id: u64, keys: &[Pubkey]) -> Result<Signature> {
        let mut ix = build::commit_and_undelegate_accounts(id, self.payer.pubkey(), keys);
        ix.program_id = self.id;
        self.chain.submit_and_confirm(self.payer, &[ix]).await
    }
}

pub fn written_id(data: &[u8]) -> Option<u64> {
    let bytes = data.get(layout::ID_OFFSET..layout::ID_OFFSET + layout::ID_SIZE)?;
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}

pub fn copy_three(pool: &[Pubkey], id: u64) -> (Instruction, [Pubkey; 2]) {
    let len = pool.len() as u64;
    let base_index = ((id - 1) * 3) % len;
    let source = pool[base_index as usize];
    let first_dest = pool[((base_index + 1) % len) as usize];
    let second_dest = pool[((base_index + 2) % len) as usize];
    (
        build::account_data_copy(id, &[source], &[first_dest, second_dest]),
        [first_dest, second_dest],
    )
}

pub fn execute_kernel<K>(
    config: ThreadRunConfig,
    rpc_url: Url,
    payers: Arc<Vec<[u8; 64]>>,
    first_id: u64,
    kernel: K,
    clock: Option<Arc<SendClock>>,
) -> Result<RunOutcome>
where
    K: Fn(u64) -> Instruction + Clone + Send + 'static,
{
    let threads = config.threads;
    execute_threaded(config, move |worker| {
        let senders = prep::worker_senders(&rpc_url, &payers, worker, threads);
        let kernel = kernel.clone();
        let clock = clock.clone();
        move |id: u64| {
            let id = first_id + id;
            let ix = kernel(id);
            let sender = senders[(id as usize) % senders.len()].clone();
            let clock = clock.clone();
            let staged = sender.staged([ix]);
            async move { Ok(Stamped::new(clock, id, staged.await?)) }
        }
    })
}

pub struct Accounts {
    pub program_id: Pubkey,
    pub space: u32,
    pub authority: Pubkey,
}

impl Accounts {
    pub fn new(space: u32, authority: Pubkey) -> Self {
        Self {
            program_id: redline_interface::id(),
            space,
            authority,
        }
    }

    async fn prepare(
        &self,
        base: &impl ChainCtx,
        payer: &Keypair,
        seeds: Range<usize>,
        delegated: bool,
    ) -> Result<Vec<Pubkey>> {
        let per_tx = if delegated { 3 } else { 6 };
        let owner = payer.pubkey();
        let mut pdas = Vec::with_capacity(seeds.len());
        for first in seeds.clone().step_by(per_tx) {
            let end = (first + per_tx).min(seeds.end);
            let mut instructions = Vec::new();
            for seed in first..end {
                let (init, pda) = build::init_account_at(
                    self.program_id,
                    owner,
                    owner,
                    self.space,
                    seed as u8,
                    self.authority,
                );
                pdas.push(pda);
                instructions.push(init);
                if delegated {
                    instructions.push(build::delegate_at(
                        self.program_id,
                        owner,
                        pda,
                        owner,
                        seed as u8,
                        self.authority,
                    ));
                }
            }
            base.submit_and_confirm(payer, &instructions).await.map_err(|error| {
                CheckError::new(format!(
                    "program {} payer {owner} seeds {first}..{end}",
                    self.program_id
                ))
                .caused_by(error)
            })?;
        }
        Ok(pdas)
    }

    pub async fn init(
        &self,
        base: &impl ChainCtx,
        payer: &Keypair,
        seed: u8,
        delegated: bool,
    ) -> Result<Pubkey> {
        let seed = usize::from(seed);
        Ok(self.prepare(base, payer, seed..seed + 1, delegated).await?[0])
    }

    pub async fn init_delegated(
        &self,
        base: &impl ChainCtx,
        payer: &Keypair,
        count: u8,
    ) -> Result<Vec<Pubkey>> {
        self.prepare(base, payer, 0..count as usize, true).await
    }

    pub async fn init_batched(
        &self,
        base: &impl ChainCtx,
        payers: &[Keypair],
        count: usize,
        delegated: bool,
    ) -> Result<Vec<Pubkey>> {
        if payers.is_empty() {
            return Err("at least one prep payer is required".into());
        }
        let per_payer = count.div_ceil(payers.len());
        if per_payer > u8::MAX as usize + 1 {
            return Err(format!(
                "{count} accounts over {} payers exceeds the u8 seed namespace",
                payers.len()
            )
            .into());
        }
        let batches = prep::bounded(payers.len().min(count), "accounts for payer", |index| {
            let len = per_payer.min(count.saturating_sub(index * per_payer));
            self.prepare(base, &payers[index], 0..len, delegated)
        })
        .await?;
        Ok(batches.into_iter().flatten().collect())
    }
}
