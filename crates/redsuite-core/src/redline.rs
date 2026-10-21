use std::{ops::Range, sync::Arc};

use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;

use signer::Signer;
use url::Url;

use crate::{
    prep,
    programs::{CellId, Redline},
    runner::{execute_threaded, RunOutcome, ThreadRunConfig},
    transport::clock::{SendClock, Stamped},
    ChainCtx, CheckError, Result,
};

pub mod causal;

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
        // Delegation metadata and signatures must also fit the transaction packet.
        let per_tx = if delegated { 2 } else { 4 };
        let owner = payer.pubkey();
        let program = Redline::at(self.program_id);
        let mut pdas = Vec::with_capacity(seeds.len());
        for first in seeds.clone().step_by(per_tx) {
            let end = (first + per_tx).min(seeds.end);
            let mut instructions = Vec::new();
            for seed in first..end {
                let id = CellId::new(owner, seed as u64);
                let (init, pda) = program.create(owner, id, self.space);
                pdas.push(pda);
                instructions.push(init);
                if delegated {
                    instructions.push(program.delegate(owner, id, Some(self.authority), u32::MAX));
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
        let batches = prep::bounded(payers.len().min(count), "accounts for payer", |index| {
            let len = per_payer.min(count.saturating_sub(index * per_payer));
            self.prepare(base, &payers[index], 0..len, delegated)
        })
        .await?;
        Ok(batches.into_iter().flatten().collect())
    }
}
