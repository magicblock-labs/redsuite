use std::ops::Range;

use keypair::Keypair;
use pubkey::Pubkey;
use redline_interface::instruction::build;
use signer::Signer;

use crate::{prep, ChainCtx, Result};

pub mod causal;

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
        per_tx: usize,
    ) -> Result<Vec<Pubkey>> {
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
            base.submit_and_confirm(payer, &instructions)
                .await
                .map_err(|error| {
                    format!(
                        "program {} payer {owner} seeds {first}..{end}: {error}",
                        self.program_id
                    )
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
        Ok(self
            .prepare(base, payer, seed..seed + 1, delegated, 1)
            .await?[0])
    }

    pub async fn init_delegated(
        &self,
        base: &impl ChainCtx,
        payer: &Keypair,
        count: u8,
    ) -> Result<Vec<Pubkey>> {
        self.prepare(base, payer, 0..count as usize, true, 1).await
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
        let batches = prep::bounded(
            payers.len().min(count),
            "accounts for payer",
            |index| {
                let len =
                    per_payer.min(count.saturating_sub(index * per_payer));
                self.prepare(
                    base,
                    &payers[index],
                    0..len,
                    delegated,
                    if delegated { 3 } else { 6 },
                )
            },
        )
        .await?;
        Ok(batches.into_iter().flatten().collect())
    }
}
