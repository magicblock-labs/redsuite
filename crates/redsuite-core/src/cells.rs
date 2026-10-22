use std::{slice, time::Duration};

use anyhow::Context;
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use signature::Signature;
use signer::Signer;

use crate::{
    prep,
    programs::{CellHeader, CellId, SettlementMode},
    redline::Accounts,
    ChainCtx, ErCtx, Result,
};

pub struct OwnedCell {
    pub authority: Keypair,
    pub address: Pubkey,
}

pub struct Program<'a, C> {
    pub(crate) chain: &'a C,
    pub(crate) payer: &'a Keypair,
    pub(crate) program: fixture::Program,
}

impl<C: ChainCtx> Program<'_, C> {
    pub fn at(mut self, id: Pubkey) -> Self {
        self.program = fixture::Program::at(id);
        self
    }

    fn accounts(&self, space: u32, validator: Pubkey) -> Accounts {
        Accounts {
            program_id: self.program.id(),
            space,
            authority: validator,
        }
    }

    pub async fn create(&self, space: u32, validator: Pubkey, index: u8) -> Result<Pubkey> {
        self.accounts(space, validator).init(self.chain, self.payer, index, false).await
    }
    pub async fn create_delegated(
        &self,
        space: u32,
        validator: Pubkey,
        index: u8,
    ) -> Result<Pubkey> {
        self.accounts(space, validator).init(self.chain, self.payer, index, true).await
    }
    pub async fn create_many(
        &self,
        space: u32,
        validator: Pubkey,
        count: usize,
    ) -> Result<Vec<Pubkey>> {
        self.accounts(space, validator)
            .init_batched(self.chain, slice::from_ref(self.payer), count, false)
            .await
    }
    pub async fn write(&self, value: u64, cells: &[Pubkey]) -> Result<Signature> {
        self.chain
            .submit_and_confirm(self.payer, &[self.program.write(value, cells)])
            .await
    }
    pub async fn settle(
        &self,
        nonce: u64,
        cells: &[Pubkey],
        mode: SettlementMode,
    ) -> Result<Signature> {
        self.chain
            .submit_and_confirm(
                self.payer,
                &[self
                    .program
                    .batch()
                    .nonce(nonce)
                    .settle(self.payer.pubkey(), cells, mode)
                    .build()],
            )
            .await
    }

    pub fn prepare_delegated(
        &self,
        validator: Pubkey,
        commit_frequency_ms: u32,
    ) -> (Pubkey, [Instruction; 2]) {
        self.prepare(
            CellId::new(self.payer.pubkey(), 0),
            validator,
            commit_frequency_ms,
            CellHeader::SIZE as u32,
        )
    }
    fn prepare(
        &self,
        id: CellId,
        validator: Pubkey,
        commit_frequency_ms: u32,
        space: u32,
    ) -> (Pubkey, [Instruction; 2]) {
        let payer = self.payer.pubkey();
        let (create, cell) = self.program.create(payer, id, space);
        (
            cell,
            [create, self.program.delegate(payer, id, Some(validator), commit_frequency_ms)],
        )
    }
    pub async fn create_primary(
        &self,
        validator: Pubkey,
        commit_frequency_ms: u32,
    ) -> Result<Pubkey> {
        let (cell, instructions) = self.prepare_delegated(validator, commit_frequency_ms);
        for instruction in instructions {
            self.chain.submit_and_confirm(self.payer, &[instruction]).await?;
        }
        Ok(cell)
    }
    pub async fn read(&self, address: &Pubkey) -> Result<CellHeader> {
        let account = self.chain.account(address).await?.context("cell missing")?;
        Ok(CellHeader::try_decode(&account.data)?)
    }
    pub async fn create_owned(&self, validator: Pubkey, count: usize) -> Result<Vec<OwnedCell>> {
        self.create_owned_sized(validator, count, CellHeader::SIZE as u32).await
    }
    pub async fn create_owned_sized(
        &self,
        validator: Pubkey,
        count: usize,
        space: u32,
    ) -> Result<Vec<OwnedCell>> {
        let mut cells = Vec::with_capacity(count);
        for _ in 0..count {
            let authority = Keypair::new();
            let (address, instructions) = self.prepare(
                CellId::new(authority.pubkey(), 0),
                validator,
                prep::COMMIT_FREQUENCY_MS,
                space,
            );
            self.chain
                .submit_and_confirm_with(self.payer, &[&authority], &instructions)
                .await?;
            cells.push(OwnedCell { authority, address });
        }
        Ok(cells)
    }
}

impl ErCtx {
    pub async fn await_cells(&self, cells: &[OwnedCell]) -> Result<()> {
        let keys: Vec<_> = cells.iter().map(|cell| cell.address).collect();
        self.await_clones(&keys, CellHeader::SIZE, Duration::from_secs(15)).await
    }
}
