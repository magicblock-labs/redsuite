use std::time::Duration;

use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::schedulecommit::{build, MainAccount};
use signer::Signer;

use crate::{dlp, prep, ChainCtx, ErCtx, Result};

const COMMITTEE_CLONE_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Committee {
    pub player: Keypair,
    pub pda: Pubkey,
}

pub struct Program<'a, C> {
    pub(crate) chain: &'a C,
    pub(crate) payer: &'a Keypair,
}

impl<C: ChainCtx> Program<'_, C> {
    pub fn prepare_counter(
        &self,
        validator: Pubkey,
        commit_frequency_ms: u32,
    ) -> (Pubkey, [Instruction; 2]) {
        self.prepare(self.payer.pubkey(), validator, commit_frequency_ms)
    }

    fn prepare(
        &self,
        player: Pubkey,
        validator: Pubkey,
        commit_frequency_ms: u32,
    ) -> (Pubkey, [Instruction; 2]) {
        let payer = self.payer.pubkey();
        let (init, counter) = build::init_account(payer, player);
        (
            counter,
            [init, build::delegate_cpi(payer, player, commit_frequency_ms, Some(validator))],
        )
    }

    pub async fn init_counter(
        &self,
        validator: Pubkey,
        commit_frequency_ms: u32,
    ) -> Result<Pubkey> {
        let (counter, instructions) = self.prepare_counter(validator, commit_frequency_ms);
        for ix in instructions {
            self.chain.submit_and_confirm(self.payer, &[ix]).await?;
        }
        Ok(counter)
    }

    pub async fn counter(&self, address: &Pubkey) -> Result<MainAccount> {
        let account = self.chain.account(address).await?.ok_or("counter account missing")?;
        Ok(MainAccount::try_decode(&account.data)?)
    }

    pub async fn init(&self, validator: Pubkey, count: usize) -> Result<Vec<Committee>> {
        let mut committees = Vec::with_capacity(count);
        for _ in 0..count {
            let player = Keypair::new();
            let (pda, instructions) =
                self.prepare(player.pubkey(), validator, prep::COMMIT_FREQUENCY_MS);
            self.chain
                .submit_and_confirm_with(self.payer, &[&player], &instructions)
                .await?;
            let on_base = self
                .chain
                .account(&pda)
                .await?
                .ok_or("the committee pda is not on base after init and delegate")?;
            crate::check_eq!(
                on_base.owner,
                dlp::dlp_id(),
                "dlp must own a delegated committee on base"
            )?;
            committees.push(Committee { player, pda });
        }
        Ok(committees)
    }
}

impl ErCtx {
    pub async fn await_committees(&self, committees: &[Committee]) -> Result<()> {
        let pdas: Vec<Pubkey> = committees.iter().map(|committee| committee.pda).collect();
        self.await_clones(&pdas, MainAccount::SIZE, COMMITTEE_CLONE_TIMEOUT).await
    }
}
