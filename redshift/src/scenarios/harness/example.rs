use std::rc::Rc;

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{system, BaseCtx, ChainCtx, ErCtx, Result, Scenario};
use signer::Signer;

const TRANSFERS: u64 = 10;
const LAMPORTS_PER_TRANSFER: u64 = 1_000_000;
const PAYER_LAMPORTS: u64 = 1_000_000_000;

/// Minimal base-to-ER observation example: distinct transfers accumulate in one mirrored balance.
pub struct Example {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
    recipient: Pubkey,
}

impl Scenario for Example {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let recipient = Keypair::new().pubkey();

        Ok(Self { base, er, payer, recipient })
    }

    /// Checks the accumulated base result before waiting for the ER mirror to agree.
    async fn run(self) -> Result<()> {
        let mut expected = 0;
        // identical transactions share a signature and get deduplicated —
        // every transaction must differ somewhere (here: the amount)
        for unique in 1..=TRANSFERS {
            let lamports = LAMPORTS_PER_TRANSFER + unique;
            let transfer = system::transfer(&self.payer.pubkey(), &self.recipient, lamports);
            self.base.submit_and_confirm(&self.payer, &[transfer]).await?;
            expected += lamports;
        }
        let on_base = self
            .base
            .account(&self.recipient)
            .await?
            .context("recipient never appeared on base")?;
        ensure!(on_base.lamports == expected);

        self.er
            .await_account(&self.recipient, |account| {
                ensure!(account.lamports == expected);
                Ok(())
            })
            .await?;
        Ok(())
    }
}
