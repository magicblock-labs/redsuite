use std::{rc::Rc, time::Duration};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{check, system, BaseCtx, ChainCtx, ErCtx, Result, Scenario};
use signer::Signer;

const TRANSFERS: u64 = 10;
const LAMPORTS_PER_TRANSFER: u64 = 1_000_000;
const PAYER_LAMPORTS: u64 = 1_000_000_000;

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

    async fn run(self) -> Result<()> {
        let Self { base, er, payer, recipient } = self;
        let mut expected = 0;
        // identical transactions share a signature and get deduplicated —
        // every transaction must differ somewhere (here: the amount)
        for unique in 1..=TRANSFERS {
            let lamports = LAMPORTS_PER_TRANSFER + unique;
            let transfer = system::transfer(&payer.pubkey(), &recipient, lamports);
            base.submit_and_confirm(&payer, &[transfer]).await?;
            expected += lamports;
        }
        let on_base =
            base.account(&recipient).await?.context("recipient never appeared on base")?;
        ensure!(on_base.lamports == expected);

        check::poll(
            "the er clones the recipient at the delivered balance",
            Duration::from_secs(15),
            || async {
                matches!(
                    er.account(&recipient).await,
                    Ok(Some(cloned)) if cloned.lamports == expected
                )
            },
        )
        .await?;
        Ok(())
    }
}
