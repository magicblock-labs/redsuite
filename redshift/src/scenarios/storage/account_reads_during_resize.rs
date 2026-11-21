use std::{cell::Cell, rc::Rc};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    programs::REDSHIFT, topology::RestartConfig, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;
use tokio::join;

const INITIAL_ACCOUNT_SPACE: u32 = 10 * 1024;
const ACCOUNTS: usize = 8;
const GROW_BYTES: u32 = 32;
const PAYER_LAMPORTS: u64 = 10_000_000_000;

/// Checks account reads and final sizes while resize traffic spans a restart.
/// Readers run alongside growth on each side of the restart, not while the RPC server is down.
pub struct AccountReadsDuringResize {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
}

impl Scenario for AccountReadsDuringResize {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        Ok(Self { base, er, payer })
    }

    /// Carries expected sizes across restart and resumes concurrent
    /// reads against recovered accounts.
    async fn run(self) -> Result<()> {
        let mut sizes = [INITIAL_ACCOUNT_SPACE; ACCOUNTS];
        let er = &self.er;
        er.await_program_clone(&redshift_interface::id()).await?;
        let accounts = self
            .base
            .cells(REDSHIFT, &self.payer)
            .create_owned_sized(er.identity(), ACCOUNTS, INITIAL_ACCOUNT_SPACE)
            .await?;
        let addresses: Vec<_> = accounts.into_iter().map(|account| account.address).collect();
        er.await_clones(&addresses, INITIAL_ACCOUNT_SPACE as usize).await?;
        let sponsor = self.base.funded_payer(crate::PAYER_LAMPORTS).await?;
        self.base.delegate_payer(&sponsor, &self.payer, er.identity()).await?;

        self.grow_with_reads(&addresses, &mut sizes, "before restart").await?;

        let timing = self.er.restart(RestartConfig::default()).await?;
        ensure!(timing.exit_code == Some(0));

        self.grow_with_reads(&addresses, &mut sizes, "after restart").await?;

        let observed: Vec<u32> = er
            .accounts(&addresses)
            .await?
            .into_iter()
            .map(|account| account.map_or(0, |a| a.data.len() as u32))
            .collect();
        ensure!(observed == sizes);

        Ok(())
    }
}

impl AccountReadsDuringResize {
    async fn grow_with_reads(
        &self,
        addresses: &[Pubkey],
        sizes: &mut [u32],
        phase: &str,
    ) -> Result<()> {
        let done = Cell::new(false);
        let grow = async {
            let result = self.grow(addresses, sizes, phase).await;
            done.set(true);
            result
        };
        let read = async {
            let mut reads = 0;
            while !done.get() {
                let accounts = self.er.accounts(addresses).await.with_context(|| {
                    format!("snapshot account reads {phase}; completed={reads}")
                })?;
                ensure!(
                    accounts.iter().all(Option::is_some),
                    "snapshot account missing {phase}; completed={reads}"
                );
                reads += 1;
            }
            ensure!(reads > 0, "no reads completed {phase}");
            Ok(())
        };
        let (grown, read) = join!(grow, read);
        grown.and(read)
    }

    /// Grows accounts round-robin, updating expected sizes only after confirmed execution.
    async fn grow(&self, addresses: &[Pubkey], sizes: &mut [u32], phase: &str) -> Result<()> {
        for (index, (&address, size)) in addresses.iter().zip(sizes).enumerate() {
            let space = *size + GROW_BYTES;
            let resize = REDSHIFT.resize(self.payer.pubkey(), address, space);
            self.er
                .submit_and_confirm(&self.payer, &[resize])
                .await
                .with_context(|| format!("resizing account {index} {phase}"))?;
            *size = space;
        }
        Ok(())
    }
}
