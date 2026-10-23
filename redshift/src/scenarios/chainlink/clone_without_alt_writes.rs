use std::{rc::Rc, time::Duration};

use anyhow::{ensure, Context};
use keypair::Keypair;

use redsuite_core::{
    api::{transaction_logs, transaction_meta},
    prep,
    programs::{CellHeader, REDSHIFT},
    topology::{self, ErOptions, PrivateEr},
    BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use signature::Signature;
use signer::Signer;
use solana_address_lookup_table_interface::instruction::create_lookup_table;
use tokio::time;

use crate::PAYER_LAMPORTS;

const LABEL: &str = "clone-without-alt-writes";
const ALT_SETTLE: Duration = Duration::from_secs(1);
const SIGNATURE_WINDOW: usize = 50;

/// Checks for unexpected L1 lookup-table writes during ER startup and cloning.
pub struct CloneWithoutAltWrites {
    base: Rc<BaseCtx>,
    identity: Keypair,
    baseline: Vec<Signature>,
    private: PrivateEr,
}

impl PrivateErScenario for CloneWithoutAltWrites {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let identity = topology::identity_for_label(LABEL)?;
        let baseline = base
            .api()
            .get_signatures_for_address(&identity.pubkey(), SIGNATURE_WINDOW)
            .await?;
        let private = topology::private_er(
            &base,
            ErOptions {
                label: LABEL.to_owned(),
                ..Default::default()
            },
        )
        .await?;
        Ok(Self {
            base,
            identity,
            baseline,
            private,
        })
    }

    async fn run(self) -> Result<()> {
        let alt_program = sdk_ids::address_lookup_table::ID.to_string();
        let alt_transactions = async || -> Result<usize> {
            let signatures = self
                .base
                .api()
                .get_signatures_for_address(&self.identity.pubkey(), SIGNATURE_WINDOW)
                .await?;
            let mut count = 0;
            for signature in signatures {
                if self.baseline.contains(&signature) {
                    continue;
                }
                let Some(tx) = self.base.api().get_transaction(&signature).await? else {
                    continue;
                };
                if transaction_logs(transaction_meta(&tx)?)
                    .iter()
                    .any(|line| line.contains(&alt_program))
                {
                    count += 1;
                }
            }
            Ok(count)
        };

        ensure!(alt_transactions().await? == 0);
        let er = self.private.ctx();
        er.await_program_clone(&redshift_interface::ID).await?;
        er.await_program_clone(&topology::COMMITTOR_ID).await?;

        let payer_chain = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let payer_ephem = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let cell = self
            .base
            .cells(REDSHIFT, &payer_ephem)
            .create_primary(er.identity(), prep::COMMIT_FREQUENCY_MS)
            .await?;
        self.base.delegate_payer(&payer_chain, &payer_ephem, er.identity()).await?;
        er.submit_and_confirm(
            &payer_ephem,
            &[REDSHIFT
                .batch()
                .add(&[REDSHIFT.primary(payer_ephem.pubkey())], 1)
                .require_signer(payer_ephem.pubkey())
                .build()],
        )
        .await?;
        let clone = er.account(&cell).await?.context("the cell clone is missing after the add")?;
        ensure!(CellHeader::try_decode(&clone.data)?.value == 1);

        time::sleep(ALT_SETTLE).await;
        ensure!(alt_transactions().await? == 0);
        // Keep the detector's positive control out of the ER observation window.
        self.private.finish().await?;

        let recent_slot = self.base.api().get_slot().await?;
        let (create, _) =
            create_lookup_table(self.identity.pubkey(), self.identity.pubkey(), recent_slot);
        self.base.submit_and_confirm(&self.identity, &[create]).await?;
        ensure!(alt_transactions().await? == 1);
        Ok(())
    }
}
