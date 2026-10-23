use std::{rc::Rc, time::Duration};

use keypair::Keypair;

use redsuite_core::{
    check_eq, prep,
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
const CLONE_TIMEOUT: Duration = Duration::from_secs(20);
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
        let Self {
            base,
            identity,
            baseline,
            private,
        } = self;
        let alt_program = sdk_ids::address_lookup_table::ID.to_string();
        let alt_transactions = async || -> Result<usize> {
            let signatures = base
                .api()
                .get_signatures_for_address(&identity.pubkey(), SIGNATURE_WINDOW)
                .await?;
            let mut count = 0;
            for signature in signatures {
                if baseline.contains(&signature) {
                    continue;
                }
                let Some(tx) = base.api().get_transaction(&signature).await? else {
                    continue;
                };
                if tx.logs.iter().any(|line| line.contains(&alt_program)) {
                    count += 1;
                }
            }
            Ok(count)
        };

        check_eq!(
            alt_transactions().await?,
            0,
            "ER startup must not send lookup table transactions to L1"
        )?;
        let er = private.ctx();
        er.await_program_clone(&redshift_interface::ID, CLONE_TIMEOUT).await?;
        er.await_program_clone(&topology::COMMITTOR_ID, CLONE_TIMEOUT).await?;

        let payer_chain = base.funded_payer(PAYER_LAMPORTS).await?;
        let payer_ephem = base.funded_payer(PAYER_LAMPORTS).await?;
        let cell = base
            .cells(REDSHIFT, &payer_ephem)
            .create_primary(er.identity(), prep::COMMIT_FREQUENCY_MS)
            .await?;
        base.delegate_payer(&payer_chain, &payer_ephem, er.identity()).await?;
        er.submit_and_confirm(
            &payer_ephem,
            &[REDSHIFT
                .batch()
                .add(&[REDSHIFT.primary(payer_ephem.pubkey())], 1)
                .require_signer(payer_ephem.pubkey())
                .build()],
        )
        .await?;
        let clone = er.account(&cell).await?.ok_or("the cell clone is missing after the add")?;
        check_eq!(
            CellHeader::try_decode(&clone.data)?.value,
            1,
            "the ER clone must show the add"
        )?;

        time::sleep(ALT_SETTLE).await;
        check_eq!(
            alt_transactions().await?,
            0,
            "cloning must not send lookup table transactions to L1"
        )?;
        // Keep the detector's positive control out of the ER observation window.
        private.finish().await?;

        let recent_slot = base.api().get_slot().await?;
        let (create, _) = create_lookup_table(identity.pubkey(), identity.pubkey(), recent_slot);
        base.submit_and_confirm(&identity, &[create]).await?;
        check_eq!(
            alt_transactions().await?,
            1,
            "the detector must see the lookup table created by its positive control"
        )?;
        Ok(())
    }
}
