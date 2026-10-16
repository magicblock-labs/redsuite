use std::time::Duration;

use async_trait::async_trait;
use pubkey::Pubkey;
use redshift_interface::flexi::{build, FlexiCounter};
use redsuite_core::{
    check, check_eq, prep, topology, BaseCtx, ChainCtx, ErCtx,
    PrivateErScenario, Result,
};
use signer::Signer;

const ALT_SETTLE: Duration = Duration::from_secs(1);
const PROGRAM_CLONE_TIMEOUT: Duration = Duration::from_secs(20);
const LABEL: &str = "redshift config";
const OPEN_ER_LABEL: &str = "cfg-none";
const SIGNATURE_WINDOW: usize = 50;

pub struct ConfigGates;

async fn identity_signatures(
    base: &BaseCtx,
    identity: &Pubkey,
) -> Result<Vec<String>> {
    base.api()
        .get_signatures_for_address(identity, SIGNATURE_WINDOW)
        .await
}

async fn alt_transactions_since(
    base: &BaseCtx,
    identity: &Pubkey,
    baseline: &[String],
) -> Result<Vec<String>> {
    let alt_program = sdk_ids::address_lookup_table::ID.to_string();
    let mut found = Vec::new();
    for signature in identity_signatures(base, identity).await? {
        if baseline.contains(&signature) {
            continue;
        }
        let Some(tx) = base.api().get_transaction(&signature.parse()?).await?
        else {
            continue;
        };
        if tx.logs.iter().any(|line| line.contains(&alt_program)) {
            found.push(signature);
        }
    }
    Ok(found)
}

async fn delegate_and_clone_counter(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let payer_chain = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let payer_ephem = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;

    let (counter, setup) = prep::flexi_counter(
        payer_ephem.pubkey(),
        LABEL,
        er.identity(),
        prep::COMMIT_FREQUENCY_MS,
    );
    for instruction in setup {
        base.submit_and_confirm(&payer_ephem, &[instruction])
            .await?;
    }
    prep::delegate_payer(base, &payer_chain, &payer_ephem, er.identity())
        .await?;
    er.submit_and_confirm(&payer_ephem, &[build::add(payer_ephem.pubkey(), 1)])
        .await?;
    let clone = er
        .account(&counter)
        .await?
        .ok_or("the counter clone is missing on the er after the add")?;
    check_eq!(
        FlexiCounter::try_decode(&clone.data)?.count,
        1,
        "the er clone must show the add"
    )?;
    Ok(())
}

#[async_trait(?Send)]
impl PrivateErScenario for ConfigGates {
    fn name(&self) -> &str {
        "redshift/config_gates"
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        // ChainSync has no program allowlist. Default startup must still clone
        // both application and settlement programs without creating lookup tables.
        let open_identity = topology::identity_for_label(OPEN_ER_LABEL)?;
        let open_pubkey = open_identity.pubkey();
        let baseline = identity_signatures(base, &open_pubkey).await?;

        let open = topology::private_er(
            base,
            topology::ErOptions {
                label: OPEN_ER_LABEL.to_owned(),
                env: vec![],
                request_timeout: None,
                base_endpoints: None,
                ..Default::default()
            },
        )
        .await?;

        let after_start =
            alt_transactions_since(base, &open_pubkey, &baseline).await?;
        check!(
            after_start.is_empty(),
            "the er start must not send lookup table transactions on base, \
             got {after_start:?}"
        )?;

        for program in
            [redshift_interface::id(), topology::COMMITTOR_ID.parse()?]
        {
            prep::await_program_clone(
                open.ctx(),
                &program,
                PROGRAM_CLONE_TIMEOUT,
            )
            .await?;
        }

        delegate_and_clone_counter(base, open.ctx()).await?;

        tokio::time::sleep(ALT_SETTLE).await;
        let after_clone =
            alt_transactions_since(base, &open_pubkey, &baseline).await?;
        check!(
            after_clone.is_empty(),
            "cloning must not send lookup table transactions on base, got \
             {after_clone:?}"
        )?;
        open.finish().await?;

        Ok(())
    }
}
