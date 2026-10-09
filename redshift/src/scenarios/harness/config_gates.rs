use std::time::Duration;

use async_trait::async_trait;
use pubkey::Pubkey;
use redshift_interface::flexi::{build, FlexiCounter};
use redsuite_core::{
    check, check_eq, prep, topology, BaseCtx, ChainCtx, ErCtx,
    PrivateErScenario, Result,
};
use signer::Signer;
use solana_address_lookup_table_interface::instruction::create_lookup_table;

const PROGRAM_CLONE_TIMEOUT: Duration = Duration::from_secs(20);
const BLOCKED_SETTLE: Duration = Duration::from_secs(2);
const ALT_SETTLE: Duration = Duration::from_secs(1);
const LABEL: &str = "redshift config";
const OPEN_ER_LABEL: &str = "cfg-none";
const RESTRICTED_ER_LABEL: &str = "cfg-allow";
const SIGNATURE_WINDOW: usize = 50;

pub struct ConfigGates;

fn committor_id() -> Pubkey {
    topology::COMMITTOR_ID.parse().expect("committor id")
}

// The config's AllowedProgram id deserializes as a 32-byte array, not as a
// base58 string.
fn allowed_programs_env(program: &Pubkey) -> String {
    let bytes = program
        .as_ref()
        .iter()
        .map(|byte| byte.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("[{{id=[{bytes}]}}]")
}

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

async fn assert_program_blocked(er: &ErCtx, program: &Pubkey) -> Result<()> {
    let first = er.account(program).await?;
    check!(
        first.is_none(),
        "the blocked program must not be on the er before the settle"
    )?;
    tokio::time::sleep(BLOCKED_SETTLE).await;
    let second = er.account(program).await?;
    check!(
        second.is_none(),
        "the restricted er must not clone the blocked program"
    )?;
    Ok(())
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
        let allowed = redshift_interface::id();
        let blocked = committor_id();

        let restricted = topology::private_er(
            base,
            topology::ErOptions {
                label: RESTRICTED_ER_LABEL.to_owned(),
                env: vec![(
                    "MBV_CHAINLINK__ALLOWED_PROGRAMS".to_owned(),
                    allowed_programs_env(&allowed),
                )],
                request_timeout: None,
                base_endpoints: None,
            },
        )
        .await?;
        prep::await_program_clone(
            restricted.ctx(),
            &allowed,
            PROGRAM_CLONE_TIMEOUT,
        )
        .await?;
        assert_program_blocked(restricted.ctx(), &blocked).await?;
        restricted.finish().await?;

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

        prep::await_program_clone(open.ctx(), &allowed, PROGRAM_CLONE_TIMEOUT)
            .await?;
        prep::await_program_clone(open.ctx(), &blocked, PROGRAM_CLONE_TIMEOUT)
            .await?;

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

        let recent_slot = base.api().get_slot().await?;
        let (create_ix, _) =
            create_lookup_table(open_pubkey, open_pubkey, recent_slot);
        base.submit_and_confirm(&open_identity, &[create_ix])
            .await?;
        let control =
            alt_transactions_since(base, &open_pubkey, &baseline).await?;
        check_eq!(
            control.len(),
            1,
            "the lookup table detector must see the one table this identity \
             created, got {control:?}"
        )?;

        Ok(())
    }
}
