use std::time::Duration;

use async_trait::async_trait;
use pubkey::Pubkey;
use redshift_program::flexi::{build, FlexiCounter};
use redsuite_core::{
    assert::poll_until, dlp, prep, system, topology, BaseCtx, ChainCtx, ErCtx,
    PrivateErScenario, Result, ScenarioReport,
};
use signer::Signer;
use solana_address_lookup_table_interface::instruction::create_lookup_table;

const PROGRAM_CLONE_TIMEOUT: Duration = Duration::from_secs(20);
const BLOCKED_SETTLE: Duration = Duration::from_secs(2);
const ALT_SETTLE: Duration = Duration::from_secs(1);
const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;
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

async fn await_program_clone(er: &ErCtx, program: &Pubkey) -> Result<()> {
    poll_until(PROGRAM_CLONE_TIMEOUT, || async {
        matches!(er.account(program).await, Ok(Some(clone)) if clone.executable)
    })
    .await;
    Ok(())
}

async fn assert_program_blocked(er: &ErCtx, program: &Pubkey) -> Result<()> {
    let first = er.account(program).await?;
    assert!(
        first.is_none(),
        "the blocked program must not be on the er before the settle"
    );
    tokio::time::sleep(BLOCKED_SETTLE).await;
    let second = er.account(program).await?;
    assert!(
        second.is_none(),
        "the restricted er must not clone the blocked program"
    );
    Ok(())
}

async fn delegate_and_clone_counter(
    base: &BaseCtx,
    er: &ErCtx,
) -> Result<Pubkey> {
    let payer_chain = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let payer_ephem = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;

    let (init, counter) = build::init_counter(payer_ephem.pubkey(), LABEL);
    base.send(&payer_ephem, &[init]).await?;
    base.send(
        &payer_ephem,
        &[build::delegate_counter(
            payer_ephem.pubkey(),
            COMMIT_FREQUENCY_MS,
            Some(er.identity()),
        )],
    )
    .await?;

    let delegate_setup = [
        system::assign(&payer_ephem.pubkey(), &dlp::dlp_id()),
        dlp::delegate_account(
            &payer_chain.pubkey(),
            &payer_ephem.pubkey(),
            &er.identity(),
        ),
    ];
    base.send_with(&payer_chain, &[&payer_ephem], &delegate_setup)
        .await?;

    er.send(&payer_ephem, &[build::add(payer_ephem.pubkey(), 1)])
        .await?;
    let clone = er
        .account(&counter)
        .await?
        .ok_or("the counter clone is missing on the er after the add")?;
    assert_eq!(
        FlexiCounter::try_decode(&clone.data)?.count,
        1,
        "the er clone must show the add"
    );
    Ok(counter)
}

#[async_trait(?Send)]
impl PrivateErScenario for ConfigGates {
    fn name(&self) -> &str {
        "redshift/config_gates"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let allowed = redshift_program::id();
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
                ..Default::default()
            },
        )
        .await?;
        await_program_clone(restricted.ctx(), &allowed).await?;
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
                ..Default::default()
            },
        )
        .await?;

        let after_start =
            alt_transactions_since(base, &open_pubkey, &baseline).await?;
        assert!(
            after_start.is_empty(),
            "the er start must not send lookup table transactions on base, \
             got {after_start:?}"
        );

        await_program_clone(open.ctx(), &allowed).await?;
        await_program_clone(open.ctx(), &blocked).await?;

        let counter = delegate_and_clone_counter(base, open.ctx()).await?;

        tokio::time::sleep(ALT_SETTLE).await;
        let after_clone =
            alt_transactions_since(base, &open_pubkey, &baseline).await?;
        assert!(
            after_clone.is_empty(),
            "cloning must not send lookup table transactions on base, got \
             {after_clone:?}"
        );
        open.finish().await?;

        let recent_slot = base.api().get_slot().await?;
        let (create_ix, table) =
            create_lookup_table(open_pubkey, open_pubkey, recent_slot);
        base.send(&open_identity, &[create_ix]).await?;
        let control =
            alt_transactions_since(base, &open_pubkey, &baseline).await?;
        assert_eq!(
            control.len(),
            1,
            "the lookup table detector must see the one table this identity \
             created, got {control:?}"
        );

        Ok(ScenarioReport::ok(self.name())
            .setting("allowed program", allowed)
            .setting("blocked program", blocked)
            .setting("cloned counter", counter)
            .setting("open er identity", open_pubkey)
            .setting("control lookup table", table))
    }
}
