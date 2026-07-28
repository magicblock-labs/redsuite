use std::time::Duration;

use async_trait::async_trait;
use pubkey::Pubkey;
use redshift_program::flexi::{build, FlexiCounter};
use redsuite_core::{
    assert::poll_until, dlp, prep, system, topology, BaseCtx, ChainCtx, ErCtx,
    Result, Scenario, ScenarioReport,
};
use signer::Signer;

const ALT_PROGRAM_ID: &str = "AddressLookupTab1e1111111111111111111111111";
const PROGRAM_CLONE_TIMEOUT: Duration = Duration::from_secs(20);
const BLOCKED_SETTLE: Duration = Duration::from_secs(2);
const ALT_SETTLE: Duration = Duration::from_secs(1);
const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;
const LABEL: &str = "redshift config";

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

fn alt_id() -> Pubkey {
    ALT_PROGRAM_ID.parse().expect("alt program id")
}

async fn latest_alt_signature(base: &BaseCtx) -> Result<Option<String>> {
    let signatures =
        base.api().get_signatures_for_address(&alt_id(), 1).await?;
    Ok(signatures.into_iter().next())
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
impl Scenario for ConfigGates {
    fn name(&self) -> &str {
        "redshift/config_gates"
    }

    async fn run(&self, base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        let allowed = redshift_program::id();
        let blocked = committor_id();

        {
            let restricted = topology::private_er(
                base,
                topology::ErOptions {
                    label: "cfg-allow".to_owned(),
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
        }

        let alt_before = latest_alt_signature(base).await?;
        let open = topology::private_er(
            base,
            topology::ErOptions {
                label: "cfg-none".to_owned(),
                env: vec![],
                request_timeout: None,
                ..Default::default()
            },
        )
        .await?;

        let alt_after_start = latest_alt_signature(base).await?;
        assert_eq!(
            alt_after_start, alt_before,
            "the er start must not send lookup table transactions on base"
        );

        await_program_clone(open.ctx(), &allowed).await?;
        await_program_clone(open.ctx(), &blocked).await?;

        let counter = delegate_and_clone_counter(base, open.ctx()).await?;

        tokio::time::sleep(ALT_SETTLE).await;
        let alt_after_clone = latest_alt_signature(base).await?;
        assert_eq!(
            alt_after_clone, alt_before,
            "cloning must not send lookup table transactions on base"
        );

        Ok(ScenarioReport::ok(self.name())
            .setting("allowed program", allowed)
            .setting("blocked program", blocked)
            .setting("cloned counter", counter)
            .setting(
                "alt signature before",
                alt_before.unwrap_or_else(|| "none".to_owned()),
            ))
    }
}
