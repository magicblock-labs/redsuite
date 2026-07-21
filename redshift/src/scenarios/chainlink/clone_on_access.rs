use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use redsuite_core::{
    assert::poll_until, prep, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
    ScenarioReport,
};
use signer::Signer;

use crate::program::{instruction::build, layout, DELEGATION_PROGRAM_ID};

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const PROPAGATION_TIMEOUT: Duration = Duration::from_secs(15);
const PRE_CLONE_WRITE: u64 = 41;
const POST_CLONE_WRITE: u64 = 42;
const ER_WRITE: u64 = 43;

pub struct CloneOnAccess;

#[async_trait(?Send)]
impl Scenario for CloneOnAccess {
    fn name(&self) -> &str {
        "redshift/clone_on_access"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;

        let ghost = Keypair::new().pubkey();
        assert!(
            er.account(&ghost).await?.is_none(),
            "an account that exists nowhere must read as absent on the ER"
        );

        let plain = crate::init_account(base, &payer, 0, er.identity()).await?;
        base.send(&payer, &[build::simple_byte_set(PRE_CLONE_WRITE, &[plain])])
            .await?;

        let first_access = Instant::now();
        poll_until(CLONE_TIMEOUT, || async {
            matches!(er.account(&plain).await, Ok(Some(clone)) if clone.data.len() == crate::ACCOUNT_SPACE as usize)
        })
        .await;
        let clone_visibility_ms = first_access.elapsed().as_secs_f64() * 1e3;
        let plain_clone =
            er.account(&plain).await?.ok_or("plain clone vanished")?;
        assert_eq!(
            plain_clone.owner,
            crate::program::id(),
            "an undelegated clone must keep its base owner"
        );
        assert_eq!(
            crate::written_id(&plain_clone.data),
            Some(PRE_CLONE_WRITE),
            "the first ER access must observe the state written on base before cloning"
        );

        base.send(
            &payer,
            &[build::simple_byte_set(POST_CLONE_WRITE, &[plain])],
        )
        .await?;
        let mutation_confirmed = Instant::now();
        poll_until(PROPAGATION_TIMEOUT, || async {
            matches!(er.account(&plain).await, Ok(Some(clone)) if crate::written_id(&clone.data) == Some(POST_CLONE_WRITE))
        })
        .await;
        let propagation_ms = mutation_confirmed.elapsed().as_secs_f64() * 1e3;

        let delegated =
            crate::init_delegated_account(base, &payer, 1, er.identity())
                .await?;
        let delegated_on_base = base
            .account(&delegated)
            .await?
            .ok_or("delegated pda missing on base")?;
        assert_eq!(
            delegated_on_base.owner, DELEGATION_PROGRAM_ID,
            "a delegated pda must be dlp-owned on base"
        );
        poll_until(CLONE_TIMEOUT, || async {
            matches!(er.account(&delegated).await, Ok(Some(clone)) if clone.data.len() == crate::ACCOUNT_SPACE as usize)
        })
        .await;
        let delegated_clone = er
            .account(&delegated)
            .await?
            .ok_or("delegated clone vanished")?;
        assert_eq!(
            delegated_clone.owner,
            crate::program::id(),
            "the ER must present a delegated account as owned by its program"
        );

        er.send(&payer, &[build::simple_byte_set(ER_WRITE, &[delegated])])
            .await?;
        let after_er_write = er
            .account(&delegated)
            .await?
            .ok_or("delegated clone vanished after the ER write")?;
        assert_eq!(
            crate::written_id(&after_er_write.data),
            Some(ER_WRITE),
            "the ER copy of a delegated account must accept program writes"
        );
        let base_copy = base
            .account(&delegated)
            .await?
            .ok_or("delegated pda gone on base")?;
        assert!(
            base_copy.data[layout::DATA_OFFSET..]
                .iter()
                .all(|&byte| byte == 0),
            "ER writes must not reach the base copy without an explicit commit"
        );

        Ok(ScenarioReport::ok(self.name())
            .setting("account space", crate::ACCOUNT_SPACE)
            .metric("fresh clone visibility ms", clone_visibility_ms)
            .metric("base-to-er propagation ms", propagation_ms))
    }
}
