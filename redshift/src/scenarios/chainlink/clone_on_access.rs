use std::time::{Duration, Instant};

use async_trait::async_trait;
use dlp_api::{
    args::CommitStateArgs,
    instruction_builder::{commit_state, finalize, undelegate},
};
use keypair::Keypair;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, dlp, prep, system, topology, BaseCtx, ChainCtx, ErCtx,
    Result, Scenario, ScenarioReport,
};
use signer::Signer;

use crate::program::{instruction::build, layout, DELEGATION_PROGRAM_ID};

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const PROPAGATION_TIMEOUT: Duration = Duration::from_secs(15);
const PRE_CLONE_WRITE: u64 = 41;
const POST_CLONE_WRITE: u64 = 42;
const ER_WRITE: u64 = 43;
const UNDELEGATED_WRITE: u64 = 44;
const GHOST_LAMPORTS: u64 = 500_000_000;
const WALLET_LAMPORTS: u64 = 1_000_000_000;
const CYCLE_SPEND: u64 = 1_000;
const CONTINUITY_WINDOW: Duration = Duration::from_secs(2);
const CONTINUITY_STEP: Duration = Duration::from_millis(50);

pub struct CloneOnAccess;

#[async_trait(?Send)]
impl Scenario for CloneOnAccess {
    fn name(&self) -> &str {
        "redshift/clone_on_access"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;

        let ghost = Keypair::new().pubkey();
        check!(
            er.account(&ghost).await?.is_none(),
            "an account that exists nowhere must read as absent on the ER"
        )?;
        base.airdrop(&ghost, GHOST_LAMPORTS).await?;
        check::poll(
            "the base airdrop invalidates the ER's negative cache for the \
             ghost",
            PROPAGATION_TIMEOUT,
            || async {
                matches!(er.account(&ghost).await, Ok(Some(acc)) if acc.lamports == GHOST_LAMPORTS)
            },
        )
        .await?;

        let plain = crate::init_account(base, &payer, 0, er.identity()).await?;
        base.send(&payer, &[build::simple_byte_set(PRE_CLONE_WRITE, &[plain])])
            .await?;

        let first_access = Instant::now();
        check::poll(
            "the ER clones the plain account on first access",
            CLONE_TIMEOUT,
            || async {
                matches!(er.account(&plain).await, Ok(Some(clone)) if clone.data.len() == crate::ACCOUNT_SPACE as usize)
            },
        )
        .await?;
        let clone_visibility_ms = first_access.elapsed().as_secs_f64() * 1e3;
        let plain_clone =
            er.account(&plain).await?.ok_or("plain clone vanished")?;
        let plain_on_base =
            base.account(&plain).await?.ok_or("plain gone on base")?;
        check_eq!(
            plain_clone.lamports,
            plain_on_base.lamports,
            "the ER clone must mirror the base account's lamports"
        )?;
        check_eq!(
            plain_clone.owner,
            crate::program::id(),
            "an undelegated clone must keep its base owner"
        )?;
        check_eq!(
            crate::written_id(&plain_clone.data),
            Some(PRE_CLONE_WRITE),
            "the first ER access must observe the state written on base before cloning"
        )?;

        base.send(
            &payer,
            &[build::simple_byte_set(POST_CLONE_WRITE, &[plain])],
        )
        .await?;
        let mutation_confirmed = Instant::now();
        check::poll(
            "the base write propagates to the existing ER clone",
            PROPAGATION_TIMEOUT,
            || async {
                matches!(er.account(&plain).await, Ok(Some(clone)) if crate::written_id(&clone.data) == Some(POST_CLONE_WRITE))
            },
        )
        .await?;
        let propagation_ms = mutation_confirmed.elapsed().as_secs_f64() * 1e3;

        let undelegated_write = er
            .send(
                &payer,
                &[build::simple_byte_set(UNDELEGATED_WRITE, &[plain])],
            )
            .await;
        check!(
            undelegated_write.is_err(),
            "the ER must reject a write to an undelegated clone, got {undelegated_write:?}"
        )?;

        let delegated =
            crate::init_delegated_account(base, &payer, 1, er.identity())
                .await?;
        let delegated_on_base = base
            .account(&delegated)
            .await?
            .ok_or("delegated pda missing on base")?;
        check_eq!(
            delegated_on_base.owner,
            DELEGATION_PROGRAM_ID,
            "a delegated pda must be dlp-owned on base"
        )?;
        check::poll(
            "the ER clones the delegated account",
            CLONE_TIMEOUT,
            || async {
                matches!(er.account(&delegated).await, Ok(Some(clone)) if clone.data.len() == crate::ACCOUNT_SPACE as usize)
            },
        )
        .await?;
        let delegated_clone = er
            .account(&delegated)
            .await?
            .ok_or("delegated clone vanished")?;
        check_eq!(
            delegated_clone.owner,
            crate::program::id(),
            "the ER must present a delegated account as owned by its program"
        )?;

        er.send(&payer, &[build::simple_byte_set(ER_WRITE, &[delegated])])
            .await?;
        let after_er_write = er
            .account(&delegated)
            .await?
            .ok_or("delegated clone vanished after the ER write")?;
        check_eq!(
            crate::written_id(&after_er_write.data),
            Some(ER_WRITE),
            "the ER copy of a delegated account must accept program writes"
        )?;
        let base_copy = base
            .account(&delegated)
            .await?
            .ok_or("delegated pda gone on base")?;
        check!(
            base_copy.data[layout::DATA_OFFSET..]
                .iter()
                .all(|&byte| byte == 0),
            "ER writes must not reach the base copy without an explicit commit"
        )?;

        // redelegation continuity — undelegate + redelegate-to-us composed
        // in ONE base transaction (validator-signed dlp undelegate, then
        // assign + delegate) must never interrupt the er clone.
        let wallet = Keypair::new();
        base.airdrop(&wallet.pubkey(), WALLET_LAMPORTS).await?;
        let delegate_setup = [
            system::assign(&wallet.pubkey(), &dlp::dlp_id()),
            dlp::delegate_account(
                &payer.pubkey(),
                &wallet.pubkey(),
                &er.identity(),
            ),
        ];
        base.send_with(&payer, &[&wallet], &delegate_setup).await?;
        check::poll(
            "the ER clones the delegated wallet at full balance",
            CLONE_TIMEOUT,
            || async {
                matches!(
                    er.api().get_balance(&wallet.pubkey()).await,
                    Ok(balance) if balance == WALLET_LAMPORTS
                )
            },
        )
        .await?;

        let identity = topology::er_identity_keypair()?;
        let cycle = [
            commit_state(
                er.identity(),
                wallet.pubkey(),
                system::system_id(),
                CommitStateArgs {
                    nonce: 1,
                    lamports: WALLET_LAMPORTS,
                    allow_undelegation: true,
                    data: vec![],
                },
            ),
            finalize(er.identity(), wallet.pubkey()),
            undelegate(
                er.identity(),
                wallet.pubkey(),
                system::system_id(),
                payer.pubkey(),
            ),
            system::assign(&wallet.pubkey(), &dlp::dlp_id()),
            dlp::delegate_account(
                &payer.pubkey(),
                &wallet.pubkey(),
                &er.identity(),
            ),
        ];
        base.send_with(&payer, &[&identity, &wallet], &cycle)
            .await?;

        let continuity_deadline = Instant::now() + CONTINUITY_WINDOW;
        while Instant::now() < continuity_deadline {
            check!(
                er.account(&wallet.pubkey()).await?.is_some(),
                "the er clone must survive a one-transaction undelegate + \
                 redelegate"
            )?;
            tokio::time::sleep(CONTINUITY_STEP).await;
        }
        let wallet_on_base = base
            .account(&wallet.pubkey())
            .await?
            .ok_or("the cycled wallet vanished on base")?;
        check_eq!(
            wallet_on_base.owner,
            dlp::dlp_id(),
            "the wallet must be delegated again on base after the cycle"
        )?;
        check_eq!(
            er.api().get_balance(&wallet.pubkey()).await?,
            WALLET_LAMPORTS,
            "the er clone balance must be intact after the cycle"
        )?;
        er.send(
            &wallet,
            &[system::transfer(
                &wallet.pubkey(),
                &wallet.pubkey(),
                CYCLE_SPEND,
            )],
        )
        .await?;

        Ok(ScenarioReport::ok(self.name())
            .setting("account space", crate::ACCOUNT_SPACE)
            .setting("redelegation continuity window ms", 2_000u64)
            .metric(
                "fresh clone visibility ms",
                Unit::Millis,
                clone_visibility_ms,
            )
            .metric("base-to-er propagation ms", Unit::Millis, propagation_ms))
    }
}
