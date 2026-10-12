use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::dlp::{commit_state, finalize, undelegate, CommitStateArgs};
use redsuite_core::{
    check, check_eq, dlp, prep, system, topology, BaseCtx, ChainCtx, ErCtx,
    Result, Scenario,
};
use signer::Signer;

use crate::program::{instruction::build, layout, DELEGATION_PROGRAM_ID};

const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const PROPAGATION_TIMEOUT: Duration = Duration::from_secs(15);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(20);
const UNDELEGATE_TIMEOUT: Duration = Duration::from_secs(30);
const PRE_CLONE_WRITE: u64 = 41;
const POST_CLONE_WRITE: u64 = 42;
const ER_WRITE: u64 = 43;
const UNDELEGATED_WRITE: u64 = 44;
const SECOND_WRITE: u64 = 45;
const LOCKOUT_WRITE: u64 = 46;
const GHOST_LAMPORTS: u64 = 500_000_000;
const WALLET_LAMPORTS: u64 = 1_000_000_000;
const CYCLE_SPEND: u64 = 1_000;
const CONTINUITY_WINDOW: Duration = Duration::from_secs(2);
const CONTINUITY_STEP: Duration = Duration::from_millis(50);

pub struct CloneOnAccess;

fn untouched(data: &[u8]) -> bool {
    data.get(layout::DATA_OFFSET..)
        .is_some_and(|rest| rest.iter().all(|&byte| byte == 0))
}

fn head(bytes: &[u8]) -> String {
    format!("{:02x?}", &bytes[..48.min(bytes.len())])
}

async fn await_landed(
    base: &BaseCtx,
    pda: Pubkey,
    owner: Pubkey,
    data: &[u8],
    lamports: Option<u64>,
    timeout: Duration,
) -> Result<()> {
    check::poll_for(
        &format!("{pda} lands on base with the expected owner and bytes"),
        timeout,
        || async {
            match base.account(&pda).await {
                Ok(Some(acc))
                    if acc.owner == owner
                        && acc.data == data
                        && lamports.is_none_or(|lamports| {
                            acc.lamports == lamports
                        }) =>
                {
                    Ok(())
                }
                Ok(Some(acc)) => Err(format!(
                    "owner {} lamports {} data[..48] {}",
                    acc.owner,
                    acc.lamports,
                    head(&acc.data)
                )),
                Ok(None) => Err("absent".to_owned()),
                Err(error) => Err(format!("read failed: {error}")),
            }
        },
    )
    .await
    .map_err(|error| {
        error.expected(format!(
            "owner {owner} lamports {lamports:?} data[..48] {}",
            head(data)
        ))
    })?;
    Ok(())
}

#[async_trait(?Send)]
impl Scenario for CloneOnAccess {
    fn name(&self) -> &str {
        "redshift/clone_on_access"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<()> {
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let identity = er.identity();

        crate::phase("negative cache", async {
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
            Ok(())
        })
        .await?;

        crate::phase("plain clone", async {
            let plain = crate::init_account(base, &payer, 0, identity).await?;
            base.submit_and_confirm(
                &payer,
                &[build::simple_byte_set(PRE_CLONE_WRITE, &[plain])],
            )
            .await?;
            check::poll(
                "the ER clones the plain account on first access",
                CLONE_TIMEOUT,
                || async {
                    matches!(er.account(&plain).await, Ok(Some(clone)) if clone.data.len() == crate::ACCOUNT_SPACE as usize)
                },
            )
            .await?;
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
            base.submit_and_confirm(
                &payer,
                &[build::simple_byte_set(POST_CLONE_WRITE, &[plain])],
            )
            .await?;
            check::poll(
                "the base write propagates to the existing ER clone",
                PROPAGATION_TIMEOUT,
                || async {
                    matches!(er.account(&plain).await, Ok(Some(clone)) if crate::written_id(&clone.data) == Some(POST_CLONE_WRITE))
                },
            )
            .await?;
            let undelegated_write = er
                .submit_and_confirm(
                    &payer,
                    &[build::simple_byte_set(UNDELEGATED_WRITE, &[plain])],
                )
                .await;
            check!(
                undelegated_write.is_err(),
                "the ER must reject a write to an undelegated clone, got {undelegated_write:?}"
            )?;
            Ok(())
        })
        .await?;

        let (accounts, snapshot) = crate::phase("delegated write", async {
            let delegated =
                crate::init_delegated_account(base, &payer, 1, identity)
                    .await?;
            let sibling =
                crate::init_delegated_account(base, &payer, 2, identity)
                    .await?;
            let accounts = [delegated, sibling];
            for pda in &accounts {
                let on_base = base
                    .account(pda)
                    .await?
                    .ok_or("delegated pda missing on base")?;
                check_eq!(
                    on_base.owner,
                    DELEGATION_PROGRAM_ID,
                    "a delegated pda must be dlp-owned on base"
                )?;
            }
            prep::await_clones(
                er,
                &accounts,
                crate::ACCOUNT_SPACE as usize,
                CLONE_TIMEOUT,
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
            er.submit_and_confirm(
                &payer,
                &[build::simple_byte_set(ER_WRITE, &accounts)],
            )
            .await?;
            let snapshot = er
                .account(&delegated)
                .await?
                .ok_or("delegated clone vanished after the ER write")?
                .data;
            check_eq!(
                crate::written_id(&snapshot),
                Some(ER_WRITE),
                "the ER copy of a delegated account must accept program writes"
            )?;
            for pda in &accounts {
                let on_base =
                    base.account(pda).await?.ok_or("delegated pda gone on base")?;
                check!(
                    untouched(&on_base.data),
                    "ER writes must not reach the base copy without an explicit commit"
                )?;
            }
            Ok((accounts, snapshot))
        })
        .await?;
        let [delegated, sibling] = accounts;

        crate::phase("commit", async {
            let signature = er
                .submit_and_confirm(
                    &payer,
                    &[build::commit_accounts(
                        1,
                        payer.pubkey(),
                        &accounts[..1],
                    )],
                )
                .await?;
            let receipt = crate::assert_commit_receipt(
                base,
                er,
                &signature,
                &accounts[..1],
                false,
            )
            .await?;
            if !receipt.failure_is_duplicate_rejection() {
                check_eq!(
                    receipt.payer,
                    Some(payer.pubkey()),
                    "the receipt must name the scheduling payer"
                )?;
                check!(
                    !receipt.base_signatures.is_empty(),
                    "a commit receipt must name at least one base tx"
                )?;
            }
            await_landed(
                base,
                delegated,
                DELEGATION_PROGRAM_ID,
                &snapshot,
                None,
                BASE_STATE_TIMEOUT,
            )
            .await?;
            let sibling_on_base = base
                .account(&sibling)
                .await?
                .ok_or("sibling pda gone on base")?;
            check!(
                untouched(&sibling_on_base.data),
                "committing one account must not touch its sibling on base"
            )?;
            Ok(())
        })
        .await?;

        let wallet =
            prep::delegated_payer(base, &payer, identity, WALLET_LAMPORTS)
                .await?;

        crate::phase("undelegate", async {
            er.submit_and_confirm(
                &payer,
                &[build::simple_byte_set(SECOND_WRITE, &accounts)],
            )
            .await?;
            let mut finals = Vec::new();
            for pda in &accounts {
                finals.push(er.account(pda).await?.ok_or(
                    "er copy vanished before the undelegating commit",
                )?);
            }
            check_eq!(
                crate::written_id(&finals[0].data),
                Some(SECOND_WRITE),
                "the second er write must land before the undelegating commit"
            )?;
            let signature = er
                .submit_and_confirm(
                    &payer,
                    &[build::commit_and_undelegate_accounts(
                        2,
                        payer.pubkey(),
                        &accounts,
                    )],
                )
                .await?;
            let receipt = crate::assert_commit_receipt(
                base, er, &signature, &accounts, true,
            )
            .await?;
            if !receipt.failure_is_duplicate_rejection() {
                check!(
                    !receipt.base_signatures.is_empty(),
                    "an undelegating commit must name at least one base tx"
                )?;
            }
            for (pda, expected) in accounts.iter().zip(&finals) {
                await_landed(
                    base,
                    *pda,
                    crate::program::id(),
                    &expected.data,
                    Some(expected.lamports),
                    UNDELEGATE_TIMEOUT,
                )
                .await?;
            }
            check::poll(
                "the er re-clones the undelegated account with its base owner",
                UNDELEGATE_TIMEOUT,
                || async {
                    matches!(
                        er.account(&delegated).await,
                        Ok(Some(clone)) if clone.owner == crate::program::id()
                    )
                },
            )
            .await?;
            let write_after_undelegate = er
                .submit_and_confirm(
                    &wallet,
                    &[build::simple_byte_set(LOCKOUT_WRITE, &[delegated])],
                )
                .await;
            check!(
                write_after_undelegate.is_err(),
                "the ER copy of an undelegated account must reject writes \
                 (locked out after undelegation), got {write_after_undelegate:?}"
            )?;
            crate::rejection_code(
                "the write after undelegation",
                &crate::LOCKOUT_REJECTIONS,
                &format!("{:?}", write_after_undelegate.unwrap_err()),
            )?;
            Ok(())
        })
        .await?;

        crate::phase("redelegation continuity", async {
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
            let validator = topology::identity_keypair(&identity)?;
            let cycle = [
                commit_state(
                    &identity,
                    &wallet.pubkey(),
                    &system::system_id(),
                    CommitStateArgs {
                        nonce: 1,
                        lamports: WALLET_LAMPORTS,
                        allow_undelegation: true,
                        data: vec![],
                    },
                ),
                finalize(&identity, &wallet.pubkey()),
                undelegate(
                    &identity,
                    &wallet.pubkey(),
                    &system::system_id(),
                    &payer.pubkey(),
                ),
                system::assign(&wallet.pubkey(), &dlp::dlp_id()),
                dlp::delegate_account(
                    &payer.pubkey(),
                    &wallet.pubkey(),
                    &identity,
                ),
            ];
            base.submit_and_confirm_with(
                &payer,
                &[&validator, &wallet],
                &cycle,
            )
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
            er.submit_and_confirm(
                &wallet,
                &[system::transfer(
                    &wallet.pubkey(),
                    &wallet.pubkey(),
                    CYCLE_SPEND,
                )],
            )
            .await?;
            Ok(())
        })
        .await
    }
}
