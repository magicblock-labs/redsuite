use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check,
    dlp::{self, commit_state, finalize, undelegate, CommitStateArgs},
    programs::{CellHeader, SettlementMode, REDLINE},
    system, topology, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;
use tokio::time;

use super::ER_WRITE;
use crate::{
    program::{layout, DELEGATION_PROGRAM_ID},
    ACCOUNT_SPACE, LOCKOUT_REJECTIONS, PAYER_LAMPORTS,
};

const PRE_CLONE_WRITE: u64 = 41;
const POST_CLONE_WRITE: u64 = 42;
const UNDELEGATED_WRITE: u64 = 44;
const SECOND_WRITE: u64 = 45;
const LOCKOUT_WRITE: u64 = 46;
const GHOST_LAMPORTS: u64 = 500_000_000;
const WALLET_LAMPORTS: u64 = 1_000_000_000;
const CYCLE_SPEND: u64 = 1_000;
const CONTINUITY_WINDOW: Duration = Duration::from_secs(2);
const CONTINUITY_STEP: Duration = Duration::from_millis(50);

pub struct CloneOnAccess {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
    identity: Pubkey,
}

fn untouched(data: &[u8]) -> bool {
    data.get(layout::DATA_OFFSET..)
        .is_some_and(|rest| rest.iter().all(|&byte| byte == 0))
}

async fn await_landed(
    base: &BaseCtx,
    pda: Pubkey,
    owner: Pubkey,
    data: &[u8],
    lamports: Option<u64>,
) -> Result<()> {
    base.api()
        .await_accounts(&[pda], "base account state", async || {
            let account = base.account(&pda).await?.context("base account missing")?;
            ensure!(account.owner == owner);
            ensure!(account.data == data);
            if let Some(lamports) = lamports {
                ensure!(account.lamports == lamports);
            }
            Ok(())
        })
        .await
}

impl Scenario for CloneOnAccess {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let identity = er.identity();

        Ok(Self { base, er, payer, identity })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er, payer, identity } = self;
        crate::phase("negative cache", async {
            let ghost = Keypair::new().pubkey();
            ensure!(er.account(&ghost).await?.is_none());
            base.airdrop(&ghost, GHOST_LAMPORTS).await?;
            er.api()
                .await_accounts(&[ghost], "negative cache invalidated", async || {
                    let account = er.account(&ghost).await?.context("ghost not cloned")?;
                    ensure!(account.lamports == GHOST_LAMPORTS);
                    Ok(())
                })
                .await?;
            Ok(())
        })
        .await?;

        crate::phase("plain clone", async {
            let plain = base.cells(REDLINE, &payer).create(ACCOUNT_SPACE, identity, 0).await?;
            base.cells(REDLINE, &payer).write(PRE_CLONE_WRITE, &[plain]).await?;
            let plain_clone = er
                .api()
                .await_accounts(&[plain], "plain account cloned", async || {
                    let account = er.account(&plain).await?.context("plain clone missing")?;
                    ensure!(account.data.len() == ACCOUNT_SPACE as usize);
                    Ok(account)
                })
                .await?;
            let plain_on_base = base.account(&plain).await?.context("plain gone on base")?;
            ensure!(plain_clone.lamports == plain_on_base.lamports);
            ensure!(plain_clone.owner == crate::program::id());
            ensure!(CellHeader::written_id(&plain_clone.data) == Some(PRE_CLONE_WRITE));
            base.cells(REDLINE, &payer).write(POST_CLONE_WRITE, &[plain]).await?;
            er.api()
                .await_accounts(&[plain], "base write propagated", async || {
                    let account = er.account(&plain).await?.context("plain clone missing")?;
                    ensure!(CellHeader::written_id(&account.data) == Some(POST_CLONE_WRITE));
                    Ok(())
                })
                .await?;
            let undelegated_write =
                er.cells(REDLINE, &payer).write(UNDELEGATED_WRITE, &[plain]).await;
            ensure!(undelegated_write.is_err());
            Ok(())
        })
        .await?;

        let (accounts, snapshot) = crate::phase("delegated write", async {
            let delegated =
                base.cells(REDLINE, &payer).create_delegated(ACCOUNT_SPACE, identity, 1).await?;
            let sibling =
                base.cells(REDLINE, &payer).create_delegated(ACCOUNT_SPACE, identity, 2).await?;
            let accounts = [delegated, sibling];
            for pda in &accounts {
                let on_base = base.account(pda).await?.context("delegated pda missing on base")?;
                ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
            }
            er.await_clones(&accounts, ACCOUNT_SPACE as usize).await?;
            let delegated_clone =
                er.account(&delegated).await?.context("delegated clone vanished")?;
            ensure!(delegated_clone.owner == crate::program::id());
            er.cells(REDLINE, &payer).write(ER_WRITE, &accounts).await?;
            let snapshot = er
                .account(&delegated)
                .await?
                .context("delegated clone vanished after the ER write")?
                .data;
            ensure!(CellHeader::written_id(&snapshot) == Some(ER_WRITE));
            for pda in &accounts {
                let on_base = base.account(pda).await?.context("delegated pda gone on base")?;
                ensure!(untouched(&on_base.data));
            }
            Ok((accounts, snapshot))
        })
        .await?;
        let [delegated, sibling] = accounts;

        crate::phase("commit", async {
            let signature = er
                .cells(REDLINE, &payer)
                .settle(1, &accounts[..1], SettlementMode::Commit)
                .await?;
            let receipt = er.check_commit(&base, &signature, &accounts[..1], false).await?;
            if !receipt.failure_is_duplicate_rejection() {
                ensure!(receipt.payer == Some(payer.pubkey()));
                ensure!(!receipt.base_signatures.is_empty());
            }
            await_landed(&base, delegated, DELEGATION_PROGRAM_ID, &snapshot, None).await?;
            let sibling_on_base =
                base.account(&sibling).await?.context("sibling pda gone on base")?;
            ensure!(untouched(&sibling_on_base.data));
            Ok(())
        })
        .await?;

        let wallet = base.delegated_payer(&payer, identity, WALLET_LAMPORTS).await?;

        crate::phase("undelegate", async {
            er.cells(REDLINE, &payer).write(SECOND_WRITE, &accounts).await?;
            let mut finals = Vec::new();
            for pda in &accounts {
                finals.push(
                    er.account(pda)
                        .await?
                        .context("er copy vanished before the undelegating commit")?,
                );
            }
            ensure!(CellHeader::written_id(&finals[0].data) == Some(SECOND_WRITE));
            let signature = er
                .cells(REDLINE, &payer)
                .settle(2, &accounts, SettlementMode::Undelegate)
                .await?;
            let receipt = er.check_commit(&base, &signature, &accounts, true).await?;
            if !receipt.failure_is_duplicate_rejection() {
                ensure!(!receipt.base_signatures.is_empty());
            }
            for (pda, expected) in accounts.iter().zip(&finals) {
                await_landed(
                    &base,
                    *pda,
                    crate::program::id(),
                    &expected.data,
                    Some(expected.lamports),
                )
                .await?;
            }
            er.api()
                .await_accounts(
                    &[delegated],
                    "the er re-clones the undelegated account with its base owner",
                    async || {
                        let account = er.account(&delegated).await?.context("clone missing")?;
                        ensure!(account.owner == crate::program::id());
                        Ok(())
                    },
                )
                .await?;
            let write_after_undelegate =
                er.cells(REDLINE, &wallet).write(LOCKOUT_WRITE, &[delegated]).await;
            let error =
                check::expect_error(write_after_undelegate, "ER write locked after undelegation")?;
            crate::rejection_code(
                "the write after undelegation",
                &LOCKOUT_REJECTIONS,
                &format!("{error:#}"),
            )?;
            Ok(())
        })
        .await?;

        crate::phase("redelegation continuity", async {
            er.api()
                .await_accounts(
                    &[wallet.pubkey()],
                    "the ER clones the delegated wallet at full balance",
                    async || {
                        let balance = er.api().get_balance(&wallet.pubkey()).await?;
                        ensure!(balance == WALLET_LAMPORTS);
                        Ok(())
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
                dlp::delegate_account(&payer.pubkey(), &wallet.pubkey(), &identity),
            ];
            base.submit_and_confirm_with(&payer, &[&validator, &wallet], &cycle).await?;
            let continuity_deadline = Instant::now() + CONTINUITY_WINDOW;
            while Instant::now() < continuity_deadline {
                ensure!(er.account(&wallet.pubkey()).await?.is_some());
                time::sleep(CONTINUITY_STEP).await;
            }
            let wallet_on_base = base
                .account(&wallet.pubkey())
                .await?
                .context("the cycled wallet vanished on base")?;
            ensure!(wallet_on_base.owner == dlp::dlp_id());
            ensure!(er.api().get_balance(&wallet.pubkey()).await? == WALLET_LAMPORTS);
            er.submit_and_confirm(
                &wallet,
                &[system::transfer(&wallet.pubkey(), &wallet.pubkey(), CYCLE_SPEND)],
            )
            .await?;
            Ok(())
        })
        .await
    }
}
