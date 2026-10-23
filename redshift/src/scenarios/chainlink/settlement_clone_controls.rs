use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    dlp::{self, commit_state, finalize, undelegate, CommitStateArgs},
    programs::{CellHeader, SettlementMode, REDLINE},
    receipt::CommitWatch,
    system, topology, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;
use tokio::time;
use tracing::instrument;

use crate::{
    program::{layout, DELEGATION_PROGRAM_ID},
    ACCOUNT_SPACE, PAYER_LAMPORTS,
};

const ER_WRITE: u64 = 700;
const WALLET_LAMPORTS: u64 = 1_000_000_000;
const CYCLE_SPEND: u64 = 1_000;
const CONTINUITY_WINDOW: Duration = Duration::from_secs(2);
const CONTINUITY_STEP: Duration = Duration::from_millis(50);

/// Checks selective snapshot settlement and atomic base-chain redelegation continuity.
pub struct SettlementCloneControls {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
    identity: Pubkey,
}

fn untouched(data: &[u8]) -> bool {
    data.get(layout::DATA_OFFSET..)
        .is_some_and(|rest| rest.iter().all(|&byte| byte == 0))
}

impl Scenario for SettlementCloneControls {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let identity = er.identity();

        Ok(Self { base, er, payer, identity })
    }

    async fn run(self) -> Result<()> {
        let (accounts, snapshot) = self.write_delegated().await?;

        self.commit(&accounts, &snapshot).await?;

        let wallet = self.base.delegated_payer(&self.payer, self.identity, WALLET_LAMPORTS).await?;

        self.check_redelegation(&wallet).await
    }
}

impl SettlementCloneControls {
    #[instrument(skip_all, err)]
    async fn write_delegated(&self) -> Result<([Pubkey; 2], Vec<u8>)> {
        let delegated = self
            .base
            .cells(REDLINE, &self.payer)
            .create_delegated(ACCOUNT_SPACE, self.identity, 1)
            .await?;
        let sibling = self
            .base
            .cells(REDLINE, &self.payer)
            .create_delegated(ACCOUNT_SPACE, self.identity, 2)
            .await?;
        let accounts = [delegated, sibling];
        for pda in &accounts {
            let on_base = self.base.account(pda).await?.context("delegated pda missing on base")?;
            ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
        }
        self.er.await_clones(&accounts, ACCOUNT_SPACE as usize).await?;
        let delegated_clone =
            self.er.account(&delegated).await?.context("delegated clone vanished")?;
        ensure!(delegated_clone.owner == crate::program::id());
        self.er.cells(REDLINE, &self.payer).write(ER_WRITE, &accounts).await?;
        let snapshot = self
            .er
            .account(&delegated)
            .await?
            .context("delegated clone vanished after the ER write")?
            .data;
        ensure!(CellHeader::written_id(&snapshot) == Some(ER_WRITE));
        for pda in &accounts {
            let on_base = self.base.account(pda).await?.context("delegated pda gone on base")?;
            ensure!(untouched(&on_base.data));
        }
        Ok((accounts, snapshot))
    }

    #[instrument(skip_all, err)]
    async fn commit(&self, accounts: &[Pubkey; 2], snapshot: &[u8]) -> Result<()> {
        let [delegated, sibling] = *accounts;
        let receipt = CommitWatch::new(&self.er, &self.payer.pubkey()).await?;
        let signature = self
            .er
            .cells(REDLINE, &self.payer)
            .settle(1, &accounts[..1], SettlementMode::Commit)
            .await?;
        let receipt = receipt.check(&self.base, &signature, &accounts[..1], false).await?;
        if !receipt.failure_is_duplicate_rejection() {
            ensure!(receipt.payer == Some(self.payer.pubkey()));
            ensure!(!receipt.base_signatures.is_empty());
        }
        self.await_landed(delegated, DELEGATION_PROGRAM_ID, snapshot, None).await?;
        let sibling_on_base =
            self.base.account(&sibling).await?.context("sibling pda gone on base")?;
        ensure!(untouched(&sibling_on_base.data));
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_redelegation(&self, wallet: &Keypair) -> Result<()> {
        self.er
            .api()
            .await_accounts(
                &[wallet.pubkey()],
                "the ER clones the delegated wallet at full balance",
                |accounts| {
                    let balance = accounts[0].as_ref().context("wallet missing")?.lamports;
                    ensure!(balance == WALLET_LAMPORTS);
                    Ok(())
                },
            )
            .await?;
        let validator = topology::identity_keypair(&self.identity)?;
        let cycle = [
            commit_state(
                &self.identity,
                &wallet.pubkey(),
                &system::system_id(),
                CommitStateArgs {
                    nonce: 1,
                    lamports: WALLET_LAMPORTS,
                    allow_undelegation: true,
                    data: vec![],
                },
            ),
            finalize(&self.identity, &wallet.pubkey()),
            undelegate(
                &self.identity,
                &wallet.pubkey(),
                &system::system_id(),
                &self.payer.pubkey(),
            ),
            system::assign(&wallet.pubkey(), &dlp::dlp_id()),
            dlp::delegate_account(&self.payer.pubkey(), &wallet.pubkey(), &self.identity),
        ];
        self.base
            .submit_and_confirm_with(&self.payer, &[&validator, wallet], &cycle)
            .await?;
        let continuity_deadline = Instant::now() + CONTINUITY_WINDOW;
        while Instant::now() < continuity_deadline {
            ensure!(self.er.account(&wallet.pubkey()).await?.is_some());
            time::sleep(CONTINUITY_STEP).await;
        }
        let wallet_on_base = self
            .base
            .account(&wallet.pubkey())
            .await?
            .context("the cycled wallet vanished on base")?;
        ensure!(wallet_on_base.owner == dlp::dlp_id());
        ensure!(self.er.api().get_balance(&wallet.pubkey()).await? == WALLET_LAMPORTS);
        self.er
            .submit_and_confirm(
                wallet,
                &[system::transfer(&wallet.pubkey(), &wallet.pubkey(), CYCLE_SPEND)],
            )
            .await?;
        Ok(())
    }

    async fn await_landed(
        &self,
        pda: Pubkey,
        owner: Pubkey,
        data: &[u8],
        lamports: Option<u64>,
    ) -> Result<()> {
        self.base
            .api()
            .await_accounts(&[pda], "base account state", |accounts| {
                let account = accounts[0].as_ref().context("base account missing")?;
                ensure!(account.owner == owner);
                ensure!(account.data == data);
                if let Some(lamports) = lamports {
                    ensure!(account.lamports == lamports);
                }
                Ok(())
            })
            .await
    }
}

pub(crate) async fn run(base: Rc<BaseCtx>, er: ErCtx) -> Result<()> {
    SettlementCloneControls::setup(base, er, ()).await?.run().await
}
