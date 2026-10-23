use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    dlp,
    netfault::{BaseProxies, RuleHandle, Selector},
    programs::{Action, CellHeader, CellId, IntentBundle, REDLINE, REDSHIFT},
    receipt::{self, CommitReceipt},
    topology::RestartConfig,
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use tokio::time;

use super::{hold_nonces, prove_landed, write_and_commit, PROXIED_GROUP};
use crate::{program::DELEGATION_PROGRAM_ID, ACCOUNT_SPACE, PAYER_LAMPORTS};

const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const BLACKOUT_WINDOW: Duration = Duration::from_secs(5);
const SETTLE_WINDOW: Duration = Duration::from_secs(3);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
const WARMUP_WRITE: u64 = 51;
const BLACKOUT_WRITE: u64 = 52;
const RECOVERY_WRITE: u64 = 53;
const RESTART_WRITE: u64 = 54;
const RESTART_RECOVERY_WRITE: u64 = 55;
const ACTION_ESCROW_LAMPORTS: u64 = 500_000_000;
const ACTION_COMPUTE_UNITS: u32 = 50_000;
const ACTION_COUNT: u8 = 1;

struct FaultedCommit<'a> {
    phase: &'a str,
    commit_id: u64,
    write: u64,
    restart: bool,
}

fn withhold_confirmations(proxies: &BaseProxies, signature: &Signature) -> [RuleHandle; 2] {
    [
        proxies
            .stall(Selector::method("getSignatureStatuses").http().response().signature(signature)),
        proxies.stall(
            Selector::method("signatureNotification")
                .ws()
                .notification()
                .signature(signature),
        ),
    ]
}

pub struct CommitExactlyOnce {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
    account: Pubkey,
}

impl Scenario for CommitExactlyOnce {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        er.proxies().context(PROXIED_GROUP)?;
        let identity = er.identity();
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let account =
            base.cells(REDLINE, &payer).create_delegated(ACCOUNT_SPACE, identity, 0).await?;
        let on_base = base.account(&account).await?.context("pda not on base")?;
        ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
        er.await_clones(&[account], ACCOUNT_SPACE as usize).await?;

        Ok(Self { base, er, payer, account })
    }

    async fn run(self) -> Result<()> {
        let proxies = self.er.proxies().context(PROXIED_GROUP)?;
        self.follow_up_commit(1, WARMUP_WRITE, "warm-up").await?;
        self.commit_blackout(
            proxies,
            FaultedCommit {
                phase: "blackout",
                commit_id: 2,
                write: BLACKOUT_WRITE,
                restart: false,
            },
        )
        .await?;
        self.follow_up_commit(3, RECOVERY_WRITE, "post-blackout").await?;
        self.action_blackout(proxies).await?;
        self.commit_blackout(
            proxies,
            FaultedCommit {
                phase: "restart",
                commit_id: 4,
                write: RESTART_WRITE,
                restart: true,
            },
        )
        .await?;
        self.follow_up_commit(5, RESTART_RECOVERY_WRITE, "post-restart").await?;

        super::scheduled_intents::run(&self.base, &self.er).await
    }
}

impl CommitExactlyOnce {
    async fn base_count(&self, cell: &Pubkey) -> Result<u64> {
        let account = self.base.account(cell).await?.context("the base cell is missing")?;
        Ok(CellHeader::try_decode(&account.data)?.value)
    }

    async fn hold_count(&self, cell: &Pubkey, expected: u64, window: Duration) -> Result<()> {
        let deadline = Instant::now() + window;
        loop {
            let count = self.base_count(cell).await?;
            ensure!(count == expected);
            if Instant::now() >= deadline {
                return Ok(());
            }
            time::sleep(SAMPLE_INTERVAL).await;
        }
    }

    async fn settled_receipt(
        &self,
        commit_signature: &Signature,
        restart_intent_id: Option<u64>,
        phase: &str,
    ) -> Result<CommitReceipt> {
        let receipt = if let Some(intent_id) = restart_intent_id {
            receipt::fetch_commit_receipt_by_intent(self.er.api(), &self.er.identity(), intent_id)
                .await?
        } else {
            receipt::fetch_commit_receipt(self.er.api(), commit_signature).await?
        };
        ensure!(!receipt.failure_is_duplicate_rejection());
        receipt.check_success(phase)?;
        receipt.confirm(self.base.api()).await?;
        Ok(receipt)
    }

    async fn commit_blackout(
        &self,
        proxies: &BaseProxies,
        faulted: FaultedCommit<'_>,
    ) -> Result<()> {
        let FaultedCommit { phase, commit_id, write, restart } = faulted;
        let nonce_before = crate::last_commit_id(self.base.as_ref(), &self.account).await?;
        let submission = proxies.intercept(
            Selector::method("sendTransaction").http().response().account(&self.account),
        );
        let (snapshot, commit_signature) =
            write_and_commit(&self.er, &self.payer, commit_id, write, &self.account).await?;
        let held = submission.wait(INTERCEPT_TIMEOUT).await?;
        let landed = held.operation.signature()?;
        prove_landed(self.base.as_ref(), &landed, &self.account, &snapshot).await?;
        let nonce = crate::last_commit_id(self.base.as_ref(), &self.account).await?;
        ensure!(nonce == (nonce_before + 1));
        let restart_intent_id = if restart {
            Some(receipt::scheduled_intent_id(self.er.api(), &commit_signature).await?)
        } else {
            None
        };
        let confirmations = withhold_confirmations(proxies, &landed);
        if restart {
            self.er.restart(RestartConfig::default()).await?;
        }
        held.discard();
        // The base transaction already landed and advanced the DLP nonce. Recovery
        // must reconcile that landed transaction instead of producing another base
        // effect for the same intent.
        let settled = nonce;
        hold_nonces(
            self.base.as_ref(),
            &[self.account],
            &[settled],
            BLACKOUT_WINDOW,
        )
        .await?;
        for rule in confirmations {
            rule.remove();
        }
        let receipt = self.settled_receipt(&commit_signature, restart_intent_id, phase).await?;
        ensure!(receipt.base_signatures.contains(&landed));
        hold_nonces(
            self.base.as_ref(),
            &[self.account],
            &[settled],
            SETTLE_WINDOW,
        )
        .await?;
        let on_base = self
            .base
            .account(&self.account)
            .await?
            .context("the delegated pda vanished from base")?;
        ensure!(on_base.data == snapshot);
        Ok(())
    }

    async fn follow_up_commit(&self, commit_id: u64, write: u64, phase: &str) -> Result<()> {
        let account = self.account;

        let nonce_before = crate::last_commit_id(self.base.as_ref(), &account).await?;
        let (snapshot, commit_signature) =
            write_and_commit(&self.er, &self.payer, commit_id, write, &account).await?;
        self.settled_receipt(&commit_signature, None, phase).await?;
        self.base
            .api()
            .await_accounts(
                &[account],
                "the base copy matches the er snapshot after the follow-up commit",
                |accounts| {
                    let account = accounts[0].as_ref().context("base account missing")?;
                    ensure!(account.data == snapshot);
                    Ok(())
                },
            )
            .await?;
        let nonce = crate::last_commit_id(self.base.as_ref(), &account).await?;
        ensure!(nonce == (nonce_before + 1));
        Ok(())
    }

    async fn action_blackout(&self, proxies: &BaseProxies) -> Result<()> {
        let payer = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let (init, cell) = REDSHIFT.create(
            payer.pubkey(),
            CellId::new(payer.pubkey(), 0),
            CellHeader::SIZE as u32,
        );
        self.base
            .submit_and_confirm(
                &payer,
                &[init, dlp::top_up_ephemeral_balance(&payer.pubkey(), ACTION_ESCROW_LAMPORTS, 1)],
            )
            .await?;
        ensure!(self.base_count(&cell).await? == 0);
        let submission =
            proxies.intercept(Selector::method("sendTransaction").http().response().account(&cell));
        let action = REDSHIFT
            .batch()
            .add(&[cell], i64::from(ACTION_COUNT))
            .action(cell, REDSHIFT.id());
        let mut bundle = IntentBundle::new(payer.pubkey());
        bundle.actions.push(Action::new(payer.pubkey(), action, ACTION_COMPUTE_UNITS));
        let intent_signature = self
            .er
            .submit_and_confirm(&payer, &[REDSHIFT.batch().intents(bundle).build()])
            .await?;
        let held = submission.wait(INTERCEPT_TIMEOUT).await?;
        let landed = held.operation.signature()?;
        let base_tx = self.base.api().await_transaction(&landed).await?;
        let meta = transaction_meta(&base_tx)?;
        ensure!(meta.err == None);
        self.base
            .api()
            .await_accounts(
                &[cell],
                "the base cell shows the action once before the fault is applied",
                |accounts| {
                    let account = accounts[0].as_ref().context("base cell missing")?;
                    ensure!(CellHeader::try_decode(&account.data)?.value == 1);
                    Ok(())
                },
            )
            .await?;
        let confirmations = withhold_confirmations(proxies, &landed);
        held.discard();
        self.hold_count(&cell, 1, BLACKOUT_WINDOW).await?;
        for rule in confirmations {
            rule.remove();
        }
        let receipt = receipt::fetch_commit_receipt(self.er.api(), &intent_signature).await?;
        if receipt.succeeded() {
            ensure!(!receipt.base_signatures.is_empty());
            receipt.confirm(self.base.api()).await?;
        }
        self.hold_count(&cell, 1, SETTLE_WINDOW).await?;
        Ok(())
    }
}
