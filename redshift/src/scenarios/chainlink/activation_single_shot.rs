use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use account::Account;
use anyhow::{ensure, Context};
use instruction::AccountMeta;
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    dlp::{self, delegate_with_actions, DelegateArgs},
    netfault::{Faults, Selector, Transport},
    prep,
    programs::{CellHeader, REDSHIFT},
    system,
    topology::RestartConfig,
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use tokio::{
    task::{self, JoinHandle},
    time,
};

use crate::PAYER_LAMPORTS;

const AIRDROP: u64 = 2_000_000_000;
const DEPENDENCY_LAMPORTS: u64 = 1_000_000;
const ACTION_COUNT: u8 = 7;
const CONCURRENT_READS: usize = 16;
const CONCURRENT_SUBMISSIONS: usize = 8;
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(30);
const HOLD_WINDOW: Duration = Duration::from_millis(2_500);
const STEADY_WINDOW: Duration = Duration::from_secs(4);
const RECOVERY_WINDOW: Duration = Duration::from_secs(6);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(200);

/// Value and revision together distinguish a single action from repeated identical writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CellState {
    value: u64,
    revision: u64,
}

/// Competing client discovery paths kept alive while activation's dependency fetch is held.
struct Discovery {
    reads: Vec<JoinHandle<Result<Option<Account>>>>,
    submissions: Vec<JoinHandle<Result<Signature>>>,
}

/// One activation target and the actors needed to race its discovery and observe its effects.
struct Activation<'a> {
    base: &'a BaseCtx,
    faults: &'a Faults,
    er: &'a ErCtx,
    funder: &'a Keypair,
    er_payer: &'a Rc<Keypair>,
    cell: Pubkey,
}

impl Activation<'_> {
    async fn state(&self) -> Result<CellState> {
        let er = &self.er;
        let cell = &self.cell;
        let account = er.account(cell).await?.context("the cell vanished from the er")?;
        let decoded = CellHeader::try_decode(&account.data)?;
        Ok(CellState {
            value: decoded.value,
            revision: decoded.revision,
        })
    }

    async fn await_cell(&self, expected: CellState, what: &str) -> Result<()> {
        self.er
            .api()
            .await_accounts(&[self.cell], what, |accounts| {
                let account = accounts[0].as_ref().context("activation cell missing")?;
                let cell = CellHeader::try_decode(&account.data)?;
                ensure!(
                    CellState {
                        value: cell.value,
                        revision: cell.revision
                    } == expected
                );
                Ok(())
            })
            .await
    }
    /// Watches for delayed duplicate activation, not just the first correct value.
    async fn hold_steady(&self, expected: CellState, window: Duration) -> Result<()> {
        let started = Instant::now();
        while started.elapsed() < window {
            ensure!(self.state().await? == expected);
            time::sleep(POLL).await;
        }
        Ok(())
    }
    /// Delegates a trigger whose activation body either updates the cell or deliberately fails.
    async fn delegate_trigger(&self, dependency: &Pubkey, fail: bool) -> Result<Keypair> {
        let mut action = if fail {
            REDSHIFT.batch().writable(&[self.cell]).fail(0).build()
        } else {
            REDSHIFT.batch().add(&[self.cell], i64::from(ACTION_COUNT)).build()
        };
        action.accounts.push(AccountMeta::new_readonly(*dependency, false));
        let trigger = self.base.funded_payer(AIRDROP).await?;
        let delegate = delegate_with_actions(
            &self.funder.pubkey(),
            &trigger.pubkey(),
            None,
            DelegateArgs {
                commit_frequency_ms: u32::MAX,
                seeds: vec![],
                validator: Some(self.er.identity()),
            },
            &[action],
        );
        // Publish the owner change with its record so discovery sees both.
        self.base
            .submit_and_confirm_with(
                self.funder,
                &[&trigger],
                &[system::assign(&trigger.pubkey(), &dlp::dlp_id()), delegate],
            )
            .await?;
        Ok(trigger)
    }
    /// Races reads and submissions while a required dependency prevents activation completion.
    async fn race_discovery(&self, trigger: &Pubkey, before: CellState) -> Result<Discovery> {
        let started = Instant::now();
        let sender = self.er.sender(self.er_payer.clone());
        let trigger = *trigger;
        let mut reads = Vec::with_capacity(CONCURRENT_READS);
        for _ in 0..CONCURRENT_READS {
            let api = self.er.api().clone();
            reads.push(task::spawn_local(
                async move { api.get_account(&trigger).await },
            ));
        }
        let mut submissions = Vec::with_capacity(CONCURRENT_SUBMISSIONS);
        for _ in 0..CONCURRENT_SUBMISSIONS {
            let sender = sender.clone();
            submissions.push(task::spawn_local(async move {
                sender
                    .submit_fresh(&[system::transfer(&sender.payer().pubkey(), &trigger, 1)])
                    .await
            }));
        }
        self.base
            .submit_and_confirm(
                self.funder,
                &[system::transfer(&self.funder.pubkey(), &trigger, 1)],
            )
            .await?;
        while started.elapsed() < HOLD_WINDOW {
            ensure!(self.state().await? == before);
            time::sleep(POLL).await;
        }
        Ok(Discovery { reads, submissions })
    }

    /// Releases the blocked dependency, requiring one action effect or failed-activation rescue.
    async fn run(&self, fail: bool) -> Result<Pubkey> {
        let before = self.state().await?;
        let dependency = self.base.funded_payer(DEPENDENCY_LAMPORTS).await?.pubkey();
        let selector = Selector::methods(&["getMultipleAccounts", "getAccountInfo"])
            .http()
            .response()
            .account(&dependency);
        let stall = self.faults.stall(selector);
        let trigger = self.delegate_trigger(&dependency, fail).await?;
        stall.wait_hit(INTERCEPT_TIMEOUT).await?;
        let discovery = self.race_discovery(&trigger.pubkey(), before).await?;
        stall.remove();
        discovery.settle().await;
        if fail {
            self.base
                .api()
                .await_accounts(
                    &[trigger.pubkey()],
                    "the failed activation completes the rescue undelegation on base",
                    |accounts| {
                        let owner = accounts[0].as_ref().map(|account| account.owner);
                        ensure!(owner == Some(system::system_id()));
                        Ok(())
                    },
                )
                .await?;
            ensure!(self.state().await? == before);
        } else {
            let expected = CellState {
                value: before.value + u64::from(ACTION_COUNT),
                revision: before.revision + 1,
            };
            self.await_cell(
                expected,
                "the released activation applies the cell action exactly once",
            )
            .await?;
            ensure!(self.er.account(&trigger.pubkey()).await?.is_some());
        }
        Ok(trigger.pubkey())
    }
}

impl Discovery {
    /// Lets discovery probes settle without treating their individual transport
    /// results as the oracle.
    async fn settle(self) {
        for read in self.reads {
            let _ = time::timeout(SETTLE_TIMEOUT, read).await;
        }
        for submission in self.submissions {
            let _ = time::timeout(SETTLE_TIMEOUT, submission).await;
        }
    }
}

/// Checks single activation effects through notifications, reconnect, restart, and rescue.
/// Repeated discovery of a successful trigger must not replay its post-delegation action.
pub struct ActivationSingleShot {
    base: Rc<BaseCtx>,
    faults: Rc<Faults>,
    er: ErCtx,
}

impl Scenario for ActivationSingleShot {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let faults = er.faults()?;
        Ok(Self { base, faults, er })
    }

    /// Reobserves a completed trigger across reconnect/restart before probing failed-action rescue.
    async fn run(self) -> Result<()> {
        let er = &self.er;
        let funder = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let actor = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let er_payer =
            Rc::new(self.base.delegated_payer(&funder, er.identity(), PAYER_LAMPORTS).await?);

        let cell = self
            .base
            .cells(REDSHIFT, &actor)
            .create_primary(er.identity(), prep::COMMIT_FREQUENCY_MS)
            .await?;
        let activation = Activation {
            base: &self.base,
            faults: &self.faults,
            er,
            funder: &funder,
            er_payer: &er_payer,
            cell,
        };
        let zero = CellState { value: 0, revision: 0 };
        activation.await_cell(zero, "the er clones the delegated cell").await?;

        let trigger = activation.run(false).await?;
        let applied = CellState {
            value: u64::from(ACTION_COUNT),
            revision: 1,
        };
        activation.hold_steady(applied, STEADY_WINDOW).await?;

        self.base
            .submit_and_confirm(&funder, &[system::transfer(&funder.pubkey(), &trigger, 1)])
            .await?;
        self.faults.disconnect(Transport::Http);
        self.faults.disconnect(Transport::Ws);
        self.base
            .submit_and_confirm(&funder, &[system::transfer(&funder.pubkey(), &trigger, 1)])
            .await?;
        activation.hold_steady(applied, STEADY_WINDOW).await?;

        let restart = self.er.restart(RestartConfig::default()).await?;
        ensure!(restart.exit_code == Some(0));
        let er = &self.er;
        let activation = Activation {
            base: &self.base,
            faults: &self.faults,
            er,
            funder: &funder,
            er_payer: &er_payer,
            cell,
        };
        activation
            .await_cell(
                applied,
                "the restarted er serves the cell with the action applied",
            )
            .await?;
        self.base
            .submit_and_confirm(&funder, &[system::transfer(&funder.pubkey(), &trigger, 1)])
            .await?;
        activation.hold_steady(applied, RECOVERY_WINDOW).await?;

        activation.run(true).await?;
        activation.hold_steady(applied, STEADY_WINDOW).await?;

        self.faults.reset().await?;
        Ok(())
    }
}
