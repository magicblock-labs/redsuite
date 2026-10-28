use std::rc::Rc;

use anyhow::{ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use magic_api::args::{CommitTypeArgs, MagicIntentBundleArgs};
use pubkey::Pubkey;
use redsuite_core::{
    api::{custom_error_code, transaction_meta},
    dlp, prep,
    programs::{
        Action, CellHeader, CellId, Commit, IntentBundle, Refund, ResultCallback, SettlementMode,
        MAGIC, REDSHIFT,
    },
    receipt, system, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use tracing::instrument;

use crate::PAYER_LAMPORTS;

const PAID_COMMIT_LIMIT: u64 = 25;
const COMMIT_FEE_LAMPORTS: u64 = 1_000_000;
const ACTION_FEE: u64 = 5_000;
const CALLBACK_FEE: u64 = 5_000;
const TRANSFER_LAMPORTS: u64 = 1_000_000;
const ACTION_COMPUTE_UNITS: u32 = 100_000;
const ACTORS: usize = 3;

#[derive(Clone, Copy)]
enum Mode {
    Commit,
    Undelegate(i64),
    Finalize,
}

fn intents(
    authorities: &[Pubkey],
    validator: Pubkey,
    destination: Pubkey,
    modes: &[Mode],
    actions: bool,
) -> Instruction {
    let cells = REDSHIFT.primaries(authorities);
    if matches!(modes, [Mode::Commit, Mode::Finalize]) {
        let args = MagicIntentBundleArgs {
            commit: Some(CommitTypeArgs::Standalone(vec![3])),
            commit_finalize: Some(CommitTypeArgs::Standalone(vec![4])),
            ..Default::default()
        };
        return REDSHIFT
            .batch()
            .require_signer(authorities[1])
            .invoke(MAGIC.intents(
                authorities[0],
                dlp::magic_fee_vault_pda(&validator),
                &cells,
                args,
            ))
            .build();
    }
    let mut bundle = IntentBundle::new(authorities[0]);
    bundle.validator = Some(validator);
    for ((&authority, &cell), &mode) in authorities.iter().zip(&cells).zip(modes) {
        let settlement = match mode {
            Mode::Undelegate(_) => SettlementMode::Undelegate,
            _ => SettlementMode::Commit,
        };
        let mut commit = Commit::new(settlement, &[cell]);
        if actions {
            let transfer = REDSHIFT
                .batch()
                .transfer_from_escrow(destination, TRANSFER_LAMPORTS)
                .action(cell, dlp::dlp_id());
            commit.after_commit.push(Action::new(authority, transfer, ACTION_COMPUTE_UNITS));
            if let Mode::Undelegate(change) = mode {
                let transfer = REDSHIFT
                    .batch()
                    .add(&[cell], change)
                    .transfer_from_escrow(destination, TRANSFER_LAMPORTS)
                    .action(cell, REDSHIFT.id());
                commit.after_undelegate.push(Action::new(
                    authority,
                    transfer,
                    ACTION_COMPUTE_UNITS,
                ));
            }
        }
        bundle.commits.push(commit);
    }
    REDSHIFT.batch().intents(bundle).build()
}

fn transfer_with_refund(
    authority: Pubkey,
    validator: Pubkey,
    destination: Pubkey,
    amount: u64,
    fail: bool,
) -> Instruction {
    let cell = REDSHIFT.primary(authority);
    let mut action = REDSHIFT.batch();
    if fail {
        action = action.fail(0xFA11);
    }
    let transfer = action.transfer_from_escrow(destination, amount).action(cell, dlp::dlp_id());
    let callback = ResultCallback {
        cell,
        refund: Some(Refund { recipient: authority, amount }),
        compute_units: 20_000,
    };
    let mut commit = Commit::new(SettlementMode::Commit, &[cell]);
    commit
        .after_commit
        .push(Action::new(authority, transfer, ACTION_COMPUTE_UNITS).then(callback));
    let mut bundle = IntentBundle::new(authority);
    bundle.validator = Some(validator);
    bundle.commits.push(commit);
    REDSHIFT
        .batch()
        .invoke(system::transfer(&authority, &cell, amount))
        .intents(bundle)
        .build()
}

struct IntentFixture<'a> {
    base: &'a BaseCtx,
    er: &'a ErCtx,
    funder: Keypair,
    actors: Vec<Actor>,
}

struct Actor {
    payer: Keypair,
    cell: Pubkey,
    expected: CellHeader,
    delegated: bool,
}

impl<'a> IntentFixture<'a> {
    async fn delegate_payer(&self, payer: &Keypair) -> Result<()> {
        self.base.delegate_payer(&self.funder, payer, self.er.identity()).await?;
        self.er.await_delegated(&payer.pubkey(), "local delegation").await
    }

    async fn check_settled(
        &self,
        signature: &Signature,
        accounts: &[Pubkey],
        undelegates: bool,
    ) -> Result<()> {
        let receipt = self.er.check_commit(self.base, signature, accounts, undelegates).await?;
        receipt.check_success("scheduled intent")
    }
    #[instrument(skip_all, err, fields(case = label))]
    async fn check_bundle(
        &mut self,
        label: &str,
        modes: &[Mode],
        additions: &[i32],
        actions: bool,
    ) -> Result<()> {
        use Mode::Undelegate;

        let n = modes.len();
        let destination = Keypair::new().pubkey();
        for (i, &addition) in additions[..n].iter().enumerate() {
            if !self.actors[i].delegated {
                self.redelegate(&self.actors[i].payer, label).await?;
                self.actors[i].delegated = true;
            }
            self.er.await_delegated(&self.actors[i].cell, "local delegation").await?;
            let actor = &mut self.actors[i];
            let key = actor.payer.pubkey();
            self.er
                .api()
                .await_accounts(
                    &[actor.cell],
                    &format!("{} restored for {label}", actor.cell),
                    |accounts| {
                        let account = accounts[0].as_ref().context("ER cell missing")?;
                        ensure!(account.owner == redshift_interface::ID);
                        ensure!(CellHeader::try_decode(&account.data)? == actor.expected);
                        Ok(())
                    },
                )
                .await?;
            if addition != 0 {
                self.er
                    .submit_and_confirm(
                        &actor.payer,
                        &[REDSHIFT
                            .batch()
                            .add(&[actor.cell], i64::from(addition))
                            .require_signer(key)
                            .build()],
                    )
                    .await?;
                actor.expected.value += addition as u64;
                actor.expected.revision += 1;
            }
        }
        let actors = &self.actors[..n];
        let keys: [_; ACTORS] = std::array::from_fn(|i| self.actors[i].payer.pubkey());
        let cells: [_; ACTORS] = std::array::from_fn(|i| self.actors[i].cell);
        let signature = self
            .er
            .submit_and_confirm_with(
                &actors[0].payer,
                &actors[1..].iter().map(|actor| &actor.payer).collect::<Vec<_>>(),
                &[intents(&keys[..n], self.er.identity(), destination, modes, actions)],
            )
            .await?;
        let undelegates = modes.iter().any(|mode| matches!(mode, Undelegate(_)));
        self.check_settled(&signature, &cells[..n], undelegates).await?;
        let mut transfers = 0;
        for (actor, mode) in self.actors.iter_mut().zip(modes) {
            transfers += usize::from(actions);
            if let Undelegate(change) = mode {
                actor.expected.value = actor
                    .expected
                    .value
                    .checked_add_signed(*change)
                    .context("cell model underflow")?;
                actor.expected.revision += 1;
                actor.delegated = false;
                transfers += 1;
            }
            let account =
                self.base.account(&actor.cell).await?.context("committed cell missing")?;
            ensure!(CellHeader::try_decode(&account.data)? == actor.expected);
            ensure!(
                account.owner
                    == if actor.delegated { dlp::dlp_id() } else { redshift_interface::ID }
            );
        }
        ensure!(
            self.base.api().get_balance(&destination).await?
                == (transfers as u64 * TRANSFER_LAMPORTS)
        );
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_sponsored_quota(&self) -> Result<()> {
        use Mode::{Commit, Undelegate};
        let actor = &self.actors[0];
        let key = actor.payer.pubkey();
        let payer = &actor.payer;
        let destination = self.actors[1].payer.pubkey();
        let initial_nonce = crate::last_commit_id(self.base, &actor.cell).await?;
        ensure!(initial_nonce < receipt::SPONSORED_COMMIT_LIMIT);
        for nonce in initial_nonce..receipt::SPONSORED_COMMIT_LIMIT {
            let signature = self.increment_and_commit(nonce, destination, false).await?;
            self.check_settled(&signature, &[actor.cell], false).await?;
            ensure!(crate::last_commit_id(self.base, &actor.cell).await? == (nonce + 1));
        }
        let before = self.er.account(&actor.cell).await?;
        let signature = self
            .er
            .sender(Rc::new(prep::payer_from_bytes(&payer.to_bytes())))
            .submit(&[
                REDSHIFT.batch().add(&[actor.cell], 99).require_signer(key).build(),
                intents(&[key], self.er.identity(), destination, &[Commit], false),
            ])
            .await?;
        let tx = self.er.api().await_transaction(&signature).await?;
        ensure!(
            transaction_meta(&tx)?.err.clone().and_then(custom_error_code)
                == Some(receipt::COMMIT_LIMIT_ERR)
        );
        ensure!(self.er.account(&actor.cell).await? == before);
        let signature = self
            .er
            .submit_and_confirm(
                payer,
                &[intents(
                    &[key],
                    self.er.identity(),
                    destination,
                    &[Undelegate(0)],
                    false,
                )],
            )
            .await?;
        self.check_settled(&signature, &[actor.cell], true).await?;
        let account = self.base.account(&actor.cell).await?.context("undelegated cell missing")?;
        ensure!(account.owner == redshift_interface::ID);
        ensure!(account.data == before.context("ER cell missing")?.data);
        self.redelegate(payer, "after sponsored quota").await?;
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_paid_threshold(&self) -> Result<()> {
        let actor = &self.actors[0];
        let key = actor.payer.pubkey();
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        let initial_nonce = crate::last_commit_id(self.base, &actor.cell).await?;
        ensure!(initial_nonce < PAID_COMMIT_LIMIT);
        for nonce in initial_nonce..=PAID_COMMIT_LIMIT + 1 {
            let actions = nonce == PAID_COMMIT_LIMIT + 1;
            let destination = Keypair::new().pubkey();
            let payer_before = self.er.api().get_balance(&key).await?;
            let vault_before = self.er.api().get_balance(&vault).await?;
            let signature = self.increment_and_commit(nonce, destination, actions).await?;
            self.check_settled(&signature, &[actor.cell], false).await?;
            let fee = if nonce >= PAID_COMMIT_LIMIT { COMMIT_FEE_LAMPORTS } else { 0 }
                + if actions { ACTION_FEE } else { 0 };
            ensure!(self.er.api().get_balance(&key).await? == (payer_before - fee));
            ensure!(self.er.api().get_balance(&vault).await? == (vault_before + fee));
            ensure!(crate::last_commit_id(self.base, &actor.cell).await? == (nonce + 1));
            ensure!(
                self.base.api().get_balance(&destination).await?
                    == if actions { TRANSFER_LAMPORTS } else { 0 }
            );
        }
        Ok(())
    }

    async fn increment_and_commit(
        &self,
        nonce: u64,
        destination: Pubkey,
        actions: bool,
    ) -> Result<Signature> {
        let actor = &self.actors[0];
        let key = actor.payer.pubkey();
        self.er
            .submit_and_confirm(
                &actor.payer,
                &[
                    REDSHIFT
                        .batch()
                        .add(&[actor.cell], i64::from(nonce as u8 + 1))
                        .require_signer(key)
                        .build(),
                    intents(
                        &[key],
                        self.er.identity(),
                        destination,
                        &[Mode::Commit],
                        actions,
                    ),
                ],
            )
            .await
    }

    #[instrument(skip_all, err, fields(fail = fail))]
    async fn check_callback(&self, fail: bool) -> Result<()> {
        let actor = &self.actors[1];
        let key = actor.payer.pubkey();
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());

        const AMOUNT: u64 = 890_880;
        let transferred = if fail { 0 } else { AMOUNT };
        let fee = ACTION_FEE + CALLBACK_FEE;
        let destination = Keypair::new().pubkey();
        let before = self.er.account(&actor.cell).await?.context("transfer cell missing")?;
        let state = CellHeader::try_decode(&before.data)?;
        let payer_before = self.er.api().get_balance(&key).await?;
        let vault_before = self.er.api().get_balance(&vault).await?;
        let signature = self
            .er
            .submit_and_confirm(
                &actor.payer,
                &[transfer_with_refund(key, self.er.identity(), destination, AMOUNT, fail)],
            )
            .await?;
        self.check_settled(&signature, &[actor.cell], false).await?;
        self.er
            .api()
            .await_accounts(
                &[actor.cell, key],
                "callback completes exactly once",
                |accounts| {
                    let account = accounts[0].as_ref().context("callback cell missing")?;
                    let actual = CellHeader::try_decode(&account.data)?;
                    ensure!(actual.revision == (state.revision + 1));
                    ensure!(actual.value == (state.value + u64::from(fail)));
                    ensure!(account.lamports == (before.lamports + transferred));
                    ensure!(
                        accounts[1].as_ref().context("callback payer missing")?.lamports
                            == (payer_before - fee - transferred)
                    );
                    Ok(())
                },
            )
            .await?;
        ensure!(self.base.api().get_balance(&destination).await? == transferred);
        ensure!(self.er.api().get_balance(&vault).await? == (vault_before + fee));
        Ok(())
    }

    async fn prepare(base: &'a BaseCtx, er: &'a ErCtx) -> Result<Self> {
        let funder = base.funded_payer(PAYER_LAMPORTS).await?;
        let payers = base.funded_payers(ACTORS, PAYER_LAMPORTS).await?;
        let mut actors = Vec::with_capacity(payers.len());
        for payer in payers {
            let (cell, [create, delegate]) = base
                .cells(REDSHIFT, &payer)
                .prepare_delegated(er.identity(), prep::COMMIT_FREQUENCY_MS);
            base.submit_and_confirm(
                &payer,
                &[create, delegate, dlp::top_up_ephemeral_balance(&payer.pubkey(), 500_000_000, 1)],
            )
            .await?;
            let expected =
                CellHeader::try_decode(&base.account(&cell).await?.context("cell missing")?.data)?;
            actors.push(Actor {
                payer,
                cell,
                expected,
                delegated: true,
            });
        }
        Ok(Self { base, er, funder, actors })
    }

    async fn redelegate(&self, payer: &Keypair, phase: &str) -> Result<()> {
        self.base
            .submit_and_confirm(
                payer,
                &[
                    REDSHIFT.delegate(
                        payer.pubkey(),
                        CellId::new(payer.pubkey(), 0),
                        Some(self.er.identity()),
                        prep::COMMIT_FREQUENCY_MS,
                    ),
                    REDSHIFT.log(phase),
                ],
            )
            .await?;
        Ok(())
    }
}

/// Verifies action execution, session quotas, fees, and callback outcomes.
pub struct IntentExecutionAndAccounting {
    base: Rc<BaseCtx>,
    er: ErCtx,
}
impl Scenario for IntentExecutionAndAccounting {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }
    async fn run(self) -> Result<()> {
        let mut suite = IntentFixture::prepare(&self.base, &self.er).await?;
        use Mode::{Commit, Finalize, Undelegate};
        for (label, modes, additions, actions) in [
            (
                "two-payer undelegate actions",
                &[Undelegate(-50), Undelegate(25)][..],
                &[100, 200][..],
                true,
            ),
            ("action after redelegation", &[Undelegate(102)], &[0], true),
            (
                "mixed commit/undelegate bundle",
                &[Commit, Commit, Undelegate(-25)],
                &[0, 0, 200],
                true,
            ),
            (
                "same-session two-payer commit actions",
                &[Commit, Commit],
                &[2, 0],
                true,
            ),
            (
                "mixed commit/finalize bundle",
                &[Commit, Finalize],
                &[0, 0],
                false,
            ),
        ] {
            suite.check_bundle(label, modes, additions, actions).await?;
        }

        suite.check_sponsored_quota().await?;
        suite.delegate_payer(&suite.actors[0].payer).await?;
        self.er.await_delegated(&suite.actors[0].cell, "local delegation").await?;
        suite.check_paid_threshold().await?;

        suite.delegate_payer(&suite.actors[1].payer).await?;
        for fail in [false, true] {
            suite.check_callback(fail).await?;
        }
        Ok(())
    }
}
