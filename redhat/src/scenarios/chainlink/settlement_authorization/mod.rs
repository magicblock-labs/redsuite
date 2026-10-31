use std::rc::Rc;

use anyhow::{ensure, Context};
use instruction::{AccountMeta, Instruction};
use pubkey::Pubkey;

use rand::seq::SliceRandom;
use redsuite_core::{
    api::{transaction_logs, transaction_meta},
    dlp,
    programs::{CellHeader, CellId, SettlementMode, MAGIC, REDHAT, REDSHIFT},
    random::{self, Rng},
    receipt,
    rpc_types::response::TransactionError,
    system,
    transaction_types::UiTransactionError,
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, TxSender,
};
use signer::Signer;
use solana_instruction_error::InstructionError;

use crate::PAYER_LAMPORTS;

mod callbacks;
mod commits;
mod fee_roles;

const PROGRAM_ID_NOT_FOUND: &str = "failed to find parent program id";
const IMMUTABLE: &str = "Account is immutable";
const INVALID_ACCOUNT_OWNER: &str = "Invalid account owner";
const NEEDS_TO_BE_OWNED: &str = "needs to be owned by the invoking program";
const MIRRORED_INDEX: u8 = 2;

/// Checks settlement entrypoint authorization and the mapped-signing self-commit exception.
///
/// Covers CPI privilege aliases, fee-vault roles, ordered/buffered base commits, and
/// callback provenance. Refused operations must leave protected state unchanged;
/// successful controls prove that the intended settlement path remains usable.
pub struct SettlementAuthorization {
    base: Rc<BaseCtx>,
    er: ErCtx,
    sender: TxSender,
    pdas: Vec<Pubkey>,
    foreign: Pubkey,
    mirrored: Pubkey,
    roles: Roles,
    case_order: [usize; 13],
    commit_plan: commits::Plan,
    callback_plan: callbacks::Plan,
}

/// Expected execution error and log evidence for a single authorization probe.
struct Case<'a> {
    name: &'a str,
    instructions: &'a [Instruction],
    refusals: &'a [&'a [&'a str]],
    error: Option<InstructionError>,
}

/// Seeded alias positions that vary privilege propagation without changing the oracle.
struct Roles {
    target: usize,
    inspect_sibling: bool,
    aliases: usize,
    signer_alias: usize,
}

impl Scenario for SettlementAuthorization {
    /// Separates delegated, foreign-owned, and readonly-mirrored victims before probing.
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let mut plan = random::rng(
            base.config().seed,
            &["redhat", "settlement_authorization", "roles"],
            0,
        );
        let aliases = plan.gen_range(2..=3);
        let roles = Roles {
            target: plan.gen_range(0..2),
            inspect_sibling: plan.gen_range(0..2usize) != 0,
            aliases,
            signer_alias: plan.gen_range(0..aliases),
        };
        let mut case_order = std::array::from_fn(|index| index);
        case_order.shuffle(&mut random::rng(
            base.config().seed,
            &["redhat", "settlement_authorization", "order"],
            0,
        ));
        let commit_plan = commits::plan(base.config().seed)?;
        let callback_plan = callbacks::plan(base.config().seed);
        let mut rng = random::rng(
            base.config().seed,
            &["redhat", "settlement_authorization", "fixtures"],
            0,
        );
        let funder = base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let mut pdas = Vec::new();
        for index in 0..2 {
            pdas.push(
                base.cells(REDSHIFT, &funder)
                    .create_delegated(CellHeader::SIZE as u32, er.identity(), index)
                    .await?,
            );
        }
        er.await_clones(&pdas, CellHeader::SIZE).await?;
        let foreign = base
            .cells(REDHAT, &funder)
            .create_delegated(CellHeader::SIZE as u32, er.identity(), 0)
            .await?;
        er.await_clones(&[foreign], CellHeader::SIZE).await?;
        let mirrored = base
            .cells(REDSHIFT, &funder)
            .create(CellHeader::SIZE as u32, er.identity(), MIRRORED_INDEX)
            .await?;
        er.await_clones(&[mirrored], CellHeader::SIZE).await?;
        let mirror_local = er.account(&mirrored).await?.context("mirrored victim missing")?;
        let mirror_remote =
            base.account(&mirrored).await?.context("mirrored victim missing on base")?;
        ensure!(
            mirror_local.owner == REDSHIFT.id()
                && mirror_local.data == mirror_remote.data
                && mirror_local.lamports == mirror_remote.lamports,
            "mirrored victim is not a read-only clone of its base image"
        );

        // A plain (non-delegated) payer for the direct-invocation cells: the
        // committed accounts are the delegated PDAs, so the tx pays no ER fee
        // through the payer itself.
        let sender = er.sender(Rc::new(funder));
        Ok(Self {
            base,
            er,
            sender,
            pdas,
            foreign,
            mirrored,
            roles,
            case_order,
            commit_plan,
            callback_plan,
        })
    }

    /// Checks invocation privileges first, then settlement delivery and callback authority.
    async fn run(self) -> Result<()> {
        let payer = self.sender.payer().pubkey();
        let target = &self.pdas[self.roles.target..self.roles.target + 1];
        let aliases = vec![target[0]; self.roles.aliases];
        let mut readonly = REDSHIFT.batch();
        if self.roles.inspect_sibling {
            readonly = readonly.inspect(&[self.pdas[1 - self.roles.target]]);
        }
        let readonly_write = readonly.add(&aliases, 13).readonly(target).build();
        let mut signer_child = REDSHIFT.inspect(target);
        signer_child.accounts[0].is_signer = self.roles.signer_alias == 0;
        for index in 1..self.roles.aliases {
            signer_child.accounts.push(AccountMeta::new_readonly(
                target[0],
                index == self.roles.signer_alias,
            ));
        }
        let mut unsigned_cpi = REDSHIFT.batch().invoke(signer_child.clone()).build();
        for account in &mut unsigned_cpi.accounts {
            account.is_signer = false;
        }
        let nested = REDHAT
            .batch()
            .invoke(REDSHIFT.batch().add(&aliases, 13).build())
            .readonly(target)
            .build();
        let nested = REDHAT.batch().invoke(nested).readonly(target).build();
        let entered_nested = format!("Program {} invoke [2]", REDHAT.id());
        let mirror_aliases = vec![self.mirrored; self.roles.aliases];
        let mut mirror_write = REDSHIFT.batch();
        if self.roles.inspect_sibling {
            mirror_write = mirror_write.inspect(&[self.pdas[1 - self.roles.target]]);
        }
        let mirror_write = mirror_write.add(&mirror_aliases, 13).build();
        let mut mirror_child = REDSHIFT.batch().add(&mirror_aliases, 13).build();
        mirror_child.accounts[0].is_signer = self.roles.signer_alias == 0;
        for index in 1..self.roles.aliases {
            mirror_child.accounts.push(AccountMeta::new(
                self.mirrored,
                index == self.roles.signer_alias,
            ));
        }
        let mirror_cpi = REDSHIFT
            .batch()
            .invoke_signed(
                mirror_child,
                &[CellId::new(payer, u64::from(MIRRORED_INDEX))],
            )
            .build();
        let entered_mirror = format!("Program {} invoke [2]", REDSHIFT.id());
        let mut wrong_context = MAGIC.commit(payer, None, &self.pdas, SettlementMode::Commit);
        wrong_context.accounts[1].pubkey = self.foreign;
        let cases = [
            Case {
                name: "aliased PDA signer escalation",
                instructions: &[unsigned_cpi],
                refusals: &[&["signer privilege escalated"]],
                error: Some(InstructionError::PrivilegeEscalation),
            },
            Case {
                name: "readonly owner write",
                instructions: &[readonly_write],
                refusals: &[&["instruction modified data of a read-only account"]],
                error: Some(InstructionError::ReadonlyDataModified),
            },
            Case {
                name: "mirrored victim write",
                instructions: &[mirror_write],
                refusals: &[&[IMMUTABLE]],
                error: Some(InstructionError::Immutable),
            },
            Case {
                name: "mirrored victim through signed CPI",
                instructions: &[mirror_cpi],
                refusals: &[&[IMMUTABLE], &[&entered_mirror]],
                error: Some(InstructionError::Immutable),
            },
            Case {
                name: "nested writable upgrade",
                instructions: &[nested],
                refusals: &[&["writable privilege escalated"], &[&entered_nested]],
                error: Some(InstructionError::PrivilegeEscalation),
            },
            Case {
                name: "substituted magic context",
                instructions: &[REDSHIFT.batch().invoke(wrong_context).build()],
                refusals: &[&["invalid magic context account"]],
                error: Some(InstructionError::MissingAccount),
            },
            Case {
                name: "direct invocation",
                instructions: &[MAGIC.commit(payer, None, &self.pdas, SettlementMode::Commit)],
                refusals: &[&[PROGRAM_ID_NOT_FOUND]],
                error: None,
            },
            Case {
                name: "surrounding transfers",
                instructions: &[
                    system::transfer(&payer, &self.pdas[0], 1_000_000),
                    MAGIC.commit(payer, None, &self.pdas, SettlementMode::Commit),
                    system::transfer(&payer, &self.pdas[0], 2_000_000),
                ],
                refusals: &[&[PROGRAM_ID_NOT_FOUND, IMMUTABLE]],
                error: None,
            },
            Case {
                name: "sibling CPIs",
                instructions: &[REDHAT
                    .batch()
                    .invoke(REDSHIFT.settle(payer, &self.pdas, SettlementMode::Commit))
                    .invoke(MAGIC.commit(payer, None, &self.pdas, SettlementMode::Commit))
                    .build()],
                refusals: &[&[INVALID_ACCOUNT_OWNER], &[NEEDS_TO_BE_OWNED]],
                error: None,
            },
            Case {
                name: "nested CPI",
                instructions: &[REDSHIFT
                    .batch()
                    .invoke(
                        REDHAT
                            .batch()
                            .invoke(MAGIC.commit(payer, None, &self.pdas, SettlementMode::Commit))
                            .build(),
                    )
                    .build()],
                refusals: &[&[NEEDS_TO_BE_OWNED]],
                error: None,
            },
            Case {
                name: "mixed-owner commit",
                instructions: &[REDSHIFT.settle(
                    payer,
                    &[self.pdas[0], self.foreign],
                    SettlementMode::Commit,
                )],
                refusals: &[&[NEEDS_TO_BE_OWNED]],
                error: None,
            },
            Case {
                name: "mixed-owner undelegate",
                instructions: &[REDSHIFT.settle(
                    payer,
                    &[self.pdas[0], self.foreign],
                    SettlementMode::Undelegate,
                )],
                refusals: &[&[NEEDS_TO_BE_OWNED]],
                error: None,
            },
            Case {
                name: "malicious tail",
                instructions: &[
                    REDHAT.batch().build(),
                    REDSHIFT
                        .batch()
                        .add(&self.pdas, 1)
                        .settle(payer, &self.pdas, SettlementMode::Commit)
                        .build(),
                    REDHAT
                        .batch()
                        .invoke(MAGIC.commit(payer, None, &self.pdas, SettlementMode::Commit))
                        .build(),
                ],
                refusals: &[&[INVALID_ACCOUNT_OWNER], &[NEEDS_TO_BE_OWNED]],
                error: None,
            },
        ];
        let mut cases = cases.map(Some);
        let mut protected = self.pdas.clone();
        protected.extend([payer, magic_api::MAGIC_CONTEXT_PUBKEY, self.foreign, self.mirrored]);
        self.er.accounts(&protected).await?;
        let case_count = cases.len();
        for (
            case_index,
            Case {
                name: case,
                instructions,
                refusals,
                error: expected_error,
            },
        ) in self
            .case_order
            .map(|index| cases[index].take().expect("permission case missing from plan"))
            .into_iter()
            .enumerate()
        {
            let local = self.er.local_images(&protected).await?;
            ensure!(
                local.iter().all(Option::is_some),
                "#249: protected local image missing"
            );
            let base = self.base.accounts(&protected).await?;
            let signature = self.sender.submit(instructions).await?;
            let tx = self.er.api().await_transaction(&signature).await?;
            let error = transaction_meta(&tx)?
                .err
                .as_ref()
                .with_context(|| format!("{case}: attack unexpectedly succeeded"))?;
            if let Some(expected_error) = expected_error {
                let expected: UiTransactionError =
                    TransactionError::InstructionError(0, expected_error).into();
                ensure!(error == &expected, "{case}: {error:?} against {expected:?}");
            }
            let observed = format!(
                "{}\n{error:?}",
                transaction_logs(transaction_meta(&tx)?).join("\n")
            );
            // Every group must match; alternatives within a group are ORed.
            for alternatives in refusals {
                ensure!(alternatives.iter().any(|needle| observed.contains(needle)));
            }
            let after = self.er.local_images(&protected).await?;
            for (key, (before, after)) in protected.iter().zip(local.iter().zip(&after)) {
                ensure!(
                    before == after,
                    "{case}: local state changed for {key}: {before:?} -> {after:?}"
                );
            }
            ensure!(
                self.base.accounts(&protected).await? == base,
                "{case}: base state changed"
            );
            for (program, targets) in
                [(REDSHIFT, self.pdas.as_slice()), (REDHAT, std::slice::from_ref(&self.foreign))]
            {
                let publish = case_index % 2 == 1 || case_index + 1 == case_count;
                let mut write = program.batch().nonce(case_index as u64 + 1).add(targets, 1);
                if publish {
                    write = write.settle(payer, targets, SettlementMode::Commit);
                }
                let signature = self
                    .er
                    .submit_and_confirm(
                        self.sender.payer(),
                        &[program.batch().inspect(targets).build(), write.build()],
                    )
                    .await?;
                if publish {
                    self.er
                        .check_commit(&self.base, &signature, targets, false)
                        .await?
                        .check_success(case)?;
                }
                for key in targets {
                    let index = protected.iter().position(|tracked| tracked == key).unwrap();
                    let mut expected = local[index].clone().context("protected cell missing")?;
                    let mut header = CellHeader::try_decode(&expected.data)?;
                    header.value += 1;
                    header.revision += 1;
                    header.write(&mut expected.data)?;
                    let actual = self.er.local_images(&[*key]).await?;
                    ensure!(
                        actual == vec![Some(expected.clone())],
                        "{case}: owner control {key}: {actual:?} against {expected:?}"
                    );
                    let mut committed = base[index].clone().context("base cell missing")?;
                    if publish {
                        committed.data = expected.data;
                    }
                    ensure!(self.base.account(key).await? == Some(committed));
                }
            }
        }

        let mut expected = self
            .er
            .local_images(target)
            .await?
            .pop()
            .flatten()
            .context("alias control missing")?;
        let mut committed =
            self.base.account(&target[0]).await?.context("alias base control missing")?;
        let signature = self
            .er
            .submit_and_confirm(
                self.sender.payer(),
                &[REDSHIFT
                    .batch()
                    .invoke_signed(
                        signer_child,
                        &[CellId::new(payer, self.roles.target as u64)],
                    )
                    .add(&aliases, 1)
                    .settle(payer, target, SettlementMode::Commit)
                    .build()],
            )
            .await?;
        self.er
            .check_commit(&self.base, &signature, target, false)
            .await?
            .check_success("repaired alias and signer permissions")?;
        let mut header = CellHeader::try_decode(&expected.data)?;
        header.value += aliases.len() as u64;
        header.revision += aliases.len() as u64;
        header.write(&mut expected.data)?;
        ensure!(self.er.local_images(target).await? == vec![Some(expected.clone())]);
        committed.data = expected.data;
        ensure!(self.base.account(&target[0]).await? == Some(committed));

        self.check_self_commit().await?;
        self.check_fee_roles().await?;
        self.check_commit_ordering().await.context("#243: successor ordering")?;
        self.check_buffers().await.context("#244: buffered settlement")?;
        self.check_callbacks().await.context("#255: callback provenance")
    }
}

impl SettlementAuthorization {
    /// Proves the delegated-payer self-commit exception with the validator's fee vault.
    async fn check_self_commit(&self) -> Result<()> {
        let mut rng = random::rng(
            self.base.config().seed,
            &["redhat", "settlement_authorization", "mapped_signer"],
            0,
        );
        let funder = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let payer = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        self.base.delegate_payer(&funder, &payer, self.er.identity()).await?;
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        ensure!(self.er.account(&vault).await?.is_some());
        let signature = self
            .er
            .submit_and_confirm(
                &payer,
                &[MAGIC.commit(
                    payer.pubkey(),
                    Some(vault),
                    &[payer.pubkey()],
                    SettlementMode::Commit,
                )],
            )
            .await?;
        let commit_receipt = receipt::fetch_commit_receipt(self.er.api(), &signature).await?;
        commit_receipt.check_success("mapped-signing self-commit")?;
        ensure!(commit_receipt.included.contains(&payer.pubkey()));
        commit_receipt.confirm(self.base.api()).await
    }
}
