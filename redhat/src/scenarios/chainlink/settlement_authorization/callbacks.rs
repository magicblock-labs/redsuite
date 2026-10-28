use super::*;
use account::Account;
use instruction::AccountMeta;
use keypair::Keypair;
use magic_api::{
    args::{AddActionCallbackArgs, ShortAccountMeta},
    pda::CALLBACK_SIGNER,
};
use rand::seq::SliceRandom;
use redsuite_core::{
    netfault::Selector,
    programs::{
        Action, CellHeader, CellId, Commit, FixtureInstruction, IntentBundle, ResultCallback,
    },
    random::{self, Rng},
    rpc_types::{request::RpcRequest, response::TransactionError},
    wait,
};
use signature::Signature;
use solana_instruction_error::InstructionError;
use std::time::Duration;
use transaction::Transaction;

fn callback_args(cell: Pubkey, action_index: u8) -> Result<AddActionCallbackArgs> {
    Ok(AddActionCallbackArgs {
        action_index,
        destination_program: REDSHIFT.id(),
        discriminator: FixtureInstruction::HandleResult.into_data(),
        payload: wincode::serialize(&ResultCallback {
            cell,
            refund: None,
            compute_units: 20_000,
        })?,
        compute_units: 20_000,
        accounts: vec![
            ShortAccountMeta {
                pubkey: CALLBACK_SIGNER,
                is_writable: false,
            },
            ShortAccountMeta { pubkey: cell, is_writable: true },
        ],
    })
}

#[derive(Clone, Copy, Debug)]
enum CallbackCategory {
    Commit,
    Standalone,
    Undelegate,
}

pub(super) struct Plan {
    nonce: u64,
    cases: Vec<u8>,
    controls: [(CallbackCategory, usize, bool, u32); 5],
    leads: [u8; 2],
    gap: u64,
    padding: u8,
    funder: Keypair,
    payer: Keypair,
    alternate: Keypair,
    cells: [Pubkey; 2],
    handoff: Pubkey,
}

pub(super) fn plan(seed: u64) -> Plan {
    let mut rng = random::rng(seed, &["redhat", "callback_provenance", "plan"], 0);
    let nonce = rng.gen_range(1..u64::MAX / 2);
    let mut cases: Vec<_> = (0..11u8).collect();
    cases.shuffle(&mut rng);
    let leads = [rng.gen_range(0..3u8), rng.gen_range(0..3u8)];
    let gap = rng.gen_range(2..6u64);
    let padding: u8 = rng.gen();
    let fixture = |role| {
        random::keypair(&mut random::rng(
            seed,
            &["redhat", "callback_provenance", "fixtures", role],
            0,
        ))
    };
    let funder = fixture("funder");
    let cells = std::array::from_fn(|index| {
        REDSHIFT.pda(CellId::new(funder.pubkey(), index as u64 + 10)).0
    });
    let handoff = REDSHIFT.pda(CellId::new(funder.pubkey(), 12)).0;
    Plan {
        nonce,
        cases,
        controls: [
            (CallbackCategory::Commit, 0, false, 0),
            (CallbackCategory::Commit, 1, true, 20_000),
            (CallbackCategory::Standalone, 0, false, 20_000),
            (CallbackCategory::Undelegate, 0, false, 20_000),
            (CallbackCategory::Standalone, 2, false, 20_000),
        ],
        leads,
        gap,
        padding,
        funder,
        payer: fixture("payer"),
        alternate: fixture("alternate"),
        cells,
        handoff,
    }
}

impl Plan {
    pub(super) fn log(&self, seed: u64) {
        tracing::debug!(seed, cases = ?self.cases, nonce = self.nonce, controls = ?self.controls, leads = ?self.leads, gap = self.gap, padding = self.padding, funder = %self.funder.pubkey(), payer = %self.payer.pubkey(), alternate = %self.alternate.pubkey(), cells = ?self.cells, handoff = %self.handoff, "callback plan");
    }
}

fn scheduled(
    payer: Pubkey,
    validator: Pubkey,
    cells: &[Pubkey],
    nonce: u64,
    attached: bool,
    failure: Option<usize>,
    category: CallbackCategory,
) -> Instruction {
    let mut bundle = IntentBundle::new(payer);
    bundle.validator = Some(validator);
    let mode = if matches!(category, CallbackCategory::Undelegate) {
        SettlementMode::Undelegate
    } else {
        SettlementMode::Commit
    };
    let mut commit = Commit::new(mode, cells);
    for (index, &cell) in cells.iter().enumerate() {
        let mut batch = REDSHIFT
            .batch()
            .nonce(nonce + index as u64)
            .log(&format!("callback action {}", nonce + index as u64));
        if failure == Some(index) {
            batch = batch.fail(0x255);
        }
        let owner = if matches!(category, CallbackCategory::Undelegate) {
            REDSHIFT.id()
        } else {
            dlp::dlp_id()
        };
        let mut action = Action::new(payer, batch.action(cell, owner), 100_000);
        if attached {
            action = action.then(ResultCallback {
                cell,
                refund: None,
                compute_units: 20_000,
            });
        }
        match category {
            CallbackCategory::Commit => commit.after_commit.push(action),
            CallbackCategory::Standalone => bundle.actions.push(action),
            CallbackCategory::Undelegate => commit.after_undelegate.push(action),
        }
    }
    if !matches!(category, CallbackCategory::Standalone) {
        bundle.commits.push(commit);
    }
    REDSHIFT.batch().nonce(nonce).intents(bundle).build()
}

impl SettlementAuthorization {
    async fn callback_images(
        &self,
        keys: &[Pubkey],
    ) -> Result<(Vec<Option<Account>>, Vec<Option<Account>>)> {
        let local = self.er.local_images(keys).await?;
        ensure!(
            local.iter().all(Option::is_some),
            "protected callback image missing"
        );
        Ok((local, self.base.accounts(keys).await?))
    }

    async fn callback_record(
        &self,
        cell: Pubkey,
        previous: &[Signature],
        error: Option<TransactionError>,
    ) -> Result<()> {
        wait::retry(
            "retained callback outcome",
            Duration::from_secs(30),
            async || {
                for signature in self.er.api().get_signatures_for_address(&cell, 32).await? {
                    if previous.contains(&signature) {
                        continue;
                    }
                    let Some(record) = self.er.api().get_transaction(&signature).await? else {
                        continue;
                    };
                    let Some(transaction) = record.transaction.transaction.decode() else {
                        continue;
                    };
                    if !transaction.message.instructions().iter().any(|instruction| {
                        transaction
                            .message
                            .static_account_keys()
                            .get(instruction.program_id_index as usize)
                            == Some(&magic_api::CALLBACK_PROGRAM_ID)
                    }) {
                        continue;
                    }
                    let expected = error.clone().map(Into::into);
                    let meta = transaction_meta(&record)?;
                    ensure!(
                        meta.err == expected,
                        "callback record {signature}: {:?} against {expected:?}",
                        meta.err
                    );
                    let status = self
                        .er
                        .api()
                        .get_signature_status(&signature)
                        .await?
                        .context("callback terminal status missing")?;
                    ensure!(status.slot == record.slot && status.err == error);
                    return Ok(());
                }
                anyhow::bail!("callback execution record missing")
            },
        )
        .await
    }

    pub(super) async fn check_callbacks(&self) -> Result<()> {
        let seed = self.base.config().seed;
        let plan = &self.callback_plan;
        let nonce = plan.nonce;
        let (funder, payer, alternate) = (&plan.funder, &plan.payer, &plan.alternate);
        let cells = plan.cells;
        let handoff = plan.handoff;
        for role in ["funder", "payer", "alternate"] {
            let mut rng = random::rng(
                seed,
                &["redhat", "callback_provenance", "fixtures", role],
                0,
            );
            self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        }
        for (index, cell) in cells.iter().chain([&handoff]).enumerate() {
            ensure!(
                *cell
                    == self
                        .base
                        .cells(REDSHIFT, funder)
                        .create_delegated(
                            CellHeader::SIZE as u32,
                            self.er.identity(),
                            index as u8 + 10
                        )
                        .await?
            );
        }
        self.base
            .submit_and_confirm(
                payer,
                &[dlp::top_up_ephemeral_balance(&payer.pubkey(), 500_000_000, 1)],
            )
            .await?;
        self.base.delegate_payer(funder, payer, self.er.identity()).await?;
        self.base.delegate_payer(funder, alternate, self.er.identity()).await?;
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        let keys = [
            cells[0],
            cells[1],
            handoff,
            payer.pubkey(),
            alternate.pubkey(),
            vault,
            magic_api::MAGIC_CONTEXT_PUBKEY,
        ];
        for key in &keys {
            self.er.account(key).await?.context("callback fixture missing")?;
        }
        let sender = self.er.sender(Rc::new(payer.insecure_clone()));
        for &case in &plan.cases {
            let tag = nonce + u64::from(case) * 10;
            let mut args = callback_args(cells[0], 0)?;
            let (error, needle) = match case {
                0 => {
                    args.action_index = 2;
                    (InstructionError::InvalidInstructionData, "out of range")
                }
                1 => {
                    args.destination_program = magic_api::id();
                    (
                        InstructionError::InvalidInstructionData,
                        "does not match action source_program",
                    )
                }
                2 => {
                    args.accounts[0].is_writable = true;
                    (
                        InstructionError::Immutable,
                        "callback signer PDA cannot be a writable",
                    )
                }
                3 => {
                    args.accounts.push(ShortAccountMeta {
                        pubkey: self.er.identity(),
                        is_writable: false,
                    });
                    (
                        InstructionError::IncorrectAuthority,
                        "validator authority cannot be used",
                    )
                }
                4 => (InstructionError::InvalidAccountData, "callback already set"),
                5 | 6 => (
                    InstructionError::InvalidAccountData,
                    "does not match action source_program",
                ),
                7 => (
                    InstructionError::InvalidAccountData,
                    "does not match intent payer",
                ),
                8 => (InstructionError::MissingRequiredSignature, "not in signers"),
                9 => (
                    InstructionError::IncorrectAuthority,
                    "is not the expected validator",
                ),
                _ => (InstructionError::MissingRequiredSignature, "not in signers"),
            };
            let schedule = scheduled(
                payer.pubkey(),
                self.er.identity(),
                &cells,
                tag,
                case == 4,
                Some(1),
                CallbackCategory::Commit,
            );
            let callback_payer =
                if matches!(case, 7 | 8) { alternate.pubkey() } else { payer.pubkey() };
            let mut attach = MAGIC.add_callback(callback_payer, vault, args);
            if case == 8 {
                attach.accounts[0].is_signer = false;
            }
            let mut instructions = match case {
                5 => vec![schedule, REDHAT.batch().invoke(attach).build()],
                6 => vec![schedule, attach],
                9 | 10 => vec![MAGIC.execute_callback(
                    if case == 9 {
                        AccountMeta::new_readonly(payer.pubkey(), true)
                    } else {
                        AccountMeta::new_readonly(self.er.identity(), false)
                    },
                    REDSHIFT.batch().set(&cells, 99).build(),
                )],
                _ => vec![schedule, REDSHIFT.batch().invoke(attach).build()],
            };
            instructions.push(REDSHIFT.batch().nonce(tag + 9).build());
            let before = self.callback_images(&keys).await?;
            let mut signers = vec![payer];
            if case == 7 {
                signers.push(alternate);
            }
            let transaction = Transaction::new_signed_with_payer(
                &instructions,
                Some(&payer.pubkey()),
                &signers,
                self.er.api().get_latest_blockhash().await?,
            );
            let signature = sender.submit_prepared(&transaction).await?;
            let tx = self.er.api().await_transaction(&signature).await?;
            let meta = transaction_meta(&tx)?;
            ensure!(
                meta.err
                    == Some(
                        TransactionError::InstructionError(if case >= 9 { 0 } else { 1 }, error)
                            .into()
                    ),
                "callback case {case}: {:?}",
                meta.err
            );
            ensure!(
                transaction_logs(meta).iter().any(|line| line.contains(needle)),
                "callback case {case} did not reach {needle}"
            );
            ensure!(
                self.callback_images(&keys).await? == before,
                "callback case {case} changed protected state or retained a charge"
            );
        }
        self.callback_control(funder, payer, &cells, handoff, nonce + 200, &plan.controls)
            .await?;
        self.callback_freshness(
            &sender,
            payer,
            cells[0],
            &keys,
            nonce + 300,
            (plan.leads, plan.gap),
        )
        .await?;
        let accepted = callback_args(cells[0], 0)?.payload.len();
        tracing::debug!(
            accepted,
            rejected = accepted + 1,
            padding = plan.padding,
            "callback payload boundary"
        );
        self.callback_delivery(
            payer,
            cells[0],
            nonce + 400,
            Some(plan.padding),
            "oversized callback payload",
        )
        .await?;
        self.callback_delivery(
            payer,
            cells[0],
            nonce + 410,
            None,
            "accepted callback payload",
        )
        .await
    }

    async fn callback_control(
        &self,
        funder: &Keypair,
        payer: &Keypair,
        cells: &[Pubkey; 2],
        handoff: Pubkey,
        nonce: u64,
        controls: &[(CallbackCategory, usize, bool, u32)],
    ) -> Result<()> {
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        let tracked = [cells[0], cells[1], handoff];
        let mut expected = self.er.accounts(&tracked).await?;
        let handoff_before =
            self.base.account(&handoff).await?.context("callback handoff missing")?;
        let payer_before =
            self.er.account(&payer.pubkey()).await?.context("callback payer missing")?;
        let vault_before = self.er.account(&vault).await?.context("callback vault missing")?;
        for (index, &(category, target, fails, compute_units)) in controls.iter().enumerate() {
            let cell = tracked[target];
            let action_cell = match category {
                CallbackCategory::Undelegate => handoff,
                CallbackCategory::Standalone if target == 2 => cells[0],
                _ => cell,
            };
            let included = if matches!(category, CallbackCategory::Standalone) {
                Vec::new()
            } else {
                vec![action_cell]
            };
            let base_before = self.base.accounts(cells).await?;
            let previous = self.er.api().get_signatures_for_address(&cell, 32).await?;
            let barrier = if target == 2 {
                let status: serde_json::Value = self
                    .er
                    .api()
                    .call(
                        RpcRequest::Custom { method: "getDelegationStatus" },
                        &(handoff.to_string(),),
                    )
                    .await?;
                ensure!(status["isDelegated"] == false);
                ensure!(self.er.local_images(&tracked).await? == expected);
                let pending = self
                    .er
                    .api()
                    .get_program_accounts(&magic_api::OUTBOX_INTENT_PROGRAM_ID)
                    .await?;
                let trap = self.er.http_proxy()?.intercept(
                    Selector::method("sendTransaction").http().request().account(&action_cell),
                );
                Some((trap, pending))
            } else {
                None
            };
            let mut args = callback_args(cell, 0)?;
            args.compute_units = compute_units;
            let instructions = [
                scheduled(
                    payer.pubkey(),
                    self.er.identity(),
                    &[action_cell],
                    nonce + index as u64,
                    false,
                    fails.then_some(0),
                    category,
                ),
                REDSHIFT.batch().invoke(MAGIC.add_callback(payer.pubkey(), vault, args)).build(),
            ];
            let signature = self.er.submit_and_confirm(payer, &instructions).await?;
            let held_signature = if let Some((trap, previous_outbox)) = barrier {
                let held = trap.wait(Duration::from_secs(30)).await?;
                let held_signature = held.operation.signature()?;
                let pending: Vec<_> = self
                    .er
                    .api()
                    .get_program_accounts(&magic_api::OUTBOX_INTENT_PROGRAM_ID)
                    .await?
                    .into_iter()
                    .filter(|(key, _)| !previous_outbox.iter().any(|(old, _)| old == key))
                    .collect();
                ensure!(pending.len() == 1, "accepted callback outbox missing");
                ensure!(
                    pending[0].1.data.windows(32).any(|bytes| bytes == handoff.as_ref()),
                    "accepted outbox lost callback destination"
                );
                ensure!(self.er.local_images(&tracked).await? == expected);
                self.base
                    .submit_and_confirm(
                        funder,
                        &[REDSHIFT.delegate(
                            funder.pubkey(),
                            CellId::new(funder.pubkey(), 12),
                            Some(self.er.identity()),
                            u32::MAX,
                        )],
                    )
                    .await?;
                self.er
                    .await_delegated(&handoff, "accepted callback destination redelegated")
                    .await?;
                ensure!(self.er.local_images(&tracked).await? == expected);
                ensure!(
                    self.er.local_images(&[pending[0].0]).await?
                        == vec![Some(pending[0].1.clone())]
                );
                let mut base_handoff = handoff_before.clone();
                base_handoff.owner = dlp::dlp_id();
                ensure!(self.base.account(&handoff).await? == Some(base_handoff));
                tracing::debug!(%handoff, outbox = %pending[0].0, "accepted callback survives destination redelegation");
                held.release();
                Some(held_signature)
            } else {
                None
            };
            let receipt = self
                .er
                .check_commit(
                    &self.base,
                    &signature,
                    &included,
                    matches!(category, CallbackCategory::Undelegate),
                )
                .await?;
            receipt.check_success("rightful callback attachment")?;
            if let Some(held_signature) = held_signature {
                ensure!(receipt.base_signatures.contains(&held_signature));
            }
            if !fails {
                let mut observed = false;
                let marker = format!("callback action {}", nonce + index as u64);
                for signature in &receipt.base_signatures {
                    let base = self.base.api().await_transaction(signature).await?;
                    observed |= transaction_logs(transaction_meta(&base)?)
                        .iter()
                        .any(|line| line.contains(&marker));
                }
                ensure!(observed, "base action body missing: {marker}");
            }
            if matches!(
                category,
                CallbackCategory::Standalone | CallbackCategory::Undelegate
            ) {
                ensure!(self.base.accounts(cells).await? == base_before);
            }
            if matches!(category, CallbackCategory::Undelegate) {
                let mut handoff_expected = handoff_before.clone();
                handoff_expected.owner = REDSHIFT.id();
                ensure!(self.base.account(&handoff).await? == Some(handoff_expected));
            }
            self.callback_record(cell, &previous, None).await?;
            let account = expected[target].as_mut().context("callback cell missing")?;
            let mut header = CellHeader::try_decode(&account.data)?;
            header.revision += 1;
            header.value += u64::from(fails);
            header.write(&mut account.data)?;
        }
        wait::retry(
            "callbacks delivered to their own action cells",
            Duration::from_secs(30),
            async || {
                let actual = self.er.local_images(&tracked).await?;
                ensure!(
                    actual == expected,
                    "callback images {actual:?}, expected {expected:?}"
                );
                Ok(())
            },
        )
        .await?;
        ensure!(
            self.er
                .account(&payer.pubkey())
                .await?
                .context("callback payer missing")?
                .lamports
                == payer_before.lamports - controls.len() as u64 * 10_000
        );
        ensure!(
            self.er.account(&vault).await?.context("callback vault missing")?.lamports
                == vault_before.lamports + controls.len() as u64 * 10_000
        );
        let final_signature = self
            .er
            .submit_and_confirm(
                payer,
                &[REDSHIFT
                    .batch()
                    .nonce(nonce + 1)
                    .invoke(MAGIC.commit(
                        payer.pubkey(),
                        Some(vault),
                        &tracked,
                        SettlementMode::Commit,
                    ))
                    .build()],
            )
            .await?;
        self.er
            .check_commit(&self.base, &final_signature, &tracked, false)
            .await?
            .check_success("owner settlement after callbacks")?;
        for (cell, expected) in tracked.iter().zip(expected) {
            ensure!(
                self.base.account(cell).await?.context("settled callback cell missing")?.data
                    == expected.context("callback model missing")?.data
            );
        }
        Ok(())
    }

    async fn callback_delivery(
        &self,
        payer: &Keypair,
        cell: Pubkey,
        nonce: u64,
        padding: Option<u8>,
        phase: &str,
    ) -> Result<()> {
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        let mut args = callback_args(cell, 0)?;
        if let Some(byte) = padding {
            args.payload.push(byte);
        }
        let mut expected = self
            .er
            .local_images(&[cell])
            .await?
            .pop()
            .flatten()
            .with_context(|| format!("{phase}: callback cell missing"))?;
        let base_before = self.base.account(&cell).await?;
        let payer_before = self
            .er
            .account(&payer.pubkey())
            .await?
            .with_context(|| format!("{phase}: callback payer missing"))?;
        let vault_before = self
            .er
            .account(&vault)
            .await?
            .with_context(|| format!("{phase}: callback vault missing"))?;
        let previous = self.er.api().get_signatures_for_address(&cell, 32).await?;
        let signature = self
            .er
            .submit_and_confirm(
                payer,
                &[
                    scheduled(
                        payer.pubkey(),
                        self.er.identity(),
                        &[cell],
                        nonce,
                        false,
                        None,
                        CallbackCategory::Standalone,
                    ),
                    REDSHIFT
                        .batch()
                        .nonce(nonce + 1)
                        .invoke(MAGIC.add_callback(payer.pubkey(), vault, args))
                        .build(),
                ],
            )
            .await?;
        self.er
            .check_commit(&self.base, &signature, &[], false)
            .await?
            .check_success(phase)?;
        self.callback_record(
            cell,
            &previous,
            padding.map(|_| {
                TransactionError::InstructionError(1, InstructionError::InvalidInstructionData)
            }),
        )
        .await?;
        if padding.is_none() {
            let mut header = CellHeader::try_decode(&expected.data)?;
            header.revision += 1;
            header.write(&mut expected.data)?;
        }
        wait::retry(phase, Duration::from_secs(30), async || {
            let actual = self.er.local_images(&[cell]).await?;
            ensure!(
                actual == vec![Some(expected.clone())],
                "{phase}: callback image {actual:?}, expected {expected:?}"
            );
            Ok(())
        })
        .await?;
        ensure!(
            self.base.account(&cell).await? == base_before,
            "{phase}: uncommitted base image changed"
        );
        ensure!(
            self.er
                .account(&payer.pubkey())
                .await?
                .with_context(|| format!("{phase}: callback payer missing"))?
                .lamports
                == payer_before.lamports - 10_000,
            "{phase}: attachment charge"
        );
        ensure!(
            self.er
                .account(&vault)
                .await?
                .with_context(|| format!("{phase}: callback vault missing"))?
                .lamports
                == vault_before.lamports + 10_000,
            "{phase}: attachment credit"
        );
        Ok(())
    }

    async fn callback_freshness(
        &self,
        sender: &TxSender,
        payer: &Keypair,
        cell: Pubkey,
        keys: &[Pubkey],
        nonce: u64,
        freshness: ([u8; 2], u64),
    ) -> Result<()> {
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        let (leads, gap) = freshness;
        for (index, lead) in leads.into_iter().enumerate() {
            let tag = nonce + index as u64 * 20;
            let schedule = scheduled(
                payer.pubkey(),
                self.er.identity(),
                &[cell],
                tag,
                false,
                None,
                CallbackCategory::Standalone,
            );
            let attach = REDSHIFT
                .batch()
                .nonce(tag + 1)
                .invoke(MAGIC.add_callback(payer.pubkey(), vault, callback_args(cell, 0)?))
                .build();
            let mut instructions: Vec<_> = (0..u64::from(lead))
                .map(|step| REDSHIFT.batch().nonce(tag + 2 + step).build())
                .collect();
            if index == 0 {
                instructions.extend([attach, schedule]);
                tracing::debug!(
                    lead,
                    slot = self.er.api().get_slot().await?,
                    "callback attached ahead of its intent"
                );
            } else {
                let scheduled_slot = self.er.api().get_slot().await?;
                let accepted = self.er.submit_and_confirm(payer, &[schedule]).await?;
                self.er
                    .check_commit(&self.base, &accepted, &[], false)
                    .await?
                    .check_success("callback freshness intent")?;
                let deadline = scheduled_slot + gap;
                wait::retry(
                    "expired callback attachment slot",
                    Duration::from_secs(30),
                    async || {
                        let slot = self.er.api().get_slot().await?;
                        ensure!(slot >= deadline, "slot {slot} before {deadline}");
                        Ok(())
                    },
                )
                .await?;
                instructions.push(attach);
                tracing::debug!(
                    lead,
                    gap,
                    scheduled_slot,
                    slot = self.er.api().get_slot().await?,
                    "callback attached after its intent row expired"
                );
            }
            let before = self.callback_images(keys).await?;
            let signature = sender.submit(&instructions).await?;
            let tx = self.er.api().await_transaction(&signature).await?;
            let meta = transaction_meta(&tx)?;
            ensure!(
                meta.err
                    == Some(
                        TransactionError::InstructionError(
                            lead,
                            InstructionError::InvalidAccountData
                        )
                        .into()
                    ),
                "callback freshness {index}: {:?}",
                meta.err
            );
            ensure!(
                transaction_logs(meta)
                    .iter()
                    .any(|line| line.contains("no scheduled intents found")),
                "callback freshness {index} did not reach the latest-intent gate"
            );
            ensure!(
                self.callback_images(keys).await? == before,
                "callback freshness {index} changed protected state or retained a charge"
            );
        }
        self.callback_delivery(payer, cell, nonce + 40, None, "fresh callback attachment")
            .await?;
        Ok(())
    }
}
