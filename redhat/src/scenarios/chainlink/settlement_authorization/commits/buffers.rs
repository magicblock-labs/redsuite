use super::*;
use borsh::BorshDeserialize;
use committor_interface::instruction::CommittorInstruction;
use instruction::AccountMeta;
use magic_api::{
    instruction::OutboxIntentInstruction,
    outbox::{ExecutionStage, PendingTransaction},
    OUTBOX_INTENT_PROGRAM_ID,
};
use message::{Message, VersionedMessage};
use redsuite_core::{
    netfault::Selector, programs::CellId, redline::causal::compute_unit_limit, signed::Signed,
};
use std::time::Duration;

const OUTBOX_INTENT_SEED: &[u8] = b"outbox-intent";
const OUTBOX_INTENT_DISCRIMINATOR: &[u8] = b"obintent";
const OUTBOX_BUNDLE_BODY: usize = 256;
const CONTROL_INDEX: u64 = 108;

fn outbox_intent_pda(intent_id: u64) -> Pubkey {
    Pubkey::find_program_address(
        &[OUTBOX_INTENT_SEED, &intent_id.to_le_bytes()],
        &OUTBOX_INTENT_PROGRAM_ID,
    )
    .0
}

pub(super) struct Plan {
    markers: Vec<u64>,
    order: Vec<usize>,
    forged_intent: u64,
    authority: Keypair,
    fixtures: [Pubkey; 2],
}

pub(super) fn plan(seed: u64) -> Result<Plan> {
    let mut rng = random::rng(seed, &["redhat", "commit_buffers"], 0);
    let markers = (0..5).map(|_| rng.gen_range(1..u64::MAX)).collect();
    let mut order = (0..8).collect::<Vec<_>>();
    order.shuffle(&mut rng);
    let forged_intent = rng.gen_range(1..u64::MAX);
    let mut fixtures = random::rng(seed, &["redhat", "commit_buffers", "fixtures"], 0);
    Ok(Plan {
        markers,
        order,
        forged_intent,
        authority: topology::identity_for_label("redhat-commit-buffer-authority")?,
        fixtures: std::array::from_fn(|_| random::keypair(&mut fixtures).pubkey()),
    })
}

impl Plan {
    pub(super) fn log(&self, seed: u64) {
        let accounts = (0..2)
            .map(|index| REDSHIFT.pda(CellId::new(self.fixtures[0], index)).0)
            .collect::<Vec<_>>();
        let large = (100..108)
            .map(|index| REDSHIFT.pda(CellId::new(self.fixtures[0], index)).0)
            .collect::<Vec<_>>();
        let control = REDSHIFT.pda(CellId::new(self.fixtures[0], CONTROL_INDEX)).0;
        tracing::debug!(seed, order = ?self.order, markers = ?self.markers, fixtures = ?self.fixtures, ?accounts, ?large, %control, "commit buffer plan");
        tracing::debug!(authority = %self.authority.pubkey(), vault = %dlp::validator_fees_vault_pda(&self.authority.pubkey()), "buffer authority fixture");
        tracing::debug!(seed, forged_intent = self.forged_intent, forged_outbox = %outbox_intent_pda(self.forged_intent), "forged outbox substitution plan");
    }
}

impl SettlementAuthorization {
    pub(in super::super) async fn check_buffers(&self) -> Result<()> {
        let plan = &self.commit_plan.buffers;
        let markers = &plan.markers;
        let mut fixtures = random::rng(
            self.base.config().seed,
            &["redhat", "commit_buffers", "fixtures"],
            0,
        );
        let payer = self.base.seeded_payer(&mut fixtures, PAYER_LAMPORTS).await?;
        ensure!(payer.pubkey() == plan.fixtures[0]);
        let authority = &plan.authority;
        self.base.airdrop(&authority.pubkey(), PAYER_LAMPORTS).await?;
        let mut accounts = Vec::new();
        let sequence = Cell::new(0);
        for index in 0..2 {
            let id = CellId::new(payer.pubkey(), index);
            let (create, address) = REDSHIFT.create(payer.pubkey(), id, 2_048);
            self.base
                .submit_and_confirm(
                    &payer,
                    &[
                        create,
                        REDSHIFT.delegate(payer.pubkey(), id, Some(authority.pubkey()), u32::MAX),
                    ],
                )
                .await?;
            accounts.push(address);
        }
        let fixture = CommitFixture {
            base: &self.base,
            er: &self.er,
            payer: &payer,
            authority,
            account: accounts[0],
            sequence: &sequence,
        };
        let other = CommitFixture { account: accounts[1], ..fixture };
        fixture.commit(1, markers[0], true).await?;
        other.commit(1, markers[1], true).await?;
        let data = fixture.data(markers[2]).await?;
        let (chunks, buffer) = fixture.initialize_buffer(2, data.len()).await?;
        let foreign = self.base.seeded_payer(&mut fixtures, PAYER_LAMPORTS).await?;
        ensure!(foreign.pubkey() == plan.fixtures[1]);
        for (signer, account, nonce) in [
            (&foreign, fixture.account, 2),
            (authority, other.account, 2),
            (authority, fixture.account, 3),
        ] {
            let mut write = create_write_ix(CreateWriteIxArgs {
                authority: signer.pubkey(),
                pubkey: account,
                offset: 0,
                data_chunk: vec![0xa5; CHUNK],
                commit_id: nonce,
            });
            write.accounts[1].pubkey = chunks;
            write.accounts[2].pubkey = buffer;
            fixture.rejected(&[write], &[signer], InstructionError::Custom(1)).await?;
        }
        for (offset, expected) in [
            (1, InstructionError::Custom(0x69002)),
            (data.len() as u32, InstructionError::Custom(0x69003)),
        ] {
            fixture
                .rejected(
                    &[create_write_ix(CreateWriteIxArgs {
                        authority: authority.pubkey(),
                        pubkey: fixture.account,
                        offset,
                        data_chunk: vec![0xa5],
                        commit_id: 2,
                    })],
                    &[authority],
                    expected,
                )
                .await?;
        }
        let before = self.base.accounts(&accounts).await?;
        for &index in &plan.order {
            let write = create_write_ix(CreateWriteIxArgs {
                authority: authority.pubkey(),
                pubkey: fixture.account,
                offset: (index * CHUNK) as u32,
                data_chunk: data[index * CHUNK..(index + 1) * CHUNK].to_vec(),
                commit_id: 2,
            });
            self.base.submit_and_confirm_with(&payer, &[authority], &[write]).await?;
            ensure!(self.base.accounts(&accounts).await? == before);
        }
        ensure!(self.base.account(&buffer).await?.context("complete buffer missing")?.data == data);
        self.base
            .submit_and_confirm_with(
                &payer,
                &[authority],
                &[
                    create_write_ix(CreateWriteIxArgs {
                        authority: authority.pubkey(),
                        pubkey: fixture.account,
                        offset: 0,
                        data_chunk: data[..CHUNK].to_vec(),
                        commit_id: 2,
                    }),
                    REDSHIFT.batch().nonce(244).build(),
                ],
            )
            .await?;
        ensure!(
            self.base.account(&buffer).await?.context("repeated chunk lost buffer")?.data == data
        );
        let mut unsigned = create_close_ix(CreateCloseIxArgs {
            authority: authority.pubkey(),
            pubkey: fixture.account,
            commit_id: 2,
        });
        unsigned.accounts[0].is_signer = false;
        fixture
            .rejected(&[unsigned], &[], InstructionError::MissingRequiredSignature)
            .await?;
        let valid = fixture.instruction(authority.pubkey(), 2, data.clone(), Some(buffer)).await?;
        let stale_data = other.data(markers[3]).await?;
        let stale_buffer = other.buffer(1, &stale_data).await?;
        let stale =
            other.instruction(authority.pubkey(), 1, stale_data, Some(stale_buffer)).await?;
        fixture
            .rejected_at(
                &[valid.clone(), dlp::finalize(&authority.pubkey(), &fixture.account), stale],
                &[authority],
                2,
                InstructionError::Custom(DlpError::NonceOutOfOrder as u32),
            )
            .await?;
        other.close(1).await?;
        ensure!(
            self.base.account(&buffer).await?.context("buffer lost after rollback")?.data == data
        );
        let replacement = fixture.data(markers[4]).await?;
        for (index, chunk) in replacement.chunks(CHUNK).enumerate() {
            self.base
                .submit_and_confirm_with(
                    &payer,
                    &[authority],
                    &[create_write_ix(CreateWriteIxArgs {
                        authority: authority.pubkey(),
                        pubkey: fixture.account,
                        offset: (index * CHUNK) as u32,
                        data_chunk: chunk.to_vec(),
                        commit_id: 2,
                    })],
                )
                .await?;
        }
        self.base
            .submit_and_confirm_with(
                &payer,
                &[authority],
                &[valid, dlp::finalize(&authority.pubkey(), &fixture.account)],
            )
            .await?;
        ensure!(
            self.base
                .account(&fixture.account)
                .await?
                .context("reused nonce commit missing")?
                .data
                == replacement
        );
        fixture.close(2).await?;
        ensure!(self.base.accounts(&[chunks, buffer]).await?.iter().all(Option::is_none));
        fixture.commit(3, markers[4], true).await?;
        other.commit(2, markers[4], true).await?;
        self.check_pending_outbox(&payer).await
    }

    async fn check_pending_outbox(&self, attacker: &Keypair) -> Result<()> {
        let plan = &self.commit_plan.buffers;
        let proxies = self.er.http_proxy()?;
        let mut large = Vec::new();
        for index in 0..8 {
            let id = CellId::new(attacker.pubkey(), 100 + index);
            let (create, address) = REDSHIFT.create(attacker.pubkey(), id, 9 * 1024);
            self.base
                .submit_and_confirm(
                    attacker,
                    &[
                        compute_unit_limit(1_400_000),
                        create,
                        REDSHIFT.delegate(
                            attacker.pubkey(),
                            id,
                            Some(self.er.identity()),
                            u32::MAX,
                        ),
                    ],
                )
                .await?;
            large.push(address);
        }
        let control_id = CellId::new(attacker.pubkey(), CONTROL_INDEX);
        let (create, control) =
            REDSHIFT.create(attacker.pubkey(), control_id, CellHeader::SIZE as u32);
        self.base
            .submit_and_confirm(
                attacker,
                &[
                    create,
                    REDSHIFT.delegate(
                        attacker.pubkey(),
                        control_id,
                        Some(self.er.identity()),
                        u32::MAX,
                    ),
                ],
            )
            .await?;
        self.er.await_clones(&large, 9 * 1024).await?;
        self.er.await_clones(&[control], CellHeader::SIZE).await?;
        let mut expected = self.er.local_images(&large).await?;
        let original_base = self.base.accounts(&large).await?;
        let mut expected_base = original_base.clone();
        for image in expected.iter_mut().chain(&mut expected_base) {
            let image = image.as_mut().context("large fixture missing")?;
            let mut header = CellHeader::try_decode(&image.data)?;
            header.revision += 1;
            CellHeader::fill_payload(&mut image.data, 0xa5a5_a5a5_a5a5_a5a5)?;
            header.write(&mut image.data)?;
        }
        let old_outbox = self.er.api().get_program_accounts(&OUTBOX_INTENT_PROGRAM_ID).await?;
        let trap = proxies.intercept(
            Selector::method("sendTransaction")
                .http()
                .response()
                .account(&committor_interface::ID),
        );
        let signature = self
            .er
            .submit_and_confirm(
                attacker,
                &[
                    compute_unit_limit(1_400_000),
                    REDSHIFT
                        .batch()
                        .fill(&large, 0xa5a5_a5a5_a5a5_a5a5, 1)
                        .settle(attacker.pubkey(), &large, SettlementMode::Commit)
                        .build(),
                ],
            )
            .await?;
        let initialization = trap.wait(Duration::from_secs(30)).await?;
        let initial_signature = initialization.operation.signature()?;
        let prepared = self.base.api().await_transaction(&initial_signature).await?;
        ensure!(transaction_meta(&prepared)?.err.is_none());
        let transaction =
            prepared.transaction.transaction.decode().context("buffer init decode")?;
        let mut initialized = None;
        for instruction in transaction.message.instructions() {
            if transaction.message.static_account_keys()[instruction.program_id_index as usize]
                != committor_interface::ID
            {
                continue;
            }
            if let CommittorInstruction::Init {
                pubkey,
                commit_id,
                buffer_account_size,
                chunk_count,
                chunk_size,
                ..
            } = CommittorInstruction::try_from_slice(&instruction.data)?
            {
                ensure!(large.contains(&pubkey) && buffer_account_size > 0);
                let buffer = committor_interface::pdas::buffer_pda(
                    &self.er.identity(),
                    &pubkey,
                    &commit_id.to_le_bytes(),
                )
                .0;
                let chunks = committor_interface::pdas::chunks_pda(
                    &self.er.identity(),
                    &pubkey,
                    &commit_id.to_le_bytes(),
                )
                .0;
                let image = self.base.account(&buffer).await?.context("held buffer missing")?;
                ensure!(image.owner == committor_interface::ID);
                ensure!(
                    image.data.len()
                        == buffer_account_size.min(
                            committor_interface::consts::MAX_ACCOUNT_ALLOC_PER_INSTRUCTION_SIZE
                                as u64
                        ) as usize
                );
                let image = self.base.account(&chunks).await?.context("held chunks missing")?;
                ensure!(image.owner == committor_interface::ID);
                let state = Chunks::try_from_slice(&image.data)?;
                ensure!(state.count() == chunk_count && state.chunk_size() == chunk_size);
                ensure!(chunk_count > 0 && !state.is_complete());
                tracing::debug!(%pubkey, %buffer, commit_id, buffer_account_size, "held buffer preparation");
                initialized.get_or_insert((pubkey, buffer, chunks, commit_id));
            }
        }
        let (prepared_cell, buffer, chunks, commit_id) = initialized
            .context("held preparation did not initialize an owned large-account buffer")?;
        let cell_index = large
            .iter()
            .position(|key| *key == prepared_cell)
            .context("prepared cell missing")?;
        let write_trap = proxies
            .intercept(Selector::method("sendTransaction").http().request().account(&buffer));
        initialization.release();
        let held = write_trap.wait(Duration::from_secs(30)).await?;
        let write_signature = held.operation.signature()?;
        ensure!(write_signature != initial_signature);
        let image = self.base.account(&chunks).await?.context("pending chunks missing")?;
        ensure!(!Chunks::try_from_slice(&image.data)?.is_complete());
        ensure!(self.base.account(&prepared_cell).await? == original_base[cell_index]);
        let intent_id = receipt::scheduled_intent_id(self.er.api(), &signature).await?;
        let mut pending: Vec<_> = self
            .er
            .api()
            .get_program_accounts(&OUTBOX_INTENT_PROGRAM_ID)
            .await?
            .into_iter()
            .filter(|(key, _)| !old_outbox.iter().any(|(old, _)| old == key))
            .collect();
        ensure!(
            pending.len() == 1,
            "held delivery has no unique accepted outbox intent"
        );
        let (outbox, accepted) = pending.pop().unwrap();
        ensure!(
            outbox_intent_pda(intent_id) == outbox,
            "accepted outbox {outbox} is not the canonical PDA of intent {intent_id}"
        );
        ensure!(
            accepted.data.starts_with(OUTBOX_INTENT_DISCRIMINATOR),
            "accepted outbox {outbox} does not carry a discriminated outbox intent bundle"
        );
        let forged = outbox_intent_pda(plan.forged_intent);
        ensure!(
            forged != outbox && self.base.account(&forged).await?.is_none(),
            "forged intent {} collides with live outbox work",
            plan.forged_intent
        );
        let mut bundle = OUTBOX_INTENT_DISCRIMINATOR.to_vec();
        bundle.extend(plan.forged_intent.to_le_bytes());
        bundle.resize(OUTBOX_INTENT_DISCRIMINATOR.len() + OUTBOX_BUNDLE_BODY, 0);
        tracing::debug!(intent_id, %outbox, forged_intent = plan.forged_intent, %forged, %control, bundle = bundle.len(), "outbox substitution targets");
        let stage = ExecutionStage::SingleStage(PendingTransaction {
            signature: write_signature,
            blockhash: self.base.api().get_latest_blockhash().await?,
        });
        let operations = [
            (
                OutboxIntentInstruction::CreateOutboxIntent { data: bundle },
                InstructionError::IncorrectAuthority,
            ),
            (
                OutboxIntentInstruction::CloseOutboxIntent(intent_id),
                InstructionError::IncorrectAuthority,
            ),
            (
                OutboxIntentInstruction::ScheduledCommitSent((intent_id, 0)),
                InstructionError::IncorrectAuthority,
            ),
            (
                OutboxIntentInstruction::SetIntentExecutionStage {
                    intent_id,
                    stage,
                    recovery_commit_nonces: vec![],
                },
                InstructionError::InvalidArgument,
            ),
        ];
        let mut keys = large.clone();
        keys.extend([outbox, magic_api::MAGIC_CONTEXT_PUBKEY, control]);
        ensure!(self.er.local_images(&large).await? == expected);
        let substitutions = [
            ("accepted outbox row", outbox, &[false, true][..], false),
            (
                "outbox PDA of a foreign intent",
                forged,
                &[false, true][..],
                true,
            ),
            (
                "substituted magic context",
                magic_api::MAGIC_CONTEXT_PUBKEY,
                &[false][..],
                true,
            ),
        ];
        for (index, (substitution, row, unsigned_variants, settle)) in
            substitutions.into_iter().enumerate()
        {
            let local = self.er.local_images(&keys).await?;
            ensure!(
                local.iter().all(Option::is_some),
                "{substitution}: protected outbox image missing"
            );
            let base = self.base.accounts(&keys).await?;
            for (operation, expected) in &operations {
                for &unsigned in unsigned_variants {
                    let instruction = Instruction {
                        program_id: OUTBOX_INTENT_PROGRAM_ID,
                        accounts: vec![
                            AccountMeta::new_readonly(
                                if unsigned { self.er.identity() } else { attacker.pubkey() },
                                !unsigned,
                            ),
                            AccountMeta::new(row, false),
                        ],
                        data: operation.try_to_vec()?,
                    };
                    let message = Message::new_with_blockhash(
                        &[instruction],
                        Some(&attacker.pubkey()),
                        &self.er.api().get_latest_blockhash().await?,
                    );
                    let error = if unsigned {
                        InstructionError::MissingRequiredSignature
                    } else {
                        expected.clone()
                    };
                    tracing::debug!(substitution, %row, unsigned, ?error, "forged outbox operation");
                    let tx = Signed::from_versioned(
                        &self.er,
                        attacker,
                        VersionedMessage::Legacy(message),
                        Some(TransactionError::InstructionError(0, error).into()),
                    )
                    .await?;
                    ensure!(tx.send_config(&self.er, true).await?? == tx.signature.to_string());
                    tx.outcome(&self.er).await?;
                    ensure!(self.er.local_images(&keys).await? == local);
                    ensure!(self.base.accounts(&keys).await? == base);
                    ensure!(
                        !self
                            .er
                            .local_program_accounts(&[OUTBOX_INTENT_PROGRAM_ID])
                            .await?
                            .contains_key(&forged),
                        "{substitution}: forged outbox row {forged} materialized"
                    );
                    ensure!(
                        self.base.account(&forged).await?.is_none(),
                        "{substitution}: forged outbox row {forged} reached base"
                    );
                }
            }
            if settle {
                self.settle_control(attacker, control, 244 + index as u64, substitution).await?;
            }
        }
        let image = self.base.account(&chunks).await?.context("pending chunks missing")?;
        ensure!(!Chunks::try_from_slice(&image.data)?.is_complete());
        ensure!(self.base.account(&prepared_cell).await? == original_base[cell_index]);
        held.release();
        let written = self.base.api().await_transaction(&write_signature).await?;
        ensure!(transaction_meta(&written)?.err.is_none());
        let transaction =
            written.transaction.transaction.decode().context("buffer write decode")?;
        ensure!(
            transaction.message.instructions().iter().any(|instruction| {
                transaction.message.static_account_keys()[instruction.program_id_index as usize]
                    == committor_interface::ID
                    && matches!(
                        CommittorInstruction::try_from_slice(&instruction.data),
                        Ok(CommittorInstruction::Write { pubkey, commit_id: nonce, .. })
                            if pubkey == prepared_cell && nonce == commit_id
                    )
            })
        );
        let receipt = self.er.check_commit(&self.base, &signature, &large, false).await?;
        receipt.check_success("held large-account commit survives foreign outbox inputs")?;
        ensure!(self.er.local_images(&large).await? == expected);
        ensure!(self.base.accounts(&large).await? == expected_base);
        for image in expected.iter_mut().chain(&mut expected_base) {
            let image = image.as_mut().context("large control image missing")?;
            let mut header = CellHeader::try_decode(&image.data)?;
            header.value += 1;
            header.revision += 1;
            header.write(&mut image.data)?;
        }
        let next = self
            .er
            .submit_and_confirm(
                attacker,
                &[
                    compute_unit_limit(1_400_000),
                    REDSHIFT
                        .batch()
                        .add(&large, 1)
                        .settle(attacker.pubkey(), &large, SettlementMode::Commit)
                        .build(),
                ],
            )
            .await?;
        self.er
            .check_commit(&self.base, &next, &large, false)
            .await?
            .check_success("large buffer reuse recovery")?;
        ensure!(self.er.local_images(&large).await? == expected);
        ensure!(self.base.accounts(&large).await? == expected_base);
        proxies.reset().await?;
        Ok(())
    }

    async fn settle_control(
        &self,
        payer: &Keypair,
        cell: Pubkey,
        nonce: u64,
        what: &str,
    ) -> Result<()> {
        let mut expected = self
            .er
            .local_images(&[cell])
            .await?
            .pop()
            .flatten()
            .context("outbox control cell missing")?;
        let mut committed =
            self.base.account(&cell).await?.context("outbox control cell missing on base")?;
        let signature = self
            .er
            .submit_and_confirm(
                payer,
                &[REDSHIFT
                    .batch()
                    .nonce(nonce)
                    .add(&[cell], 1)
                    .settle(payer.pubkey(), &[cell], SettlementMode::Commit)
                    .build()],
            )
            .await?;
        self.er
            .check_commit(&self.base, &signature, &[cell], false)
            .await?
            .check_success(what)?;
        let mut header = CellHeader::try_decode(&expected.data)?;
        header.value += 1;
        header.revision += 1;
        header.write(&mut expected.data)?;
        ensure!(self.er.local_images(&[cell]).await? == vec![Some(expected.clone())]);
        committed.data = expected.data;
        ensure!(self.base.account(&cell).await? == Some(committed));
        Ok(())
    }
}
