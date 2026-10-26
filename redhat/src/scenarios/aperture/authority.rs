use super::{hostile::Fixture, *};
use account::Account;
use base64::{engine::general_purpose::STANDARD, Engine};
use magic_api::instruction::{AccountModificationForInstruction, MagicBlockInstruction};
use message::{v1, VersionedMessage};
use rand::seq::SliceRandom;
use redsuite_core::random::{self, Rng};
use transaction::versioned::VersionedTransaction;

pub struct TransactionAuthority {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

type Projection = Vec<Option<(Pubkey, bool, Vec<u8>, Option<u64>)>>;

fn project(accounts: &[Option<Account>]) -> Projection {
    accounts
        .iter()
        .enumerate()
        .map(|(index, account)| {
            account.as_ref().map(|account| {
                (
                    account.owner,
                    account.executable,
                    account.data.clone(),
                    (index < 2).then_some(account.lamports),
                )
            })
        })
        .collect()
}

async fn privileged_images(fixture: &Fixture) -> Result<(Projection, Projection)> {
    let keys = [
        magic_api::MAGIC_CONTEXT_PUBKEY,
        magic_api::EPHEMERAL_VAULT_PUBKEY,
        fixture.er.identity(),
    ];
    let local = fixture
        .er
        .local_accounts(&[magic_api::id(), sdk_ids::system_program::ID], &keys)
        .await?;
    ensure!(local[..2].iter().all(Option::is_some));
    let floor: u64 = fixture
        .base
        .api()
        .call(RpcRequest::GetMinimumBalanceForRentExemption, &(0usize,))
        .await?;
    ensure!(local[1].as_ref().unwrap().lamports >= floor);
    let base = fixture.base.accounts(&keys).await?;
    Ok((project(&local), project(&base)))
}

fn readonly_refusal(error: impl std::fmt::Debug) -> bool {
    let error = format!("{error:?}").to_lowercase();
    [
        "invalidwritableaccount",
        "readonlydatamodified",
        "externalaccountdatamodified",
        "immutable",
    ]
    .iter()
    .any(|gate| error.contains(gate))
}

impl Scenario for TransactionAuthority {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        let mut rng = random::rng(
            self.base.config().seed,
            &["redhat", "transaction_authority"],
            0,
        );
        let mut fixture = Fixture::new(self.base, self.er, &mut rng).await?;
        fixture
            .er
            .account(&fixture.er.identity())
            .await?
            .context("validator identity control missing")?;
        for skip_preflight in [false, true] {
            let nonce = rng.gen_range(1..u64::MAX);
            let legacy = fixture.message(nonce, 1, &[]).await?;
            let message = VersionedMessage::V1(v1::Message {
                header: legacy.header,
                account_keys: legacy.account_keys,
                lifetime_specifier: legacy.recent_blockhash,
                instructions: legacy.instructions,
                config: v1::TransactionConfig::empty()
                    .with_compute_unit_limit(1_400_000)
                    .with_loaded_accounts_data_size_limit(64 * 1024 * 1024),
            });
            let control =
                Signed::from_versioned(&fixture.er, &fixture.payer, message.clone(), None).await?;
            let mut private = VersionedTransaction {
                signatures: vec![Default::default()],
                message: message.clone(),
            };
            for impersonate in [false, true] {
                if impersonate {
                    let VersionedMessage::V1(message) = &mut private.message else {
                        unreachable!()
                    };
                    message.account_keys[0] = fixture.er.identity();
                }
                let mut bytes = wincode::serialize(&private)?;
                bytes[0] = 0xff;
                let end = bytes.len() - v1::SIGNATURE_SIZE;
                let signature = fixture.payer.sign_message(&bytes[..end]);
                ensure!(signature.verify(fixture.payer.pubkey().as_ref(), &bytes[..end]));
                bytes[end..].copy_from_slice(signature.as_ref());
                let attack = Signed {
                    encoded: STANDARD.encode(bytes),
                    signature,
                    blockhash: control.blockhash,
                    fee: 0,
                    error: None,
                };
                let before = fixture.images(&[]).await?;
                let privileged_before = privileged_images(&fixture).await?;
                let start = fixture.er.api().get_slot().await?;
                let error = attack
                    .send_config(&fixture.er, skip_preflight)
                    .await?
                    .expect_err("private wire accepted an attacker signature");
                ensure!(
                    error.error.code.code() == -32003 && error.error.message.contains("signature"),
                    "{error}"
                );
                fixture.statusless(&attack, start, None).await?;
                fixture.unchanged(&before).await?;
                let privileged = privileged_images(&fixture).await?;
                ensure!(
                    privileged == privileged_before,
                    "privileged state changed: {privileged:?} against {privileged_before:?}"
                );
                tracing::info!(
                    skip_preflight,
                    impersonate,
                    "private wire authority rejected before execution"
                );
            }
            fixture.repaired(message, 1, 1, skip_preflight).await?;
        }

        let target = fixture.keys[1];
        let attacker = fixture.payer.pubkey();
        let native = |operation: MagicBlockInstruction| -> Result<Instruction> {
            Ok(Instruction {
                program_id: magic_api::id(),
                accounts: vec![AccountMeta::new(attacker, true), AccountMeta::new(target, false)],
                data: operation.try_to_vec()?,
            })
        };
        let mut cases = vec![
            (
                "removed account modification",
                native(MagicBlockInstruction::ModifyAccounts {
                    accounts: [(
                        target,
                        AccountModificationForInstruction {
                            owner: Some(attacker),
                            ..Default::default()
                        },
                    )]
                    .into(),
                    message: None,
                })?,
                InstructionError::Custom(20),
            ),
            (
                "removed program authority update",
                native(MagicBlockInstruction::SetProgramAuthority { authority: attacker })?,
                InstructionError::Custom(20),
            ),
            (
                "engine account deletion authority",
                Instruction {
                    program_id: Pubkey::from_str_const(
                        "MagicRootDRJ5atQjSJUxFjXzjeZXMADHUDznbk22gy",
                    ),
                    accounts: vec![AccountMeta::new(target, false)],
                    data: 2u32.to_le_bytes().to_vec(),
                },
                InstructionError::MissingRequiredSignature,
            ),
        ];
        for (authority, signer, error) in [
            (attacker, true, InstructionError::InvalidArgument),
            (
                fixture.er.identity(),
                false,
                InstructionError::MissingRequiredSignature,
            ),
        ] {
            cases.push((
                "scheduled commit authority",
                Instruction {
                    program_id: magic_api::id(),
                    accounts: vec![
                        AccountMeta::new_readonly(authority, signer),
                        AccountMeta::new_readonly(
                            Pubkey::from_str_const("outboxintent1111111111111111111111111111111"),
                            false,
                        ),
                        AccountMeta::new(magic_api::MAGIC_CONTEXT_PUBKEY, false),
                        AccountMeta::new(target, false),
                    ],
                    data: MagicBlockInstruction::AcceptScheduleCommits.try_to_vec()?,
                },
                error,
            ));
        }
        cases.shuffle(&mut rng);
        for (label, instruction, error) in cases {
            for skip_preflight in [false, true] {
                let nonce = rng.gen_range(1..u64::MAX);
                let message = transaction::Message::new_with_blockhash(
                    &[instruction.clone(), REDSHIFT.batch().nonce(nonce).build()],
                    Some(&attacker),
                    &fixture.er.api().get_latest_blockhash().await?,
                );
                let tx = Signed::from_versioned(
                    &fixture.er,
                    &fixture.payer,
                    VersionedMessage::Legacy(message),
                    Some(TransactionError::InstructionError(0, error.clone()).into()),
                )
                .await?;
                let mut before = fixture.images(&[]).await?;
                let privileged_before = privileged_images(&fixture).await?;
                let start = fixture.er.api().get_slot().await?;
                let result = tx.send_config(&fixture.er, skip_preflight).await?;
                if skip_preflight {
                    ensure!(result? == tx.signature.to_string());
                } else {
                    ensure!(
                        result.expect_err("privileged instruction succeeded").error.code.code()
                            == -32003
                    );
                }
                let slot = tx.outcome(&fixture.er).await.with_context(|| label)?;
                audit(&fixture.er, start, &[(&tx, 1)]).await?;
                let status = fixture
                    .er
                    .api()
                    .get_signature_status(&tx.signature)
                    .await?
                    .context("executed authority rejection lost its status")?;
                ensure!(
                    status.slot == slot && status.err.map(UiTransactionError::from) == tx.error
                );
                before.local[0].as_mut().context("payer missing")?.lamports -= tx.fee;
                fixture.expected[0].as_mut().context("payer missing")?.lamports -= tx.fee;
                fixture.unchanged(&before).await?;
                let privileged = privileged_images(&fixture).await?;
                ensure!(
                    privileged == privileged_before,
                    "privileged state changed: {privileged:?} against {privileged_before:?}"
                );
                tracing::info!(
                    label,
                    skip_preflight,
                    "privileged instruction rejected during execution"
                );
                let repaired = fixture.message(nonce + 1, 1, &[]).await?;
                fixture
                    .repaired(VersionedMessage::Legacy(repaired), 1, 1, skip_preflight)
                    .await?;
            }
        }
        for skip_preflight in [false, true] {
            let nonce = rng.gen_range(1..u64::MAX);
            let message = transaction::Message::new_with_blockhash(
                &[REDSHIFT.batch().nonce(nonce).add(&fixture.keys[5..6], 13).build()],
                Some(&attacker),
                &fixture.er.api().get_latest_blockhash().await?,
            );
            let mut tx = Signed::from_versioned(
                &fixture.er,
                &fixture.payer,
                VersionedMessage::Legacy(message),
                None,
            )
            .await?;
            let mut before = fixture.images(&[]).await?;
            let privileged_before = privileged_images(&fixture).await?;
            let start = fixture.er.api().get_slot().await?;
            let acknowledged = match tx.send_config(&fixture.er, skip_preflight).await? {
                Ok(signature) => {
                    ensure!(signature == tx.signature.to_string());
                    true
                }
                Err(error) => {
                    ensure!(
                        error.error.code.code() == -32003 && readonly_refusal(&error),
                        "{error}"
                    );
                    false
                }
            };
            let end = fixture.er.api().get_slot().await? + 2;
            fixture.er.api().await_slot(end).await?;
            fixture
                .er
                .api()
                .await_query("foreign-write ledger boundary published", async || {
                    ensure!(fixture.er.api().get_block(end).await?.is_some());
                    Ok(())
                })
                .await?;
            let record = fixture.er.api().get_transaction(&tx.signature).await?;
            if let Some(record) = &record {
                let meta = transaction_meta(record)?;
                let error = meta.err.as_ref().context("foreign write executed successfully")?;
                ensure!(
                    readonly_refusal(error),
                    "unexpected foreign write error: {error:?}"
                );
                tx.error = meta.err.clone();
                let slot = tx.outcome(&fixture.er).await?;
                let status = fixture
                    .er
                    .api()
                    .get_signature_status(&tx.signature)
                    .await?
                    .context("accepted foreign write lost its terminal status")?;
                ensure!(
                    status.slot == slot && status.err.map(UiTransactionError::from) == tx.error
                );
                audit(&fixture.er, start, &[(&tx, 1)]).await?;
                before.local[0].as_mut().context("payer missing")?.lamports -= tx.fee;
                fixture.expected[0].as_mut().context("payer missing")?.lamports -= tx.fee;
            }
            fixture.unchanged(&before).await?;
            let privileged = privileged_images(&fixture).await?;
            ensure!(
                privileged == privileged_before,
                "privileged state changed: {privileged:?} against {privileged_before:?}"
            );
            let repaired = fixture.message(nonce + 1, 1, &fixture.keys[5..6]).await?;
            let control = fixture
                .repaired(VersionedMessage::Legacy(repaired), 1, 1, skip_preflight)
                .await?;
            if record.is_none() {
                fixture.statusless(&tx, start, Some(&control)).await?;
                tracing::info!(
                    skip_preflight,
                    acknowledged,
                    signal_gap = 261,
                    "foreign writable account refused without an execution record"
                );
            }
        }
        Ok(())
    }
}
