use super::*;
use account::Account;
use base64::{engine::general_purpose::STANDARD, Engine};
use message::{
    compiled_instruction::CompiledInstruction, v0, v1, Message, MessageHeader, VersionedMessage,
};
use redsuite_core::{
    random::{self, Rng},
    wait,
};
use std::time::Instant;
use tracing::info;
use transaction::versioned::VersionedTransaction;

const OBSERVE: Duration = Duration::from_millis(250);
const ADMITTED: &str = "mbv_transaction_skip_preflight_count";
const ADMISSION_WINDOW: Duration = Duration::from_secs(90);

pub struct TransactionStructure {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

pub struct TransactionAdmission {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

pub(super) struct Images {
    pub(super) keys: Vec<Pubkey>,
    pub(super) local: Vec<Option<Account>>,
    pub(super) base: Vec<Option<Account>>,
}

pub(super) struct Fixture {
    pub(super) base: Rc<BaseCtx>,
    pub(super) er: ErCtx,
    pub(super) payer: Keypair,
    pub(super) keys: Vec<Pubkey>,
    pub(super) base_images: Vec<Option<Account>>,
    pub(super) expected: Vec<Option<Account>>,
    pub(super) foreign_image: Account,
}

async fn base_images(base: &BaseCtx, keys: &[Pubkey]) -> Result<Vec<Option<Account>>> {
    let mut images = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(100) {
        images.extend(base.accounts(chunk).await?);
    }
    Ok(images)
}

fn vacant(account: &Account) -> bool {
    account.lamports == 0
        && account.data.is_empty()
        && account.owner == sdk_ids::system_program::ID
        && !account.executable
}

pub(super) fn seeded_key(rng: &mut impl Rng) -> Pubkey {
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    Pubkey::new_from_array(bytes)
}

impl Fixture {
    pub(super) async fn new(base: Rc<BaseCtx>, er: ErCtx, rng: &mut impl Rng) -> Result<Self> {
        let owner = base.seeded_payer(rng, PAYER_LAMPORTS).await?;
        let cell = base.cells(REDSHIFT, &owner).create_primary(er.identity(), u32::MAX).await?;
        let payer = base.seeded_payer(rng, PAYER_LAMPORTS).await?;
        base.delegate_payer(&owner, &payer, er.identity()).await?;
        er.api()
            .await_accounts(
                &[payer.pubkey(), cell],
                "hostile transaction fixtures cloned",
                decode_state,
            )
            .await?;
        let warm = base.seeded_payer(rng, 1_000_000).await?.pubkey();
        let cold = base.seeded_payer(rng, 1_000_000).await?.pubkey();
        let absent = seeded_key(rng);
        er.account(&warm).await?.context("warm control missing")?;
        let foreign_owner = base.seeded_payer(rng, PAYER_LAMPORTS).await?;
        let foreign = base
            .cells(REDSHIFT, &foreign_owner)
            .create_primary(seeded_key(rng), u32::MAX)
            .await?;
        let foreign_image =
            er.account(&foreign).await?.context("foreign readonly control missing")?;
        let keys = vec![payer.pubkey(), cell, warm, cold, absent, foreign];
        let expected = er.local_images(&keys).await?;
        ensure!(expected[..2].iter().all(Option::is_some));
        ensure!(expected[3].is_none() && expected[4].is_none());
        let base_images = base_images(&base, &keys).await?;
        Ok(Self {
            base,
            er,
            payer,
            keys,
            base_images,
            expected,
            foreign_image,
        })
    }

    pub(super) async fn images(&self, extra: &[Pubkey]) -> Result<Images> {
        let mut keys = self.keys.clone();
        for key in extra {
            if !keys.contains(key) {
                keys.push(*key);
            }
        }
        let local = self.er.local_images(&keys).await?;
        let base = base_images(&self.base, &keys).await?;
        Ok(Images { keys, local, base })
    }

    pub(super) async fn unchanged(&self, before: &Images) -> Result<()> {
        let until = Instant::now() + OBSERVE;
        loop {
            ensure!(self.er.local_images(&before.keys).await? == before.local);
            ensure!(base_images(&self.base, &before.keys).await? == before.base);
            if Instant::now() >= until {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub(super) async fn message(
        &self,
        nonce: u64,
        amount: i64,
        readonly: &[Pubkey],
    ) -> Result<Message> {
        let mut instruction = REDSHIFT.batch().nonce(nonce).add(&self.keys[1..2], amount).build();
        instruction
            .accounts
            .extend(readonly.iter().map(|key| AccountMeta::new_readonly(*key, false)));
        Ok(Message::new_with_blockhash(
            &[instruction],
            Some(&self.payer.pubkey()),
            &self.er.api().get_latest_blockhash().await?,
        ))
    }

    pub(super) async fn repaired(
        &mut self,
        message: VersionedMessage,
        amount: u64,
        revisions: u64,
        skip_preflight: bool,
    ) -> Result<Signature> {
        let tx = Signed::from_versioned(&self.er, &self.payer, message, None).await?;
        ensure!(tx.valid(&self.er).await?);
        let start = self.er.api().get_slot().await?;
        let signature = tx.send_config(&self.er, skip_preflight).await??;
        ensure!(signature == tx.signature.to_string());
        let slot = tx.outcome(&self.er).await?;
        let status = self
            .er
            .api()
            .get_signature_status(&tx.signature)
            .await?
            .context("repaired terminal status missing")?;
        ensure!(status.slot == slot && status.err.is_none());
        audit(&self.er, start, &[(&tx, 1)]).await?;
        self.expected[0].as_mut().unwrap().lamports -= tx.fee;
        let cell = self.expected[1].as_mut().unwrap();
        let mut header = CellHeader::try_decode(&cell.data)?;
        header.value += amount;
        header.revision += revisions;
        header.write(&mut cell.data)?;
        let actual = self.er.local_images(&self.keys).await?;
        ensure!(actual[..2] == self.expected[..2]);
        for (index, account) in actual.iter().enumerate().skip(2) {
            let expected = if index == 5 {
                Some(&self.foreign_image)
            } else {
                self.base_images[index].as_ref()
            };
            let held = match (account, expected) {
                (None, _) => true,
                (Some(account), None) => vacant(account),
                (Some(account), Some(expected)) => account == expected,
            };
            ensure!(
                held,
                "readonly input {} diverged locally: {account:?} against {expected:?}",
                self.keys[index]
            );
        }
        ensure!(base_images(&self.base, &self.keys).await? == self.base_images);
        Ok(tx.signature)
    }

    pub(super) async fn statusless(
        &self,
        tx: &Signed,
        start: u64,
        control: Option<&Signature>,
    ) -> Result<()> {
        let mut expected = vec![(&tx.signature, 0)];
        expected.extend(control.map(|control| (control, 1)));
        audit_signatures(&self.er, start, &expected).await?;
        ensure!(self.er.api().get_transaction(&tx.signature).await?.is_none());
        ensure!(self.er.api().get_signature_status(&tx.signature).await?.is_none());
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Format {
    Legacy,
    V0,
    V1,
}

impl Format {
    fn message(self, legacy: Message) -> VersionedMessage {
        match self {
            Self::Legacy => VersionedMessage::Legacy(legacy),
            Self::V0 => VersionedMessage::V0(v0::Message {
                header: legacy.header,
                account_keys: legacy.account_keys,
                recent_blockhash: legacy.recent_blockhash,
                instructions: legacy.instructions,
                address_table_lookups: Vec::new(),
            }),
            Self::V1 => VersionedMessage::V1(v1::Message {
                header: legacy.header,
                account_keys: legacy.account_keys,
                lifetime_specifier: legacy.recent_blockhash,
                instructions: legacy.instructions,
                config: v1::TransactionConfig::empty()
                    .with_compute_unit_limit(1_400_000)
                    .with_loaded_accounts_data_size_limit(64 * 1024 * 1024),
            }),
        }
    }
}

fn header(message: &mut VersionedMessage) -> &mut MessageHeader {
    match message {
        VersionedMessage::Legacy(message) => &mut message.header,
        VersionedMessage::V0(message) => &mut message.header,
        VersionedMessage::V1(message) => &mut message.header,
    }
}

fn keys(message: &mut VersionedMessage) -> &mut Vec<Pubkey> {
    match message {
        VersionedMessage::Legacy(message) => &mut message.account_keys,
        VersionedMessage::V0(message) => &mut message.account_keys,
        VersionedMessage::V1(message) => &mut message.account_keys,
    }
}

fn instructions(message: &mut VersionedMessage) -> &mut Vec<CompiledInstruction> {
    match message {
        VersionedMessage::Legacy(message) => &mut message.instructions,
        VersionedMessage::V0(message) => &mut message.instructions,
        VersionedMessage::V1(message) => &mut message.instructions,
    }
}

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Duplicate,
    ReadonlySigner,
    ReadonlyUnsigned,
    ProgramPayer,
    ProgramIndex,
    AccountIndex,
    Truncated,
    Trailing,
    Unsupported,
    LookupWritable,
    LookupReadonly,
    Accounts,
    Instructions,
    Size,
    Heap,
    Encoding,
    Signatures,
}

fn wire(message: VersionedMessage, payer: &Keypair) -> Result<(Vec<u8>, Signature)> {
    let tx = VersionedTransaction::try_new(message, &[payer])?;
    Ok((wincode::serialize(&tx)?, tx.signatures[0]))
}

impl Scenario for TransactionStructure {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        let mut rng = random::rng(
            self.base.config().seed,
            &["redhat", "transaction_structure"],
            0,
        );
        let mut fixture = Fixture::new(self.base, self.er, &mut rng).await?;
        let mut cases = Vec::new();
        for format in [Format::Legacy, Format::V0, Format::V1] {
            for mutation in [
                Mutation::Duplicate,
                Mutation::ReadonlySigner,
                Mutation::ReadonlyUnsigned,
                Mutation::ProgramPayer,
                Mutation::ProgramIndex,
                Mutation::AccountIndex,
                Mutation::Truncated,
                Mutation::Trailing,
                Mutation::Unsupported,
                Mutation::Accounts,
                Mutation::Instructions,
                Mutation::Size,
                Mutation::Encoding,
                Mutation::Signatures,
            ] {
                cases.push((format, mutation));
            }
        }
        cases.extend([
            (Format::V0, Mutation::LookupWritable),
            (Format::V0, Mutation::LookupReadonly),
            (Format::V1, Mutation::Heap),
        ]);
        for index in (1..cases.len()).rev() {
            cases.swap(index, rng.gen_range(0..=index));
        }
        let admitted_before = fixture
            .er
            .metrics()
            .scrape()
            .await?
            .get(ADMITTED)
            .context("admission counter missing from the validator metrics")?;
        let mut admitted_expected = 0.0;
        for (index, (format, mutation)) in cases.into_iter().enumerate() {
            for skip_preflight in [false, true] {
                let amount = rng.gen_range(1..8u64);
                let legacy = fixture
                    .message(
                        index as u64 * 2 + u64::from(skip_preflight) + 1,
                        amount as i64,
                        &fixture.keys[2..],
                    )
                    .await?;
                let mut repaired = format.message(legacy);
                let mut malformed = repaired.clone();
                let mut extra = Vec::new();
                let mut control_instructions = 1;
                let gate = match mutation {
                    Mutation::Duplicate => {
                        let duplicate = keys(&mut malformed)[0];
                        keys(&mut malformed).push(duplicate);
                        header(&mut malformed).num_readonly_unsigned_accounts += 1;
                        "SanitizeError"
                    }
                    Mutation::ReadonlySigner => {
                        header(&mut malformed).num_readonly_signed_accounts = 1;
                        "SanitizeError"
                    }
                    Mutation::ReadonlyUnsigned => {
                        header(&mut malformed).num_readonly_unsigned_accounts = u8::MAX;
                        "SanitizeError"
                    }
                    Mutation::ProgramPayer => {
                        instructions(&mut malformed)[0].program_id_index = 0;
                        "SanitizeError"
                    }
                    Mutation::ProgramIndex => {
                        instructions(&mut malformed)[0].program_id_index = u8::MAX;
                        "SanitizeError"
                    }
                    Mutation::AccountIndex => {
                        instructions(&mut malformed)[0].accounts.push(u8::MAX);
                        "SanitizeError"
                    }
                    Mutation::LookupWritable | Mutation::LookupReadonly => {
                        let VersionedMessage::V0(message) = &mut malformed else { unreachable!() };
                        message.address_table_lookups.push(v0::MessageAddressTableLookup {
                            account_key: fixture.keys[4],
                            writable_indexes: if matches!(mutation, Mutation::LookupWritable) {
                                vec![0]
                            } else {
                                Vec::new()
                            },
                            readonly_indexes: if matches!(mutation, Mutation::LookupReadonly) {
                                vec![u8::MAX]
                            } else {
                                Vec::new()
                            },
                        });
                        "AddressLookupMismatch"
                    }
                    Mutation::Accounts => {
                        let count = if matches!(format, Format::V1) { 65 } else { 39 };
                        while keys(&mut malformed).len() < count {
                            let key = seeded_key(&mut rng);
                            if !keys(&mut malformed).contains(&key) {
                                keys(&mut malformed).push(key);
                                extra.push(key);
                            }
                        }
                        header(&mut malformed).num_readonly_unsigned_accounts = (count - 2) as u8;
                        repaired = malformed.clone();
                        keys(&mut repaired).pop();
                        header(&mut repaired).num_readonly_unsigned_accounts -= 1;
                        if matches!(format, Format::V1) {
                            "SanitizeError"
                        } else {
                            "ParseError"
                        }
                    }
                    Mutation::Instructions => {
                        let instruction = instructions(&mut malformed)[0].clone();
                        *instructions(&mut malformed) = vec![instruction.clone(); 65];
                        *instructions(&mut repaired) = vec![instruction; 64];
                        control_instructions = 64;
                        "SanitizeError"
                    }
                    Mutation::Heap => {
                        let VersionedMessage::V1(message) = &mut malformed else { unreachable!() };
                        message.config.heap_size = Some(32 * 1024 + 1);
                        let VersionedMessage::V1(control) = &mut repaired else { unreachable!() };
                        control.config.heap_size = Some(32 * 1024);
                        "SanitizeError"
                    }
                    Mutation::Size => {
                        instructions(&mut malformed)[0].data.resize(65_000, 0);
                        let (bytes, _) = wire(malformed.clone(), &fixture.payer)?;
                        let target = 65_536usize;
                        let current = instructions(&mut malformed)[0].data.len();
                        instructions(&mut malformed)[0]
                            .data
                            .resize(current + target - bytes.len(), 0);
                        "SanitizeError"
                    }
                    Mutation::Encoding => "error parsing request body",
                    Mutation::Signatures => "ParseError",
                    Mutation::Truncated | Mutation::Trailing | Mutation::Unsupported => {
                        "ParseError"
                    }
                };
                let (mut bytes, mut signature) = wire(malformed, &fixture.payer)?;
                match mutation {
                    Mutation::Signatures => bytes[0] = 2,
                    Mutation::Truncated => {
                        bytes.pop();
                    }
                    Mutation::Trailing => bytes.push(0),
                    Mutation::Unsupported => {
                        if matches!(format, Format::V1) {
                            bytes[0] = 0x82;
                            let end = bytes.len() - 64;
                            signature = fixture.payer.sign_message(&bytes[..end]);
                            bytes[end..].copy_from_slice(signature.as_ref());
                        } else {
                            bytes[65] = 0x82;
                            signature = fixture.payer.sign_message(&bytes[65..]);
                            bytes[1..65].copy_from_slice(signature.as_ref());
                        }
                    }
                    _ => {}
                }
                info!(
                    ?format,
                    ?mutation,
                    skip_preflight,
                    bytes = bytes.len(),
                    gate,
                    "hostile transaction graph"
                );
                let mut payload = STANDARD.encode(&bytes);
                if matches!(mutation, Mutation::Encoding) {
                    payload.insert(payload.len() / 2, '!');
                }
                let before = fixture.images(&extra).await?;
                let start = fixture.er.api().get_slot().await?;
                let result: result::Result<String, RpcError> = fixture
                    .er
                    .api()
                    .try_call(
                        RpcRequest::SendTransaction,
                        &(
                            payload,
                            RpcSendTransactionConfig {
                                skip_preflight,
                                encoding: Some(UiTransactionEncoding::Base64),
                                max_retries: Some(0),
                                ..Default::default()
                            },
                        ),
                    )
                    .await?;
                let error = result.expect_err("malformed transaction acknowledged");
                let code = if matches!(mutation, Mutation::Encoding) { -32700 } else { -32602 };
                ensure!(
                    error.error.code.code() == code && error.error.message.contains(gate),
                    "{error}"
                );
                let rejected = Signed {
                    encoded: String::new(),
                    signature,
                    blockhash: *repaired.recent_blockhash(),
                    fee: 0,
                    error: None,
                };
                fixture.statusless(&rejected, start, None).await?;
                fixture.unchanged(&before).await?;
                fixture
                    .repaired(
                        repaired,
                        amount * control_instructions,
                        control_instructions,
                        skip_preflight,
                    )
                    .await?;
                if skip_preflight {
                    admitted_expected += 1.0;
                }
                ensure!(fixture
                    .er
                    .local_images(&extra)
                    .await?
                    .iter()
                    .all(|account| account.as_ref().is_none_or(vacant)));
                ensure!(base_images(&fixture.base, &extra).await?.iter().all(Option::is_none));
            }
        }
        wait::retry(
            "skip-preflight admissions match the repaired controls",
            ADMISSION_WINDOW,
            async || {
                let admitted = fixture
                    .er
                    .metrics()
                    .scrape()
                    .await?
                    .get(ADMITTED)
                    .context("admission counter missing from the validator metrics")?
                    - admitted_before;
                ensure!(
                    admitted == admitted_expected,
                    "{admitted} skip-preflight admissions against {admitted_expected} controls"
                );
                Ok(())
            },
        )
        .await
    }
}

impl Scenario for TransactionAdmission {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        let mut rng = random::rng(
            self.base.config().seed,
            &["redhat", "transaction_admission"],
            0,
        );
        let mut fixture = Fixture::new(self.base, self.er, &mut rng).await?;
        let proxies = fixture.er.http_proxy()?;
        for skip_preflight in [false, true] {
            let cold = fixture.base.seeded_payer(&mut rng, 1_000_000).await?.pubkey();
            let source = fixture.base.account(&cold).await?;
            ensure!(fixture.er.local_images(&[cold]).await? == vec![None]);
            let selector = Selector::methods(&["getAccountInfo", "getMultipleAccounts"])
                .http()
                .request()
                .account(&cold);
            let refused = proxies.reject(selector, "hostile account read refused");
            let before = fixture.images(&[]).await?;
            let read = fixture.er.account(&cold).await;
            match read {
                Ok(account) => {
                    ensure!(
                        account.as_ref().is_none_or(vacant),
                        "refused account read leaked {account:?}"
                    );
                    info!(
                        skip_preflight,
                        signal_gap = 261,
                        placeholder = account.is_some(),
                        "failed account preparation stays unresolved without a client error"
                    );
                }
                Err(error) => {
                    let error = format!("{error:#}");
                    ensure!(
                        error.contains("hostile account read refused")
                            || error.contains(&cold.to_string()),
                        "{error}"
                    )
                }
            }
            fixture.unchanged(&before).await?;
            let message = fixture.message(1_000 + u64::from(skip_preflight), 3, &[cold]).await?;
            let tx = Signed::from_versioned(
                &fixture.er,
                &fixture.payer,
                VersionedMessage::Legacy(message.clone()),
                None,
            )
            .await?;
            let start = fixture.er.api().get_slot().await?;
            let error = tx
                .send_config(&fixture.er, skip_preflight)
                .await?
                .expect_err("faulted preparation acknowledged");
            ensure!(
                error.error.code.code() == -32003
                    && (error.error.message.contains("hostile account read refused")
                        || error.error.message.contains(&cold.to_string())),
                "{error}"
            );
            fixture.statusless(&tx, start, None).await?;
            fixture.unchanged(&before).await?;
            refused.remove();
            wait::retry("faulted account input recovers", TIMEOUT, async || {
                ensure!(fixture.er.account(&cold).await?.is_some());
                Ok(())
            })
            .await?;
            fixture
                .repaired(VersionedMessage::Legacy(message), 3, 1, skip_preflight)
                .await?;
            let local = fixture.er.local_images(&[cold]).await?;
            ensure!(
                local[0]
                    .as_ref()
                    .is_none_or(|account| vacant(account) || Some(account) == source.as_ref()),
                "faulted input diverged locally: {:?} against {source:?}",
                local[0]
            );
            ensure!(fixture.base.account(&cold).await? == source);
        }

        let expiry_message = fixture.message(2_000, 7, &[]).await?;
        let expired = expiry_message.recent_blockhash;
        wait::retry(
            "hostile probe blockhash expires",
            Duration::from_secs(90),
            async || {
                let valid: bool = fixture
                    .er
                    .api()
                    .call_value(RpcRequest::IsBlockhashValid, &(expired.to_string(),))
                    .await?;
                ensure!(!valid);
                Ok(())
            },
        )
        .await?;
        let hashes = [
            Hash::new_from_array(seeded_key(&mut rng).to_bytes()),
            fixture.base.api().get_latest_blockhash().await?,
            expired,
        ];
        for (index, hash) in hashes.into_iter().enumerate() {
            for skip_preflight in [false, true] {
                let cold = fixture.base.seeded_payer(&mut rng, 1_000_000).await?.pubkey();
                let source = fixture.base.account(&cold).await?;
                ensure!(fixture.er.local_images(&[cold]).await? == vec![None]);
                let mut message = fixture
                    .message(
                        3_000 + index as u64 * 2 + u64::from(skip_preflight),
                        7,
                        &[cold],
                    )
                    .await?;
                message.recent_blockhash = hash;
                let transaction = VersionedTransaction::try_new(
                    VersionedMessage::Legacy(message.clone()),
                    &[&fixture.payer],
                )?;
                let tx = Signed {
                    encoded: STANDARD.encode(wincode::serialize(&transaction)?),
                    signature: transaction.signatures[0],
                    blockhash: hash,
                    fee: 0,
                    error: None,
                };
                let before = fixture.images(&[]).await?;
                let trap = proxies.intercept(
                    Selector::methods(&["getAccountInfo", "getMultipleAccounts"])
                        .http()
                        .request()
                        .account(&cold),
                );
                ensure!(!tx.valid(&fixture.er).await?);
                let start = fixture.er.api().get_slot().await?;
                let preparation_started = Instant::now();
                let (sent, held) =
                    tokio::join!(tx.send_config(&fixture.er, skip_preflight), async {
                        let held = trap.wait(TIMEOUT).await?;
                        let held_started = Instant::now();
                        ensure!(fixture.er.local_images(&[cold]).await? == vec![None]);
                        fixture.unchanged(&before).await?;
                        held.release();
                        Result::Ok(held_started.elapsed())
                    });
                let held_elapsed = held?;
                let preparation_elapsed = preparation_started.elapsed();
                if skip_preflight {
                    ensure!(sent?? == tx.signature.to_string());
                } else {
                    let error = sent?.expect_err("invalid blockhash admitted with preflight");
                    ensure!(
                        error.error.code.code() == -32003
                            && (error.error.message.contains("BlockhashNotFound")
                                || error.error.message.contains("Blockhash not found")),
                        "{error}"
                    );
                }
                fixture.unchanged(&before).await?;
                let local = fixture.er.local_images(&[cold]).await?;
                ensure!(
                    local[0]
                        .as_ref()
                        .is_none_or(|account| vacant(account) || Some(account) == source.as_ref()),
                    "faulted input diverged locally: {:?} against {source:?}",
                    local[0]
                );
                ensure!(fixture.base.account(&cold).await? == source);
                info!(
                    index,
                    skip_preflight,
                    preparation_ms = preparation_elapsed.saturating_sub(held_elapsed).as_millis(),
                    held_ms = held_elapsed.as_millis(),
                    "invalid hash preparation witnessed without execution"
                );
                message.recent_blockhash = fixture.er.api().get_latest_blockhash().await?;
                let control = fixture
                    .repaired(VersionedMessage::Legacy(message), 7, 1, skip_preflight)
                    .await?;
                fixture.statusless(&tx, start, Some(&control)).await?;
            }
        }
        let message = fixture.message(4_000, 5, &fixture.keys[4..5]).await?;
        fixture.repaired(VersionedMessage::Legacy(message), 5, 1, true).await?;
        for skip_preflight in [false, true] {
            let message = Message::new_with_blockhash(
                &[
                    REDSHIFT
                        .batch()
                        .nonce(5_000 + u64::from(skip_preflight))
                        .add(&fixture.keys[1..2], 11)
                        .build(),
                    REDSHIFT.batch().writable(&fixture.keys[1..2]).fail(7).build(),
                ],
                Some(&fixture.payer.pubkey()),
                &fixture.er.api().get_latest_blockhash().await?,
            );
            let error: UiTransactionError =
                TransactionError::InstructionError(1, InstructionError::Custom(7)).into();
            let tx = Signed::from_versioned(
                &fixture.er,
                &fixture.payer,
                VersionedMessage::Legacy(message),
                Some(error),
            )
            .await?;
            let mut before = fixture.images(&[]).await?;
            let (mut listener, subscription) = subscribe(&fixture.er, &tx).await?;
            let start = fixture.er.api().get_slot().await?;
            let sent = tx.send_config(&fixture.er, skip_preflight).await?;
            if skip_preflight {
                ensure!(sent? == tx.signature.to_string());
            } else {
                let error = sent.expect_err("deliberate execution failure succeeded");
                ensure!(
                    error.error.code.code() == -32003
                        && (error.error.message.contains("Custom(7)")
                            || error.error.message.contains("custom program error: 0x7")),
                    "{error}"
                );
            }
            let slot = tx.outcome(&fixture.er).await?;
            let status = fixture
                .er
                .api()
                .get_signature_status(&tx.signature)
                .await?
                .context("executed failure lost its terminal status")?;
            ensure!(status.slot == slot && status.err.map(Into::into) == tx.error);
            let (method, id, payload) = listener
                .next_notification(TIMEOUT)
                .await?
                .context("live failure notification missing")?;
            ensure!(method == "signatureNotification" && id == subscription);
            ensure!(
                serde_json::from_str::<Value>(&payload.to_string())?["value"]
                    == json!({"err":tx.error})
            );
            listener.close().await?;
            audit(&fixture.er, start, &[(&tx, 1)]).await?;
            before.local[0].as_mut().unwrap().lamports -= tx.fee;
            fixture.expected[0].as_mut().unwrap().lamports -= tx.fee;
            fixture.unchanged(&before).await?;
            let repaired = fixture.message(6_000 + u64::from(skip_preflight), 11, &[]).await?;
            fixture
                .repaired(VersionedMessage::Legacy(repaired), 11, 1, skip_preflight)
                .await?;
        }
        proxies.reset().await?;
        Ok(())
    }
}
