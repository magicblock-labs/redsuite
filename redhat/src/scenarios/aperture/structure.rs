use super::fixture::{base_accounts, vacant, Fixture};
use anyhow::{ensure, Context};
use base64::{engine::general_purpose::STANDARD, Engine};
use keypair::Keypair;
use message::{
    compiled_instruction::CompiledInstruction, v0, v1, Message, MessageHeader, VersionedMessage,
};
use pubkey::Pubkey;
use redsuite_core::{
    random::{self, Rng},
    rpc_types::{config::RpcSendTransactionConfig, request::RpcRequest},
    transaction_types::UiTransactionEncoding,
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use std::{rc::Rc, time::Duration};
use tracing::debug;
use transaction::versioned::VersionedTransaction;

const ADMITTED: &str = "mbv_transaction_skip_preflight_count";
const ADMISSION_WINDOW: Duration = Duration::from_secs(90);

pub struct TransactionStructure {
    base: Rc<BaseCtx>,
    er: ErCtx,
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
                Mutation::Signatures,
            ] {
                cases.push((format, mutation));
            }
        }
        cases.extend([
            (Format::Legacy, Mutation::Encoding),
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
                let nonce = index as u64 * 2 + u64::from(skip_preflight) + 1;
                check_case(
                    &mut fixture,
                    &mut rng,
                    format,
                    mutation,
                    nonce,
                    skip_preflight,
                )
                .await
                .with_context(|| {
                    format!("{format:?}/{mutation:?}, skip_preflight={skip_preflight}")
                })?;
                if skip_preflight {
                    admitted_expected += 1.0;
                }
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

async fn check_case(
    fixture: &mut Fixture,
    rng: &mut impl Rng,
    format: Format,
    mutation: Mutation,
    nonce: u64,
    skip_preflight: bool,
) -> Result<()> {
    let amount = rng.gen_range(1..8u64);
    let legacy = fixture.message(nonce, amount as i64, &fixture.keys[2..]).await?;
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
                let key = random::pubkey(rng);
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
            instructions(&mut malformed)[0].data.resize(current + target - bytes.len(), 0);
            "SanitizeError"
        }
        Mutation::Encoding => "error parsing request body",
        Mutation::Signatures => "ParseError",
        Mutation::Truncated | Mutation::Trailing | Mutation::Unsupported => "ParseError",
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
    debug!(
        ?format,
        ?mutation,
        skip_preflight,
        "transaction structure rejection"
    );
    let mut payload = STANDARD.encode(&bytes);
    if matches!(mutation, Mutation::Encoding) {
        payload.insert(payload.len() / 2, '!');
    }
    let before = fixture.snapshot(&extra).await?;
    let start = fixture.er.api().get_slot().await?;
    let config = RpcSendTransactionConfig {
        skip_preflight,
        encoding: Some(UiTransactionEncoding::Base64),
        max_retries: Some(0),
        ..Default::default()
    };
    let result = fixture
        .er
        .api()
        .try_call::<String>(RpcRequest::SendTransaction, &(payload, config))
        .await?;
    let error = result.expect_err("malformed transaction acknowledged");
    let code = if matches!(mutation, Mutation::Encoding) { -32700 } else { -32602 };
    ensure!(
        error.error.code.code() == code && error.error.message.contains(gate),
        "{error}"
    );
    fixture.not_admitted(&signature, start, None).await?;
    fixture.unchanged(&before).await?;
    fixture
        .check_control(
            repaired,
            amount * control_instructions,
            control_instructions,
            skip_preflight,
        )
        .await?;
    ensure!(fixture
        .er
        .local_images(&extra)
        .await?
        .iter()
        .all(|account| account.as_ref().is_none_or(vacant)));
    ensure!(base_accounts(&fixture.base, &extra).await?.iter().all(Option::is_none));
    Ok(())
}
