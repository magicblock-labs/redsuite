use std::time::Duration;

use account::Account;
use async_trait::async_trait;
use futures_util::future::try_join_all;
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use magic_api::instruction::{
    MagicBlockInstruction,
    MagicBlockInstruction::{
        CloseEphemeralAccount as Close, CreateEphemeralAccount as Create,
        ResizeEphemeralAccount as Resize,
    },
};
use pubkey::Pubkey;
use redshift_interface::{ephemeral_cpi, log_msg_data};
use redsuite_core::{
    api::TransactionInfo,
    check, check_eq, manifest, prep, system,
    topology::{self, ErOptions},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport,
};
use sdk::{consts::EPHEMERAL_VAULT_ID, ephemeral_accounts::rent};
use signer::Signer;
use transaction::Transaction;

const TIMEOUT: Duration = Duration::from_secs(30);
const PAYER: usize = 0;
const SPONSOR: usize = 1;
const TARGET: usize = 2;
const VAULT: usize = 3;
const SUBSTITUTE: usize = 4;

pub struct EphemeralAccounts;

#[derive(Clone)]
struct Action {
    operation: MagicBlockInstruction,
    caller: Pubkey,
    fill: u8,
}

impl Action {
    fn new(operation: MagicBlockInstruction) -> Self {
        Self {
            operation,
            caller: redshift_interface::id(),
            fill: 0,
        }
    }

    fn instruction(&self, keys: &[Pubkey]) -> Instruction {
        let native = Instruction {
            program_id: magic_api::id(),
            accounts: vec![
                AccountMeta::new(keys[SPONSOR], true),
                AccountMeta::new(
                    keys[TARGET],
                    matches!(self.operation, Create { .. }),
                ),
                AccountMeta::new(keys[VAULT], false),
            ],
            data: self
                .operation
                .try_to_vec()
                .expect("ephemeral instruction serialization"),
        };
        if self.caller == magic_api::id() {
            return native;
        }
        let mut ix = ephemeral_cpi(native, self.fill);
        ix.program_id = self.caller;
        ix
    }

    fn apply(&self, state: &mut [Account]) -> Option<&'static str> {
        let account = &mut state[TARGET];
        let exists = account.owner != system::system_id();
        let old_rent = if exists {
            rent(account.data.len() as u32)
        } else {
            0
        };
        match self.operation {
            Create { data_len } => {
                if exists || account.lamports != 0 {
                    return Some("InvalidAccountData");
                }
                account.owner = self.caller;
                account.rent_epoch = u64::MAX;
                account.data = vec![0; data_len as usize];
            }
            Resize { .. } | Close => {
                if !exists {
                    return Some("InvalidAccountData");
                }
                if account.owner != self.caller {
                    return Some("InvalidAccountOwner");
                }
                if let Resize { new_data_len } = self.operation {
                    account.data.resize(new_data_len as usize, 0);
                } else {
                    *account = Account::default();
                }
            }
            _ => unreachable!(),
        }
        if self.fill != 0 {
            account.data.fill(self.fill);
        }
        let new_rent = if matches!(self.operation, Close) {
            0
        } else {
            rent(account.data.len() as u32)
        };
        state[SPONSOR].lamports += old_rent;
        state[SPONSOR].lamports -= new_rent;
        state[VAULT].lamports += new_rent;
        state[VAULT].lamports -= old_rent;
        None
    }
}

fn error(index: usize, name: &str) -> json::Value {
    json::json!({"InstructionError": [index, name]})
}

fn observation(state: &[Account]) -> String {
    let account = &state[TARGET];
    format!(
        "{} {} {} {} {} {}",
        account.owner,
        account.data.len(),
        account.lamports,
        state[SPONSOR].lamports,
        state[VAULT].lamports,
        manifest::content_hash(&account.data),
    )
}

async fn state(er: &ErCtx, keys: &[Pubkey]) -> Result<Vec<Account>> {
    Ok(er
        .accounts(keys)
        .await?
        .into_iter()
        .map(Option::unwrap_or_default)
        .collect())
}

struct Record {
    tx: Transaction,
    info: TransactionInfo,
}

impl Record {
    fn check(
        &self,
        keys: &[Pubkey],
        before: &[Account],
        after: &[Account],
        expected_error: Option<json::Value>,
    ) -> Result<()> {
        check_eq!(
            self.info.err,
            expected_error,
            "{}: execution outcome; logs: {:?}",
            self.tx.signatures[0],
            self.info.logs
        )?;
        let mut balances = self.info.pre_balances.clone();
        check_eq!(
            balances.len(),
            self.tx.message.account_keys.len(),
            "complete transaction balances"
        )?;
        for (index, key) in self.tx.message.account_keys.iter().enumerate() {
            if let Some(i) = keys.iter().position(|tracked| tracked == key) {
                check_eq!(
                    balances[index],
                    before[i].lamports,
                    "{key}: transaction pre-balance"
                )?;
                balances[index] = after[i].lamports;
            }
        }
        check_eq!(
            self.info.post_balances,
            balances,
            "only rent transfers and payer fees change balances"
        )?;
        if self.info.err.is_none() {
            let observed: Vec<_> = self
                .info
                .logs
                .iter()
                .filter_map(|line| {
                    line.strip_prefix("Program log: Ephemeral: ")
                })
                .collect();
            check_eq!(
                observed,
                [observation(before), observation(after)],
                "fixture account observations"
            )?;
        }
        check!(
            self.info.logs.iter().any(|line| line
                .starts_with(&format!("Program {} invoke", magic_api::id()))),
            "ephemeral instruction executed"
        )?;
        Ok(())
    }
}

fn serialized(
    keys: &[Pubkey],
    before: &[Account],
    final_state: &[Account],
    records: &[(Action, Record)],
    used: u32,
) -> Option<Vec<usize>> {
    if used.count_ones() as usize == records.len() {
        return (before == final_state).then(Vec::new);
    }
    for (index, (action, record)) in records
        .iter()
        .enumerate()
        .filter(|(i, _)| used & (1 << i) == 0)
    {
        let mut after = before.to_vec();
        let outcome = action.apply(&mut after).map(|name| error(0, name));
        after[PAYER].lamports =
            after[PAYER].lamports.checked_sub(record.info.fee)?;
        if record.check(keys, before, &after, outcome).is_ok() {
            if let Some(mut order) = serialized(
                keys,
                &after,
                final_state,
                records,
                used | (1 << index),
            ) {
                order.insert(0, index);
                return Some(order);
            }
        }
    }
    None
}

struct Workload<'a> {
    er: &'a ErCtx,
    signers: Vec<Keypair>,
    keys: [Pubkey; 5],
    expected: Vec<Account>,
    report: ScenarioReport,
    nonce: usize,
    failures: Vec<String>,
}

impl Workload<'_> {
    async fn step(&mut self, label: &str, action: &Action) -> Result<()> {
        self.run(label, vec![action.instruction(&self.keys)], Ok(action))
            .await
    }

    async fn prepare(
        &mut self,
        mut instructions: Vec<Instruction>,
    ) -> Result<Transaction> {
        self.nonce += 1;
        instructions.push(Instruction {
            program_id: redshift_interface::id(),
            accounts: vec![],
            data: log_msg_data(&self.nonce.to_string()),
        });
        let mut tx =
            Transaction::new_with_payer(&instructions, Some(&self.keys[PAYER]));
        let required = &tx.message.account_keys
            [..tx.message.header.num_required_signatures as usize];
        let signers: Vec<_> = self
            .signers
            .iter()
            .filter(|s| required.contains(&s.pubkey()))
            .collect();
        tx.try_sign(&signers, self.er.api().get_latest_blockhash().await?)?;
        Ok(tx)
    }

    fn record(
        &mut self,
        label: &str,
        record: &Record,
        before: &[Account],
        after: &[Account],
    ) {
        self.report.config.push((
            label.into(),
            format!(
                "{} {}; fee={}; {} -> {}; payer={} -> {}; substitute={} -> {}",
                record.tx.signatures[0],
                record
                    .info
                    .err
                    .as_ref()
                    .map_or_else(|| "success".into(), ToString::to_string),
                record.info.fee,
                observation(before),
                observation(after),
                before[PAYER].lamports,
                after[PAYER].lamports,
                before[SUBSTITUTE].lamports,
                after[SUBSTITUTE].lamports,
            ),
        ));
    }

    async fn run(
        &mut self,
        label: &str,
        instructions: Vec<Instruction>,
        effect: std::result::Result<&Action, &str>,
    ) -> Result<()> {
        check_eq!(
            state(self.er, &self.keys).await?,
            self.expected,
            "{label}: before"
        )?;
        let error_index = instructions.len() - 1;
        let tx = self.prepare(instructions).await?;
        self.er.api().send_transaction(&tx).await?;
        let info = self
            .er
            .api()
            .await_transaction(&tx.signatures[0], TIMEOUT)
            .await?;
        let record = Record { tx, info };
        let before = self.expected.clone();
        let mut after = before.clone();
        let expected_error = match effect {
            Ok(action) => {
                check_eq!(
                    action.apply(&mut after),
                    None,
                    "{label}: valid operation"
                )?;
                None
            }
            Err(name) => Some(error(error_index, name)),
        };
        after[PAYER].lamports = after[PAYER]
            .lamports
            .checked_sub(record.info.fee)
            .ok_or("payer exhausted")?;
        check_eq!(
            state(self.er, &self.keys).await?,
            after,
            "{label}: state and accounting"
        )?;
        if let Err(error) =
            record.check(&self.keys, &before, &after, expected_error)
        {
            self.failures.push(format!(
                "{label} ({}): {error}",
                record.tx.signatures[0]
            ));
        }
        self.record(label, &record, &before, &after);
        self.expected = after;
        Ok(())
    }
}

#[async_trait(?Send)]
impl PrivateErScenario for EphemeralAccounts {
    fn name(&self) -> &str {
        "redhat/ephemeral_accounts"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let private = topology::private_er(
            base,
            ErOptions {
                label: "ephemeral-accounts".into(),
                ..ErOptions::default()
            },
        )
        .await?;
        let er = private.ctx();
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let mut signers = Vec::new();
        for _ in 0..3 {
            signers.push(
                prep::delegated_payer(
                    base,
                    &funder,
                    er.identity(),
                    crate::PAYER_LAMPORTS,
                )
                .await?,
            );
        }
        let target = Keypair::new();
        let keys = [
            signers[0].pubkey(),
            signers[1].pubkey(),
            target.pubkey(),
            EPHEMERAL_VAULT_ID,
            signers[2].pubkey(),
        ];
        signers.push(target);
        let foreign = topology::redshift_loader_v3_target().0;
        let mut workload = Workload {
            er,
            signers,
            keys,
            expected: state(er, &keys).await?,
            report: ScenarioReport::ok(self.name())
                .setting(
                    "accounts (payer, sponsor, target, vault, substitute)",
                    format!("{keys:?}"),
                )
                .setting("foreign caller", foreign)
                .setting(
                    "state columns",
                    "owner bytes lamports sponsor vault data-hash",
                ),
            nonce: 0,
            failures: Vec::new(),
        };
        check_eq!(
            workload.expected[TARGET],
            Account::default(),
            "fresh target"
        )?;
        for (label, operation) in [
            ("create", Create { data_len: 32 }),
            ("resize", Resize { new_data_len: 96 }),
            ("close", Close),
        ] {
            let mut action = Action::new(operation);
            let ix = action.instruction(&keys);
            for (case, index, value, reason) in [
                ("unsigned sponsor", 0, None, "MissingRequiredSignature"),
                (
                    "substituted vault",
                    2,
                    Some(keys[SUBSTITUTE]),
                    "InvalidAccountData",
                ),
            ] {
                let mut invalid = ix.clone();
                if let Some(key) = value {
                    invalid.accounts[index].pubkey = key;
                } else {
                    invalid.accounts[index].is_signer = false;
                }
                workload
                    .run(
                        &format!("{label}: {case}"),
                        vec![invalid],
                        Err(reason),
                    )
                    .await?;
            }
            let mut direct = action.clone();
            direct.caller = magic_api::id();
            workload
                .run(
                    &format!("{label}: top-level"),
                    vec![direct.instruction(&keys)],
                    Err("IncorrectProgramId"),
                )
                .await?;
            if matches!(action.operation, Create { .. }) {
                let mut unsigned = ix.clone();
                unsigned.accounts[1].is_signer = false;
                workload
                    .run(
                        "create: unsigned address",
                        vec![unsigned],
                        Err("MissingRequiredSignature"),
                    )
                    .await?;
                let mut occupied = ix.clone();
                occupied.accounts[1].pubkey = keys[SUBSTITUTE];
                workload
                    .run(
                        "create: funded address",
                        vec![occupied],
                        Err("InvalidAccountData"),
                    )
                    .await?;
            } else {
                let mut outsider = action.clone();
                outsider.caller = foreign;
                workload
                    .run(
                        &format!("{label}: foreign caller"),
                        vec![outsider.instruction(&keys)],
                        Err("InvalidAccountOwner"),
                    )
                    .await?;
            }
            workload
                .run(
                    &format!("{label}: rollback"),
                    vec![ix, direct.instruction(&keys)],
                    Err("IncorrectProgramId"),
                )
                .await?;
            action.fill = if matches!(action.operation, Create { .. }) {
                0xa5
            } else {
                0
            };
            workload.step(label, &action).await?;
            if matches!(action.operation, Create { .. }) {
                workload
                    .run(
                        "create: occupied ephemeral",
                        vec![action.instruction(&keys)],
                        Err("InvalidAccountData"),
                    )
                    .await?;
            }
            if matches!(action.operation, Resize { .. }) {
                for (size, fill) in [(16, 0), (64, 0), (0, 0), (24, 0x3c)] {
                    let mut resize = Action::new(Resize { new_data_len: size });
                    resize.fill = fill;
                    workload.step(&format!("resize: {size}"), &resize).await?;
                }
            }
        }
        for caller in [foreign, redshift_interface::id()] {
            let mut create = Action::new(Create { data_len: 48 });
            create.caller = caller;
            workload.step("recreate", &create).await?;
            let mut close = Action::new(Close);
            close.caller = if caller == foreign {
                redshift_interface::id()
            } else {
                foreign
            };
            workload
                .run(
                    "recreate: previous/foreign owner",
                    vec![close.instruction(&keys)],
                    Err("InvalidAccountOwner"),
                )
                .await?;
            close.caller = caller;
            workload.step("recreate: close", &close).await?;
        }
        for round in 0..3 {
            let mut create = Action::new(Create { data_len: 40 });
            create.fill = 0x5a;
            workload.step("conflicts: initialize", &create).await?;
            let mut prepared = Vec::new();
            for operation in [
                Resize { new_data_len: 80 },
                Resize { new_data_len: 8 },
                Close,
                Close,
                Create { data_len: 56 },
                Create { data_len: 112 },
            ] {
                let action = Action::new(operation);
                let tx =
                    workload.prepare(vec![action.instruction(&keys)]).await?;
                prepared.push((action, tx));
            }
            let records = try_join_all(prepared.into_iter().map(
                |(action, tx)| async move {
                    er.api().send_transaction(&tx).await?;
                    let info = er
                        .api()
                        .await_transaction(&tx.signatures[0], TIMEOUT)
                        .await?;
                    Ok::<_, redsuite_core::DynError>((
                        action,
                        Record { tx, info },
                    ))
                },
            ))
            .await?;
            let after = state(er, &keys).await?;
            let order =
                serialized(&keys, &workload.expected, &after, &records, 0)
                    .ok_or("conflicts have no valid serialized outcome")?;
            for index in order {
                let (action, record) = &records[index];
                let before = workload.expected.clone();
                if record.info.err.is_none() {
                    action.apply(&mut workload.expected);
                }
                workload.expected[PAYER].lamports -= record.info.fee;
                let current = workload.expected.clone();
                workload.record(
                    &format!(
                        "conflict {round}/{index}: {:?}",
                        action.operation
                    ),
                    record,
                    &before,
                    &current,
                );
            }
            check_eq!(workload.expected, after, "serialized final state")?;
            if after[TARGET].owner != system::system_id() {
                let close = Action::new(Close);
                workload.step("conflicts: close", &close).await?;
            }
        }
        let Workload {
            mut report,
            failures,
            nonce,
            ..
        } = workload;
        private.finish().await?;
        report.passed = failures.is_empty();
        Ok(report
            .setting("transactions inspected", nonce)
            .setting("failures", failures.join("; ")))
    }
}
