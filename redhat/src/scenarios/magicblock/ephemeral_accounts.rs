use std::{rc::Rc, result, time::Duration};

use account::Account;
use anyhow::{ensure, Context};
use futures_util::{future::try_join_all, TryFutureExt};
use instruction::{AccountMeta, Instruction};
use json::Value;
use keypair::Keypair;
use magic_api::instruction::MagicBlockInstruction::{
    self, CloseEphemeralAccount as Close, CreateEphemeralAccount as Create,
    ResizeEphemeralAccount as Resize,
};
use pubkey::Pubkey;

use redsuite_core::{
    api::TransactionInfo,
    check, dlp, phase,
    programs::{Redshift, SettlementMode, MAGIC, REDSHIFT},
    receipt, system, topology, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use sdk::{consts::EPHEMERAL_VAULT_ID, ephemeral_accounts::rent};
use signer::Signer;
use transaction::Transaction;

use crate::PAYER_LAMPORTS;

const TIMEOUT: Duration = Duration::from_secs(30);
const PAYER: usize = 0;
const SPONSOR: usize = 1;
const TARGET: usize = 2;
const VAULT: usize = 3;
const SUBSTITUTE: usize = 4;

pub struct EphemeralAccounts {
    base: Rc<BaseCtx>,
    er: ErCtx,
    funder: Keypair,
    signers: Vec<Keypair>,
}

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
                AccountMeta::new(keys[TARGET], matches!(self.operation, Create { .. })),
                AccountMeta::new(keys[VAULT], false),
            ],
            data: self.operation.try_to_vec().expect("ephemeral instruction serialization"),
        };
        if self.caller == magic_api::id() {
            return native;
        }
        let observed = [keys[TARGET], keys[SPONSOR], keys[VAULT]];
        let mut batch = Redshift::at(self.caller).batch().observe(&observed).invoke(native);
        if self.fill != 0 {
            batch = batch.fill_bytes(keys[TARGET], self.fill);
        }
        batch.observe(&observed).build()
    }

    fn apply(&self, state: &mut [Account]) -> Option<&'static str> {
        let account = &mut state[TARGET];
        let exists = account.owner != system::system_id();
        let old_rent = if exists { rent(account.data.len() as u32) } else { 0 };
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

fn error(index: usize, name: &str) -> Value {
    json::json!({"InstructionError": [index, name]})
}

fn observation(state: &[Account]) -> Vec<String> {
    [TARGET, SPONSOR, VAULT]
        .into_iter()
        .map(|index| {
            let account = &state[index];
            format!(
                "{} {} {} {}",
                account.owner,
                account.data.len(),
                account.lamports,
                check::content_hash(&account.data)
            )
        })
        .collect()
}

async fn state(er: &ErCtx, keys: &[Pubkey]) -> Result<Vec<Account>> {
    Ok(er.accounts(keys).await?.into_iter().map(Option::unwrap_or_default).collect())
}

struct Record {
    tx: Transaction,
    info: TransactionInfo,
}

impl Record {
    async fn submit(er: &ErCtx, tx: Transaction) -> Result<Self> {
        let api = er.api();
        api.send_transaction(&tx).await?;
        let info = api.await_transaction(&tx.signatures[0], TIMEOUT).await?;
        Ok(Self { tx, info })
    }

    fn check(
        &self,
        keys: &[Pubkey],
        before: &[Account],
        after: &[Account],
        expected_error: Option<Value>,
    ) -> Result<()> {
        ensure!(
            self.info.err == expected_error,
            "expected {expected_error:?}, got {:?}; logs={:?}",
            self.info.err,
            self.info.logs
        );
        let mut balances = self.info.pre_balances.clone();
        ensure!(balances.len() == self.tx.message.account_keys.len());
        for (index, key) in self.tx.message.account_keys.iter().enumerate() {
            if let Some(i) = keys.iter().position(|tracked| tracked == key) {
                ensure!(balances[index] == before[i].lamports);
                balances[index] = after[i].lamports;
            }
        }
        ensure!(self.info.post_balances == balances);
        if self.info.err.is_none() {
            let observed: Vec<_> = self
                .info
                .logs
                .iter()
                .filter_map(|line| line.strip_prefix("Program log: CellObservation: "))
                .collect();
            ensure!(observed == [observation(before), observation(after)].concat());
        }
        ensure!(
            self.info
                .logs
                .iter()
                .any(|line| line.starts_with(&format!("Program {} invoke", magic_api::id()))),
            "ephemeral instruction executed"
        );
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
    for (index, (action, record)) in
        records.iter().enumerate().filter(|(i, _)| used & (1 << i) == 0)
    {
        let mut after = before.to_vec();
        let outcome = action.apply(&mut after).map(|name| error(0, name));
        after[PAYER].lamports = after[PAYER].lamports.checked_sub(record.info.fee)?;
        if record.check(keys, before, &after, outcome).is_ok() {
            if let Some(mut order) =
                serialized(keys, &after, final_state, records, used | (1 << index))
            {
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
    nonce: usize,
    failures: Vec<String>,
}

impl Workload<'_> {
    async fn step(&mut self, label: &str, action: &Action) -> Result<()> {
        self.run(label, vec![action.instruction(&self.keys)], Ok(action)).await
    }

    async fn prepare(&mut self, mut instructions: Vec<Instruction>) -> Result<Transaction> {
        self.nonce += 1;
        instructions.push(REDSHIFT.batch().nonce(self.nonce as u64).build());
        let mut tx = Transaction::new_with_payer(&instructions, Some(&self.keys[PAYER]));
        let required =
            &tx.message.account_keys[..tx.message.header.num_required_signatures as usize];
        let signers: Vec<_> =
            self.signers.iter().filter(|s| required.contains(&s.pubkey())).collect();
        tx.try_sign(&signers, self.er.api().get_latest_blockhash().await?)?;
        Ok(tx)
    }

    async fn run(
        &mut self,
        label: &str,
        instructions: Vec<Instruction>,
        effect: result::Result<&Action, &str>,
    ) -> Result<()> {
        ensure!(state(self.er, &self.keys).await? == self.expected);
        let error_index = instructions.len() - 1;
        let record = Record::submit(self.er, self.prepare(instructions).await?)
            .await
            .with_context(|| label.to_owned())?;
        let before = self.expected.clone();
        let mut after = before.clone();
        let expected_error = match effect {
            Ok(action) => {
                ensure!(action.apply(&mut after) == None);
                None
            }
            Err(name) => Some(error(error_index, name)),
        };
        after[PAYER].lamports =
            after[PAYER].lamports.checked_sub(record.info.fee).context("payer exhausted")?;
        ensure!(state(self.er, &self.keys).await? == after);
        if let Err(error) = record.check(&self.keys, &before, &after, expected_error) {
            self.failures.push(format!("{label}: {error:#}"));
        }
        self.expected = after;
        Ok(())
    }
}

impl Scenario for EphemeralAccounts {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let funder = base.funded_payer(PAYER_LAMPORTS).await?;
        let mut signers = Vec::new();
        for _ in 0..3 {
            signers.push(base.delegated_payer(&funder, er.identity(), PAYER_LAMPORTS).await?);
        }
        Ok(Self { base, er, funder, signers })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er, funder, mut signers } = self;
        // A delegated (mapped-signing) fee payer may commit ITSELF directly
        // via the magic program: it is the transaction signer and is delegated
        // to this ER, so the pipeline maps the signature and schedules the
        // commit. The vault is required because the payer is delegated.
        phase("mapped-signing self-commit", async {
            let payer = &signers[2];
            let vault = dlp::magic_fee_vault_pda(&er.identity());
            ensure!(
                er.account(&vault).await?.is_some(),
                "magic fee vault exists"
            );
            let signature = er
                .submit_and_confirm(
                    payer,
                    &[MAGIC.commit(
                        payer.pubkey(),
                        Some(vault),
                        &[payer.pubkey()],
                        SettlementMode::Commit,
                    )],
                )
                .await?;
            let commit_receipt =
                receipt::fetch_commit_receipt(er.api(), &signature, TIMEOUT).await?;
            commit_receipt.check_success("mapped-signing self-commit")?;
            ensure!(
                commit_receipt.included.contains(&payer.pubkey()),
                "self-commit includes payer; actual={:?}",
                commit_receipt.included
            );
            commit_receipt.confirm(base.api(), TIMEOUT).await
        })
        .await?;

        phase("readonly payer", async {
            let accounts = [funder.pubkey(), signers[0].pubkey()];
            let before = er.accounts(&accounts).await?;
            let attempt = er
                .submit_and_confirm(&funder, &[system::transfer(&accounts[0], &accounts[1], 1)])
                .await;
            let error = check::expect_error(attempt, "non-delegated payer cannot spend")?;
            let error = format!("{error:#}");
            ensure!(
                error.contains("InvalidAccountForFee") || error.contains("Immutable"),
                "non-delegated debit rejected by the access gate: {error:#}"
            );
            ensure!(er.accounts(&accounts).await? == before);
            Ok(())
        })
        .await?;

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
            er: &er,
            signers,
            keys,
            expected: state(&er, &keys).await?,
            nonce: 0,
            failures: Vec::new(),
        };
        ensure!(workload.expected[TARGET] == Account::default());
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
                workload.run(&format!("{label}: {case}"), vec![invalid], Err(reason)).await?;
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
            action.fill = if matches!(action.operation, Create { .. }) { 0xa5 } else { 0 };
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
            close.caller = if caller == foreign { redshift_interface::id() } else { foreign };
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
        for _ in 0..3 {
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
                let tx = workload.prepare(vec![action.instruction(&keys)]).await?;
                prepared.push((action, tx));
            }
            let records = phase(
                "conflicts",
                try_join_all(prepared.into_iter().map(|(action, tx)| {
                    Record::submit(&er, tx).map_ok(move |record| (action, record))
                })),
            )
            .await?;
            let after = state(&er, &keys).await?;
            let order = serialized(&keys, &workload.expected, &after, &records, 0)
                .context("conflicts have no valid serialized outcome")?;
            for index in order {
                let (action, record) = &records[index];
                if record.info.err.is_none() {
                    action.apply(&mut workload.expected);
                }
                workload.expected[PAYER].lamports -= record.info.fee;
            }
            ensure!(workload.expected == after);
            if after[TARGET].owner != system::system_id() {
                let close = Action::new(Close);
                workload.step("conflicts: close", &close).await?;
            }
        }
        let failures = workload.failures;
        ensure!(
            failures.is_empty(),
            "ephemeral receipt/accounting failures: {}",
            failures.join("; ")
        );
        Ok(())
    }
}
