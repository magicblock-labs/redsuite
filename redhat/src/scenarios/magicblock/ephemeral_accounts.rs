use std::{rc::Rc, result};

use account::Account;
use anyhow::{ensure, Context};
use futures_util::{future::try_join_all, TryFutureExt};
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use magic_api::instruction::MagicBlockInstruction::{
    self, CloseEphemeralAccount as Close, CreateEphemeralAccount as Create,
    ResizeEphemeralAccount as Resize,
};
use pubkey::Pubkey;
use solana_instruction_error::InstructionError;

use redsuite_core::{
    api::transaction_logs,
    check, dlp,
    programs::{Redshift, SettlementMode, MAGIC, REDSHIFT},
    receipt,
    rpc_types::response::TransactionError,
    system, topology,
    transaction_types::{UiTransactionError, UiTransactionStatusMeta},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use sdk::{consts::EPHEMERAL_VAULT_ID, ephemeral_accounts::rent};
use signer::Signer;
use tracing::instrument;
use transaction::Transaction;

use crate::PAYER_LAMPORTS;

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
        self.wrap(self.native(keys), keys)
    }

    fn native(&self, keys: &[Pubkey]) -> Instruction {
        Instruction {
            program_id: magic_api::id(),
            accounts: vec![
                AccountMeta::new(keys[SPONSOR], true),
                AccountMeta::new(keys[TARGET], matches!(self.operation, Create { .. })),
                AccountMeta::new(keys[VAULT], false),
            ],
            data: self.operation.try_to_vec().expect("ephemeral instruction serialization"),
        }
    }

    fn wrap(&self, native: Instruction, keys: &[Pubkey]) -> Instruction {
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

    fn apply(&self, state: &mut [Account]) -> Option<InstructionError> {
        let account = &mut state[TARGET];
        let exists = account.owner != system::system_id();
        let old_rent = if exists { rent(account.data.len() as u32) } else { 0 };
        match self.operation {
            Create { data_len } => {
                if exists || account.lamports != 0 {
                    return Some(InstructionError::InvalidAccountData);
                }
                account.owner = self.caller;
                account.rent_epoch = u64::MAX;
                account.data = vec![0; data_len as usize];
            }
            Resize { .. } | Close => {
                if !exists {
                    return Some(InstructionError::InvalidAccountData);
                }
                if account.owner != self.caller {
                    return Some(InstructionError::InvalidAccountOwner);
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

fn error(index: usize, error: InstructionError) -> UiTransactionError {
    TransactionError::InstructionError(index as u8, error).into()
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
    info: UiTransactionStatusMeta,
}

impl Record {
    async fn submit(er: &ErCtx, tx: Transaction) -> Result<Self> {
        let api = er.api();
        api.send_transaction(&tx).await?;
        let info = api
            .await_transaction(&tx.signatures[0])
            .await?
            .transaction
            .meta
            .context("transaction has no execution metadata")?;
        Ok(Self { tx, info })
    }

    fn check(
        &self,
        keys: &[Pubkey],
        before: &[Account],
        after: &[Account],
        expected_error: Option<UiTransactionError>,
    ) -> Result<()> {
        ensure!(self.info.err == expected_error);
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
            let observed: Vec<_> = transaction_logs(&self.info)
                .iter()
                .filter_map(|line| line.strip_prefix("Program log: CellObservation: "))
                .collect();
            ensure!(observed == [observation(before), observation(after)].concat());
        }
        ensure!(transaction_logs(&self.info)
            .iter()
            .any(|line| line.starts_with(&format!("Program {} invoke", magic_api::id()))));
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

struct Workload {
    er: ErCtx,
    signers: Vec<Keypair>,
    keys: [Pubkey; 5],
    expected: Vec<Account>,
    nonce: usize,
    failures: Vec<String>,
}

impl Workload {
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
        effect: result::Result<&Action, InstructionError>,
    ) -> Result<()> {
        ensure!(state(&self.er, &self.keys).await? == self.expected);
        let error_index = instructions.len() - 1;
        let tx = self.prepare(instructions).await?;
        let record = Record::submit(&self.er, tx).await.with_context(|| label.to_owned())?;
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
        let observed = state(&self.er, &self.keys).await?;
        ensure!(
            observed == after,
            "{label}: state {observed:?}, expected {after:?}"
        );
        if let Err(error) = record.check(&self.keys, &before, &after, expected_error) {
            self.failures.push(format!("{label}: {error:#}"));
        }
        self.expected = after;
        Ok(())
    }

    #[instrument(skip_all, err, fields(case = label))]
    async fn check_operation(
        &mut self,
        label: &str,
        operation: MagicBlockInstruction,
    ) -> Result<()> {
        let keys = self.keys;
        let foreign = topology::redshift_loader_v3_target().0;

        let mut action = Action::new(operation);
        let native = action.native(&keys);
        for (case, index, value, reason) in [
            (
                "unsigned sponsor",
                0,
                None,
                InstructionError::MissingRequiredSignature,
            ),
            (
                "substituted vault",
                2,
                Some(keys[SUBSTITUTE]),
                InstructionError::InvalidAccountData,
            ),
        ] {
            let mut invalid = native.clone();
            if let Some(key) = value {
                invalid.accounts[index].pubkey = key;
            } else {
                invalid.accounts[index].is_signer = false;
            }
            self.run(
                &format!("{label}: {case}"),
                vec![action.wrap(invalid, &keys)],
                Err(reason),
            )
            .await?;
        }
        let mut direct = action.clone();
        direct.caller = magic_api::id();
        self.run(
            &format!("{label}: top-level"),
            vec![direct.instruction(&keys)],
            Err(InstructionError::IncorrectProgramId),
        )
        .await?;
        if matches!(action.operation, Create { .. }) {
            let mut unsigned = native.clone();
            unsigned.accounts[1].is_signer = false;
            self.run(
                "create: unsigned address",
                vec![action.wrap(unsigned, &keys)],
                Err(InstructionError::MissingRequiredSignature),
            )
            .await?;
            let mut occupied = native;
            occupied.accounts[1].pubkey = keys[SUBSTITUTE];
            self.run(
                "create: funded address",
                vec![action.wrap(occupied, &keys)],
                Err(InstructionError::InvalidAccountData),
            )
            .await?;
        } else {
            let mut outsider = action.clone();
            outsider.caller = foreign;
            self.run(
                &format!("{label}: foreign caller"),
                vec![outsider.instruction(&keys)],
                Err(InstructionError::InvalidAccountOwner),
            )
            .await?;
        }
        self.run(
            &format!("{label}: rollback"),
            vec![action.instruction(&keys), direct.instruction(&keys)],
            Err(InstructionError::IncorrectProgramId),
        )
        .await?;
        action.fill = if matches!(action.operation, Create { .. }) { 0xa5 } else { 0 };
        self.step(label, &action).await?;
        if matches!(action.operation, Create { .. }) {
            self.run(
                "create: occupied ephemeral",
                vec![action.instruction(&keys)],
                Err(InstructionError::InvalidAccountData),
            )
            .await?;
        }
        if matches!(action.operation, Resize { .. }) {
            for (size, fill) in [(16, 0), (64, 0), (0, 0), (24, 0x3c)] {
                let mut resize = Action::new(Resize { new_data_len: size });
                resize.fill = fill;
                self.step(&format!("resize: {size}"), &resize).await?;
            }
        }

        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_recreation(&mut self, caller: Pubkey) -> Result<()> {
        let keys = self.keys;
        let foreign = topology::redshift_loader_v3_target().0;

        let mut create = Action::new(Create { data_len: 48 });
        create.caller = caller;
        self.step("recreate", &create).await?;
        let mut close = Action::new(Close);
        close.caller = if caller == foreign { redshift_interface::id() } else { foreign };
        self.run(
            "recreate: previous/foreign owner",
            vec![close.instruction(&keys)],
            Err(InstructionError::InvalidAccountOwner),
        )
        .await?;
        close.caller = caller;
        self.step("recreate: close", &close).await?;

        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_conflicts(&mut self) -> Result<()> {
        let keys = self.keys;

        let mut create = Action::new(Create { data_len: 40 });
        create.fill = 0x5a;
        self.step("conflicts: initialize", &create).await?;
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
            let tx = self.prepare(vec![action.instruction(&keys)]).await?;
            prepared.push((action, tx));
        }
        let records = try_join_all(prepared.into_iter().map(|(action, tx)| {
            Record::submit(&self.er, tx).map_ok(move |record| (action, record))
        }))
        .await?;
        let after = state(&self.er, &keys).await?;
        let order = serialized(&keys, &self.expected, &after, &records, 0)
            .context("conflicts have no valid serialized outcome")?;
        for index in order {
            let (action, record) = &records[index];
            if record.info.err.is_none() {
                action.apply(&mut self.expected);
            }
            self.expected[PAYER].lamports -= record.info.fee;
        }
        ensure!(self.expected == after);
        if after[TARGET].owner != system::system_id() {
            let close = Action::new(Close);
            self.step("conflicts: close", &close).await?;
        }

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
        self.check_self_commit().await?;
        self.check_readonly_payer().await?;

        let mut workload = self.into_workload().await?;
        ensure!(workload.expected[TARGET] == Account::default());
        for (label, operation) in [
            ("create", Create { data_len: 32 }),
            ("resize", Resize { new_data_len: 96 }),
            ("close", Close),
        ] {
            workload.check_operation(label, operation).await?;
        }
        let foreign = topology::redshift_loader_v3_target().0;
        for caller in [foreign, redshift_interface::id()] {
            workload.check_recreation(caller).await?;
        }
        for _ in 0..3 {
            workload.check_conflicts().await?;
        }
        ensure!(workload.failures.is_empty());
        Ok(())
    }
}

impl EphemeralAccounts {
    #[instrument(skip_all, err)]
    async fn check_self_commit(&self) -> Result<()> {
        // A delegated fee payer can commit itself through mapped signing;
        // its delegation requires the validator's fee vault.
        let payer = &self.signers[2];
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        ensure!(self.er.account(&vault).await?.is_some());
        let signature = self
            .er
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
        let commit_receipt = receipt::fetch_commit_receipt(self.er.api(), &signature).await?;
        commit_receipt.check_success("mapped-signing self-commit")?;
        ensure!(commit_receipt.included.contains(&payer.pubkey()));
        commit_receipt.confirm(self.base.api()).await
    }

    #[instrument(skip_all, err)]
    async fn check_readonly_payer(&self) -> Result<()> {
        let accounts = [self.funder.pubkey(), self.signers[0].pubkey()];
        let before = self.er.accounts(&accounts).await?;
        let attempt = self
            .er
            .submit_and_confirm(
                &self.funder,
                &[system::transfer(&accounts[0], &accounts[1], 1)],
            )
            .await;
        let error = check::expect_error(attempt, "non-delegated payer cannot spend")?;
        let error = format!("{error:#}");
        ensure!(error.contains("InvalidAccountForFee") || error.contains("Immutable"));
        ensure!(self.er.accounts(&accounts).await? == before);
        Ok(())
    }

    async fn into_workload(mut self) -> Result<Workload> {
        let target = Keypair::new();
        let keys = [
            self.signers[0].pubkey(),
            self.signers[1].pubkey(),
            target.pubkey(),
            EPHEMERAL_VAULT_ID,
            self.signers[2].pubkey(),
        ];
        self.signers.push(target);
        let expected = state(&self.er, &keys).await?;
        Ok(Workload {
            er: self.er,
            signers: self.signers,
            keys,
            expected,
            nonce: 0,
            failures: Vec::new(),
        })
    }
}
