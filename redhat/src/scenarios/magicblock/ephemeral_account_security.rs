use std::{collections::BTreeMap, rc::Rc, result};

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
use rand::seq::SliceRandom;
use redshift::scenarios::chainlink::{cache_lifecycle, spl};
use solana_instruction_error::InstructionError;

use redsuite_core::{
    api::transaction_logs,
    dlp, expect_error,
    programs::{
        content_hash, CellHeader, Commit, IntentBundle, Redshift, SettlementMode, Task, MAGIC,
        REDSHIFT,
    },
    random::{self, Rng},
    rpc_types::response::TransactionError,
    system, topology,
    transaction_types::{UiTransactionError, UiTransactionStatusMeta},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use sdk::{
    consts::EPHEMERAL_VAULT_ID,
    ephemeral_accounts::rent,
    spl::builders::{
        DelegateEphemeralAtaBuilder, DepositSplTokensBuilder, InitializeEphemeralAtaBuilder,
        InitializeGlobalVaultBuilder,
    },
};
use signer::Signer;
use tracing::instrument;
use transaction::Transaction;

use crate::PAYER_LAMPORTS;

const PAYER: usize = 0;
const SPONSOR: usize = 1;
const TARGET: usize = 2;
const VAULT: usize = 3;
const SUBSTITUTE: usize = 4;
const VICTIM: usize = 5;

fn funding_plan(seed: u64) -> (impl Rng, Keypair) {
    let keys = &["redhat", "funding_attribution"];
    (
        random::rng(seed, keys, 0),
        random::keypair(&mut random::rng(seed, keys, 1)),
    )
}

fn funding_accounts(wallet: Pubkey, mint: Pubkey) -> [Pubkey; 10] {
    let eata = spl::derive_eata(&wallet, &mint);
    let global = spl::derive_global_vault(&mint);
    [
        wallet,
        mint,
        spl::derive_ata(&wallet, &mint),
        global,
        spl::derive_eata(&global, &mint),
        sdk::spl::find_vault_ata(&mint, &global),
        eata,
        dlp::delegation_record_pda(&eata),
        dlp::delegation_metadata_pda(&eata),
        sdk::pda::delegate_buffer_pda_from_delegated_account_and_owner_program(
            &eata,
            &sdk::consts::ESPL_TOKEN_PROGRAM_ID,
        ),
    ]
}

fn cache_pool(seed: u64) -> Vec<Pubkey> {
    (0..2 * cache_lifecycle::CACHE_CAPACITY)
        .map(|index| {
            random::keypair(&mut random::rng(
                seed,
                &["redhat", "confined_accounts", "pressure"],
                index as u64,
            ))
            .pubkey()
        })
        .collect()
}

fn failure_plan(seed: u64, target: Pubkey) -> (Vec<(SettlementMode, usize)>, [[Pubkey; 2]; 2]) {
    let owner = random::keypair(&mut random::rng(
        seed,
        &["redhat", "failure_placement", "fixtures"],
        0,
    ));
    let mut rng = random::rng(seed, &["redhat", "failure_placement", "plan"], 0);
    let mut cases = vec![
        (SettlementMode::Commit, 0),
        (SettlementMode::Commit, 2),
        (SettlementMode::Commit, 4),
        (SettlementMode::Undelegate, 0),
        (SettlementMode::Undelegate, 2),
        (SettlementMode::Undelegate, 4),
    ];
    cases.shuffle(&mut rng);
    let mut batches = [[target, target], [target, REDSHIFT.primary(owner.pubkey())]];
    for targets in &mut batches {
        targets.shuffle(&mut rng);
    }
    (cases, batches)
}

/// Checks ephemeral lifecycle authorization, rollback, rent, zeroing, and serializability.
pub struct EphemeralAccountSecurity {
    base: Rc<BaseCtx>,
    er: ErCtx,
    funder: Keypair,
    signers: Vec<Keypair>,
    target: Keypair,
    victim: Keypair,
    sizes: [u32; 4],
}

#[derive(Clone)]
struct Action {
    operation: MagicBlockInstruction,
    caller: Pubkey,
    fill: u8,
    sponsor: usize,
}

impl Action {
    fn new(operation: MagicBlockInstruction) -> Self {
        Self {
            operation,
            caller: redshift_interface::id(),
            fill: 0,
            sponsor: SPONSOR,
        }
    }

    fn instruction(&self, keys: &[Pubkey]) -> Instruction {
        self.wrap(self.native(keys), keys)
    }

    fn native(&self, keys: &[Pubkey]) -> Instruction {
        Instruction {
            program_id: magic_api::id(),
            accounts: vec![
                AccountMeta::new(keys[self.sponsor], true),
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

    fn apply(&self, state: &mut [Account], deposit: u64) -> Option<InstructionError> {
        let account = &state[TARGET];
        let exists = account.owner != system::system_id();
        let old_rent = if exists { rent(account.data.len() as u32) } else { 0 };
        let new_rent = match self.operation {
            Create { data_len } => {
                if exists || account.lamports != 0 {
                    return Some(InstructionError::InvalidAccountData);
                }
                rent(data_len)
            }
            Resize { .. } | Close => {
                if !exists {
                    return Some(InstructionError::InvalidAccountData);
                }
                if account.owner != self.caller {
                    return Some(InstructionError::InvalidAccountOwner);
                }
                if let Resize { new_data_len } = self.operation {
                    rent(new_data_len)
                } else {
                    0
                }
            }
            _ => unreachable!(),
        };
        let refund = old_rent.saturating_sub(new_rent).min(deposit);
        let payment = new_rent.saturating_sub(old_rent);
        let Some(sponsor) = state[self.sponsor]
            .lamports
            .checked_add(refund)
            .and_then(|balance| balance.checked_sub(payment))
        else {
            return Some(InstructionError::InsufficientFunds);
        };
        let Some(vault) = state[VAULT]
            .lamports
            .checked_add(payment)
            .and_then(|balance| balance.checked_sub(refund))
        else {
            return Some(InstructionError::InsufficientFunds);
        };
        let account = &mut state[TARGET];
        match self.operation {
            Create { data_len } => {
                account.owner = self.caller;
                account.rent_epoch = u64::MAX;
                account.data = vec![0; data_len as usize];
            }
            Resize { new_data_len } => account.data.resize(new_data_len as usize, 0),
            Close => *account = empty(),
            _ => unreachable!(),
        }
        if self.fill != 0 {
            account.data.fill(self.fill);
        }
        state[self.sponsor].lamports = sponsor;
        state[VAULT].lamports = vault;
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
                content_hash(&account.data)
            )
        })
        .collect()
}

fn empty() -> Account {
    Account {
        rent_epoch: u64::MAX,
        ..Account::default()
    }
}

async fn state(er: &ErCtx, keys: &[Pubkey]) -> Result<Vec<Account>> {
    let images = er.local_images(keys).await?;
    for index in [PAYER, SPONSOR, VAULT, SUBSTITUTE] {
        ensure!(
            images[index].is_some(),
            "protected local account {} missing",
            keys[index]
        );
    }
    Ok(images.into_iter().map(|account| account.unwrap_or_else(empty)).collect())
}

struct Record {
    tx: Transaction,
    info: UiTransactionStatusMeta,
}

impl Record {
    async fn submit(ctx: &impl ChainCtx, tx: Transaction) -> Result<Self> {
        let api = ctx.api();
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
        ensure!(
            self.info.err == expected_error,
            "{:?} against {expected_error:?}",
            self.info.err
        );
        self.check_balances(keys, before, after)?;
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

    fn check_balances(&self, keys: &[Pubkey], before: &[Account], after: &[Account]) -> Result<()> {
        let pre: u128 = self.info.pre_balances.iter().copied().map(u128::from).sum();
        let post: u128 = self.info.post_balances.iter().copied().map(u128::from).sum();
        ensure!(pre == post + u128::from(self.info.fee));
        let mut balances = self.info.pre_balances.clone();
        ensure!(balances.len() == self.tx.message.account_keys.len());
        for (index, key) in self.tx.message.account_keys.iter().enumerate() {
            if let Some(i) = keys.iter().position(|tracked| tracked == key) {
                ensure!(balances[index] == before[i].lamports);
                balances[index] = after[i].lamports;
            }
        }
        ensure!(self.info.post_balances == balances);
        Ok(())
    }
}

fn serialized(
    keys: &[Pubkey],
    before: &[Account],
    final_state: &[Account],
    records: &[(Action, Record)],
    used: u32,
    ledger: &RentLedger,
) -> Option<RentLedger> {
    if used.count_ones() as usize == records.len() {
        return (before == final_state).then(|| ledger.clone());
    }
    for (index, (action, record)) in
        records.iter().enumerate().filter(|(i, _)| used & (1 << i) == 0)
    {
        let mut after = before.to_vec();
        let deposit = ledger.deposits.get(&keys[TARGET]).copied()?;
        let outcome = action.apply(&mut after, deposit).map(|name| error(0, name));
        after[PAYER].lamports = after[PAYER].lamports.checked_sub(record.info.fee)?;
        if record.check(keys, before, &after, outcome).is_ok() {
            let mut next = ledger.clone();
            if next.apply(keys, keys[action.sponsor], before, &after).is_ok() {
                if let Some(next) = serialized(
                    keys,
                    &after,
                    final_state,
                    records,
                    used | (1 << index),
                    &next,
                ) {
                    return Some(next);
                }
            }
        }
    }
    None
}

#[derive(Clone, Default)]
struct RentLedger {
    deposits: BTreeMap<Pubkey, u64>,
    sponsors: BTreeMap<Pubkey, (u64, u64)>,
    opening_sponsors: BTreeMap<Pubkey, u64>,
    floor: u64,
    surplus: u64,
}

impl RentLedger {
    fn apply(
        &mut self,
        keys: &[Pubkey],
        sponsor: Pubkey,
        before: &[Account],
        after: &[Account],
    ) -> Result<()> {
        if after[VAULT].lamports == before[VAULT].lamports {
            self.check(after[VAULT].lamports)?;
            return self.check_sponsors(keys, after);
        }
        let deposit = self.deposits.entry(keys[TARGET]).or_default();
        let sponsor = self.sponsors.entry(sponsor).or_default();
        if after[VAULT].lamports >= before[VAULT].lamports {
            let paid = after[VAULT].lamports - before[VAULT].lamports;
            *deposit += paid;
            sponsor.0 += paid;
        } else {
            let refund = before[VAULT].lamports - after[VAULT].lamports;
            ensure!(
                refund <= *deposit,
                "refund {refund} exceeds funded deposit {deposit}"
            );
            *deposit -= refund;
            sponsor.1 += refund;
        }
        self.check(after[VAULT].lamports)?;
        self.check_sponsors(keys, after)
    }

    fn check_sponsors(&self, keys: &[Pubkey], images: &[Account]) -> Result<()> {
        for (key, (paid, refunded)) in &self.sponsors {
            let index =
                keys.iter().position(|candidate| candidate == key).context("sponsor missing")?;
            let surplus = if *key == keys[PAYER] { self.surplus } else { 0 };
            let expected = self
                .opening_sponsors
                .get(key)
                .context("sponsor opening balance missing")?
                .checked_add(*refunded)
                .and_then(|balance| balance.checked_sub(*paid))
                .and_then(|balance| balance.checked_sub(surplus))
                .context("sponsor ledger underflow")?;
            ensure!(
                images[index].lamports == expected,
                "sponsor {key}: expected {expected}, observed {}",
                images[index].lamports
            );
        }
        Ok(())
    }

    fn check(&self, vault: u64) -> Result<()> {
        ensure!(vault >= self.floor, "ephemeral vault crossed its floor");
        ensure!(
            self.deposits.values().copied().map(u128::from).sum::<u128>()
                + u128::from(self.surplus)
                <= u128::from(vault - self.floor)
        );
        Ok(())
    }
}

struct Workload {
    base: Rc<BaseCtx>,
    er: ErCtx,
    signers: Vec<Keypair>,
    keys: [Pubkey; 6],
    expected: Vec<Account>,
    nonce: usize,
    ledger: RentLedger,
    starting_vault: u64,
    base_images: Vec<Option<Account>>,
}

impl Workload {
    async fn step(&mut self, label: &str, action: &Action) -> Result<()> {
        self.run(label, vec![action.instruction(&self.keys)], Ok(action)).await
    }

    async fn step_victim(&mut self, label: &str, action: &Action) -> Result<()> {
        self.keys.swap(TARGET, VICTIM);
        self.expected.swap(TARGET, VICTIM);
        self.base_images.swap(TARGET, VICTIM);
        let result = self.step(label, action).await;
        self.keys.swap(TARGET, VICTIM);
        self.expected.swap(TARGET, VICTIM);
        self.base_images.swap(TARGET, VICTIM);
        result
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
        let sponsor =
            effect.as_ref().map_or(self.keys[SPONSOR], |action| self.keys[action.sponsor]);
        let expected_error = match effect {
            Ok(action) => action
                .apply(
                    &mut after,
                    self.ledger
                        .deposits
                        .get(&self.keys[TARGET])
                        .copied()
                        .context("target missing from the rent ledger")?,
                )
                .map(|reason| error(0, reason)),
            Err(name) => Some(error(error_index, name)),
        };
        after[PAYER].lamports =
            after[PAYER].lamports.checked_sub(record.info.fee).context("payer exhausted")?;
        let observed = state(&self.er, &self.keys).await?;
        ensure!(
            observed == after,
            "{label}: state {observed:?}, expected {after:?}"
        );
        record
            .check(&self.keys, &before, &after, expected_error)
            .with_context(|| label.to_owned())?;
        self.ledger.apply(&self.keys, sponsor, &before, &after)?;
        ensure!(self.base.accounts(&self.keys).await? == self.base_images);
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

    async fn check_raw_resize(&mut self) -> Result<()> {
        let length = self.expected[TARGET].data.len() as u32;
        for resized in [length.checked_sub(1), length.checked_add(1)].into_iter().flatten() {
            let before = self.expected.clone();
            let instruction =
                REDSHIFT.batch().resize_bytes(self.keys[TARGET], resized, 0x79).build();
            let tx = self.prepare(vec![instruction]).await?;
            let record = Record::submit(&self.er, tx).await?;
            ensure!(record.info.err == Some(error(0, InstructionError::InvalidRealloc)));
            ensure!(state(&self.er, &self.keys).await? == before);
            let byte = self.expected[TARGET].data.first().map_or(0x79, |byte| byte ^ 0xff);
            let instruction =
                REDSHIFT.batch().resize_bytes(self.keys[TARGET], length, byte).build();
            let tx = self.prepare(vec![instruction]).await?;
            let record = Record::submit(&self.er, tx).await?;
            ensure!(record.info.err.is_none() && record.info.fee == 0);
            if let Some(first) = self.expected[TARGET].data.first_mut() {
                *first = byte;
            }
            ensure!(state(&self.er, &self.keys).await? == self.expected);
            ensure!(self.base.accounts(&self.keys).await? == self.base_images);
        }
        Ok(())
    }

    async fn check_funding_roles(&mut self) -> Result<()> {
        for operation in [Create { data_len: 32 }, Resize { new_data_len: 33 }, Close] {
            let action = Action::new(operation);
            for omitted in [false, true] {
                let mut native = action.native(&self.keys);
                let reason = if omitted {
                    native.accounts.pop();
                    InstructionError::MissingAccount
                } else {
                    native.accounts[2].is_writable = false;
                    if matches!(action.operation, Close) {
                        InstructionError::ReadonlyLamportChange
                    } else {
                        InstructionError::ExternalAccountLamportSpend
                    }
                };
                self.run(
                    &format!("rent vault {:?}, omitted={omitted}", action.operation),
                    vec![action.wrap(native, &self.keys)],
                    Err(reason),
                )
                .await?;
            }
            let mut action = action;
            action.sponsor =
                if matches!(action.operation, Resize { .. }) { SUBSTITUTE } else { PAYER };
            self.step("funded sponsor substitution", &action).await?;
        }
        let mut unfunded = Action::new(Create { data_len: 1 });
        unfunded.sponsor = TARGET;
        self.step("target cannot fund its own rent", &unfunded).await?;
        ensure!(self.expected[TARGET] == empty());
        self.step(
            "funding role recovery",
            &Action::new(Create { data_len: 1 }),
        )
        .await?;
        self.step("funding role recovery close", &Action::new(Close)).await
    }

    #[instrument(skip_all, err)]
    async fn check_materialized_funding(&mut self) -> Result<()> {
        let (mut fixtures, mint) = funding_plan(self.base.config().seed);
        let owner = self.base.seeded_payer(&mut fixtures, PAYER_LAMPORTS).await?;
        let wallet = owner.pubkey();
        let mint_key = mint.pubkey();
        let keys = funding_accounts(wallet, mint_key);
        let ata = keys[2];
        let amount = 25;
        for (stage, instructions) in [
            vec![
                spl::allocate_mint(&wallet, &mint_key),
                spl::initialize_mint(&mint_key, &wallet),
                spl::create_ata_idempotent(&wallet, &wallet, &mint_key),
            ],
            vec![
                InitializeGlobalVaultBuilder { payer: wallet, mint: mint_key }.instruction(),
                InitializeEphemeralAtaBuilder {
                    payer: wallet,
                    user: wallet,
                    mint: mint_key,
                }
                .instruction(),
                spl::mint_to(&mint_key, &ata, &wallet, amount),
                DepositSplTokensBuilder {
                    authority: wallet,
                    user: wallet,
                    mint: mint_key,
                    amount,
                }
                .instruction(),
            ],
            vec![DelegateEphemeralAtaBuilder {
                payer: wallet,
                user: wallet,
                mint: mint_key,
                validator: Some(self.er.identity()),
            }
            .instruction()],
        ]
        .into_iter()
        .enumerate()
        {
            let before = self.base.accounts(&keys).await?;
            let mut transaction = Transaction::new_with_payer(&instructions, Some(&wallet));
            let required = &transaction.message.account_keys
                [..transaction.message.header.num_required_signatures as usize];
            let signers: Vec<_> = [&owner, &mint]
                .into_iter()
                .filter(|signer| required.contains(&signer.pubkey()))
                .collect();
            transaction.try_sign(&signers, self.base.api().get_latest_blockhash().await?)?;
            let record = Record::submit(self.base.as_ref(), transaction).await?;
            ensure!(record.info.err.is_none());
            let after = self.base.accounts(&keys).await?;
            let balances = |images: &[Option<Account>]| {
                images
                    .iter()
                    .cloned()
                    .map(|image| image.unwrap_or_else(empty))
                    .collect::<Vec<_>>()
            };
            record.check_balances(&keys, &balances(&before), &balances(&after))?;
            if stage == 0 {
                let cloned = after[2].clone().context("funded base ATA missing")?;
                ensure!(cloned.lamports > 0 && spl::account_balance(&cloned) == Some(0));
                ensure!(self.er.local_images(&[ata]).await? == vec![None]);
                self.er.await_program_clone(&sdk::consts::TOKEN_PROGRAM_ID).await?;
                self.er.accounts(&[wallet, mint_key]).await?;
                let mut source = self
                    .er
                    .local_images(&[self.er.identity()])
                    .await?
                    .pop()
                    .flatten()
                    .context("materialization sponsor missing")?;
                ensure!(self.er.account(&ata).await? == Some(cloned.clone()));
                source.lamports = source
                    .lamports
                    .checked_sub(cloned.lamports)
                    .context("materialization sponsor exhausted")?;
                let observed = self.er.local_images(&[self.er.identity()]).await?;
                ensure!(
                    observed == vec![Some(source.clone())],
                    "cloned rent funding: source {observed:?}, expected {source:?}"
                );
                ensure!(self.er.local_images(&[ata]).await? == vec![Some(cloned)]);
            }
            ensure!(state(&self.er, &self.keys).await? == self.expected);
            ensure!(self.base.accounts(&self.keys).await? == self.base_images);
        }
        let base = self.base.accounts(&keys).await?;
        let backing = base[6].as_ref().context("funded eATA missing")?;
        ensure!(backing.owner == dlp::dlp_id());
        ensure!(backing.data.get(64..72) == Some(amount.to_le_bytes().as_slice()));
        let mut projected = base[2].clone().context("projection base ATA missing")?;
        let cloned_funding = projected.lamports;
        ensure!(spl::account_balance(&projected) == Some(0));
        projected.data[64..72].copy_from_slice(&amount.to_le_bytes());
        projected.data[129..133].copy_from_slice(&1u32.to_le_bytes());
        projected.data[133..165].fill(0);
        self.er.accounts(&keys).await?;
        self.er.await_delegated(&ata, "funding projection").await?;
        self.er
            .await_account(&ata, |account| {
                ensure!(account == &projected);
                Ok(())
            })
            .await?;
        let source = self.er.local_images(&[self.er.identity()]).await?;
        ensure!(source.iter().all(Option::is_some));
        tracing::info!(
            cloned_funding,
            projected_reserve = projected.lamports,
            projected_funding = projected.lamports - cloned_funding,
            vault_credit = 0,
            "nonrefundable materialization funding"
        );
        for operation in [Create { data_len: 1 }, Close] {
            self.step(
                "funded lifecycle beside projected rent",
                &Action::new(operation),
            )
            .await?;
            ensure!(self.er.local_images(&[ata]).await? == vec![Some(projected.clone())]);
            ensure!(self.er.local_images(&[self.er.identity()]).await? == source);
            ensure!(self.base.accounts(&keys).await? == base);
        }
        self.ledger.check(self.expected[VAULT].lamports)
    }

    async fn check_commit_vault_roles(&mut self) -> Result<()> {
        let mut rng = random::rng(self.base.config().seed, &["redhat", "commit_fee_roles"], 0);
        let owner = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let cell = self
            .base
            .cells(REDSHIFT, &owner)
            .create_delegated(CellHeader::SIZE as u32 + 8, self.er.identity(), 0)
            .await?;
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        let tracked = [cell, vault, self.keys[PAYER], self.keys[VICTIM]];
        self.er.accounts(&tracked).await?;
        let base = self.base.accounts(&tracked).await?;
        let before = self.er.local_images(&tracked).await?;
        let context = self.er.local_images(&[magic_api::MAGIC_CONTEXT_PUBKEY]).await?;
        for (replacement, readonly, reason) in [
            (None, false, InstructionError::MissingAccount),
            (
                Some(self.keys[SUBSTITUTE]),
                false,
                InstructionError::MissingAccount,
            ),
            (Some(vault), true, InstructionError::IllegalOwner),
        ] {
            let mut native = MAGIC.commit(
                self.keys[PAYER],
                replacement,
                &[cell],
                SettlementMode::Commit,
            );
            if readonly {
                native.accounts[2].is_writable = false;
            }
            let tx = self.prepare(vec![REDSHIFT.batch().invoke(native).build()]).await?;
            let record = Record::submit(&self.er, tx).await?;
            ensure!(record.info.err == Some(error(0, reason)));
            ensure!(record.info.fee == 0 && record.info.pre_balances == record.info.post_balances);
            ensure!(self.er.local_images(&tracked).await? == before);
            ensure!(self.er.local_images(&[magic_api::MAGIC_CONTEXT_PUBKEY]).await? == context);
            ensure!(self.base.accounts(&tracked).await? == base);
        }
        let value = rng.gen_range(1..1_000_000);
        tracing::info!(%cell, %vault, value, "duplicate committee plan");
        let duplicate = MAGIC.commit(
            self.keys[PAYER],
            Some(vault),
            &[cell, cell],
            SettlementMode::Commit,
        );
        let tx = self
            .prepare(vec![REDSHIFT
                .batch()
                .set(&[cell], value)
                .invoke(duplicate)
                .build()])
            .await?;
        let record = Record::submit(&self.er, tx).await?;
        ensure!(
            record.info.err.is_none(),
            "duplicate committee: {:?}, {:?}",
            record.info.err,
            transaction_logs(&record.info)
        );
        ensure!(record.info.fee == 0 && record.info.pre_balances == record.info.post_balances);
        ensure!(self.er.local_images(&tracked[1..]).await? == before[1..]);
        ensure!(self.base.accounts(&tracked[1..]).await? == base[1..]);
        ensure!(state(&self.er, &self.keys).await? == self.expected);
        ensure!(self.base.accounts(&self.keys).await? == self.base_images);
        self.er
            .check_commit(&self.base, &record.tx.signatures[0], &[cell], false)
            .await?
            .check_success("duplicate committee commits one account once")?;
        let signature = self
            .er
            .cells(REDSHIFT, &owner)
            .settle(253, &[cell], SettlementMode::Commit)
            .await?;
        self.er
            .check_commit(&self.base, &signature, &[cell], false)
            .await?
            .check_success("commit after fee vault role refusals")?;
        Ok(())
    }

    async fn check_surplus_transfer(&mut self, amount: u64) -> Result<()> {
        ensure!(state(&self.er, &self.keys).await? == self.expected);
        let before = self.expected.clone();
        let tx = self
            .prepare(vec![system::transfer(
                &self.keys[PAYER],
                &self.keys[VAULT],
                amount,
            )])
            .await?;
        let record = Record::submit(&self.er, tx).await?;
        ensure!(record.info.err.is_none() && record.info.fee == 0);
        let mut after = before.clone();
        after[PAYER].lamports =
            after[PAYER].lamports.checked_sub(amount).context("payer exhausted")?;
        after[VAULT].lamports =
            after[VAULT].lamports.checked_add(amount).context("vault overflow")?;
        ensure!(state(&self.er, &self.keys).await? == after);
        record.check_balances(&self.keys, &before, &after)?;
        self.ledger.surplus += amount;
        self.ledger.check(after[VAULT].lamports)?;
        self.ledger.check_sponsors(&self.keys, &after)?;
        ensure!(self.base.accounts(&self.keys).await? == self.base_images);
        self.expected = after;
        Ok(())
    }

    async fn check_prior_refund(&mut self) -> Result<()> {
        self.check_surplus_transfer(1).await?;
        self.step_victim("prior refundable deposit close", &Action::new(Close)).await?;
        ensure!(self.expected[VICTIM] == empty());
        ensure!(self.expected[VAULT].lamports < self.starting_vault);
        ensure!(self.expected[VAULT].lamports == self.ledger.floor + self.ledger.surplus);
        Ok(())
    }

    async fn check_failure_placement(&mut self) -> Result<()> {
        let mut rng = random::rng(
            self.base.config().seed,
            &["redhat", "failure_placement", "fixtures"],
            0,
        );
        let owner = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let delegated = self
            .base
            .cells(REDSHIFT, &owner)
            .create_delegated(CellHeader::SIZE as u32 + 8, self.er.identity(), 0)
            .await?;
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        self.er.accounts(&[delegated, vault]).await?;
        self.er.await_program_clone(&topology::HYDRA_EPHEMERAL_ID).await?;
        self.step(
            "failure placement initialize",
            &Action::new(Create { data_len: 40 }),
        )
        .await?;
        let tracked = [delegated, vault];
        let baseline = self.base.accounts(&tracked).await?;
        let local = self.er.local_images(&tracked).await?;
        let (cases, batches) = failure_plan(self.base.config().seed, self.keys[TARGET]);
        ensure!(batches[1].contains(&delegated));
        for (case, (mode, failure)) in cases.into_iter().enumerate() {
            let mut bundle = IntentBundle::new(self.keys[PAYER]);
            bundle.validator = Some(self.er.identity());
            bundle.commits.push(Commit::new(mode, &[delegated]));
            let task = MAGIC.schedule(
                self.keys[PAYER],
                self.keys[TARGET],
                Task {
                    id: 250_000 + case as i64,
                    interval_ms: 100,
                    iterations: 1,
                },
                vec![REDSHIFT.batch().fill_bytes(self.keys[TARGET], 0xde).build()],
            );
            let mut instructions = vec![
                REDSHIFT.batch().fill_bytes(self.keys[TARGET], 0x25).build(),
                Action::new(Resize { new_data_len: 41 }).instruction(&self.keys),
                REDSHIFT.batch().intents(bundle).build(),
                task,
            ];
            instructions.insert(failure, REDSHIFT.batch().fail(250).build());
            let service = self.er.local_program_accounts(topology::SERVICE_OWNERS).await?;
            let tx = self.prepare(instructions).await?;
            let record = Record::submit(&self.er, tx).await?;
            ensure!(record.info.err == Some(error(failure, InstructionError::Custom(250))));
            ensure!(record.info.fee == 0 && record.info.pre_balances == record.info.post_balances);
            let boundary = self.er.api().get_slot().await? + 2;
            self.er.api().await_slot(boundary).await?;
            ensure!(state(&self.er, &self.keys).await? == self.expected);
            ensure!(
                self.er.local_program_accounts(topology::SERVICE_OWNERS).await? == service,
                "failed transaction published task or outbox state"
            );
            ensure!(self.base.accounts(&self.keys).await? == self.base_images);
            ensure!(self.base.accounts(&tracked).await? == baseline);
            ensure!(self.er.local_images(&tracked).await? == local);
        }
        for targets in batches {
            let service = self.er.local_program_accounts(topology::SERVICE_OWNERS).await?;
            let mut prepared = Vec::new();
            for target in targets {
                prepared.push(
                    self.prepare(vec![
                        REDSHIFT.batch().fill_bytes(target, 0x75).build(),
                        REDSHIFT.batch().fail(250).build(),
                    ])
                    .await?,
                );
            }
            let records =
                try_join_all(prepared.into_iter().map(|tx| Record::submit(&self.er, tx))).await?;
            for record in records {
                ensure!(record.info.err == Some(error(1, InstructionError::Custom(250))));
                ensure!(
                    record.info.fee == 0 && record.info.pre_balances == record.info.post_balances
                );
            }
            ensure!(state(&self.er, &self.keys).await? == self.expected);
            ensure!(self.er.local_program_accounts(topology::SERVICE_OWNERS).await? == service);
            ensure!(self.base.accounts(&self.keys).await? == self.base_images);
            ensure!(self.base.accounts(&tracked).await? == baseline);
            ensure!(self.er.local_images(&tracked).await? == local);
        }
        self.step(
            "failure placement same-address continuation",
            &Action::new(Resize { new_data_len: 41 }),
        )
        .await?;
        self.step("failure placement close", &Action::new(Close)).await?;
        let signature = self
            .er
            .cells(REDSHIFT, &owner)
            .settle(250, &[delegated], SettlementMode::Commit)
            .await?;
        self.er
            .check_commit(&self.base, &signature, &[delegated], false)
            .await?
            .check_success("settlement after failed service requests")?;
        Ok(())
    }

    async fn check_confined(&mut self) -> Result<()> {
        let mut rng = random::rng(self.base.config().seed, &["redhat", "confined_accounts"], 0);
        let owner = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let space = CellHeader::SIZE as u32 + 4;
        let confined = self
            .base
            .cells(REDSHIFT, &owner)
            .create_delegated(space, Pubkey::default(), 0)
            .await?;
        let delegated = self
            .base
            .cells(REDSHIFT, &owner)
            .create_delegated(space, self.er.identity(), 1)
            .await?;
        self.er.accounts(&[confined, delegated]).await?;
        let metadata = self
            .base
            .account(&dlp::delegation_metadata_pda(&confined))
            .await?
            .context("confined metadata missing")?;
        let metadata = sdk::dlp_api::state::DelegationMetadata::try_from_bytes_with_discriminator(
            &metadata.data,
        )?;
        ensure!(metadata.rent_payer == owner.pubkey());
        self.step_victim(
            "confined victim deposit",
            &Action::new(Create { data_len: 40 }),
        )
        .await?;
        let original_target = (
            self.keys[TARGET],
            self.expected[TARGET].clone(),
            self.base_images[TARGET].clone(),
        );
        self.keys[TARGET] = confined;
        self.expected[TARGET] = self
            .er
            .local_images(&[confined])
            .await?
            .pop()
            .flatten()
            .context("confined local image missing")?;
        self.base_images[TARGET] = self.base.account(&confined).await?;
        self.ledger.deposits.insert(confined, 0);
        ensure!(
            self.expected[TARGET].lamports == 0 && self.expected[TARGET].owner == REDSHIFT.id()
        );
        self.check_raw_resize().await?;
        let delegated_base = self.base.account(&delegated).await?;
        for resized in [space - 1, space] {
            let before = self
                .er
                .local_images(&[delegated])
                .await?
                .pop()
                .flatten()
                .context("delegated resize control missing")?;
            let instruction =
                REDSHIFT.batch().resize_bytes(delegated, resized, before.data[0]).build();
            let tx = self.prepare(vec![instruction]).await?;
            let record = Record::submit(&self.er, tx).await?;
            ensure!(record.info.err.is_none());
            let mut expected = before;
            expected.data.resize(resized as usize, 0);
            ensure!(self.er.local_images(&[delegated]).await? == vec![Some(expected)]);
            ensure!(self.base.account(&delegated).await? == delegated_base);
            ensure!(state(&self.er, &self.keys).await? == self.expected);
            ensure!(self.base.accounts(&self.keys).await? == self.base_images);
        }
        let delegated_local = self.er.local_images(&[delegated]).await?;
        let amount = rng.gen_range(1..64);
        let thief = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        self.base.delegate_payer(&owner, &thief, self.er.identity()).await?;
        let attacker = thief.pubkey();
        self.signers.push(thief);
        let drained = self
            .base
            .cells(REDSHIFT, &owner)
            .create_delegated(space, Pubkey::default(), 2)
            .await?;
        self.er.accounts(&[attacker, drained]).await?;
        tracing::info!(%confined, %delegated, %drained, %attacker, amount, "confined rent authority plan");
        let mut probe = self.keys;
        probe[TARGET] = delegated;
        self.run(
            "delegated close is not an ephemeral refund",
            vec![Action::new(Close).instruction(&probe)],
            Err(InstructionError::InvalidAccountData),
        )
        .await?;
        ensure!(self.er.local_images(&[delegated]).await? == delegated_local);
        ensure!(self.base.account(&delegated).await? == delegated_base);
        let restored = (
            self.keys[TARGET],
            self.expected[TARGET].clone(),
            self.base_images[TARGET].clone(),
        );
        self.keys[TARGET] = drained;
        self.expected[TARGET] = self
            .er
            .local_images(&[drained])
            .await?
            .pop()
            .flatten()
            .context("drained local image missing")?;
        self.base_images[TARGET] = self.base.account(&drained).await?;
        self.ledger.deposits.insert(drained, 0);
        ensure!(
            self.expected[TARGET].lamports == 0 && self.expected[TARGET].owner == REDSHIFT.id()
        );
        let theft = Action::new(Close);
        probe = self.keys;
        probe[SPONSOR] = attacker;
        self.run(
            "unrelated signer cannot collect confined rent",
            vec![theft.wrap(theft.native(&probe), &self.keys)],
            Ok(&theft),
        )
        .await
        .context("#251: an ephemeral close refunds only its own funded deposit")?;
        (
            self.keys[TARGET],
            self.expected[TARGET],
            self.base_images[TARGET],
        ) = restored;
        self.step_victim(
            "funded growth after the unrelated close",
            &Action::new(Resize { new_data_len: 41 }),
        )
        .await?;
        self.step_victim(
            "funded refund after the unrelated close",
            &Action::new(Resize { new_data_len: 40 }),
        )
        .await?;
        self.check_surplus_transfer(amount).await?;
        for mode in [SettlementMode::Commit, SettlementMode::Undelegate] {
            for targets in [vec![confined], vec![delegated, confined]] {
                let before = state(&self.er, &self.keys).await?;
                let contexts = self.er.local_images(&[magic_api::MAGIC_CONTEXT_PUBKEY]).await?;
                let transaction = Transaction::new_signed_with_payer(
                    &[REDSHIFT.settle(owner.pubkey(), &targets, mode)],
                    Some(&owner.pubkey()),
                    &[&owner],
                    self.er.api().get_latest_blockhash().await?,
                );
                let signature = self.er.api().send_transaction(&transaction).await?;
                let info = self.er.api().await_transaction(&signature).await?;
                let meta = redsuite_core::api::transaction_meta(&info)?;
                let refusal = match mode {
                    SettlementMode::Commit => InstructionError::InvalidAccountData,
                    SettlementMode::Undelegate => InstructionError::ReadonlyDataModified,
                };
                ensure!(
                    meta.err == Some(error(0, refusal)),
                    "confined settlement {targets:?}: {:?}, {:?}",
                    meta.err,
                    transaction_logs(meta)
                );
                ensure!(state(&self.er, &self.keys).await? == before);
                ensure!(
                    self.er.local_images(&[magic_api::MAGIC_CONTEXT_PUBKEY]).await? == contexts
                );
                ensure!(self.base.accounts(&self.keys).await? == self.base_images);
                ensure!(self.base.account(&delegated).await? == delegated_base);
                ensure!(self.er.local_images(&[delegated]).await? == delegated_local);
            }
        }
        let signature = self
            .er
            .cells(REDSHIFT, &owner)
            .settle(251, &[delegated], SettlementMode::Commit)
            .await?;
        self.er
            .check_commit(&self.base, &signature, &[delegated], false)
            .await?
            .check_success("delegated control beside confined account")?;

        ensure!(state(&self.er, &self.keys).await? == self.expected);
        ensure!(self.base.accounts(&self.keys).await? == self.base_images);
        let mut growth = Action::new(Resize { new_data_len: space + 1 });
        growth.sponsor = SUBSTITUTE;
        self.step("confined substitute-funded growth", &growth).await?;
        cache_lifecycle::pressure_preserves(
            &self.base,
            &self.er,
            owner.pubkey(),
            &cache_pool(self.base.config().seed),
            &self.keys,
        )
        .await?;
        ensure!(state(&self.er, &self.keys).await? == self.expected);
        self.ledger.check(self.expected[VAULT].lamports)?;
        self.ledger.check_sponsors(&self.keys, &self.expected)?;
        self.step(
            "confined funded shrink",
            &Action::new(Resize { new_data_len: space }),
        )
        .await?;
        self.step(
            "confined close cannot refund victim rent",
            &Action::new(Close),
        )
        .await
        .context("#251: confined close must refund only its own funded deposit")?;
        (
            self.keys[TARGET],
            self.expected[TARGET],
            self.base_images[TARGET],
        ) = original_target;
        self.step_victim("confined victim close", &Action::new(Close)).await
    }

    async fn check_charge_boundary(
        &mut self,
        label: &str,
        action: &Action,
        revert: &Action,
    ) -> Result<()> {
        ensure!(
            action.sponsor != PAYER && action.sponsor == revert.sponsor,
            "{label}: boundary needs one sponsor distinct from the payer"
        );
        let before = self.expected.clone();
        let &deposit = self
            .ledger
            .deposits
            .get(&self.keys[TARGET])
            .context("boundary target missing from the rent ledger")?;
        let mut probe = before.clone();
        probe[action.sponsor].lamports = u64::MAX;
        ensure!(
            action.apply(&mut probe, deposit).is_none(),
            "{label}: fully funded charge must be accepted"
        );
        let charge = probe[VAULT]
            .lamports
            .checked_sub(before[VAULT].lamports)
            .context("boundary charge must credit the vault")?;
        let balance = before[action.sponsor].lamports;
        ensure!(
            charge > 0 && balance > charge,
            "{label}: charge {charge} out of reach of balance {balance}"
        );
        let drain = balance - charge;
        tracing::info!(
            label,
            balance,
            deposit,
            affordable = charge,
            short = charge - 1,
            "ephemeral rent charge boundary"
        );
        for short in [true, false] {
            let moved = drain + u64::from(short);
            let mut funded = before.clone();
            funded[action.sponsor].lamports =
                balance.checked_sub(moved).context("sweep point below zero")?;
            funded[PAYER].lamports =
                before[PAYER].lamports.checked_add(moved).context("payer overflow")?;
            let drive = system::transfer(&self.keys[action.sponsor], &self.keys[PAYER], moved);
            let mut charged = funded.clone();
            let refusal = action.apply(&mut charged, deposit);
            if short {
                let reason = refusal
                    .with_context(|| format!("{label}: one lamport short must be refused"))?;
                let tx = self.prepare(vec![drive, action.instruction(&self.keys)]).await?;
                let record = Record::submit(&self.er, tx).await?;
                ensure!(
                    record.info.err == Some(error(1, reason)),
                    "{label}: short charge {:?}, {:?}",
                    record.info.err,
                    transaction_logs(&record.info)
                );
                ensure!(
                    record.info.fee == 0 && record.info.pre_balances == record.info.post_balances
                );
                ensure!(transaction_logs(&record.info)
                    .iter()
                    .any(|line| line.starts_with(&format!("Program {} invoke", magic_api::id()))));
                ensure!(state(&self.er, &self.keys).await? == before);
            } else {
                ensure!(
                    refusal.is_none(),
                    "{label}: exact balance refused as {refusal:?}"
                );
                ensure!(
                    charged[action.sponsor].lamports == 0,
                    "{label}: exact charge left {} unspent",
                    charged[action.sponsor].lamports
                );
                let funding = deposit.checked_add(charge).context("boundary deposit overflow")?;
                let mut refunded = charged.clone();
                ensure!(
                    revert.apply(&mut refunded, funding).is_none(),
                    "{label}: boundary revert refused"
                );
                let mut after = refunded.clone();
                after[action.sponsor].lamports = refunded[action.sponsor]
                    .lamports
                    .checked_add(moved)
                    .context("sponsor overflow")?;
                after[PAYER].lamports =
                    refunded[PAYER].lamports.checked_sub(moved).context("payer exhausted")?;
                ensure!(
                    after == before,
                    "{label}: sweep point must restore its opening state"
                );
                let tx = self
                    .prepare(vec![
                        drive,
                        action.instruction(&self.keys),
                        revert.instruction(&self.keys),
                        system::transfer(&self.keys[PAYER], &self.keys[action.sponsor], moved),
                    ])
                    .await?;
                let record = Record::submit(&self.er, tx).await?;
                ensure!(
                    record.info.err.is_none() && record.info.fee == 0,
                    "{label}: exact charge {:?}, {:?}",
                    record.info.err,
                    transaction_logs(&record.info)
                );
                let observed: Vec<_> = transaction_logs(&record.info)
                    .iter()
                    .filter_map(|line| line.strip_prefix("Program log: CellObservation: "))
                    .collect();
                ensure!(
                    observed
                        == [
                            observation(&funded),
                            observation(&charged),
                            observation(&charged),
                            observation(&refunded),
                        ]
                        .concat(),
                    "{label}: charge observations {observed:?}"
                );
                record.check_balances(&self.keys, &before, &after)?;
                ensure!(state(&self.er, &self.keys).await? == after);
            }
            self.ledger.check(before[VAULT].lamports)?;
            self.ledger.check_sponsors(&self.keys, &before)?;
            ensure!(self.base.accounts(&self.keys).await? == self.base_images);
        }
        Ok(())
    }

    async fn check_insufficient(&mut self) -> Result<()> {
        let seed = self.base.config().seed;
        let mut plan = random::rng(seed, &["redhat", "charge_boundaries"], 0);
        let sizes = [plan.gen_range(64..1024), plan.gen_range(1024..4096)];
        let growth = plan.gen_range(1..256);
        tracing::info!(
            attack_seed = seed,
            ?sizes,
            growth,
            charges = ?sizes.map(rent),
            growth_charge = rent(sizes[0] + growth) - rent(sizes[0]),
            "ephemeral rent charge boundary plan"
        );
        for size in sizes {
            self.check_charge_boundary(
                &format!("rent boundary create {size}"),
                &Action::new(Create { data_len: size }),
                &Action::new(Close),
            )
            .await?;
        }
        self.step(
            "rent boundary resize base",
            &Action::new(Create { data_len: sizes[0] }),
        )
        .await?;
        self.check_charge_boundary(
            &format!("rent boundary resize {} by {growth}", sizes[0]),
            &Action::new(Resize { new_data_len: sizes[0] + growth }),
            &Action::new(Resize { new_data_len: sizes[0] }),
        )
        .await?;
        self.step("rent boundary resize close", &Action::new(Close)).await?;
        let balance = self.expected[SPONSOR].lamports;
        let minimum = rent(0);
        ensure!(balance >= minimum && balance < rent(10 * 1024 * 1024));
        let size = ((balance - minimum) / 32 + 1) as u32;
        let create = Action::new(Create { data_len: size });
        self.step("insufficient rent", &create).await?;
        ensure!(self.expected[TARGET] == empty());
        self.step(
            "insufficient rent recovery",
            &Action::new(Create { data_len: 1 }),
        )
        .await?;
        self.step("insufficient rent recovery close", &Action::new(Close)).await?;
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
        self.ledger = serialized(&keys, &self.expected, &after, &records, 0, &self.ledger)
            .context("conflicts have no valid serialized outcome")?;
        ensure!(self.base.accounts(&keys).await? == self.base_images);
        self.expected = after.clone();
        self.step("conflicts: close", &Action::new(Close)).await?;

        Ok(())
    }
}

impl Scenario for EphemeralAccountSecurity {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let seed = base.config().seed;
        let mut plan = random::rng(seed, &["redhat", "ephemeral_account_security", "plan"], 0);
        let sizes = [0, 1, plan.gen_range(16..64), plan.gen_range(64..128)];
        let mut secret = [0u8; 32];
        random::rng(
            seed,
            &["redhat", "ephemeral_account_security", "targets"],
            0,
        )
        .fill(&mut secret);
        let target = Keypair::new_from_array(secret);
        random::rng(
            seed,
            &["redhat", "ephemeral_account_security", "targets"],
            1,
        )
        .fill(&mut secret);
        let victim = Keypair::new_from_array(secret);
        tracing::info!(attack_seed = seed, ?sizes, target = %target.pubkey(), victim = %victim.pubkey(), "ephemeral lifecycle plan");
        let (failures, batches) = failure_plan(seed, target.pubkey());
        tracing::info!(
            attack_seed = seed,
            failures = ?failures.iter().map(|(mode, index)| (matches!(mode, SettlementMode::Undelegate), index)).collect::<Vec<_>>(),
            ?batches,
            "failure placement plan"
        );
        let (mut funding, mint) = funding_plan(seed);
        let owner = random::keypair(&mut funding).pubkey();
        tracing::info!(attack_seed = seed, accounts = ?funding_accounts(owner, mint.pubkey()), amount = 25, "funding attribution plan");
        let mirror =
            random::keypair(&mut random::rng(seed, &["redhat", "confined_accounts"], 0)).pubkey();
        tracing::info!(attack_seed = seed, %mirror, pool = ?cache_pool(seed), "confined cache pressure plan");
        let mut rng = random::rng(
            seed,
            &["redhat", "ephemeral_account_security", "fixtures"],
            0,
        );
        let funder = base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let mut signers = Vec::new();
        for index in 0..3 {
            let balance = if index == SPONSOR { 1_000_000 } else { PAYER_LAMPORTS };
            let signer = base.seeded_payer(&mut rng, balance).await?;
            base.delegate_payer(&funder, &signer, er.identity()).await?;
            signers.push(signer);
        }
        Ok(Self {
            base,
            er,
            funder,
            signers,
            target,
            victim,
            sizes,
        })
    }

    async fn run(self) -> Result<()> {
        self.check_readonly_payer().await?;

        let sizes = self.sizes;
        let mut workload = self.into_workload().await?;
        ensure!(workload.expected[TARGET] == empty());
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
        for size in sizes {
            workload.step("seeded create", &Action::new(Create { data_len: size })).await?;
            workload.check_raw_resize().await?;
            workload
                .step(
                    "seeded resize",
                    &Action::new(Resize { new_data_len: size + 1 }),
                )
                .await?;
            workload.step("seeded close", &Action::new(Close)).await?;
        }
        workload.check_insufficient().await?;
        workload.check_conflicts().await?;
        workload.check_funding_roles().await?;
        workload.check_materialized_funding().await?;
        workload.check_commit_vault_roles().await?;
        workload.check_prior_refund().await?;
        workload
            .check_failure_placement()
            .await
            .context("#250: failed transaction effects")?;
        workload.check_confined().await?;
        Ok(())
    }
}

impl EphemeralAccountSecurity {
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
        let error = expect_error(attempt, "non-delegated payer cannot spend")?;
        let error = format!("{error:#}");
        ensure!(error.contains("InvalidAccountForFee") || error.contains("Immutable"));
        ensure!(self.er.accounts(&accounts).await? == before);
        Ok(())
    }

    async fn into_workload(mut self) -> Result<Workload> {
        let target = self.target;
        let keys = [
            self.signers[0].pubkey(),
            self.signers[1].pubkey(),
            target.pubkey(),
            EPHEMERAL_VAULT_ID,
            self.signers[2].pubkey(),
            self.victim.pubkey(),
        ];
        self.signers.push(target);
        self.signers.push(self.victim);
        self.er.accounts(&keys).await?;
        let expected = state(&self.er, &keys).await?;
        let base_images = self.base.accounts(&keys).await?;
        ensure!(expected[VICTIM] == empty());
        let mut workload = Workload {
            base: self.base,
            er: self.er,
            signers: self.signers,
            keys,
            expected: expected.clone(),
            nonce: 0,
            ledger: RentLedger {
                deposits: [TARGET, VICTIM].into_iter().map(|index| (keys[index], 0)).collect(),
                opening_sponsors: [PAYER, SPONSOR, SUBSTITUTE]
                    .into_iter()
                    .map(|index| (keys[index], expected[index].lamports))
                    .collect(),
                ..RentLedger::default()
            },
            starting_vault: 0,
            base_images,
        };
        let mut prior = Action::new(Create { data_len: 32 });
        prior.fill = 0x53;
        workload.step_victim("prior refundable deposit", &prior).await?;
        let refundable = workload.ledger.deposits[&keys[VICTIM]];
        ensure!(workload.expected[VICTIM].owner == prior.caller && refundable > 1);
        workload.starting_vault = workload.expected[VAULT].lamports;
        workload.ledger.floor =
            workload.starting_vault.checked_sub(refundable).context("vault underfunded")?;
        workload.ledger.check(workload.starting_vault)?;
        tracing::info!(
            starting_vault = workload.starting_vault,
            refundable,
            floor = workload.ledger.floor,
            "ephemeral vault baseline"
        );
        Ok(workload)
    }
}
