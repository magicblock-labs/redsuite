use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    rc::Rc,
    time::Duration,
};

use account::Account;
use anyhow::{ensure, Context};
use base64::{engine::general_purpose::STANDARD, Engine};
use engine_account::{AccountMode, ReadableAccount, StateFlags};
use json::{json, JsonValueTrait, Value};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    netfault::{Faults, RuleHandle, Selector},
    programs::REDLINE,
    random,
    topology::{RestartConfig, Verifier},
    wait, BaseCtx, ChainCtx, ErCtx, Result,
};
use signature::Signature;
use signer::Signer;
use solana_rent::Rent;
use tempfile::TempDir;
use tokio::task;
use tracing::instrument::WithSubscriber;

use super::observations::SUPERBLOCKS;

pub(super) const TIMEOUT: Duration = Duration::from_secs(60);
pub(super) const RENT: Pubkey = sdk_ids::sysvar::rent::ID;
pub(super) const FEATURES: Pubkey =
    Pubkey::from_str_const("Feature111111111111111111111111111111111111");
pub(super) type Images = BTreeMap<Pubkey, (Account, AccountMode)>;

pub(super) fn signers(base: &BaseCtx, label: &str) -> [Keypair; 2] {
    let mut rng = random::rng(base.config().seed, &[label], 0);
    std::array::from_fn(|_| random::keypair(&mut rng))
}

pub(super) async fn prepare(base: &BaseCtx, label: &str) -> Result<()> {
    let [funder, payer] = signers(base, label);
    for payer in [&funder, &payer] {
        ensure!(
            base.account(&payer.pubkey()).await?.is_none(),
            "runtime fixture already exists; use a fresh stack"
        );
        base.airdrop(&payer.pubkey(), crate::PAYER_LAMPORTS).await?;
    }
    let identity = redsuite_core::topology::identity_for_label(label)?.pubkey();
    base.delegate_payer(&funder, &payer, identity).await?;
    let cells = base.cells(REDLINE, &funder);
    let plain = cells.create(crate::ACCOUNT_SPACE, identity, 0).await?;
    cells.write(11, &[plain]).await?;
    cells.create_delegated(crate::ACCOUNT_SPACE, identity, 1).await?;
    Ok(())
}

pub(super) struct Runtime {
    pub base: Rc<BaseCtx>,
    pub er: ErCtx,
    pub rent: Rent,
    faults: Rc<Faults>,
    rule: RuleHandle,
}

impl Runtime {
    pub async fn new(base: Rc<BaseCtx>, er: ErCtx) -> Result<Self> {
        let faults = er.faults()?;
        let rent = Rent {
            lamports_per_byte: 5_080,
            ..Rent::default()
        };
        let rule = rent_rule(&faults, &rent)?;
        let runtime = Self { base, er, rent, faults, rule };
        runtime.er.restart(RestartConfig::default()).await?;
        runtime.check_rent().await?;
        Ok(runtime)
    }

    pub async fn lower_rent(&mut self) -> Result<()> {
        self.rent.lamports_per_byte = 4_000;
        let rule = rent_rule(&self.faults, &self.rent)?;
        std::mem::replace(&mut self.rule, rule).remove();
        self.er.restart(RestartConfig::default()).await?;
        self.check_rent().await
    }

    pub async fn check_rent(&self) -> Result<()> {
        let account = self.er.account(&RENT).await?.context("ER Rent sysvar missing")?;
        ensure!(bincode::deserialize::<Rent>(&account.data)? == self.rent);
        Ok(())
    }

    /// Supplies matching account and delegation-record funding at the test rent.
    pub async fn materialize(&self, key: Pubkey) -> Result<Account> {
        let mut account = self.base.account(&key).await?.context("base fixture missing")?;
        let minimum = self.rent.minimum_balance(account.data.len());
        account.lamports = minimum;
        let mut accounts = vec![(key, account)];
        if accounts[0].1.owner == redsuite_core::dlp::dlp_id() {
            let record = redsuite_core::dlp::delegation_record_pda(&key);
            let mut account =
                self.base.account(&record).await?.context("delegation record missing")?;
            let record_data =
                redsuite_core::dlp::DelegationRecord::try_from_bytes_with_discriminator_mut(
                    &mut account.data,
                )?;
            record_data.lamports = minimum;
            accounts.push((record, account));
        }
        let values: BTreeMap<_, _> = accounts
            .into_iter()
            .map(|(key, account)| (key.to_string(), account_value(&account)))
            .collect();
        let rule = self.faults.rewrite(
            Selector::methods(&["getAccountInfo", "getMultipleAccounts"]).http(),
            move |operation, reply| {
                if !reply["result"].is_object() {
                    return;
                }
                for (index, address) in operation.accounts.iter().enumerate() {
                    let Some(value) = values.get(address) else { continue };
                    let mut row = &mut reply["result"]["value"];
                    if operation.method == "getMultipleAccounts" {
                        row = &mut row[index];
                    }
                    if row.is_object() {
                        *row = value.clone();
                    }
                }
            },
        );
        let image = self.er.account(&key).await?;
        ensure!(
            rule.hits() != 0,
            "cold read of {key} bypassed the account fixture"
        );
        rule.remove();
        image.with_context(|| format!("cold read did not materialize {key}"))
    }

    pub async fn images(&self, accounts: &[(Pubkey, AccountMode)]) -> Result<Images> {
        let mut images = self
            .er
            .api()
            .get_program_accounts(&FEATURES)
            .await?
            .into_iter()
            .map(|(key, account)| (key, (account, AccountMode::Runtime)))
            .collect::<Images>();
        ensure!(!images.is_empty(), "no persisted active features");
        for (key, mode) in
            accounts.iter().copied().chain(std::iter::once((RENT, AccountMode::Runtime)))
        {
            images.insert(
                key,
                (
                    self.er.account(&key).await?.context("leader account missing")?,
                    mode,
                ),
            );
        }
        Ok(images)
    }

    pub async fn signatures(&self, keys: &[Pubkey]) -> Result<Vec<(Signature, u64)>> {
        let mut signatures = BTreeMap::new();
        for key in keys {
            let history = self.er.api().get_signatures_for_address(key, 32).await?;
            ensure!(
                !history.is_empty(),
                "no materialization transaction for {key}"
            );
            for signature in history {
                if signatures.contains_key(&signature) {
                    continue;
                }
                let tx = self.er.api().await_transaction(&signature).await?;
                ensure!(transaction_meta(&tx)?.err.is_none());
                signatures.insert(signature, tx.slot);
            }
        }
        Ok(signatures.into_iter().collect())
    }
}

fn account_value(account: &Account) -> Value {
    json!({
        "lamports": account.lamports, "owner": account.owner.to_string(),
        "data": [STANDARD.encode(&account.data), "base64"],
        "executable": account.executable, "rentEpoch": account.rent_epoch,
    })
}

fn rent_rule(faults: &Faults, rent: &Rent) -> Result<RuleHandle> {
    let data = json!([STANDARD.encode(bincode::serialize(rent)?), "base64"]);
    Ok(faults.rewrite(
        Selector::method("getAccountInfo").http().account(&RENT),
        move |_, reply| {
            if reply["result"]["value"].is_object() {
                reply["result"]["value"]["data"] = data.clone();
            }
        },
    ))
}

pub(super) async fn next_snapshot(er: &ErCtx) -> Result<PathBuf> {
    let storage = er.storage_dir().await?;
    let head = er
        .scrape_metrics()
        .await?
        .get(SUPERBLOCKS)
        .context("leader superblock metric missing")?;
    wait::retry(
        "the next complete replication snapshot",
        TIMEOUT,
        async || {
            let (id, archive) = redsuite_core::topology::latest_snapshot(&storage)?
                .context("snapshot unavailable")?;
            ensure!(id as f64 >= head);
            Ok(archive)
        },
    )
    .await
}

/// Inspects account images and transaction records from the same stopped verifier.
pub(super) async fn inspect(
    verifier: &mut Verifier,
    expected: &Images,
    signatures: &[(Signature, u64)],
) -> Result<()> {
    let (temp, accounts) = stop_and_copy(verifier, true).await?;
    async {
        let db = accountsdb::AccountsDB::new(accounts)?;
        db.validate()?;
        check_images(&db, expected)?;
        let storage = temp.path().join("ledger");
        let mut shutdown = nucleus::shutdown::ShutdownManager::default();
        let ledger = ledger::Ledger::init(storage, u64::MAX, &mut shutdown)?;
        let checked = async {
            for &(signature, slot) in signatures {
                let (request, response) = ledger::request::RequestPayload::new(signature);
                ledger
                    .reader
                    .send_async(ledger::request::ReadRequest::TransactionStatus(request))
                    .await?;
                let status =
                    response.recv_timeout().await??.context("follower transaction missing")?;
                ensure!(
                    status.slot == slot && status.result.is_ok(),
                    "follower execution differs for {signature}"
                );
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        shutdown.terminate().await;
        checked
    }
    .with_subscriber(tracing::subscriber::NoSubscriber::default())
    .await
}

pub(super) async fn inspect_delegated(
    verifier: &mut Verifier,
    key: Pubkey,
    expected: Account,
) -> Result<()> {
    let (temp, accounts) = stop_and_copy(verifier, false).await?;
    task::spawn_blocking(move || {
        let _temp = temp;
        tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
            let db = accountsdb::AccountsDB::new(accounts)?;
            check_image(&db, &key, &expected, AccountMode::Delegated)
        })
    })
    .await?
    .with_context(|| format!("checking persisted state on {}", verifier.label()))
}

/// Stops the verifier before copying. Inspectors must never open its original
/// state, since they can alter it before a later restart.
async fn stop_and_copy(verifier: &mut Verifier, with_ledger: bool) -> Result<(TempDir, PathBuf)> {
    verifier.stop_gracefully().await?;
    let accounts = verifier.accountsdb_dir().to_owned();
    let ledger = with_ledger.then(|| verifier.storage_dir().to_owned());
    // Copies of preallocated files can block other scenarios on the suite thread.
    task::spawn_blocking(move || -> Result<_> {
        let temp = tempfile::tempdir()?;
        if let Some(ledger) = ledger {
            let destination = temp.path().join("ledger");
            clonetree::clone_tree(&ledger, &destination, &Default::default())?;
            if let Ok(relative) = accounts.strip_prefix(&ledger) {
                return Ok((temp, destination.join(relative)));
            }
        }
        let destination = temp.path().join("accounts");
        clonetree::clone_tree(accounts, &destination, &Default::default())?;
        Ok((temp, destination))
    })
    .await?
    .with_context(|| {
        format!(
            "copying stopped verifier `{}` for inspection",
            verifier.label()
        )
    })
}

pub(super) fn check_images(db: &accountsdb::AccountsDB, expected: &Images) -> Result<()> {
    for (key, (account, mode)) in expected {
        check_image(db, key, account, *mode)?;
    }
    let features = db.program(&FEATURES, |_, _| ())?.map(|(key, _)| key).collect::<BTreeSet<_>>();
    ensure!(
        features
            == expected
                .iter()
                .filter(|(_, (account, _))| account.owner == FEATURES)
                .map(|(key, _)| *key)
                .collect::<BTreeSet<_>>(),
        "follower feature membership differs"
    );
    Ok(())
}

fn check_image(
    db: &accountsdb::AccountsDB,
    key: &Pubkey,
    account: &Account,
    mode: AccountMode,
) -> Result<()> {
    let matches = db
        .loader()
        .read(key, |image| {
            image.mode() == mode
                && image.owner() == &account.owner
                && image.lamports() == account.lamports
                && image.data() == account.data
                && image.flags().contains(StateFlags::EXECUTABLE) == account.executable
        })?
        .with_context(|| format!("follower account {key} ({mode:?}) missing"))?;
    ensure!(matches, "account image or mode differs for {key}");
    Ok(())
}
