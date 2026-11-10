use std::{
    env, fs,
    path::{Path, PathBuf},
    rc::Rc,
};

use accountsdb::AccountsDB;
use anyhow::{ensure, Context};
use engine_account::{AccountMode, AccountSharedData, ReadableAccount};
use ledger::schema::{blockstore, BlockstoreEntry};
use magicblock_config::{
    config::LoadableProgram, consts::HEALTHCHECK_ACCOUNT_PUBKEY as SCRATCH, LeaderParams,
};
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    random,
    topology::{self, ReplicatedOptions, ReplicatedTopology, RestartConfig},
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use solana_slot_hashes::SlotHashes;
use tokio::{process::Command, time};
use v42_calculator_interface::{builder::Expr, ID as CALCULATOR};

use super::runtime::{self, Images, Runtime, TIMEOUT};

pub(crate) const LABEL: &str = "snapshot-bootstrap";
const VALUE: i64 = 4_242;
const ACCOUNTS: [(Pubkey, AccountMode); 3] = [
    (CALCULATOR, AccountMode::Runtime),
    (SCRATCH, AccountMode::Magic),
    (sdk_ids::sysvar::epoch_schedule::ID, AccountMode::Runtime),
];

pub(crate) fn settings(params: &mut LeaderParams) {
    super::settings(params);
    params.programs = vec![LoadableProgram {
        id: magicblock_config::types::SerdePubkey(CALCULATOR),
        path: calculator_path(),
    }];
}

fn calculator_path() -> PathBuf {
    env::var_os("MAGICBLOCK_CALCULATOR_SO").map(PathBuf::from).unwrap_or_else(|| {
        let bin = topology::er_bin_path().unwrap_or_default();
        bin.parent()
            .unwrap_or(Path::new("."))
            .join("../deploy/v42_calculator_program.so")
    })
}

pub struct SnapshotBootstrap {
    fixture: Runtime,
    cli: PathBuf,
}

impl Scenario for SnapshotBootstrap {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let cli = env::var_os("MAGICBLOCK_BIN")
            .map(PathBuf::from)
            .unwrap_or(topology::er_bin_path()?.with_file_name("magicblock"));
        ensure!(
            cli.is_file(),
            "magicblock CLI missing at {}; build it beside the validator or set MAGICBLOCK_BIN",
            cli.display()
        );
        Ok(Self {
            fixture: Runtime::new(base, er).await?,
            cli,
        })
    }

    async fn run(self) -> Result<()> {
        let mut topology = self.bootstrap().await?;
        let outcome = self.verify(&mut topology).await;
        outcome.and(topology.finish().await)
    }
}

impl SnapshotBootstrap {
    async fn bootstrap(&self) -> Result<ReplicatedTopology> {
        let er = &self.fixture.er;
        ensure!(
            self.fixture.base.account(&CALCULATOR).await?.is_none(),
            "calculator must be local-only"
        );
        let program = er.account(&CALCULATOR).await?.context("local calculator missing")?;
        ensure!(program.executable && program.data.ends_with(&fs::read(calculator_path())?));
        let payer = random::keypair(&mut random::rng(
            self.fixture.base.config().seed,
            &[LABEL],
            0,
        ));
        er.submit_and_confirm(&payer, &[Expr::lit(VALUE).compose(SCRATCH, &[])]).await?;
        let expected = self.fixture.images(&ACCOUNTS).await?;
        ensure!(expected[&SCRATCH].0.data == VALUE.to_le_bytes());
        let archive = runtime::next_snapshot(er).await?;
        let fixture = non_genesis(&archive, &expected)?;

        // Import authoritative state without importing the producer's ledger or counters.
        self.restart(
            &expected,
            RestartConfig {
                reset: true,
                before_start: Box::new(move |params| {
                    for path in
                        [&params.engine.accountsdb.directory, &params.engine.ledger.directory]
                    {
                        if path.exists() {
                            fs::remove_dir_all(path)?;
                        }
                    }
                    fs::create_dir_all(&params.engine.accountsdb.directory)?;
                    clonetree::clone_tree(
                        AccountsDB::directory(fixture.path()),
                        AccountsDB::directory(&params.engine.accountsdb.directory),
                        &Default::default(),
                    )?;
                    params.programs.clear();
                    Ok(())
                }),
                ..Default::default()
            },
        )
        .await?;
        runtime::next_snapshot(er).await?;
        let storage = er.storage_dir().await?;
        initial_anchor(&storage)?;

        // Freeze completed archives so the following write must arrive through replay.
        self.restart(
            &expected,
            RestartConfig {
                before_start: Box::new(|params| {
                    params.engine.blockstore.superblock = 0;
                    Ok(())
                }),
                ..Default::default()
            },
        )
        .await?;
        er.submit_and_confirm(&payer, &[Expr::lit(VALUE + 1).compose(SCRATCH, &[])])
            .await?;
        let topology = topology::replicated(
            &self.fixture.base,
            er.clone(),
            ReplicatedOptions {
                label: LABEL.into(),
                verifiers: 1,
            },
        )
        .await?;
        topology.wait_verifiers_connected(TIMEOUT).await?;
        ensure!(
            runtime::snapshots(topology.verifier(0)).await? >= 1.0,
            "empty verifier did not install a snapshot"
        );
        Ok(topology)
    }

    async fn verify(&self, topology: &mut ReplicatedTopology) -> Result<()> {
        let er = &self.fixture.er;
        let expected = self.fixture.images(&ACCOUNTS).await?;
        let signatures = self.fixture.signatures(&[SCRATCH]).await?;
        runtime::fence(topology).await?;
        runtime::inspect(topology.verifier_mut(0), &expected, &signatures).await?;

        self.restart(&expected, RestartConfig::default()).await?;
        topology.verifier_mut(0).start(TIMEOUT).await?;
        topology.wait_verifiers_connected(TIMEOUT).await?;
        runtime::fence(topology).await?;
        ensure!(
            runtime::snapshots(topology.verifier(0)).await? == 0.0,
            "warm verifier downloaded another snapshot"
        );
        runtime::inspect(topology.verifier_mut(0), &expected, &signatures).await?;

        topology.verifier_mut(0).start(TIMEOUT).await?;
        topology.wait_verifiers_connected(TIMEOUT).await?;
        let before = er.api().get_signatures_for_address(&SCRATCH, 32).await?;
        let output = time::timeout(
            TIMEOUT,
            Command::new(&self.cli)
                .args(["healthcheck", "--url", er.api().url().as_str(), "--timeout", "30s"])
                .env("RUST_LOG", "info")
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        ensure!(
            output.status.success(),
            "magicblock healthcheck failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let signature = wait::retry("healthcheck scratch transaction", TIMEOUT, async || {
            let after = er.api().get_signatures_for_address(&SCRATCH, 32).await?;
            let new: Vec<_> =
                after.into_iter().filter(|signature| !before.contains(signature)).collect();
            ensure!(
                new.len() == 1,
                "healthcheck did not produce exactly one scratch transaction"
            );
            Ok(new[0])
        })
        .await?;
        let tx = er.api().await_transaction(&signature).await?;
        ensure!(transaction_meta(&tx)?.err.is_none());
        let expected = self.fixture.images(&ACCOUNTS).await?;
        runtime::fence(topology).await?;
        ensure!(runtime::snapshots(topology.verifier(0)).await? == 0.0);
        runtime::inspect(topology.verifier_mut(0), &expected, &[(signature, tx.slot)]).await
    }

    async fn restart(&self, expected: &Images, config: RestartConfig) -> Result<()> {
        self.fixture.er.restart(config).await?;
        ensure!(
            self.fixture.images(&ACCOUNTS).await? == *expected,
            "restart changed the runtime image or scratch state"
        );
        Ok(())
    }
}

fn non_genesis(archive: &Path, expected: &Images) -> Result<tempfile::TempDir> {
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        let copy = tempfile::tempdir()?;
        tar::Archive::new(zstd::Decoder::new(fs::File::open(archive)?)?)
            .unpack(AccountsDB::directory(copy.path()))?;
        let source = AccountsDB::new(copy.path())?;
        runtime::check_images(&source, expected)?;
        let hashes = source
            .loader()
            .read(&sdk_ids::sysvar::slot_hashes::ID, |account| {
                account.data().to_vec()
            })?
            .context("snapshot SlotHashes missing")?;
        let hashes: SlotHashes = bincode::deserialize(&hashes)?;
        ensure!(
            source.slot() > 0
                && hashes
                    .slot_hashes()
                    .first()
                    .is_some_and(|(slot, hash)| *slot > 0 && *hash != Default::default()),
            "fixture has no nonzero hash-chain anchor"
        );
        let mut owners = topology::local_account_owners();
        owners.extend([runtime::FEATURES, sdk_ids::sysvar::ID, CALCULATOR]);
        owners.sort_unstable();
        owners.dedup();
        let mut entries: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        for owner in owners {
            for (key, account) in source.program(&owner, |_, account| account.owned())? {
                if account.mode().requires_persistence() {
                    entries.push((key, account.into()));
                }
            }
        }
        let temp = tempfile::tempdir()?;
        let db = AccountsDB::new(temp.path())?;
        db.store(&entries)?;
        db.set_slot(source.slot())?;
        db.advance_chain_slot(source.chain_slot());
        db.flush(true)?;
        ensure!(db.superblock() == 0 && db.transactions() == 0);
        runtime::check_images(&db, expected)?;
        Ok(temp)
    })
}

fn initial_anchor(storage: &Path) -> Result<()> {
    // This sealed prefix must remain available when the empty verifier connects.
    let bytes = fs::read(storage.join("superblock-000000001/blockstore.db"))?;
    let mut input = bytes.as_slice();
    loop {
        match blockstore::decode(&mut input)? {
            BlockstoreEntry::Reset(reset) => ensure!(reset.slot > 0),
            BlockstoreEntry::Block(block) => {
                ensure!(
                    block.slot > 0 && block.parent != Default::default(),
                    "retained history starts at genesis"
                );
                return Ok(());
            }
            _ => {}
        }
    }
}
