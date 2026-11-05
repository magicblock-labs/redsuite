pub mod aperture;
pub mod chainlink;
pub mod magicblock;
use aperture::{
    DivergenceSignals, LedgerIndexSignals, OutcomeSignals, ProtocolInputs, PrunedReadSignals,
    SimulationIsolation, SlowSubscriberSignals, TransactionAdmission, TransactionAuthority,
    TransactionStructure, UnpaidWork,
};
use magicblock_config::types::Remote;
use redshift::scenarios::replication::replication_recovery::{Coverage, ReplicationRecovery};
use redsuite_core::{
    catalog::{BasePreparation, ErGroup, Fixture, ScenarioEntry, DEFAULT},
    scenario, topology, Scenario,
};
use std::time::Duration;

static PREPARATION: ErGroup = ErGroup {
    label: "hostile-preparation",
    settings: |p| p.remotes.retain(|remote| !matches!(remote, Remote::Grpc(_))),
    proxy: true,
    followers: 0,
};

static EPHEMERAL: ErGroup = ErGroup {
    label: "ephemeral-security",
    settings: |p| {
        p.engine.accountsdb.lru_capacity =
            redshift::scenarios::chainlink::cache_lifecycle::CACHE_CAPACITY
    },
    proxy: false,
    followers: 0,
};

static REPLICATION: ErGroup = ErGroup {
    label: "replication-security",
    settings: |p| {
        p.engine.blockstore.blocktime = Duration::from_millis(40);
        p.engine.blockstore.superblock = 512;
    },
    proxy: false,
    followers: 2,
};

static SIGNAL_INTEGRITY: ErGroup = ErGroup {
    label: "signal-integrity",
    settings: |p| {
        p.engine.blockstore.blocktime = Duration::from_millis(20);
        p.engine.blockstore.superblock = 64;
        p.engine.blockstore.checkpoint = 8;
        p.engine.ledger.size_limit = 0;
    },
    proxy: false,
    followers: 1,
};

static SIGNAL_RETENTION: ErGroup = ErGroup {
    label: "signal-retention",
    settings: |p| {
        p.engine.blockstore.blocktime = Duration::from_millis(8);
        p.engine.blockstore.superblock = 256;
        p.engine.ledger.size_limit = 0;
    },
    proxy: false,
    followers: 0,
};

static SIGNAL_PRESSURE: ErGroup = ErGroup {
    label: "signal-pressure",
    settings: |p| p.engine.blockstore.blocktime = Duration::from_millis(20),
    proxy: false,
    followers: 0,
};

static IDENTITIES: BasePreparation = BasePreparation {
    run: |_| {
        Box::pin(async {
            for group in [
                &DEFAULT,
                &PREPARATION,
                &EPHEMERAL,
                &REPLICATION,
                &SIGNAL_INTEGRITY,
                &SIGNAL_RETENTION,
                &SIGNAL_PRESSURE,
            ] {
                topology::identity_for_label(group.label)?;
            }
            Ok(())
        })
    },
};

pub fn catalog() -> Vec<ScenarioEntry> {
    vec![
        scenario!(
            ReplicationRecovery,
            exclusive(
                &REPLICATION,
                &[Fixture::RedshiftProgram],
                Coverage::Settlement
            )
        )
        .named("replication_settlement_recovery"),
        scenario!(
            TransactionStructure,
            exclusive(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            TransactionAuthority,
            exclusive(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            TransactionAdmission,
            exclusive(&PREPARATION, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            UnpaidWork,
            exclusive(&PREPARATION, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            OutcomeSignals,
            exclusive(&PREPARATION, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            DivergenceSignals,
            exclusive(&SIGNAL_INTEGRITY, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            PrunedReadSignals,
            exclusive(&SIGNAL_RETENTION, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            SlowSubscriberSignals,
            exclusive(&SIGNAL_PRESSURE, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            LedgerIndexSignals,
            exclusive(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            SimulationIsolation,
            exclusive(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            ProtocolInputs,
            exclusive(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            chainlink::settlement_authorization::SettlementAuthorization,
            exclusive(
                &PREPARATION,
                &[Fixture::RedhatProgram, Fixture::RedshiftProgram],
                ()
            )
        )
        .named("settlement_authorization"),
        scenario!(
            magicblock::ephemeral_account_security::EphemeralAccountSecurity,
            exclusive(&EPHEMERAL, &[Fixture::RedshiftProgram], ())
        )
        .named("ephemeral_account_security"),
    ]
    .into_iter()
    .map(|entry| entry.prepare_with(&IDENTITIES))
    .collect()
}
