pub mod aperture;
pub mod chainlink;
pub mod magicblock;
use aperture::{
    DivergenceSignals, LedgerIndexSignals, OutcomeSignals, ProtocolInputs, SimulationIsolation,
    SlowSubscriberSignals, TransactionAdmission, TransactionAuthority, TransactionStructure,
    UnpaidWork,
};
use redshift::scenarios::replication::{
    observations::CHECKPOINT_SLOTS, replication_settlement_recovery::ReplicationSettlementRecovery,
};
use redsuite_core::{
    catalog::{BasePreparation, ErGroup, Fixture, ScenarioEntry, DEFAULT},
    scenario, topology, Scenario,
};
use std::time::Duration;

static PREPARATION: ErGroup = ErGroup {
    settings: |p| {
        for stream in &mut p.chainsync.grpc {
            stream.duplication_delay = Duration::from_secs(3600);
        }
    },
    proxy: true,
    ..ErGroup::new("hostile-preparation")
};

static SETTLEMENT: ErGroup = ErGroup {
    settings: PREPARATION.settings,
    proxy: true,
    ..ErGroup::new("settlement-authorization")
};

static EPHEMERAL: ErGroup = ErGroup {
    settings: |p| {
        p.engine.accountsdb.lru_capacity =
            redshift::scenarios::chainlink::cache_lifecycle::CACHE_CAPACITY
    },
    ..ErGroup::new("ephemeral-security")
};

static REPLICATION: ErGroup = ErGroup {
    settings: |p| {
        p.engine.blockstore.blocktime = Duration::from_millis(40);
        p.engine.blockstore.superblock = 0;
        p.engine.blockstore.checkpoint = CHECKPOINT_SLOTS;
    },
    followers: 2,
    ..ErGroup::new("replication-security")
};

static SIGNAL_INTEGRITY: ErGroup = ErGroup {
    settings: |p| {
        p.engine.blockstore.blocktime = Duration::from_millis(20);
        p.engine.blockstore.superblock = 64;
        p.engine.blockstore.checkpoint = 8;
        p.engine.ledger.size_limit = 0;
    },
    followers: 1,
    ..ErGroup::new("signal-integrity")
};

static SIGNAL_PRESSURE: ErGroup = ErGroup {
    settings: |p| p.engine.blockstore.blocktime = Duration::from_millis(20),
    ..ErGroup::new("signal-pressure")
};

static IDENTITIES: BasePreparation = BasePreparation {
    run: |_| {
        Box::pin(async {
            for group in [
                &DEFAULT,
                &PREPARATION,
                &SETTLEMENT,
                &EPHEMERAL,
                &REPLICATION,
                &SIGNAL_INTEGRITY,
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
            ReplicationSettlementRecovery,
            exclusive(&REPLICATION, &[Fixture::RedshiftProgram], ())
        ),
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
                &SETTLEMENT,
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
