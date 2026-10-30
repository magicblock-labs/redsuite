pub mod aperture;
pub mod chainlink;
pub mod magicblock;
use aperture::{
    ProtocolInputs, SimulationIsolation, TransactionAdmission, TransactionAuthority,
    TransactionStructure,
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

static IDENTITIES: BasePreparation = BasePreparation {
    run: |_| {
        Box::pin(async {
            for group in [&DEFAULT, &PREPARATION, &EPHEMERAL, &REPLICATION] {
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
