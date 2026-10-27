pub mod aperture;
pub mod chainlink;
pub mod magicblock;
use aperture::{ProtocolInputs, TransactionAdmission, TransactionAuthority, TransactionStructure};
use magicblock_config::types::Remote;
use redsuite_core::{
    catalog::{BasePreparation, ErGroup, Fixture, ProxyMode, ScenarioEntry, DEFAULT},
    scenario, topology, Scenario,
};

static PREPARATION: ErGroup = ErGroup {
    label: "hostile-preparation",
    settings: |p| p.remotes.retain(|remote| !matches!(remote, Remote::Grpc(_))),
    proxies: ProxyMode::Shared,
    followers: 0,
};

static EPHEMERAL: ErGroup = ErGroup {
    label: "ephemeral-security",
    settings: |p| {
        p.engine.accountsdb.lru_capacity =
            redshift::scenarios::chainlink::cache_lifecycle::CACHE_CAPACITY
    },
    proxies: ProxyMode::None,
    followers: 0,
};

static IDENTITIES: BasePreparation = BasePreparation {
    run: |_| {
        Box::pin(async {
            for group in [&DEFAULT, &PREPARATION, &EPHEMERAL] {
                topology::identity_for_label(group.label)?;
            }
            Ok(())
        })
    },
};

pub fn catalog() -> Vec<ScenarioEntry> {
    vec![
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
