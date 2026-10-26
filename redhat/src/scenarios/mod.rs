pub mod aperture;
pub mod chainlink;
pub mod magicblock;
use aperture::{TransactionAdmission, TransactionAuthority, TransactionStructure};
use magicblock_config::types::Remote;
use redsuite_core::{
    catalog::{ErGroup, Fixture, ProxyMode, ScenarioEntry, DEFAULT},
    scenario, Scenario,
};

static PREPARATION: ErGroup = ErGroup {
    label: "hostile-preparation",
    settings: |p| p.remotes.retain(|remote| !matches!(remote, Remote::Grpc(_))),
    proxies: ProxyMode::Shared,
    followers: 0,
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
            chainlink::settlement_authorization::SettlementAuthorization,
            exclusive(
                &DEFAULT,
                &[Fixture::RedhatProgram, Fixture::RedshiftProgram],
                ()
            )
        )
        .named("settlement_authorization"),
        scenario!(
            magicblock::ephemeral_account_security::EphemeralAccountSecurity,
            exclusive(&DEFAULT, &[Fixture::RedshiftProgram], ())
        )
        .named("ephemeral_account_security"),
    ]
}
