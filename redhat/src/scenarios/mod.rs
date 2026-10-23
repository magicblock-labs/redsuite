pub mod chainlink;
pub mod magicblock;
use redsuite_core::{
    catalog::{Fixture, ScenarioEntry, DEFAULT},
    scenario, Scenario,
};
pub fn catalog() -> Vec<ScenarioEntry> {
    vec![
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
