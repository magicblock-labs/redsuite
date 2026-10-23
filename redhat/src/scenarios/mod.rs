pub mod chainlink;
pub mod magicblock;

use redsuite_core::{
    catalog::{Fixture, ScenarioEntry, DEFAULT},
    scenario, Scenario,
};

pub fn catalog() -> Vec<ScenarioEntry> {
    vec![
        scenario!(
            magicblock::ephemeral_accounts::EphemeralAccounts,
            exclusive(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            chainlink::illegal_writable::IllegalWritable,
            shared(
                &DEFAULT,
                &[Fixture::RedhatProgram, Fixture::RedshiftProgram],
                ()
            )
        ),
    ]
}
