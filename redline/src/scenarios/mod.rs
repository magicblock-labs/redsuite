pub mod aperture;
pub mod harness;
pub mod scheduler;
use aperture::transaction_load::{Shape, TransactionLoad};
use redsuite_core::{
    catalog::{Fixture, ScenarioEntry, DEFAULT},
    scenario, Scenario,
};
pub fn catalog() -> Vec<ScenarioEntry> {
    vec![
        scenario!(
            TransactionLoad,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], Shape::Write)
        )
        .named("transaction_load_write"),
        scenario!(
            TransactionLoad,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], Shape::Copy)
        )
        .named("transaction_load_copy"),
        scenario!(
            TransactionLoad,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], Shape::Compute)
        )
        .named("compute_load"),
        scenario!(
            scheduler::scheduler_contention::SchedulerContention,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            scheduler::mixed_sustained_load::MixedSustainedLoad,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            aperture::websocket_fanout::WebsocketFanout,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            aperture::websocket_churn::WebsocketChurn,
            exclusive(&DEFAULT, &[], ())
        ),
        scenario!(
            harness::load_generator_thread_scaling::LoadGeneratorThreadScaling,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        )
        .explicit_only(),
    ]
}
