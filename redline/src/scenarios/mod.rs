pub mod aperture;
pub mod harness;
pub mod scheduler;
use aperture::transaction_load::{Shape, TransactionLoad};
use redsuite_core::{
    catalog::{ErGroup, Fixture, ScenarioEntry, DEFAULT},
    scenario, Scenario,
};

static FIXED_POOL: ErGroup = ErGroup {
    label: "fixed_pool",
    settings: |params| {
        params.engine.blockstore.blocktime = std::time::Duration::from_millis(50);
        params.engine.blockstore.superblock = 24_000;
        params.engine.ledger.size_limit = 50_000_000_000;
    },
    proxy: false,
    followers: 0,
};

pub fn catalog() -> Vec<ScenarioEntry> {
    vec![
        scenario!(
            aperture::fixed_pool::FixedPool,
            exclusive(&FIXED_POOL, &[Fixture::RedlineProgram], ())
        )
        .explicit_only(),
        scenario!(
            TransactionLoad,
            shared(&DEFAULT, &[Fixture::RedlineProgram], Shape::Write)
        )
        .named("transaction_load_write"),
        scenario!(
            TransactionLoad,
            shared(&DEFAULT, &[Fixture::RedlineProgram], Shape::Copy)
        )
        .named("transaction_load_copy"),
        scenario!(
            TransactionLoad,
            shared(&DEFAULT, &[Fixture::RedlineProgram], Shape::Compute)
        )
        .named("compute_load"),
        scenario!(
            scheduler::scheduler_contention::SchedulerContention,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            scheduler::mixed_sustained_load::MixedSustainedLoad,
            shared(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            aperture::websocket_fanout::WebsocketFanout,
            shared(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            aperture::websocket_churn::WebsocketChurn,
            shared(&DEFAULT, &[], ())
        ),
        scenario!(
            harness::load_generator_thread_scaling::LoadGeneratorThreadScaling,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        )
        .explicit_only(),
    ]
}
