pub mod aperture;
pub mod chainlink;
pub mod committor;
pub mod harness;
pub mod lifecycle;
pub mod scheduler;
pub mod storage;

use redsuite_core::{
    catalog::{Fixture, ScenarioEntry, DEFAULT},
    scenario, PrivateErScenario, Scenario,
};

pub fn catalog() -> Vec<ScenarioEntry> {
    vec![
        scenario!(
            aperture::account_write_latency::AccountWriteLatency,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            aperture::rpc_warm_ingress::WarmIngress,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        )
        .named("rpc_warm_ingress"),
        scenario!(
            aperture::rpc_ingress_capacity::RpcIngressCapacity,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            aperture::compute_cost_throughput::ComputeCostThroughput,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            aperture::ws_fanout_threshold::WsFanoutThreshold,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            aperture::ws_connection_capacity::WsConnectionCapacity,
            exclusive(&DEFAULT, &[], ())
        ),
        scenario!(
            scheduler::hot_account_cliff::HotAccountCliff,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            scheduler::executor_saturation::ExecutorSaturation,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            scheduler::conflict_ordering::ConflictOrdering,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            scheduler::mixed_sustained_load::MixedSustainedLoad,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            chainlink::clone_lru_churn::CloneLruChurn,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            chainlink::ensure_gate_stall::EnsureGateStall,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            chainlink::cold_hydration_tail::ColdHydrationTail,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            committor::commit_width_envelope::CommitWidthEnvelope,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            committor::commit_throughput_ceiling::CommitThroughputCeiling,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            storage::storage_prodsize_sustain::StorageProdsizeSustain,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            storage::superblock_boundary_latency::SuperblockBoundaryLatency,
            fresh(&[Fixture::RedlineProgram], ())
        ),
        scenario!(
            lifecycle::restart_under_load::RestartUnderLoad,
            fresh(&[Fixture::RedlineProgram], ())
        ),
        scenario!(
            harness::protocol_boundary_selftest::ProtocolBoundarySelftest,
            exclusive(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
    ]
}
