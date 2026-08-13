use redsuite_core::catalog::{Fixture, Resource, Topology};

pub mod redline {
    use super::{Fixture, Resource, Topology};
    use ::redline::scenarios;
    use redsuite_core::scenario_catalog;

    scenario_catalog! {
        family: Redline,
        simple_load => aperture::simple_load::SimpleLoad {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        rpc_warm_ingress => aperture::rpc_warm_ingress::WarmIngress {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        rpc_capacity_blast => aperture::rpc_capacity_blast::RpcCapacityBlast {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        high_cu => aperture::high_cu::HighCu {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        ws_fanout_threshold => aperture::ws_fanout_threshold::WsFanoutThreshold {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        ws_conn_capacity => aperture::ws_conn_capacity::WsConnCapacity {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        hot_account_cliff => scheduler::hot_account_cliff::HotAccountCliff {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        executor_saturation => scheduler::executor_saturation::ExecutorSaturation {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        clone_lru_churn => chainlink::clone_lru_churn::CloneLruChurn {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        ensure_gate_stall => chainlink::ensure_gate_stall::EnsureGateStall {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        cold_hydration_tail => chainlink::cold_hydration_tail::ColdHydrationTail {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        commit_width_envelope => committor::commit_width_envelope::CommitWidthEnvelope {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        commit_throughput_ceiling => committor::commit_throughput_ceiling::CommitThroughputCeiling {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        storage_prodsize_sustain => storage::storage_prodsize_sustain::StorageProdsizeSustain {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        restart_under_load => lifecycle::restart_under_load::RestartUnderLoad {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        protocol_boundary_selftest => harness::protocol_boundary_selftest::ProtocolBoundarySelftest {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
    }
}

pub mod redshift {
    use super::{Fixture, Resource, Topology};
    use ::redshift::scenarios;
    use redsuite_core::scenario_catalog;

    scenario_catalog! {
        family: Redshift,
        example => harness::example::Example {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        clone_on_access => chainlink::clone_on_access::CloneOnAccess {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        commit_roundtrip => committor::commit_roundtrip::CommitRoundtrip {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        commits => committor::commits::Commits {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        commit_and_undelegate => committor::commit_and_undelegate::CommitAndUndelegate {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        commit_limit_and_fees => committor::commit_limit_and_fees::CommitLimitAndFees {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        intent_flows => committor::intent_flows::IntentFlows {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        config_gates => harness::config_gates::ConfigGates {
            topology: Topology::PrivateEr,
            resources: [Resource::Er, Resource::BaseAlt],
            fixtures: [Fixture::RedshiftProgram],
        },
        task_scheduler => scheduler::task_scheduler::TaskScheduler {
            topology: Topology::PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        ledger_restore_basics => lifecycle::ledger_restore_basics::LedgerRestoreBasics {
            topology: Topology::PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        ledger_restore_chain => lifecycle::ledger_restore_chain::LedgerRestoreChain {
            topology: Topology::PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        api_invariants => harness::api_invariants::ApiInvariants {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        claim_fees => committor::claim_fees::ClaimFees {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        pubsub_contracts => pubsub::pubsub_contracts::PubsubContracts {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        account_info_semantics => chainlink::account_info_semantics::AccountInfoSemantics {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        escrow_cloning => chainlink::escrow_cloning::EscrowCloning {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        parallel_cloning => chainlink::parallel_cloning::ParallelCloning {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        multi_program_clone => chainlink::multi_program_clone::MultiProgramClone {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        loader_matrix => chainlink::loader_matrix::LoaderMatrix {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        post_delegation_token_transfer => chainlink::post_delegation_token_transfer::PostDelegationTokenTransfer {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        aml_gate => chainlink::aml_gate::AmlGate {
            topology: Topology::PrivateEr,
            resources: [Resource::Er],
            fixtures: [],
        },
        table_mania => committor::table_mania::TableManiaScenario {
            topology: Topology::Shared,
            resources: [Resource::Er, Resource::BaseAlt],
            fixtures: [],
        },
    }
}

pub mod redhat {
    use super::{Fixture, Resource, Topology};
    use ::redhat::scenarios;
    use redsuite_core::scenario_catalog;

    scenario_catalog! {
        family: Redhat,
        illegal_writable => chainlink::illegal_writable::IllegalWritable {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedhatProgram, Fixture::RedshiftProgram],
        },
        fee_payer_rules => aperture::fee_payer_rules::FeePayerRules {
            topology: Topology::Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
    }
}
