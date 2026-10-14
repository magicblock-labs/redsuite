use redsuite_core::catalog::{ErGroup, Fixture};

pub static DEFAULT: ErGroup = ErGroup {
    label: "default",
    env: &[],
    proxied: false,
};

pub static RETRIES: ErGroup = ErGroup {
    label: "transaction-retries",
    env: &[],
    proxied: true,
};

pub static COMMITTOR: ErGroup = ErGroup {
    label: "committor-faults",
    env: &[],
    proxied: true,
};

pub mod redline {
    use ::redline::scenarios;
    use redsuite_core::scenario_catalog;

    use super::{Fixture, DEFAULT};

    scenario_catalog! {
        family: Redline,
        simple_load => aperture::simple_load::SimpleLoad {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        rpc_warm_ingress => aperture::rpc_warm_ingress::WarmIngress {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        rpc_capacity_blast => aperture::rpc_capacity_blast::RpcCapacityBlast {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        high_cu => aperture::high_cu::HighCu {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        ws_fanout_threshold => aperture::ws_fanout_threshold::WsFanoutThreshold {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        ws_conn_capacity => aperture::ws_conn_capacity::WsConnCapacity {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [],
        },
        hot_account_cliff => scheduler::hot_account_cliff::HotAccountCliff {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        executor_saturation => scheduler::executor_saturation::ExecutorSaturation {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        conflict_ordering => scheduler::conflict_ordering::ConflictOrdering {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        mixed_sustained_load => scheduler::mixed_sustained_load::MixedSustainedLoad {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        clone_lru_churn => chainlink::clone_lru_churn::CloneLruChurn {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        ensure_gate_stall => chainlink::ensure_gate_stall::EnsureGateStall {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        cold_hydration_tail => chainlink::cold_hydration_tail::ColdHydrationTail {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        commit_width_envelope => committor::commit_width_envelope::CommitWidthEnvelope {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        commit_throughput_ceiling => committor::commit_throughput_ceiling::CommitThroughputCeiling {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        storage_prodsize_sustain => storage::storage_prodsize_sustain::StorageProdsizeSustain {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        superblock_boundary_latency => storage::superblock_boundary_latency::SuperblockBoundaryLatency {
            er: Fresh,
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        restart_under_load => lifecycle::restart_under_load::RestartUnderLoad {
            er: Fresh,
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
        protocol_boundary_selftest => harness::protocol_boundary_selftest::ProtocolBoundarySelftest {
            er: Exclusive(DEFAULT),
            host_exclusive: true,
            fixtures: [Fixture::RedlineProgram],
        },
    }
}

pub mod redshift {
    use ::redshift::scenarios;
    use redsuite_core::scenario_catalog;

    use super::{Fixture, COMMITTOR, DEFAULT, RETRIES};

    scenario_catalog! {
        family: Redshift,
        example => harness::example::Example {
            er: Shared(DEFAULT),
            fixtures: [],
            optional_fixtures: [],
        },
        clone_on_access => chainlink::clone_on_access::CloneOnAccess {
            er: Shared(DEFAULT),
            fixtures: [Fixture::RedlineProgram],
        },
        cache_lifecycle => chainlink::cache_lifecycle::CacheLifecycle::Churn {
            er: Fresh,
            fixtures: [Fixture::RedlineProgram],
        },
        undelegation_reconnect_gap => chainlink::cache_lifecycle::CacheLifecycle::UndelegationReconnectGap {
            er: Fresh,
            fixtures: [Fixture::RedlineProgram],
        },
        undelegation_grpc_redundancy => chainlink::undelegation_grpc_redundancy::GrpcRedundancy {
            er: Fresh,
            fixtures: [Fixture::RedlineProgram],
        },
        commit_blackout => committor::commit_blackout::CommitBlackout {
            er: Exclusive(COMMITTOR),
            fixtures: [Fixture::RedlineProgram],
        },
        commit_exactly_once => committor::commit_exactly_once::CommitExactlyOnce {
            er: Exclusive(COMMITTOR),
            fixtures: [Fixture::RedlineProgram, Fixture::RedshiftProgram],
        },
        commit_settlement_order => committor::commit_settlement_order::CommitSettlementOrder {
            er: Fresh,
            fixtures: [Fixture::RedlineProgram],
        },
        undelegation_recovery => committor::undelegation_recovery::UndelegationRecovery {
            er: Exclusive(COMMITTOR),
            fixtures: [Fixture::RedlineProgram],
        },
        delegation_session_isolation => committor::delegation_session_isolation::DelegationSessionIsolation {
            er: Shared(DEFAULT),
            fixtures: [Fixture::RedshiftProgram],
        },
        commit_and_undelegate => committor::commit_and_undelegate::CommitAndUndelegate {
            er: Shared(DEFAULT),
            fixtures: [Fixture::RedshiftProgram],
        },
        config_gates => harness::config_gates::ConfigGates {
            er: Fresh,
            fixtures: [Fixture::RedshiftProgram],
        },
        task_scheduler => scheduler::task_scheduler::TaskScheduler {
            er: Exclusive(DEFAULT),
            fixtures: [Fixture::RedshiftProgram],
        },
        checkpoint_durability => storage::checkpoint_durability::CheckpointDurability {
            er: Fresh,
            fixtures: [Fixture::RedshiftProgram],
        },
        ledger_retention => storage::ledger_retention::LedgerRetention {
            er: Fresh,
            fixtures: [Fixture::RedshiftProgram],
        },
        snapshot_read_race => storage::snapshot_read_race::SnapshotReadRace {
            er: Fresh,
            fixtures: [Fixture::RedshiftProgram],
        },
        verifier_lifecycle => replication::verifier_lifecycle::VerifierLifecycle {
            er: Fresh,
            fixtures: [Fixture::RedlineProgram],
        },
        transaction_retry_cold_fetch => aperture::transaction_retries::TransactionRetries::ColdFetch {
            er: Exclusive(RETRIES),
            fixtures: [Fixture::RedshiftProgram],
        },
        transaction_retry_concurrent => aperture::transaction_retries::TransactionRetries::Concurrent {
            er: Exclusive(RETRIES),
            fixtures: [Fixture::RedshiftProgram],
        },
        transaction_retry_subscriptions => aperture::transaction_retries::TransactionRetries::Subscriptions {
            er: Exclusive(RETRIES),
            fixtures: [Fixture::RedshiftProgram, Fixture::RedlineProgram],
        },
        transaction_retry_expiry_restart => aperture::transaction_retries::TransactionRetries::ExpiryRestart {
            er: Fresh,
            fixtures: [Fixture::RedshiftProgram],
        },
        rpc_lifecycle => aperture::rpc_lifecycle::RpcLifecycle {
            er: Shared(DEFAULT),
            fixtures: [Fixture::RedlineProgram],
            optional_fixtures: [],
        },
        rpc_token_queries => aperture::rpc_token_queries::RpcTokenQueries {
            er: Shared(DEFAULT),
            fixtures: [],
            optional_fixtures: [],
        },
        rpc_compat_methods => aperture::rpc_compat_methods::RpcCompatMethods {
            er: Fresh,
            fixtures: [],
            optional_fixtures: [],
        },
        api_invariants => harness::api_invariants::ApiInvariants {
            er: None,
            fixtures: [],
        },
        claim_fees => committor::claim_fees::ClaimFees {
            er: None,
            fixtures: [],
        },
        account_info_semantics => chainlink::account_info_semantics::AccountInfoSemantics {
            er: Shared(DEFAULT),
            fixtures: [],
        },
        multi_program_clone => chainlink::multi_program_clone::MultiProgramClone {
            er: Shared(DEFAULT),
            fixtures: [Fixture::RedlineProgram],
        },
        program_upgrade => chainlink::program_upgrade::ProgramUpgrade {
            er: Fresh,
            fixtures: [Fixture::RedlineProgram, Fixture::RedshiftProgramSlim, Fixture::RedshiftProgramSlimUpgraded],
        },
        loader_matrix => chainlink::loader_matrix::LoaderMatrix {
            er: Shared(DEFAULT),
            fixtures: [Fixture::RedshiftProgram],
        },
        projected_token_lifecycle => chainlink::projected_token_lifecycle::ProjectedTokenLifecycle {
            er: Fresh,
            fixtures: [Fixture::RedlineProgram],
        },
        aml_gate => chainlink::aml_gate::AmlGate {
            er: Fresh,
            fixtures: [],
        },
        activation_single_shot => chainlink::activation_single_shot::ActivationSingleShot {
            er: Fresh,
            fixtures: [Fixture::RedshiftProgram],
        },
        table_mania => committor::table_mania::TableManiaScenario {
            er: None,
            fixtures: [],
        },
    }
}

pub mod redhat {
    use ::redhat::scenarios;
    use redsuite_core::scenario_catalog;

    use super::{Fixture, DEFAULT};

    scenario_catalog! {
        family: Redhat,
        ephemeral_accounts => magicblock::ephemeral_accounts::EphemeralAccounts {
            er: Exclusive(DEFAULT),
            fixtures: [Fixture::RedshiftProgram],
        },
        illegal_writable => chainlink::illegal_writable::IllegalWritable {
            er: Shared(DEFAULT),
            fixtures: [Fixture::RedhatProgram, Fixture::RedshiftProgram],
        },
    }
}
