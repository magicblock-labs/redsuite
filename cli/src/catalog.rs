use redsuite_core::catalog::{Fixture, Resource};

pub mod redline {
    use ::redline::scenarios;
    use redsuite_core::scenario_catalog;

    use super::{Fixture, Resource};

    scenario_catalog! {
        family: Redline,
        simple_load => aperture::simple_load::SimpleLoad {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        rpc_warm_ingress => aperture::rpc_warm_ingress::WarmIngress {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        rpc_capacity_blast => aperture::rpc_capacity_blast::RpcCapacityBlast {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        high_cu => aperture::high_cu::HighCu {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        ws_fanout_threshold => aperture::ws_fanout_threshold::WsFanoutThreshold {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        ws_conn_capacity => aperture::ws_conn_capacity::WsConnCapacity {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [],
        },
        hot_account_cliff => scheduler::hot_account_cliff::HotAccountCliff {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        executor_saturation => scheduler::executor_saturation::ExecutorSaturation {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        conflict_ordering => scheduler::conflict_ordering::ConflictOrdering {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        mixed_sustained_load => scheduler::mixed_sustained_load::MixedSustainedLoad {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        clone_lru_churn => chainlink::clone_lru_churn::CloneLruChurn {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        ensure_gate_stall => chainlink::ensure_gate_stall::EnsureGateStall {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        cold_hydration_tail => chainlink::cold_hydration_tail::ColdHydrationTail {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        commit_width_envelope => committor::commit_width_envelope::CommitWidthEnvelope {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        commit_throughput_ceiling => committor::commit_throughput_ceiling::CommitThroughputCeiling {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        storage_prodsize_sustain => storage::storage_prodsize_sustain::StorageProdsizeSustain {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        superblock_boundary_latency => storage::superblock_boundary_latency::SuperblockBoundaryLatency {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        restart_under_load => lifecycle::restart_under_load::RestartUnderLoad {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
        protocol_boundary_selftest => harness::protocol_boundary_selftest::ProtocolBoundarySelftest {
            topology: Shared,
            resources: [Resource::Er, Resource::HostExclusive],
            fixtures: [Fixture::RedlineProgram],
        },
    }
}

pub mod redshift {
    use ::redshift::scenarios;
    use redsuite_core::scenario_catalog;

    use super::{Fixture, Resource};

    scenario_catalog! {
        family: Redshift,
        example => harness::example::Example {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [],
            optional_fixtures: [],
        },
        clone_on_access => chainlink::clone_on_access::CloneOnAccess {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        cache_lifecycle => chainlink::cache_lifecycle::CacheLifecycle::Churn {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        undelegation_reconnect_gap => chainlink::cache_lifecycle::CacheLifecycle::UndelegationReconnectGap {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        commit_blackout => committor::commit_blackout::CommitBlackout {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        commit_exactly_once => committor::commit_exactly_once::CommitExactlyOnce {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram, Fixture::RedshiftProgram],
        },
        commit_settlement_order => committor::commit_settlement_order::CommitSettlementOrder {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        undelegation_recovery => committor::undelegation_recovery::UndelegationRecovery {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        delegation_session_isolation => committor::delegation_session_isolation::DelegationSessionIsolation {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        commit_roundtrip => committor::commit_roundtrip::CommitRoundtrip {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        commits => committor::commits::Commits {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        commit_and_undelegate => committor::commit_and_undelegate::CommitAndUndelegate {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        config_gates => harness::config_gates::ConfigGates {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        task_scheduler => scheduler::task_scheduler::TaskScheduler {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        checkpoint_durability => storage::checkpoint_durability::CheckpointDurability {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        ledger_retention => storage::ledger_retention::LedgerRetention {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        snapshot_read_race => storage::snapshot_read_race::SnapshotReadRace {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        verifier_lifecycle => replication::verifier_lifecycle::VerifierLifecycle {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [],
        },
        replication_recovery => replication::replication_recovery::ReplicationRecovery {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        transaction_retry_cold_fetch => aperture::transaction_retries::TransactionRetries::ColdFetch {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        transaction_retry_success => aperture::transaction_retries::TransactionRetries::ConcurrentSuccess {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        transaction_retry_failure => aperture::transaction_retries::TransactionRetries::ConcurrentFailure {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        transaction_retry_subscriptions => aperture::transaction_retries::TransactionRetries::Subscriptions {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram, Fixture::RedlineProgram],
        },
        transaction_retry_expiry_restart => aperture::transaction_retries::TransactionRetries::ExpiryRestart {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        rpc_lifecycle => aperture::rpc_lifecycle::RpcLifecycle {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
            optional_fixtures: [],
        },
        rpc_token_queries => aperture::rpc_token_queries::RpcTokenQueries {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [],
            optional_fixtures: [],
        },
        rpc_compat_methods => aperture::rpc_compat_methods::RpcCompatMethods {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [],
            optional_fixtures: [],
        },
        api_invariants => harness::api_invariants::ApiInvariants {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        claim_fees => committor::claim_fees::ClaimFees {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        pubsub_contracts => pubsub::pubsub_contracts::PubsubContracts {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        account_info_semantics => chainlink::account_info_semantics::AccountInfoSemantics {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        escrow_cloning => chainlink::escrow_cloning::EscrowCloning {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        parallel_cloning => chainlink::parallel_cloning::ParallelCloning {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        multi_program_clone => chainlink::multi_program_clone::MultiProgramClone {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        program_upgrade => chainlink::program_upgrade::ProgramUpgrade {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram, Fixture::RedshiftProgramSlim, Fixture::RedshiftProgramSlimUpgraded],
        },
        loader_matrix => chainlink::loader_matrix::LoaderMatrix {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        post_delegation_token_transfer => chainlink::post_delegation_token_transfer::PostDelegationTokenTransfer {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
        projected_token_lifecycle => chainlink::projected_token_lifecycle::ProjectedTokenLifecycle {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedlineProgram],
        },
        aml_gate => chainlink::aml_gate::AmlGate {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [],
        },
        activation_single_shot => chainlink::activation_single_shot::ActivationSingleShot {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        table_mania => committor::table_mania::TableManiaScenario {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [],
        },
    }
}

pub mod redhat {
    use ::redhat::scenarios;
    use redsuite_core::scenario_catalog;

    use super::{Fixture, Resource};

    scenario_catalog! {
        family: Redhat,
        ephemeral_accounts => magicblock::ephemeral_accounts::EphemeralAccounts {
            topology: PrivateEr,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
        illegal_writable => chainlink::illegal_writable::IllegalWritable {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedhatProgram, Fixture::RedshiftProgram],
        },
        fee_payer_rules => aperture::fee_payer_rules::FeePayerRules {
            topology: Shared,
            resources: [Resource::Er],
            fixtures: [Fixture::RedshiftProgram],
        },
    }
}
