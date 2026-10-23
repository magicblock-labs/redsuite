pub mod aperture;
pub mod chainlink;
pub mod committor;
pub mod harness;
pub mod replication;
pub mod scheduler;
pub mod storage;

use redsuite_core::{
    catalog::{ErGroup, Fixture, ScenarioEntry, DEFAULT},
    scenario, PrivateErScenario, Scenario,
};

static RETRIES: ErGroup = ErGroup {
    label: "transaction-retries",
    settings: |_| {},
    proxied: true,
};
static COMMITTOR: ErGroup = ErGroup {
    label: "committor-faults",
    settings: |_| {},
    proxied: true,
};

pub fn catalog() -> Vec<ScenarioEntry> {
    vec![
        scenario!(
            storage::ledger_retention::LedgerRetention,
            fresh(&[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            replication::verifier_lifecycle::VerifierLifecycle,
            fresh(&[Fixture::RedlineProgram], ())
        ),
        scenario!(
            storage::checkpoint_durability::CheckpointDurability,
            fresh(&[Fixture::RedshiftProgram], ())
        ),
        scenario!(harness::example::Example, shared(&DEFAULT, &[], ())),
        scenario!(
            chainlink::clone_on_access::CloneOnAccess,
            shared(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            chainlink::cache_lifecycle::CacheLifecycle,
            fresh(
                &[Fixture::RedlineProgram],
                chainlink::cache_lifecycle::Mode::Churn
            )
        ),
        scenario!(
            chainlink::cache_lifecycle::CacheLifecycle,
            fresh(
                &[Fixture::RedlineProgram],
                chainlink::cache_lifecycle::Mode::UndelegationReconnectGap
            )
        )
        .named("undelegation_reconnect_gap"),
        scenario!(
            chainlink::undelegation_grpc_redundancy::GrpcRedundancy,
            fresh(&[Fixture::RedlineProgram], ())
        )
        .named("undelegation_grpc_redundancy"),
        scenario!(
            committor::commit_blackout::CommitBlackout,
            exclusive(&COMMITTOR, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            committor::commit_exactly_once::CommitExactlyOnce,
            exclusive(
                &COMMITTOR,
                &[Fixture::RedlineProgram, Fixture::RedshiftProgram],
                ()
            )
        ),
        scenario!(
            committor::commit_settlement_order::CommitSettlementOrder,
            fresh(&[Fixture::RedlineProgram], ())
        ),
        scenario!(
            committor::undelegation_recovery::UndelegationRecovery,
            exclusive(&COMMITTOR, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            committor::delegation_session_isolation::DelegationSessionIsolation,
            shared(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            committor::commit_and_undelegate::CommitAndUndelegate,
            shared(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            chainlink::program_allowlist::ProgramAllowlist,
            fresh(&[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            chainlink::clone_without_alt_writes::CloneWithoutAltWrites,
            fresh(&[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            scheduler::task_scheduler::TaskScheduler,
            exclusive(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            storage::snapshot_read_race::SnapshotReadRace,
            fresh(&[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            aperture::transaction_retries::TransactionRetries,
            exclusive(
                &RETRIES,
                &[Fixture::RedshiftProgram],
                aperture::transaction_retries::Mode::ColdFetch
            )
        )
        .named("transaction_retry_cold_fetch"),
        scenario!(
            aperture::transaction_retries::TransactionRetries,
            exclusive(
                &RETRIES,
                &[Fixture::RedshiftProgram],
                aperture::transaction_retries::Mode::Concurrent
            )
        )
        .named("transaction_retry_concurrent"),
        scenario!(
            aperture::transaction_retries::TransactionRetries,
            exclusive(
                &RETRIES,
                &[Fixture::RedshiftProgram, Fixture::RedlineProgram],
                aperture::transaction_retries::Mode::Subscriptions
            )
        )
        .named("transaction_retry_subscriptions"),
        scenario!(
            aperture::transaction_retries::ExpiryRestart,
            fresh(&[Fixture::RedshiftProgram], ())
        )
        .named("transaction_retry_expiry_restart"),
        scenario!(
            aperture::rpc_lifecycle::RpcLifecycle,
            shared(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            aperture::rpc_token_queries::RpcTokenQueries,
            shared(&DEFAULT, &[], ())
        ),
        scenario!(
            aperture::rpc_compat_methods::RpcCompatMethods,
            fresh(&[], ())
        ),
        scenario!(
            chainlink::account_info_semantics::AccountInfoSemantics,
            shared(&DEFAULT, &[], ())
        ),
        scenario!(
            chainlink::multi_program_clone::MultiProgramClone,
            shared(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            chainlink::program_upgrade::ProgramUpgrade,
            fresh(
                &[Fixture::RedlineProgram, Fixture::UpgradeProbe, Fixture::UpgradeProbeUpgraded],
                ()
            )
        ),
        scenario!(
            chainlink::loader_matrix::LoaderMatrix,
            shared(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            chainlink::projected_token_lifecycle::ProjectedTokenLifecycle,
            fresh(&[Fixture::RedlineProgram], ())
        ),
        scenario!(chainlink::aml_gate::AmlGate, fresh(&[], ())),
        scenario!(
            chainlink::activation_single_shot::ActivationSingleShot,
            fresh(&[Fixture::RedshiftProgram], ())
        ),
    ]
}
