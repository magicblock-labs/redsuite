pub mod aperture;
pub mod chainlink;
pub mod committor;
pub mod harness;
pub mod lifecycle;
pub mod replication;
pub mod scheduler;
pub mod storage;

use chainlink::projected_token_lifecycle::Mode as TokenMode;
use redsuite_core::{
    catalog::{BasePreparation, ErGroup, Fixture, ProxyMode, ScenarioEntry, DEFAULT},
    scenario, ChainCtx, PrivateErScenario, Scenario,
};
use signer::Signer;
use std::time::Duration;

static RETRIES: ErGroup = ErGroup {
    label: "transaction-retries",
    settings: |_| {},
    proxies: ProxyMode::Shared,
    followers: 0,
};
static COMMITTOR: ErGroup = ErGroup {
    label: "committor-faults",
    settings: |_| {},
    proxies: ProxyMode::Shared,
    followers: 0,
};
static CACHE: ErGroup = ErGroup {
    label: "small-cache",
    settings: |p| {
        p.engine.accountsdb.lru_capacity = 256;
    },
    proxies: ProxyMode::Split,
    followers: 0,
};
static STORAGE: ErGroup = ErGroup {
    label: "storage",
    settings: |p| {
        p.engine.blockstore.blocktime = Duration::from_millis(50);
        p.engine.blockstore.superblock = 20;
    },
    proxies: ProxyMode::None,
    followers: 0,
};
static RETENTION: ErGroup = ErGroup {
    label: "retention",
    settings: |p| {
        p.engine.blockstore.blocktime = Duration::from_millis(40);
        p.engine.blockstore.superblock = 512;
        p.engine.ledger.size_limit = 1;
    },
    proxies: ProxyMode::None,
    followers: 2,
};

pub fn catalog() -> Vec<ScenarioEntry> {
    vec![
        scenario!(
            committor::dlp_v2_lifecycle::DlpV2Lifecycle,
            shared(&DEFAULT, &[Fixture::RedshiftProgram], ())
        )
        .named("dlp_v2_lifecycle"),
        scenario!(
            committor::dlp_v2_buffered_settlement::DlpV2BufferedSettlement,
            shared(&DEFAULT, &[Fixture::RedshiftProgram], ())
        )
        .named("dlp_v2_buffered_settlement"),
        scenario!(
            lifecycle::restart_under_load::RestartUnderLoad,
            exclusive(&STORAGE, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            storage::ledger_retention_consistency::LedgerRetentionConsistency,
            exclusive(&RETENTION, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            replication::replication_recovery::ReplicationRecovery,
            exclusive(
                &RETENTION,
                &[Fixture::RedlineProgram],
                replication::replication_recovery::Coverage::Recovery
            )
        ),
        scenario!(
            storage::checkpoint_recovery::CheckpointRecovery,
            exclusive(&STORAGE, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            storage::account_reads_during_resize::AccountReadsDuringResize,
            exclusive(&STORAGE, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            chainlink::cache_lifecycle::CacheLifecycle,
            exclusive(
                &CACHE,
                &[Fixture::RedlineProgram],
                chainlink::cache_lifecycle::Mode::UndelegationReconnectGap
            )
        )
        .named("cache_transport_recovery"),
        scenario!(
            chainlink::cache_lifecycle::CacheLifecycle,
            exclusive(
                &CACHE,
                &[Fixture::RedlineProgram],
                chainlink::cache_lifecycle::Mode::Churn
            )
        )
        .named("cache_eviction_and_stale_fetch")
        .ordered(1),
        scenario!(
            committor::intent_execution_and_accounting::IntentExecutionAndAccounting,
            exclusive(&COMMITTOR, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            committor::commit_recovery::CommitRecovery,
            exclusive(
                &COMMITTOR,
                &[Fixture::RedlineProgram, Fixture::RedshiftProgram],
                ()
            )
        )
        .ordered(1),
        scenario!(
            committor::commit_snapshot_ordering::CommitSnapshotOrdering,
            exclusive(&COMMITTOR, &[Fixture::RedlineProgram], ())
        )
        .ordered(1),
        scenario!(
            committor::undelegation_recovery::UndelegationRecovery,
            exclusive(&COMMITTOR, &[Fixture::RedlineProgram], ())
        )
        .ordered(1),
        scenario!(
            committor::stale_delegation_session_isolation::StaleDelegationSessionIsolation,
            shared(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            committor::settlement_lifecycle::SettlementLifecycle,
            shared(
                &DEFAULT,
                &[Fixture::RedlineProgram, Fixture::RedshiftProgram],
                ()
            )
        ),
        scenario!(
            chainlink::program_clone_allowlist::ProgramCloneAllowlist,
            fresh(&[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            chainlink::program_materialization::ProgramMaterialization,
            fresh(&[Fixture::RedlineProgram, Fixture::RedshiftProgram], ())
        ),
        scenario!(
            scheduler::scheduled_task_lifecycle::ScheduledTaskLifecycle,
            exclusive(&DEFAULT, &[Fixture::RedshiftProgram], ())
        ),
        scenario!(
            aperture::transaction_lifecycle::TransactionLifecycle,
            exclusive(
                &RETRIES,
                &[Fixture::RedlineProgram, Fixture::RedshiftProgram],
                aperture::transaction_lifecycle::Coverage::Admission
            )
        )
        .named("transaction_admission_and_observation"),
        scenario!(
            aperture::transaction_lifecycle::TransactionLifecycle,
            exclusive(
                &RETRIES,
                &[Fixture::RedshiftProgram],
                aperture::transaction_lifecycle::Coverage::Replay
            )
        )
        .named("transaction_replay_protection"),
        scenario!(
            aperture::rpc_state_and_events::RpcStateAndEvents,
            shared(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            aperture::token_rpc_views::TokenRpcViews,
            shared(&DEFAULT, &[], ())
        ),
        scenario!(
            aperture::rpc_compat_smoke::RpcCompatSmoke,
            shared(&DEFAULT, &[], ())
        ),
        scenario!(
            chainlink::account_mirroring_and_isolation::AccountMirroringAndIsolation,
            shared(&DEFAULT, &[Fixture::RedlineProgram], ())
        ),
        scenario!(
            chainlink::program_upgrade_consistency::ProgramUpgradeConsistency,
            fresh(
                &[Fixture::RedlineProgram, Fixture::UpgradeProbe, Fixture::UpgradeProbeUpgraded],
                ()
            )
        ),
        scenario!(
            chainlink::projected_token_lifecycle::ProjectedTokenLifecycle,
            exclusive(&COMMITTOR, &[Fixture::RedlineProgram], TokenMode::Backing)
        )
        .named("projected_token_backing_lifecycle")
        .ordered(1),
        scenario!(
            chainlink::projected_token_lifecycle::ProjectedTokenLifecycle,
            exclusive(&COMMITTOR, &[Fixture::RedlineProgram], TokenMode::Overlay)
        )
        .named("magic_ata_overlay_lifecycle")
        .ordered(1)
        .prepare_with(&RENT),
        scenario!(
            chainlink::post_delegation_risk_gate::PostDelegationRiskGate,
            fresh(&[], ())
        )
        .prepare_with(&RENT),
        scenario!(
            chainlink::activation_single_shot::ActivationSingleShot,
            exclusive(&COMMITTOR, &[Fixture::RedshiftProgram], ())
        )
        .ordered(1),
    ]
}

/// Initializes the base-wide rent PDA before any dependent scenario is scheduled.
static RENT: BasePreparation = BasePreparation {
    run: |base| {
        Box::pin(async move {
            let rent = sdk::spl::find_rent_pda().0;
            if base.account(&rent).await?.is_none() {
                let payer = base.funded_payer(2_000_000_000).await?;
                let ix = sdk::spl::builders::InitializeRentPdaBuilder { payer: payer.pubkey() }
                    .instruction();
                base.submit_and_confirm(&payer, &[ix]).await?;
            }
            Ok(())
        })
    },
};
