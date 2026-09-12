use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::future::join_all;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::RpcError,
    check, check_eq,
    report::Unit,
    rpc_client::nonblocking::rpc_client::RpcClient,
    rpc_client_api::{
        config::RpcLargestAccountsConfig, request::RpcRequest,
        response::RpcBlockCommitment,
    },
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use signer::Signer;
use solana_commitment_config::CommitmentConfig;

const REQUESTS: usize = 256;
const BURST_DEADLINE: Duration = Duration::from_secs(20);
const NEGATIVE_DEADLINE: Duration = Duration::from_secs(5);
const SLOTS_IN_EPOCH: u64 = 432_000;
const SLOT_LEADER_LIMIT: u64 = 8;
const PERFORMANCE_SAMPLE_LIMIT: usize = 4;
const PERFORMANCE_SAMPLE_PERIOD_SECS: u16 = 60;
const AIRDROP_LAMPORTS: u64 = 1_000_000;
const AIRDROP_DISABLED: &str = "free airdrop faucet is disabled";
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const UNKNOWN_METHOD: &str = "getSomethingThatDoesNotExist";
const NO_PARAMS: [u8; 0] = [];

pub struct RpcCompatMethods;

struct Fixture {
    identity: Pubkey,
    mint: Pubkey,
    slot_floor: u64,
}

#[derive(Clone, Copy, Debug)]
enum Method {
    BlockCommitment,
    ClusterNodes,
    EpochInfo,
    EpochSchedule,
    FirstAvailableBlock,
    GenesisHash,
    Health,
    HighestSnapshotSlot,
    Identity,
    LargestAccounts,
    RecentPerformanceSamples,
    SlotLeader,
    SlotLeaders,
    Supply,
    TokenLargestAccounts,
    TokenSupply,
    TransactionCount,
    Version,
    VoteAccounts,
    MinimumLedgerSlot,
}

const CYCLE: [Method; 20] = [
    Method::BlockCommitment,
    Method::ClusterNodes,
    Method::EpochInfo,
    Method::EpochSchedule,
    Method::FirstAvailableBlock,
    Method::GenesisHash,
    Method::Health,
    Method::HighestSnapshotSlot,
    Method::Identity,
    Method::LargestAccounts,
    Method::RecentPerformanceSamples,
    Method::SlotLeader,
    Method::SlotLeaders,
    Method::Supply,
    Method::TokenLargestAccounts,
    Method::TokenSupply,
    Method::TransactionCount,
    Method::Version,
    Method::VoteAccounts,
    Method::MinimumLedgerSlot,
];

fn confirmed() -> CommitmentConfig {
    CommitmentConfig::confirmed()
}

async fn run_method(
    client: &RpcClient,
    fixture: &Fixture,
    method: Method,
) -> Result<Option<u64>> {
    let mut observed_slot = None;
    match method {
        Method::BlockCommitment => {
            let commitment: RpcBlockCommitment<[u64; 32]> = client
                .send(
                    RpcRequest::Custom {
                        method: "getBlockCommitment",
                    },
                    serde_json::json!([fixture.slot_floor]),
                )
                .await?;
            check_eq!(
                commitment.commitment,
                Some([0u64; 32]),
                "getBlockCommitment must report an all-zero commitment array"
            )?;
            check_eq!(
                commitment.total_stake,
                0,
                "getBlockCommitment must report zero total stake"
            )?;
        }
        Method::ClusterNodes => {
            let nodes = client.get_cluster_nodes().await?;
            check_eq!(
                nodes.len(),
                1,
                "getClusterNodes must list exactly the validator itself"
            )?;
            let node = &nodes[0];
            check_eq!(
                node.pubkey,
                fixture.identity.to_string(),
                "getClusterNodes pubkey must be the validator identity"
            )?;
            check!(
                node.gossip.is_none()
                    && node.tpu.is_none()
                    && node.rpc.is_none()
                    && node.pubsub.is_none()
                    && node.version.is_none()
                    && node.feature_set.is_none()
                    && node.shred_version.is_none(),
                "getClusterNodes must leave every optional field unset, got \
                 {node:?}"
            )?;
        }
        Method::EpochInfo => {
            let info = client.get_epoch_info().await?;
            check_eq!(
                info.slots_in_epoch,
                SLOTS_IN_EPOCH,
                "getEpochInfo slotsInEpoch"
            )?;
            check_eq!(
                info.epoch,
                info.absolute_slot / SLOTS_IN_EPOCH,
                "getEpochInfo epoch must be derived from absoluteSlot"
            )?;
            check_eq!(
                info.slot_index,
                info.absolute_slot % SLOTS_IN_EPOCH,
                "getEpochInfo slotIndex must be derived from absoluteSlot"
            )?;
            check_eq!(
                info.block_height,
                info.absolute_slot,
                "getEpochInfo blockHeight must equal absoluteSlot on the er"
            )?;
            check_eq!(
                info.transaction_count,
                Some(0),
                "getEpochInfo transactionCount"
            )?;
            observed_slot = Some(info.absolute_slot);
        }
        Method::EpochSchedule => {
            let schedule = client.get_epoch_schedule().await?;
            check_eq!(
                schedule.slots_per_epoch,
                SLOTS_IN_EPOCH,
                "getEpochSchedule slotsPerEpoch"
            )?;
            check_eq!(
                schedule.leader_schedule_slot_offset,
                0,
                "getEpochSchedule leaderScheduleSlotOffset"
            )?;
            check!(!schedule.warmup, "getEpochSchedule warmup must be false")?;
            check_eq!(
                schedule.first_normal_epoch,
                0,
                "getEpochSchedule firstNormalEpoch"
            )?;
            check_eq!(
                schedule.first_normal_slot,
                0,
                "getEpochSchedule firstNormalSlot"
            )?;
        }
        Method::FirstAvailableBlock => {
            check_eq!(
                client.get_first_available_block().await?,
                0,
                "getFirstAvailableBlock"
            )?;
        }
        Method::GenesisHash => {
            check_eq!(
                client.get_genesis_hash().await?,
                hash::Hash::default(),
                "getGenesisHash must be the default hash"
            )?;
        }
        Method::Health => {
            client.get_health().await?;
        }
        Method::HighestSnapshotSlot => {
            let snapshot = client.get_highest_snapshot_slot().await?;
            check_eq!(snapshot.full, 0, "getHighestSnapshotSlot full")?;
            check_eq!(
                snapshot.incremental,
                None,
                "getHighestSnapshotSlot incremental"
            )?;
        }
        Method::Identity => {
            check_eq!(
                client.get_identity().await?,
                fixture.identity,
                "getIdentity must be the validator identity"
            )?;
        }
        Method::LargestAccounts => {
            let largest = client
                .get_largest_accounts_with_config(RpcLargestAccountsConfig {
                    commitment: Some(confirmed()),
                    filter: None,
                    sort_results: None,
                })
                .await?;
            check!(
                largest.value.is_empty(),
                "getLargestAccounts must be empty, got {:?}",
                largest.value
            )?;
            observed_slot = Some(largest.context.slot);
        }
        Method::RecentPerformanceSamples => {
            let samples = client
                .get_recent_performance_samples(Some(PERFORMANCE_SAMPLE_LIMIT))
                .await?;
            check!(
                !samples.is_empty()
                    && samples.len() <= PERFORMANCE_SAMPLE_LIMIT,
                "getRecentPerformanceSamples must honour the limit of \
                 {PERFORMANCE_SAMPLE_LIMIT}, got {}",
                samples.len()
            )?;
            for sample in &samples {
                check_eq!(
                    sample.sample_period_secs,
                    PERFORMANCE_SAMPLE_PERIOD_SECS,
                    "getRecentPerformanceSamples samplePeriodSecs"
                )?;
                check!(
                    sample.num_slots >= 1,
                    "getRecentPerformanceSamples numSlots must be at least 1, \
                     got {sample:?}"
                )?;
            }
        }
        Method::SlotLeader => {
            check_eq!(
                client.get_slot_leader().await?,
                fixture.identity,
                "getSlotLeader must be the validator identity"
            )?;
        }
        Method::SlotLeaders => {
            let leaders = client
                .get_slot_leaders(fixture.slot_floor, SLOT_LEADER_LIMIT)
                .await?;
            check!(
                !leaders.is_empty()
                    && leaders.len() as u64 <= SLOT_LEADER_LIMIT,
                "getSlotLeaders must answer with between 1 and \
                 {SLOT_LEADER_LIMIT} leaders, got {}",
                leaders.len()
            )?;
            check!(
                leaders.iter().all(|leader| *leader == fixture.identity),
                "getSlotLeaders must only name the validator identity, got \
                 {leaders:?}"
            )?;
        }
        Method::Supply => {
            let supply = client.supply_with_commitment(confirmed()).await?;
            check_eq!(supply.value.total, u64::MAX, "getSupply total")?;
            check_eq!(
                supply.value.circulating,
                u64::MAX / 2,
                "getSupply circulating"
            )?;
            check_eq!(
                supply.value.non_circulating,
                u64::MAX / 2,
                "getSupply nonCirculating"
            )?;
            check!(
                supply.value.non_circulating_accounts.is_empty(),
                "getSupply nonCirculatingAccounts must be empty"
            )?;
            observed_slot = Some(supply.context.slot);
        }
        Method::TokenLargestAccounts => {
            let largest = client
                .get_token_largest_accounts_with_commitment(
                    &fixture.mint,
                    confirmed(),
                )
                .await?;
            check!(
                largest.value.is_empty(),
                "getTokenLargestAccounts must be empty, got {:?}",
                largest.value
            )?;
            observed_slot = Some(largest.context.slot);
        }
        Method::TokenSupply => {
            let supply = client
                .get_token_supply_with_commitment(&fixture.mint, confirmed())
                .await?;
            check_eq!(supply.value.amount, "0", "getTokenSupply amount")?;
            check_eq!(supply.value.decimals, 0, "getTokenSupply decimals")?;
            check_eq!(
                supply.value.ui_amount,
                Some(0.0),
                "getTokenSupply uiAmount"
            )?;
            check_eq!(
                supply.value.ui_amount_string,
                "0.0",
                "getTokenSupply uiAmountString"
            )?;
            observed_slot = Some(supply.context.slot);
        }
        Method::TransactionCount => {
            check_eq!(
                client.get_transaction_count().await?,
                0,
                "getTransactionCount"
            )?;
        }
        Method::Version => {
            let version = client.get_version().await?;
            let parts: Vec<&str> = version.solana_core.split('.').collect();
            check!(
                parts.len() == 3
                    && parts.iter().all(|part| part.parse::<u32>().is_ok()),
                "getVersion solana-core must be a dotted numeric version, got \
                 {:?}",
                version.solana_core
            )?;
            check!(
                version.feature_set.is_some_and(|set| set != 0),
                "getVersion feature-set must be a non-zero id, got {:?}",
                version.feature_set
            )?;
        }
        Method::VoteAccounts => {
            let accounts = client.get_vote_accounts().await?;
            check!(
                accounts.current.is_empty() && accounts.delinquent.is_empty(),
                "getVoteAccounts must list no current or delinquent accounts"
            )?;
        }
        Method::MinimumLedgerSlot => {
            check_eq!(
                client.minimum_ledger_slot().await?,
                0,
                "minimumLedgerSlot"
            )?;
        }
    }
    Ok(observed_slot)
}

async fn bounded<T>(
    label: &str,
    call: impl std::future::Future<Output = T>,
) -> Result<T> {
    tokio::time::timeout(NEGATIVE_DEADLINE, call)
        .await
        .map_err(|_| {
            format!("{label} did not answer within {NEGATIVE_DEADLINE:?}")
                .into()
        })
}

#[async_trait(?Send)]
impl Scenario for RpcCompatMethods {
    fn name(&self) -> &str {
        "redshift/rpc_compat_methods"
    }

    async fn run(&self, _base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let client = RpcClient::new_with_commitment(
            er.api().url().to_owned(),
            confirmed(),
        );
        let fixture = Fixture {
            identity: er.identity(),
            mint: Keypair::new().pubkey(),
            slot_floor: client.get_slot().await?,
        };

        let burst_started = Instant::now();
        let requests = join_all((0..REQUESTS).map(|index| {
            run_method(&client, &fixture, CYCLE[index % CYCLE.len()])
        }));
        let outcomes = tokio::time::timeout(BURST_DEADLINE, requests)
            .await
            .map_err(|_| {
                format!(
                    "the burst of {REQUESTS} requests exceeded \
                     {BURST_DEADLINE:?}"
                )
            })?;
        let burst_elapsed = burst_started.elapsed();
        let slot_ceiling = client.get_slot().await?;

        let mut failures = 0u64;
        for (index, outcome) in outcomes.iter().enumerate() {
            let method = CYCLE[index % CYCLE.len()];
            match outcome {
                Ok(Some(slot)) => {
                    if *slot < fixture.slot_floor || *slot > slot_ceiling {
                        failures += 1;
                        eprintln!(
                            "[redsuite] {}: request {index} ({method:?}) \
                             reported slot {slot} outside [{}, {}]",
                            self.name(),
                            fixture.slot_floor,
                            slot_ceiling
                        );
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    failures += 1;
                    eprintln!(
                        "[redsuite] {}: request {index} ({method:?}) failed: \
                         {error}",
                        self.name()
                    );
                }
            }
        }
        check_eq!(
            failures,
            0,
            "every request in the burst must decode and carry the documented \
             value"
        )?;

        let airdrop = bounded(
            "requestAirdrop",
            client.request_airdrop(&fixture.identity, AIRDROP_LAMPORTS),
        )
        .await?;
        check!(
            airdrop.is_err(),
            "requestAirdrop must be rejected by the official client, got \
             {airdrop:?}"
        )?;
        let raw_airdrop = bounded(
            "raw requestAirdrop",
            er.api().call::<serde_json::Value>(
                "requestAirdrop",
                &(fixture.identity.to_string(), AIRDROP_LAMPORTS),
            ),
        )
        .await?;
        match raw_airdrop {
            Ok(value) => {
                return Err(format!(
                    "raw requestAirdrop must be rejected, got {value}"
                )
                .into())
            }
            Err(error) => {
                let rejection = error.downcast_ref::<RpcError>();
                check_eq!(
                    rejection.map(|e| e.code),
                    Some(INVALID_REQUEST),
                    "requestAirdrop must be rejected with the invalid-request \
                     code, got {error}"
                )?;
                check!(
                    rejection
                        .is_some_and(|e| e.message.contains(AIRDROP_DISABLED)),
                    "requestAirdrop must be rejected with \
                     {AIRDROP_DISABLED:?}, got {error}"
                )?;
            }
        }

        let routes: Vec<serde_json::Value> =
            bounded("getRoutes", er.api().call("getRoutes", &NO_PARAMS))
                .await??;
        check!(routes.is_empty(), "getRoutes must be empty, got {routes:?}")?;

        let unknown = bounded(
            UNKNOWN_METHOD,
            er.api()
                .call::<serde_json::Value>(UNKNOWN_METHOD, &NO_PARAMS),
        )
        .await?;
        match unknown {
            Ok(value) => {
                return Err(format!(
                    "{UNKNOWN_METHOD} must be rejected, got {value}"
                )
                .into())
            }
            Err(error) => {
                let code = error.downcast_ref::<RpcError>().map(|e| e.code);
                check_eq!(
                    code,
                    Some(METHOD_NOT_FOUND),
                    "an unknown method must be rejected with the JSON-RPC \
                     method-not-found code, got {error}"
                )?;
            }
        }

        Ok(ScenarioReport::ok(self.name())
            .setting("requests", REQUESTS)
            .setting("method kinds", CYCLE.len())
            .setting("identity", fixture.identity)
            .setting("slot floor", fixture.slot_floor)
            .setting("slot ceiling", slot_ceiling)
            .metric(
                "burst elapsed ms",
                Unit::Millis,
                burst_elapsed.as_secs_f64() * 1e3,
            )
            .metric("burst failures", Unit::Count, failures as f64))
    }
}
