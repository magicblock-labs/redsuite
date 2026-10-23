use std::{future::Future, rc::Rc, time::Duration};

use anyhow::{anyhow, ensure, Context};
use futures_util::future::join_all;
use hash::Hash;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check,
    rpc_client::nonblocking::rpc_client::RpcClient,
    rpc_client_api::{
        config::RpcLargestAccountsConfig, request::RpcRequest, response::RpcBlockCommitment,
    },
    topology::{self, ErOptions, PrivateEr},
    BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use serde_json::Value;
use signer::Signer;
use solana_commitment_config::CommitmentConfig;
use tokio::time;

const REQUESTS: usize = 256;
const BURST_DEADLINE: Duration = Duration::from_secs(20);
const NEGATIVE_DEADLINE: Duration = Duration::from_secs(5);
const SUPERBLOCK_SLOTS: u64 = 40;
const SLOT_LEADER_LIMIT: u64 = 8;
const PERFORMANCE_SAMPLE_LIMIT: usize = 4;
const PERFORMANCE_SAMPLE_PERIOD_SECS: u16 = 60;
const AIRDROP_LAMPORTS: u64 = 1_000_000;
const AIRDROP_DISABLED: &str = "free airdrop faucet is disabled";
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const UNKNOWN_METHOD: &str = "getSomethingThatDoesNotExist";
const NO_PARAMS: [u8; 0] = [];

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

async fn run_method(client: &RpcClient, fixture: &Fixture, method: Method) -> Result<Option<u64>> {
    let mut observed_slot = None;
    match method {
        Method::BlockCommitment => {
            let commitment: RpcBlockCommitment<[u64; 32]> = client
                .send(
                    RpcRequest::Custom { method: "getBlockCommitment" },
                    serde_json::json!([fixture.slot_floor]),
                )
                .await?;
            ensure!(commitment.commitment == Some([0u64; 32]));
            ensure!(commitment.total_stake == 0);
        }
        Method::ClusterNodes => {
            let nodes = client.get_cluster_nodes().await?;
            ensure!(nodes.len() == 1);
            let node = &nodes[0];
            ensure!(node.pubkey == fixture.identity.to_string());
            ensure!(
                node.gossip.is_none()
                    && node.tpu.is_none()
                    && node.rpc.is_none()
                    && node.pubsub.is_none()
                    && node.version.is_none()
                    && node.feature_set.is_none()
                    && node.shred_version.is_none(),
                "getClusterNodes must leave every optional field unset, got \
                 {node:?}"
            );
        }
        Method::EpochInfo => {
            let info = client.get_epoch_info().await?;
            ensure!(info.slots_in_epoch == SUPERBLOCK_SLOTS);
            ensure!(info.epoch == (info.absolute_slot / SUPERBLOCK_SLOTS));
            ensure!(info.slot_index == (info.absolute_slot % SUPERBLOCK_SLOTS));
            ensure!(info.block_height == info.absolute_slot);
            ensure!(info.transaction_count == Some(0));
            observed_slot = Some(info.absolute_slot);
        }
        Method::EpochSchedule => {
            let schedule = client.get_epoch_schedule().await?;
            ensure!(schedule.slots_per_epoch == SUPERBLOCK_SLOTS);
            ensure!(schedule.leader_schedule_slot_offset == SUPERBLOCK_SLOTS);
            ensure!(!schedule.warmup, "getEpochSchedule warmup must be false");
            ensure!(schedule.first_normal_epoch == 0);
            ensure!(schedule.first_normal_slot == 0);
        }
        Method::FirstAvailableBlock => {
            ensure!(client.get_first_available_block().await? == 0);
        }
        Method::GenesisHash => {
            ensure!(client.get_genesis_hash().await? == Hash::default());
        }
        Method::Health => {
            client.get_health().await?;
        }
        Method::HighestSnapshotSlot => {
            let snapshot = client.get_highest_snapshot_slot().await?;
            ensure!(snapshot.full == 0);
            ensure!(snapshot.incremental == None);
        }
        Method::Identity => {
            ensure!(client.get_identity().await? == fixture.identity);
        }
        Method::LargestAccounts => {
            let largest = client
                .get_largest_accounts_with_config(RpcLargestAccountsConfig {
                    commitment: Some(confirmed()),
                    filter: None,
                    sort_results: None,
                })
                .await?;
            ensure!(
                largest.value.is_empty(),
                "getLargestAccounts must be empty, got {:?}",
                largest.value
            );
            observed_slot = Some(largest.context.slot);
        }
        Method::RecentPerformanceSamples => {
            let samples =
                client.get_recent_performance_samples(Some(PERFORMANCE_SAMPLE_LIMIT)).await?;
            ensure!(
                !samples.is_empty() && samples.len() <= PERFORMANCE_SAMPLE_LIMIT,
                "getRecentPerformanceSamples must honour the limit of \
                 {PERFORMANCE_SAMPLE_LIMIT}, got {}",
                samples.len()
            );
            for sample in &samples {
                ensure!(sample.sample_period_secs == PERFORMANCE_SAMPLE_PERIOD_SECS);
                ensure!(
                    sample.num_slots >= 1,
                    "getRecentPerformanceSamples numSlots must be at least 1, \
                     got {sample:?}"
                );
            }
        }
        Method::SlotLeader => {
            ensure!(client.get_slot_leader().await? == fixture.identity);
        }
        Method::SlotLeaders => {
            let leaders = client.get_slot_leaders(fixture.slot_floor, SLOT_LEADER_LIMIT).await?;
            ensure!(
                !leaders.is_empty() && leaders.len() as u64 <= SLOT_LEADER_LIMIT,
                "getSlotLeaders must answer with between 1 and \
                 {SLOT_LEADER_LIMIT} leaders, got {}",
                leaders.len()
            );
            ensure!(
                leaders.iter().all(|leader| *leader == fixture.identity),
                "getSlotLeaders must only name the validator identity, got \
                 {leaders:?}"
            );
        }
        Method::Supply => {
            let supply = client.supply_with_commitment(confirmed()).await?;
            ensure!(supply.value.total == u64::MAX);
            ensure!(supply.value.circulating == (u64::MAX / 2));
            ensure!(supply.value.non_circulating == (u64::MAX / 2));
            ensure!(
                supply.value.non_circulating_accounts.is_empty(),
                "getSupply nonCirculatingAccounts must be empty"
            );
            observed_slot = Some(supply.context.slot);
        }
        Method::TokenLargestAccounts => {
            let largest = client
                .get_token_largest_accounts_with_commitment(&fixture.mint, confirmed())
                .await?;
            ensure!(
                largest.value.is_empty(),
                "getTokenLargestAccounts must be empty, got {:?}",
                largest.value
            );
            observed_slot = Some(largest.context.slot);
        }
        Method::TokenSupply => {
            let supply =
                client.get_token_supply_with_commitment(&fixture.mint, confirmed()).await?;
            ensure!(supply.value.amount == "0");
            ensure!(supply.value.decimals == 0);
            ensure!(supply.value.ui_amount == Some(0.0));
            ensure!(supply.value.ui_amount_string == "0.0");
            observed_slot = Some(supply.context.slot);
        }
        Method::TransactionCount => {
            ensure!(client.get_transaction_count().await? == 0);
        }
        Method::Version => {
            let version = client.get_version().await?;
            let parts: Vec<&str> = version.solana_core.split('.').collect();
            ensure!(
                parts.len() == 3 && parts.iter().all(|part| part.parse::<u32>().is_ok()),
                "getVersion solana-core must be a dotted numeric version, got \
                 {:?}",
                version.solana_core
            );
            ensure!(
                version.feature_set.is_some_and(|set| set != 0),
                "getVersion feature-set must be a non-zero id, got {:?}",
                version.feature_set
            );
        }
        Method::VoteAccounts => {
            let accounts = client.get_vote_accounts().await?;
            ensure!(
                accounts.current.is_empty() && accounts.delinquent.is_empty(),
                "getVoteAccounts must list no current or delinquent accounts"
            );
        }
        Method::MinimumLedgerSlot => {
            ensure!(client.minimum_ledger_slot().await? == 0);
        }
    }
    Ok(observed_slot)
}

async fn bounded<T>(label: &str, call: impl Future<Output = T>) -> Result<T> {
    time::timeout(NEGATIVE_DEADLINE, call)
        .await
        .with_context(|| format!("{label} did not answer within {NEGATIVE_DEADLINE:?}"))
}

pub struct RpcCompatMethods {
    private: PrivateEr,
    client: RpcClient,
    fixture: Fixture,
}

impl PrivateErScenario for RpcCompatMethods {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let private = topology::private_er(
            &base,
            ErOptions {
                label: "rpc-compat-methods".into(),
                settings: Box::new(|params| {
                    params.engine.blockstore.superblock = SUPERBLOCK_SLOTS;
                }),
                ..ErOptions::default()
            },
        )
        .await?;
        let er = private.ctx();
        let client = RpcClient::new_with_commitment(er.api().url().to_string(), confirmed());
        let slot_floor = check::poll_for("configured epoch boundary", BURST_DEADLINE, || async {
            let slot = client.get_slot().await?;
            ensure!(slot > SUPERBLOCK_SLOTS, "advance beyond the first epoch");
            Result::Ok(slot)
        })
        .await?;
        let fixture = Fixture {
            identity: er.identity(),
            mint: Keypair::new().pubkey(),
            slot_floor,
        };

        Ok(Self { private, client, fixture })
    }

    async fn run(self) -> Result<()> {
        let Self { private, client, fixture } = self;
        let er = private.ctx();
        let requests = join_all(
            (0..REQUESTS).map(|index| run_method(&client, &fixture, CYCLE[index % CYCLE.len()])),
        );
        let outcomes = time::timeout(BURST_DEADLINE, requests).await.with_context(|| {
            format!("the burst of {REQUESTS} requests exceeded {BURST_DEADLINE:?}")
        })?;
        let slot_ceiling = client.get_slot().await?;

        let mut failures = 0u64;
        let mut first_failure = None;
        for (index, outcome) in outcomes.iter().enumerate() {
            let method = CYCLE[index % CYCLE.len()];
            match outcome {
                Ok(Some(slot)) => {
                    if *slot < fixture.slot_floor || *slot > slot_ceiling {
                        failures += 1;
                        first_failure.get_or_insert_with(|| {
                            format!(
                                "request {index} ({method:?}) \
                                 reported slot {slot} outside [{}, {}]",
                                fixture.slot_floor, slot_ceiling
                            )
                        });
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    failures += 1;
                    first_failure.get_or_insert_with(|| {
                        format!("request {index} ({method:?}) failed: {error:#}")
                    });
                }
            }
        }
        ensure!(
            failures == 0,
            "failures={:?}; first error={:?}",
            failures,
            first_failure
        );

        let airdrop = bounded(
            "requestAirdrop",
            client.request_airdrop(&fixture.identity, AIRDROP_LAMPORTS),
        )
        .await?;
        ensure!(
            airdrop.is_err(),
            "requestAirdrop must be rejected by the official client, got \
             {airdrop:?}"
        );
        let raw_airdrop = bounded(
            "raw requestAirdrop",
            er.api().try_call::<Value>(
                "requestAirdrop",
                &(fixture.identity.to_string(), AIRDROP_LAMPORTS),
            ),
        )
        .await??;
        match raw_airdrop {
            Ok(value) => return Err(anyhow!("raw requestAirdrop must be rejected, got {value}")),
            Err(error) => {
                ensure!(error.code == INVALID_REQUEST);
                ensure!(
                    error.message.contains(AIRDROP_DISABLED),
                    "requestAirdrop must be rejected with \
                     {AIRDROP_DISABLED:?}, got {error:#}"
                );
            }
        }

        let routes: Vec<Value> =
            bounded("getRoutes", er.api().call("getRoutes", &NO_PARAMS)).await??;
        ensure!(routes.is_empty(), "getRoutes must be empty, got {routes:?}");

        let unknown = bounded(
            UNKNOWN_METHOD,
            er.api().try_call::<Value>(UNKNOWN_METHOD, &NO_PARAMS),
        )
        .await??;
        match unknown {
            Ok(value) => return Err(anyhow!("{UNKNOWN_METHOD} must be rejected, got {value}")),
            Err(error) => {
                ensure!(error.code == METHOD_NOT_FOUND);
            }
        }

        private.finish().await?;
        Ok(())
    }
}
