use std::{future::Future, rc::Rc, time::Duration};

use anyhow::{anyhow, ensure, Context};
use futures_util::future::join_all;
use hash::Hash;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    rpc_types::{
        config::RpcLargestAccountsConfig,
        request::RpcRequest,
        response::{
            Response, RpcAccountBalance, RpcBlockCommitment, RpcContactInfo, RpcIdentity,
            RpcPerfSample, RpcSnapshotSlotInfo, RpcSupply, RpcTokenAccountBalance, RpcVersionInfo,
            RpcVoteAccountStatus,
        },
    },
    topology::{self, ErOptions, PrivateEr},
    Api, BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use serde_json::Value;
use signer::Signer;
use solana_account_decoder_client_types::token::UiTokenAmount;
use solana_commitment_config::CommitmentConfig;
use solana_epoch_info::EpochInfo;
use solana_epoch_schedule::EpochSchedule;
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

async fn run_method(client: &Api, fixture: &Fixture, method: Method) -> Result<Option<u64>> {
    let mut observed_slot = None;
    match method {
        Method::BlockCommitment => {
            let commitment: RpcBlockCommitment<[u64; 32]> =
                client.call(RpcRequest::GetBlockCommitment, &(fixture.slot_floor,)).await?;
            ensure!(commitment.commitment == Some([0u64; 32]));
            ensure!(commitment.total_stake == 0);
        }
        Method::ClusterNodes => {
            let nodes: Vec<RpcContactInfo> =
                client.call(RpcRequest::GetClusterNodes, &NO_PARAMS).await?;
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
                    && node.shred_version.is_none()
            );
        }
        Method::EpochInfo => {
            let info: EpochInfo = client.call(RpcRequest::GetEpochInfo, &(confirmed(),)).await?;
            ensure!(info.slots_in_epoch == SUPERBLOCK_SLOTS);
            ensure!(info.epoch == (info.absolute_slot / SUPERBLOCK_SLOTS));
            ensure!(info.slot_index == (info.absolute_slot % SUPERBLOCK_SLOTS));
            ensure!(info.block_height == info.absolute_slot);
            ensure!(info.transaction_count == Some(0));
            observed_slot = Some(info.absolute_slot);
        }
        Method::EpochSchedule => {
            let schedule: EpochSchedule =
                client.call(RpcRequest::GetEpochSchedule, &NO_PARAMS).await?;
            ensure!(schedule.slots_per_epoch == SUPERBLOCK_SLOTS);
            ensure!(schedule.leader_schedule_slot_offset == SUPERBLOCK_SLOTS);
            ensure!(!schedule.warmup);
            ensure!(schedule.first_normal_epoch == 0);
            ensure!(schedule.first_normal_slot == 0);
        }
        Method::FirstAvailableBlock => {
            ensure!(client.call::<u64>(RpcRequest::GetFirstAvailableBlock, &NO_PARAMS).await? == 0);
        }
        Method::GenesisHash => {
            ensure!(
                client.call::<String>(RpcRequest::GetGenesisHash, &NO_PARAMS).await?
                    == Hash::default().to_string()
            );
        }
        Method::Health => {
            client.get_health().await?;
        }
        Method::HighestSnapshotSlot => {
            let snapshot: RpcSnapshotSlotInfo =
                client.call(RpcRequest::GetHighestSnapshotSlot, &NO_PARAMS).await?;
            ensure!(snapshot.full == 0);
            ensure!(snapshot.incremental == None);
        }
        Method::Identity => {
            ensure!(
                client.call::<RpcIdentity>(RpcRequest::GetIdentity, &NO_PARAMS).await?.identity
                    == fixture.identity.to_string()
            );
        }
        Method::LargestAccounts => {
            let config = RpcLargestAccountsConfig {
                commitment: Some(confirmed()),
                ..RpcLargestAccountsConfig::default()
            };
            let largest: Response<Vec<RpcAccountBalance>> =
                client.call(RpcRequest::GetLargestAccounts, &(config,)).await?;
            ensure!(largest.value.is_empty());
            observed_slot = Some(largest.context.slot);
        }
        Method::RecentPerformanceSamples => {
            let samples: Vec<RpcPerfSample> = client
                .call(
                    RpcRequest::GetRecentPerformanceSamples,
                    &(PERFORMANCE_SAMPLE_LIMIT,),
                )
                .await?;
            ensure!(!samples.is_empty() && samples.len() <= PERFORMANCE_SAMPLE_LIMIT);
            for sample in &samples {
                ensure!(sample.sample_period_secs == PERFORMANCE_SAMPLE_PERIOD_SECS);
                ensure!(sample.num_slots >= 1);
            }
        }
        Method::SlotLeader => {
            ensure!(
                client.call::<String>(RpcRequest::GetSlotLeader, &(confirmed(),)).await?
                    == fixture.identity.to_string()
            );
        }
        Method::SlotLeaders => {
            let leaders: Vec<String> = client
                .call(
                    RpcRequest::GetSlotLeaders,
                    &(fixture.slot_floor, SLOT_LEADER_LIMIT),
                )
                .await?;
            ensure!(!leaders.is_empty() && leaders.len() as u64 <= SLOT_LEADER_LIMIT);
            let identity = fixture.identity.to_string();
            ensure!(leaders.iter().all(|leader| *leader == identity));
        }
        Method::Supply => {
            let supply: Response<RpcSupply> =
                client.call(RpcRequest::GetSupply, &(confirmed(),)).await?;
            ensure!(supply.value.total == u64::MAX);
            ensure!(supply.value.circulating == (u64::MAX / 2));
            ensure!(supply.value.non_circulating == (u64::MAX / 2));
            ensure!(supply.value.non_circulating_accounts.is_empty());
            observed_slot = Some(supply.context.slot);
        }
        Method::TokenLargestAccounts => {
            let largest: Response<Vec<RpcTokenAccountBalance>> = client
                .call(
                    RpcRequest::GetTokenLargestAccounts,
                    &(fixture.mint.to_string(), confirmed()),
                )
                .await?;
            ensure!(largest.value.is_empty());
            observed_slot = Some(largest.context.slot);
        }
        Method::TokenSupply => {
            let supply: Response<UiTokenAmount> = client
                .call(
                    RpcRequest::GetTokenSupply,
                    &(fixture.mint.to_string(), confirmed()),
                )
                .await?;
            ensure!(supply.value.amount == "0");
            ensure!(supply.value.decimals == 0);
            ensure!(supply.value.ui_amount == Some(0.0));
            ensure!(supply.value.ui_amount_string == "0.0");
            observed_slot = Some(supply.context.slot);
        }
        Method::TransactionCount => {
            ensure!(
                client.call::<u64>(RpcRequest::GetTransactionCount, &(confirmed(),)).await? == 0
            );
        }
        Method::Version => {
            let version: RpcVersionInfo = client.call(RpcRequest::GetVersion, &NO_PARAMS).await?;
            let parts: Vec<&str> = version.solana_core.split('.').collect();
            ensure!(parts.len() == 3 && parts.iter().all(|part| part.parse::<u32>().is_ok()));
            ensure!(version.feature_set.is_some_and(|set| set != 0));
        }
        Method::VoteAccounts => {
            let accounts: RpcVoteAccountStatus =
                client.call(RpcRequest::GetVoteAccounts, &(confirmed(),)).await?;
            ensure!(accounts.current.is_empty() && accounts.delinquent.is_empty());
        }
        Method::MinimumLedgerSlot => {
            ensure!(client.call::<u64>(RpcRequest::MinimumLedgerSlot, &NO_PARAMS).await? == 0);
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
    client: Api,
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
        let client = er.api().clone();
        let slot_floor = client.await_slot(SUPERBLOCK_SLOTS + 1).await?;
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

        for outcome in outcomes {
            if let Some(slot) = outcome? {
                ensure!(slot >= fixture.slot_floor);
                ensure!(slot <= slot_ceiling);
            }
        }

        let airdrop = bounded(
            "requestAirdrop",
            er.api().try_call::<Value>(
                RpcRequest::RequestAirdrop,
                &(fixture.identity.to_string(), AIRDROP_LAMPORTS),
            ),
        )
        .await??;
        match airdrop {
            Ok(_) => return Err(anyhow!("requestAirdrop must be rejected")),
            Err(error) => {
                ensure!(error.error.code.code() == INVALID_REQUEST);
                ensure!(error.error.message.contains(AIRDROP_DISABLED));
            }
        }

        let routes: Vec<Value> = bounded(
            "getRoutes",
            er.api().call(RpcRequest::Custom { method: "getRoutes" }, &NO_PARAMS),
        )
        .await??;
        ensure!(routes.is_empty());

        let unknown = bounded(
            UNKNOWN_METHOD,
            er.api()
                .try_call::<Value>(RpcRequest::Custom { method: UNKNOWN_METHOD }, &NO_PARAMS),
        )
        .await??;
        match unknown {
            Ok(_) => return Err(anyhow!("{UNKNOWN_METHOD} must be rejected")),
            Err(error) => {
                ensure!(error.error.code.code() == METHOD_NOT_FOUND);
            }
        }

        private.finish().await?;
        Ok(())
    }
}
