use std::{future::Future, rc::Rc, time::Duration};

use anyhow::{anyhow, ensure, Context};
use futures_util::future::join_all;
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
    Api, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use serde_json::Value;
use signer::Signer;
use solana_account_decoder_client_types::token::UiTokenAmount;
use solana_commitment_config::CommitmentConfig;
use solana_epoch_info::EpochInfo;
use solana_epoch_schedule::EpochSchedule;
use tokio::time;

const REQUESTS: usize = CYCLE.len();
const BURST_DEADLINE: Duration = Duration::from_secs(20);
const NEGATIVE_DEADLINE: Duration = Duration::from_secs(5);
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

async fn bounded<T>(label: &str, call: impl Future<Output = T>) -> Result<T> {
    time::timeout(NEGATIVE_DEADLINE, call)
        .await
        .with_context(|| format!("{label} did not answer within {NEGATIVE_DEADLINE:?}"))
}

/// Checks supported RPC response shapes, runtime-linked metadata, and explicit refusals.
pub struct RpcCompatSmoke {
    client: Api,
    fixture: Fixture,
}

impl Scenario for RpcCompatSmoke {
    async fn setup(_base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let client = er.api().clone();
        let slot_floor = client.get_slot().await?;
        let fixture = Fixture {
            identity: er.identity(),
            mint: Keypair::new().pubkey(),
            slot_floor,
        };

        Ok(Self { client, fixture })
    }

    async fn run(self) -> Result<()> {
        let requests =
            join_all((0..REQUESTS).map(|index| self.run_method(CYCLE[index % CYCLE.len()])));
        let outcomes = time::timeout(BURST_DEADLINE, requests).await.with_context(|| {
            format!("the burst of {REQUESTS} requests exceeded {BURST_DEADLINE:?}")
        })?;
        let slot_ceiling = self.client.get_slot().await?;

        for outcome in outcomes {
            if let Some(slot) = outcome? {
                ensure!(slot >= self.fixture.slot_floor);
                ensure!(slot <= slot_ceiling);
            }
        }

        let airdrop = bounded(
            "requestAirdrop",
            self.client.try_call::<Value>(
                RpcRequest::RequestAirdrop,
                &(self.fixture.identity.to_string(), AIRDROP_LAMPORTS),
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
            self.client.call(RpcRequest::Custom { method: "getRoutes" }, &NO_PARAMS),
        )
        .await??;
        ensure!(routes.is_empty());

        let unknown = bounded(
            UNKNOWN_METHOD,
            self.client
                .try_call::<Value>(RpcRequest::Custom { method: UNKNOWN_METHOD }, &NO_PARAMS),
        )
        .await??;
        match unknown {
            Ok(_) => return Err(anyhow!("{UNKNOWN_METHOD} must be rejected")),
            Err(error) => {
                ensure!(error.error.code.code() == METHOD_NOT_FOUND);
            }
        }

        Ok(())
    }
}

impl RpcCompatSmoke {
    async fn run_method(&self, method: Method) -> Result<Option<u64>> {
        let mut observed_slot = None;
        match method {
            Method::BlockCommitment => {
                let _commitment: RpcBlockCommitment<[u64; 32]> = self
                    .client
                    .call(RpcRequest::GetBlockCommitment, &(self.fixture.slot_floor,))
                    .await?;
            }
            Method::ClusterNodes => {
                let nodes: Vec<RpcContactInfo> =
                    self.client.call(RpcRequest::GetClusterNodes, &NO_PARAMS).await?;
                let node = nodes
                    .iter()
                    .find(|node| node.pubkey == self.fixture.identity.to_string())
                    .context("validator identity missing from cluster nodes")?;
                ensure!(node.pubkey == self.fixture.identity.to_string());
            }
            Method::EpochInfo => {
                let info: EpochInfo =
                    self.client.call(RpcRequest::GetEpochInfo, &(confirmed(),)).await?;
                ensure!(info.slots_in_epoch > 0);
                ensure!(info.epoch == (info.absolute_slot / info.slots_in_epoch));
                ensure!(info.slot_index == (info.absolute_slot % info.slots_in_epoch));
                ensure!(info.block_height == info.absolute_slot);
                observed_slot = Some(info.absolute_slot);
            }
            Method::EpochSchedule => {
                let schedule: EpochSchedule =
                    self.client.call(RpcRequest::GetEpochSchedule, &NO_PARAMS).await?;
                ensure!(schedule.slots_per_epoch > 0);
                ensure!(schedule.leader_schedule_slot_offset == schedule.slots_per_epoch);
                ensure!(!schedule.warmup);
                ensure!(schedule.first_normal_epoch == 0);
                ensure!(schedule.first_normal_slot == 0);
            }
            Method::FirstAvailableBlock => {
                let _: u64 =
                    self.client.call(RpcRequest::GetFirstAvailableBlock, &NO_PARAMS).await?;
            }
            Method::GenesisHash => {
                let _: String = self.client.call(RpcRequest::GetGenesisHash, &NO_PARAMS).await?;
            }
            Method::Health => {
                self.client.get_health().await?;
            }
            Method::HighestSnapshotSlot => {
                let _snapshot: RpcSnapshotSlotInfo =
                    self.client.call(RpcRequest::GetHighestSnapshotSlot, &NO_PARAMS).await?;
            }
            Method::Identity => {
                ensure!(
                    self.client
                        .call::<RpcIdentity>(RpcRequest::GetIdentity, &NO_PARAMS)
                        .await?
                        .identity
                        == self.fixture.identity.to_string()
                );
            }
            Method::LargestAccounts => {
                let config = RpcLargestAccountsConfig {
                    commitment: Some(confirmed()),
                    ..RpcLargestAccountsConfig::default()
                };
                let largest: Response<Vec<RpcAccountBalance>> =
                    self.client.call(RpcRequest::GetLargestAccounts, &(config,)).await?;
                observed_slot = Some(largest.context.slot);
            }
            Method::RecentPerformanceSamples => {
                let samples: Vec<RpcPerfSample> = self
                    .client
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
                    self.client.call::<String>(RpcRequest::GetSlotLeader, &(confirmed(),)).await?
                        == self.fixture.identity.to_string()
                );
            }
            Method::SlotLeaders => {
                let leaders: Vec<String> = self
                    .client
                    .call(
                        RpcRequest::GetSlotLeaders,
                        &(self.fixture.slot_floor, SLOT_LEADER_LIMIT),
                    )
                    .await?;
                ensure!(!leaders.is_empty() && leaders.len() as u64 <= SLOT_LEADER_LIMIT);
                let identity = self.fixture.identity.to_string();
                ensure!(leaders.iter().all(|leader| *leader == identity));
            }
            Method::Supply => {
                let supply: Response<RpcSupply> =
                    self.client.call(RpcRequest::GetSupply, &(confirmed(),)).await?;
                observed_slot = Some(supply.context.slot);
            }
            Method::TokenLargestAccounts => {
                let largest: Response<Vec<RpcTokenAccountBalance>> = self
                    .client
                    .call(
                        RpcRequest::GetTokenLargestAccounts,
                        &(self.fixture.mint.to_string(), confirmed()),
                    )
                    .await?;
                observed_slot = Some(largest.context.slot);
            }
            Method::TokenSupply => {
                let supply: Response<UiTokenAmount> = self
                    .client
                    .call(
                        RpcRequest::GetTokenSupply,
                        &(self.fixture.mint.to_string(), confirmed()),
                    )
                    .await?;
                observed_slot = Some(supply.context.slot);
            }
            Method::TransactionCount => {
                let _: u64 = self.client.call(RpcRequest::GetTransactionCount, &NO_PARAMS).await?;
            }
            Method::Version => {
                let version: RpcVersionInfo =
                    self.client.call(RpcRequest::GetVersion, &NO_PARAMS).await?;
                let parts: Vec<&str> = version.solana_core.split('.').collect();
                ensure!(parts.len() == 3 && parts.iter().all(|part| part.parse::<u32>().is_ok()));
                ensure!(version.feature_set.is_some_and(|set| set != 0));
            }
            Method::VoteAccounts => {
                let _accounts: RpcVoteAccountStatus =
                    self.client.call(RpcRequest::GetVoteAccounts, &(confirmed(),)).await?;
            }
            Method::MinimumLedgerSlot => {
                let _: u64 = self.client.call(RpcRequest::MinimumLedgerSlot, &NO_PARAMS).await?;
            }
        }
        Ok(observed_slot)
    }
}
