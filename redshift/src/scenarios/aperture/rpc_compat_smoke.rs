use std::{future::Future, rc::Rc, time::Duration};

use anyhow::{ensure, Context};
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

const BURST_DEADLINE: Duration = Duration::from_secs(20);
const NEGATIVE_DEADLINE: Duration = Duration::from_secs(5);
const SLOT_LEADER_LIMIT: u64 = 8;
const PERFORMANCE_SAMPLE_LIMIT: usize = 4;
const AIRDROP_LAMPORTS: u64 = 1_000_000;
const AIRDROP_DISABLED: &str = "free airdrop faucet is disabled";
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const UNKNOWN_METHOD: &str = "getSomethingThatDoesNotExist";
const NO_PARAMS: [u8; 0] = [];

/// Supported RPC surface sampled for decodable responses and selected runtime-linked invariants.
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

const METHODS: [Method; 20] = [
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

async fn bounded<T>(label: &str, call: impl Future<Output = T>) -> Result<T> {
    time::timeout(NEGATIVE_DEADLINE, call)
        .await
        .with_context(|| format!("{label} did not answer within {NEGATIVE_DEADLINE:?}"))
}

/// Checks supported RPC response shapes, runtime-linked metadata, and explicit refusals.
/// This is a compatibility smoke check, not an exact-content oracle for every metadata method.
pub struct RpcCompatSmoke {
    client: Api,
    identity: Pubkey,
    mint: Pubkey,
    slot_floor: u64,
}

impl Scenario for RpcCompatSmoke {
    async fn setup(_base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let client = er.api().clone();
        let slot_floor = client.get_slot().await?;
        Ok(Self {
            client,
            identity: er.identity(),
            mint: Keypair::new().pubkey(),
            slot_floor,
        })
    }

    /// Bounds slot-bearing responses by the burst interval, then
    /// checks explicit unsupported requests.
    async fn run(self) -> Result<()> {
        let requests = join_all(METHODS.into_iter().map(|method| self.run_method(method)));
        let outcomes = time::timeout(BURST_DEADLINE, requests)
            .await
            .with_context(|| format!("RPC burst exceeded {BURST_DEADLINE:?}"))?;
        let slot_ceiling = self.client.get_slot().await?;

        for outcome in outcomes {
            if let Some(slot) = outcome? {
                ensure!(slot >= self.slot_floor);
                ensure!(slot <= slot_ceiling);
            }
        }

        let airdrop = bounded(
            "requestAirdrop",
            self.client.try_call::<Value>(
                RpcRequest::RequestAirdrop,
                &(self.identity.to_string(), AIRDROP_LAMPORTS),
            ),
        )
        .await??;
        let error = airdrop.err().context("requestAirdrop must be rejected")?;
        ensure!(error.error.code.code() == INVALID_REQUEST);
        ensure!(error.error.message.contains(AIRDROP_DISABLED));

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
        let error = unknown.err().with_context(|| format!("{UNKNOWN_METHOD} must be rejected"))?;
        ensure!(error.error.code.code() == METHOD_NOT_FOUND);

        Ok(())
    }
}

impl RpcCompatSmoke {
    /// Returns a slot only for responses whose runtime context can be checked
    /// against the burst window.
    async fn run_method(&self, method: Method) -> Result<Option<u64>> {
        let client = &self.client;
        let commitment = CommitmentConfig::confirmed();
        let mut observed_slot = None;
        match method {
            Method::BlockCommitment => {
                let _commitment: RpcBlockCommitment<[u64; 32]> =
                    client.call(RpcRequest::GetBlockCommitment, &(self.slot_floor,)).await?;
            }
            Method::ClusterNodes => {
                let nodes: Vec<RpcContactInfo> =
                    client.call(RpcRequest::GetClusterNodes, &NO_PARAMS).await?;
                let identity = self.identity.to_string();
                ensure!(
                    nodes.iter().any(|node| node.pubkey == identity),
                    "validator identity missing from cluster nodes"
                );
            }
            Method::EpochInfo => {
                let info: EpochInfo = client.call(RpcRequest::GetEpochInfo, &(commitment,)).await?;
                ensure!(info.slots_in_epoch > 0);
                ensure!(info.epoch == (info.absolute_slot / info.slots_in_epoch));
                ensure!(info.slot_index == (info.absolute_slot % info.slots_in_epoch));
                ensure!(info.block_height == info.absolute_slot);
                observed_slot = Some(info.absolute_slot);
            }
            Method::EpochSchedule => {
                let _: EpochSchedule =
                    client.call(RpcRequest::GetEpochSchedule, &NO_PARAMS).await?;
            }
            Method::FirstAvailableBlock => {
                let _: u64 = client.call(RpcRequest::GetFirstAvailableBlock, &NO_PARAMS).await?;
            }
            Method::GenesisHash => {
                let _: String = client.call(RpcRequest::GetGenesisHash, &NO_PARAMS).await?;
            }
            Method::Health => {
                client.get_health().await?;
            }
            Method::HighestSnapshotSlot => {
                let _snapshot: RpcSnapshotSlotInfo =
                    client.call(RpcRequest::GetHighestSnapshotSlot, &NO_PARAMS).await?;
            }
            Method::Identity => {
                let identity: RpcIdentity =
                    client.call(RpcRequest::GetIdentity, &NO_PARAMS).await?;
                ensure!(identity.identity == self.identity.to_string());
            }
            Method::LargestAccounts => {
                let config = RpcLargestAccountsConfig {
                    commitment: Some(commitment),
                    ..RpcLargestAccountsConfig::default()
                };
                let largest: Response<Vec<RpcAccountBalance>> =
                    client.call(RpcRequest::GetLargestAccounts, &(config,)).await?;
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
            }
            Method::SlotLeader => {
                let leader: String = client.call(RpcRequest::GetSlotLeader, &(commitment,)).await?;
                ensure!(leader == self.identity.to_string());
            }
            Method::SlotLeaders => {
                let leaders: Vec<String> = client
                    .call(
                        RpcRequest::GetSlotLeaders,
                        &(self.slot_floor, SLOT_LEADER_LIMIT),
                    )
                    .await?;
                ensure!(!leaders.is_empty() && leaders.len() as u64 <= SLOT_LEADER_LIMIT);
                let identity = self.identity.to_string();
                ensure!(leaders.iter().all(|leader| *leader == identity));
            }
            Method::Supply => {
                let supply: Response<RpcSupply> =
                    client.call(RpcRequest::GetSupply, &(commitment,)).await?;
                observed_slot = Some(supply.context.slot);
            }
            Method::TokenLargestAccounts => {
                let largest: Response<Vec<RpcTokenAccountBalance>> = client
                    .call(
                        RpcRequest::GetTokenLargestAccounts,
                        &(self.mint.to_string(), commitment),
                    )
                    .await?;
                observed_slot = Some(largest.context.slot);
            }
            Method::TokenSupply => {
                let supply: Response<UiTokenAmount> = client
                    .call(
                        RpcRequest::GetTokenSupply,
                        &(self.mint.to_string(), commitment),
                    )
                    .await?;
                observed_slot = Some(supply.context.slot);
            }
            Method::TransactionCount => {
                let _: u64 = client.call(RpcRequest::GetTransactionCount, &NO_PARAMS).await?;
            }
            Method::Version => {
                let _: RpcVersionInfo = client.call(RpcRequest::GetVersion, &NO_PARAMS).await?;
            }
            Method::VoteAccounts => {
                let _accounts: RpcVoteAccountStatus =
                    client.call(RpcRequest::GetVoteAccounts, &(commitment,)).await?;
            }
            Method::MinimumLedgerSlot => {
                let _: u64 = client.call(RpcRequest::MinimumLedgerSlot, &NO_PARAMS).await?;
            }
        }
        Ok(observed_slot)
    }
}
