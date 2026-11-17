pub mod observations;
pub mod replicated_runtime;
pub mod replication_promotion;
pub mod replication_recovery;
pub mod replication_settlement_recovery;
mod runtime;
pub mod snapshot_bootstrap;

pub(super) fn settings(params: &mut magicblock_config::LeaderParams) {
    // Keep fixture reads on HTTP while retaining the required lifecycle feed.
    for stream in &mut params.chainsync.grpc {
        stream.duplication_delay = std::time::Duration::from_secs(3600);
    }
    params.engine.blockstore.blocktime = std::time::Duration::from_millis(40);
    params.engine.blockstore.superblock = 0;
    params.engine.blockstore.checkpoint = observations::CHECKPOINT_SLOTS;
    params.engine.ledger.size_limit = i64::MAX as u64;
}
