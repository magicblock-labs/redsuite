pub mod replicated_runtime;
pub mod replication_recovery;
mod runtime;
pub mod snapshot_bootstrap;

pub(super) fn settings(params: &mut magicblock_config::LeaderParams) {
    // Keep fixture reads on HTTP while retaining the required lifecycle feed.
    for stream in &mut params.chainsync.grpc {
        stream.duplication_delay = std::time::Duration::from_secs(3600);
    }
    params.engine.blockstore.blocktime = std::time::Duration::from_millis(40);
    params.engine.blockstore.superblock = 128;
    params.engine.blockstore.checkpoint = runtime::CHECKPOINT;
    params.engine.ledger.size_limit = i64::MAX as u64;
}
