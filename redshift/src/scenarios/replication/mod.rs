pub mod replicated_runtime;
pub mod replication_promotion;
pub mod replication_recovery;
mod runtime;
pub mod snapshot_bootstrap;

pub(super) fn settings(params: &mut magicblock_config::LeaderParams) {
    // Keep account fixtures on the HTTP proxy; WS supplies subsequent base updates.
    params
        .remotes
        .retain(|remote| !matches!(remote, magicblock_config::types::Remote::Grpc(..)));
    params.engine.blockstore.blocktime = std::time::Duration::from_millis(40);
    params.engine.blockstore.superblock = 128;
    params.engine.blockstore.checkpoint = runtime::CHECKPOINT;
    params.engine.ledger.size_limit = i64::MAX as u64;
}
