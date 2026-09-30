pub mod scenarios;
pub use redline_interface as program;

pub const ACCOUNT_SPACE: u32 = 256;

pub mod metrics {
    pub const ENGINE_TRANSACTIONS: &str = "engine_ledger_transactions";
    pub const RPC_HANDLED_TRANSACTIONS: &str =
        "mbv_transaction_processing_time_count";
    pub const RPC_ACCEPTED_TRANSACTIONS: &str =
        "mbv_transaction_skip_preflight_count";
    pub const FAILED_TRANSACTIONS: &str =
        "engine_processor_failed_transactions";
}

pub fn account_update_id(data: &[u8]) -> Option<u64> {
    use program::layout::{ID_OFFSET, ID_SIZE};
    let bytes = data.get(ID_OFFSET..ID_OFFSET + ID_SIZE)?;
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}
