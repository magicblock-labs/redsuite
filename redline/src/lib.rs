pub mod scenarios;
pub use redline_interface as program;
pub use redsuite_core::redline::written_id as account_update_id;

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
