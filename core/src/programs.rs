//! Fixture APIs shared by scenarios. Encoding and account order belong here.

pub use fixture::magic::{Task, MAGIC};
pub use fixture::{
    layout, Action, ActionGuard, Batch, CellHeader, CellId, Commit, HashState,
    Instruction as FixtureInstruction, IntentBundle, Refund, ResultCallback, SettlementMode,
    UndelegationOutcome,
};
pub use redhat_interface::{Program as Redhat, PROGRAM as REDHAT};
pub use redline_interface::{Program as Redline, PROGRAM as REDLINE};
pub use redshift_interface::{Program as Redshift, PROGRAM as REDSHIFT};
pub use upgrade_probe::Program as UpgradeProbe;

pub fn content_hash(bytes: &[u8]) -> hash::Hash {
    use sha2::{Digest, Sha256};
    hash::Hash::new_from_array(Sha256::digest(bytes).into())
}
