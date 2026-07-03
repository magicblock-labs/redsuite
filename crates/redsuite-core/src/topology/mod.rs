//! Topology harness: spawn / kill / restart the base L1 (`solana-test-validator`)
//! and the ER (`magicblock-validator`) on dynamically allocated ports, with a
//! readiness gate, killed on `Drop`. Net-new — the black-box replacement for
//! test-integration's `test-tools/validator.rs`.

use crate::{
    context::{BaseCtx, ErCtx},
    Result,
};

pub struct Topology {}

impl Topology {
    pub async fn up() -> Result<(Self, BaseCtx, ErCtx)> {
        todo!()
    }
}
