//! Account prep on the base chain: airdrop / `InitAccount` / `Delegate`.
//! Ported from redline's `assist/prepare.rs` in the engine-extraction step —
//! init + delegate MUST run on the base; the ER discovers delegation by
//! clone-on-access.
