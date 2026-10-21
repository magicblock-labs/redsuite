use pubkey::Pubkey;

pub use fixture::{Batch, Program};

pub const ID: Pubkey = Pubkey::from_str_const("BTczL2chGpVHw25pbmMtkFAD1t7rxoa8pVbaUjsybjiq");
pub const fn id() -> Pubkey {
    ID
}
pub const PROGRAM: Program = Program::at(ID);
