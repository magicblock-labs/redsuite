use pubkey::Pubkey;

pub use fixture::{layout, Batch, Program};
pub use sdk::consts::DELEGATION_PROGRAM_ID;

pub const ID: Pubkey = Pubkey::from_str_const("3JnJ727jWEmPVU8qfXwtH63sCNDX7nMgsLbg8qy8aaPX");
pub const fn id() -> Pubkey {
    ID
}
pub const PROGRAM: Program = Program::at(ID);
