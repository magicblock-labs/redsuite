use pubkey::Pubkey;

pub use fixture::{Batch, Program};

pub const ID: Pubkey = Pubkey::from_str_const("AijneHkXJVVWyimuwfSJdrJktARZu2WiMaZBqHsq7CS5");
pub const fn id() -> Pubkey {
    ID
}
pub const PROGRAM: Program = Program::at(ID);
