use solana_program::declare_id;

declare_id!("3JnJ727jWEmPVU8qfXwtH63sCNDX7nMgsLbg8qy8aaPX");

pub use sdk::{
    consts::DELEGATION_PROGRAM_ID,
    delegate_args::{DelegateAccountMetas, DelegateAccounts},
};

pub mod layout {
    pub const OWNER_PUBKEY_SIZE: usize = 32;
    pub const DATA_OFFSET: usize = OWNER_PUBKEY_SIZE;
    pub const ID_OFFSET: usize = DATA_OFFSET;
    pub const ID_SIZE: usize = 8;
    pub const HASH_OFFSET: usize = ID_OFFSET + ID_SIZE;
    pub const HASH_SIZE: usize = 32;
}

pub mod instruction;
pub mod utils;
