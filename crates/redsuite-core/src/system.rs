//! Hand-built system-program instructions.

use instruction::{AccountMeta, Instruction};
use pubkey::Pubkey;

pub fn system_id() -> Pubkey {
    "11111111111111111111111111111111"
        .parse()
        .expect("system program id")
}

pub fn create_account(
    from: &Pubkey,
    to: &Pubkey,
    lamports: u64,
    space: u64,
    owner: &Pubkey,
) -> Instruction {
    let mut data = 0u32.to_le_bytes().to_vec();
    data.extend_from_slice(&lamports.to_le_bytes());
    data.extend_from_slice(&space.to_le_bytes());
    data.extend_from_slice(owner.as_ref());
    Instruction {
        program_id: system_id(),
        accounts: vec![
            AccountMeta::new(*from, true),
            AccountMeta::new(*to, true),
        ],
        data,
    }
}

pub fn transfer(from: &Pubkey, to: &Pubkey, lamports: u64) -> Instruction {
    let mut data = 2u32.to_le_bytes().to_vec();
    data.extend_from_slice(&lamports.to_le_bytes());
    Instruction {
        program_id: system_id(),
        accounts: vec![
            AccountMeta::new(*from, true),
            AccountMeta::new(*to, false),
        ],
        data,
    }
}

pub fn assign(account: &Pubkey, owner: &Pubkey) -> Instruction {
    let mut data = 1u32.to_le_bytes().to_vec();
    data.extend_from_slice(owner.as_ref());
    Instruction {
        program_id: system_id(),
        accounts: vec![AccountMeta::new(*account, true)],
        data,
    }
}
