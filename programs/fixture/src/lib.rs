//! Shared cells, instruction composition and execution for RedSuite fixtures.

use instruction::{AccountMeta, Instruction as SolanaInstruction};
use pubkey::Pubkey;
use wincode::{SchemaRead, SchemaWrite};

mod build;
pub mod execute;
pub mod magic;
mod state;

pub use build::{Batch, Commit, IntentBundle, Program};
pub use state::{CellHeader, CellId, HashState, UndelegationOutcome};

pub mod layout {
    use crate::{CellHeader, HashState};
    pub const DATA_OFFSET: usize = CellHeader::SIZE;
    pub const ID_OFFSET: usize = CellHeader::VALUE_OFFSET;
    pub const ID_SIZE: usize = size_of::<u64>();
    pub const HASH_OFFSET: usize = DATA_OFFSET;
    pub const HASH_SIZE: usize = HashState::SIZE;
}

#[derive(SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum Instruction {
    Create {
        id: CellId,
        space: u32,
    },
    Delegate {
        id: CellId,
        validator: Option<Pubkey>,
        commit_frequency_ms: u32,
    },
    Resize {
        space: u32,
    },
    Execute {
        nonce: u64,
        operations: Vec<Operation>,
    },
    Action {
        nonce: u64,
        guard: ActionGuard,
        operations: Vec<Operation>,
    },
    // Magic appends its externally encoded response after this discriminator.
    HandleResult,
}

impl Instruction {
    pub fn into_data(self) -> Vec<u8> {
        wincode::serialize(&self).expect("fixture instruction serialization")
    }
}

#[derive(Clone, Copy, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum SettlementMode {
    Commit,
    Undelegate,
}

#[derive(SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum Operation {
    Set {
        cells: Vec<u16>,
        value: u64,
    },
    Add {
        cells: Vec<u16>,
        change: i64,
    },
    Fill {
        cells: Vec<u16>,
        seed: u64,
        step: u64,
    },
    Hash {
        cells: Vec<u16>,
        initial: [u8; 32],
        rounds: u32,
    },
    FoldHashes {
        cells: Vec<u16>,
        salt: u64,
        rounds: u32,
    },
    Copy {
        sources: Vec<u16>,
        destinations: Vec<u16>,
    },
    SumSizes {
        output: u16,
        inputs: Vec<u16>,
    },
    Inspect {
        accounts: Vec<u16>,
        hash: bool,
    },
    FillBytes {
        account: u16,
        byte: u8,
    },
    Undelegation {
        cells: Vec<u16>,
        outcome: UndelegationOutcome,
    },
    Invoke {
        program: u16,
        accounts: Vec<IndexedMeta>,
        data: Vec<u8>,
        signers: Vec<CellId>,
    },
    Settle(Settlement),
    TransferFromEscrow {
        destination: u16,
        system: u16,
        amount: u64,
    },
    Log(String),
    Fail(u32),
}

#[derive(SchemaRead, SchemaWrite)]
pub struct IndexedMeta {
    pub index: u16,
    pub signer: bool,
    pub writable: bool,
}

#[derive(SchemaRead, SchemaWrite)]
pub struct Settlement {
    pub payer: u16,
    pub context: u16,
    pub magic: u16,
    pub vault: Option<u16>,
    pub intents: Vec<Intent>,
    pub actions: Vec<Action>,
}

#[derive(SchemaRead, SchemaWrite)]
pub struct Intent<Cell = u16> {
    pub mode: SettlementMode,
    pub cells: Vec<Cell>,
    pub after_commit: Vec<Action>,
    pub after_undelegate: Vec<Action>,
}

/// A deferred invocation. Its accounts need not be resident on the ER.
#[derive(SchemaRead, SchemaWrite)]
pub struct Action {
    pub authority: Pubkey,
    pub instruction: SolanaInstruction,
    pub compute_units: u32,
    pub escrow_index: u8,
    pub callback: Option<ResultCallback>,
}

#[derive(SchemaRead, SchemaWrite)]
pub struct ActionGuard {
    pub cell: Pubkey,
    pub owner: Pubkey,
}

/// Records delivery on a cell; a failed action can additionally refund a deposit.
#[derive(SchemaRead, SchemaWrite)]
pub struct ResultCallback {
    pub cell: Pubkey,
    pub refund: Option<Refund>,
    pub compute_units: u32,
}

#[derive(SchemaRead, SchemaWrite)]
pub struct Refund {
    pub recipient: Pubkey,
    pub amount: u64,
}

impl IndexedMeta {
    pub fn resolve(&self, key: Pubkey) -> AccountMeta {
        AccountMeta {
            pubkey: key,
            is_signer: self.signer,
            is_writable: self.writable,
        }
    }
}
