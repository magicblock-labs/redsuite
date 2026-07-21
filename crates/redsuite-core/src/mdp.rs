//! Hand-built magic-domain-program instructions and the ErRecord byte layout.

use instruction::{AccountMeta, Instruction};
use pubkey::Pubkey;

use crate::{system::system_id, topology::MDP_ID};

pub const STATUS_ACTIVE: u8 = 0;
pub const STATUS_DRAINING: u8 = 1;
pub const STATUS_OFFLINE: u8 = 2;

const RECORD_VERSION_V0: u8 = 0;
const IX_REGISTER: u8 = 0;
const IX_UNREGISTER: u8 = 1;
const IX_SYNC: u8 = 2;
const SYNC_VERSION_V0: u8 = 0;
const BORSH_OPTION_SOME: u8 = 1;

pub fn mdp_id() -> Pubkey {
    MDP_ID.parse().expect("mdp program id")
}

pub fn record_pda(identity: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"er-record", identity.as_ref()], &mdp_id())
        .0
}

#[derive(Clone)]
pub struct DomainRecord {
    pub identity: Pubkey,
    pub status: u8,
    pub block_time_ms: u16,
    pub base_fee: u16,
    pub features: [u8; 32],
    pub load_average: u32,
    pub country_code: [u8; 3],
    pub addr: String,
}

impl DomainRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![RECORD_VERSION_V0];
        bytes.extend_from_slice(self.identity.as_ref());
        bytes.push(self.status);
        bytes.extend_from_slice(&self.block_time_ms.to_le_bytes());
        bytes.extend_from_slice(&self.base_fee.to_le_bytes());
        bytes.extend_from_slice(&self.features);
        bytes.extend_from_slice(&self.load_average.to_le_bytes());
        bytes.extend_from_slice(&self.country_code);
        bytes.extend_from_slice(&(self.addr.len() as u32).to_le_bytes());
        bytes.extend_from_slice(self.addr.as_bytes());
        bytes
    }
}

fn record_instruction(identity: &Pubkey, data: Vec<u8>) -> Instruction {
    Instruction {
        program_id: mdp_id(),
        accounts: vec![
            AccountMeta::new(*identity, true),
            AccountMeta::new(record_pda(identity), false),
            AccountMeta::new_readonly(system_id(), false),
        ],
        data,
    }
}

pub fn register(record: &DomainRecord) -> Instruction {
    let mut data = vec![IX_REGISTER];
    data.extend_from_slice(&record.encode());
    record_instruction(&record.identity, data)
}

pub fn sync(record: &DomainRecord) -> Instruction {
    let mut data = vec![IX_SYNC, SYNC_VERSION_V0];
    data.extend_from_slice(record.identity.as_ref());
    data.push(BORSH_OPTION_SOME);
    data.push(record.status);
    data.push(BORSH_OPTION_SOME);
    data.extend_from_slice(&record.block_time_ms.to_le_bytes());
    data.push(BORSH_OPTION_SOME);
    data.extend_from_slice(&record.base_fee.to_le_bytes());
    data.push(BORSH_OPTION_SOME);
    data.extend_from_slice(&record.features);
    data.push(BORSH_OPTION_SOME);
    data.extend_from_slice(&record.load_average.to_le_bytes());
    data.push(BORSH_OPTION_SOME);
    data.extend_from_slice(&record.country_code);
    data.push(BORSH_OPTION_SOME);
    data.extend_from_slice(&(record.addr.len() as u32).to_le_bytes());
    data.extend_from_slice(record.addr.as_bytes());
    record_instruction(&record.identity, data)
}

pub fn unregister(identity: &Pubkey) -> Instruction {
    let mut data = vec![IX_UNREGISTER];
    data.extend_from_slice(identity.as_ref());
    record_instruction(identity, data)
}
