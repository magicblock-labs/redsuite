use pubkey::Pubkey;
use sha2::{Digest, Sha256};
use solana_program::program_error::ProgramError;
use wincode::{SchemaRead, SchemaWrite};

#[derive(Clone, Copy, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct CellId {
    pub authority: Pubkey,
    pub index: u64,
}

impl CellId {
    pub const SEED: &'static [u8] = b"cell";

    pub const fn new(authority: Pubkey, index: u64) -> Self {
        Self { authority, index }
    }

    pub fn pda(self, program: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[Self::SEED, self.authority.as_ref(), &self.index.to_le_bytes()],
            program,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u64")]
pub enum UndelegationOutcome {
    Accept,
    Reject,
}

/// Fixed Wincode header followed by arbitrary workload bytes.
#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct CellHeader {
    pub id: CellId,
    pub value: u64,
    pub revision: u64,
    pub undelegation: UndelegationOutcome,
}

impl CellHeader {
    pub const SIZE: usize = size_of::<Pubkey>() + 4 * size_of::<u64>();
    pub const VALUE_OFFSET: usize = size_of::<Pubkey>() + size_of::<u64>();

    pub fn new(id: CellId) -> Self {
        Self {
            id,
            value: 0,
            revision: 0,
            undelegation: UndelegationOutcome::Accept,
        }
    }

    pub fn try_decode(data: &[u8]) -> wincode::ReadResult<Self> {
        wincode::deserialize(data)
    }

    pub fn write(&self, data: &mut [u8]) -> Result<(), ProgramError> {
        let header = data.get_mut(..Self::SIZE).ok_or(ProgramError::AccountDataTooSmall)?;
        wincode::serialize_into(header, self).map_err(|_| ProgramError::InvalidAccountData)
    }

    pub fn written_id(data: &[u8]) -> Option<u64> {
        Self::try_decode(data).ok().map(|cell| cell.value)
    }

    pub fn payload(data: &[u8]) -> Result<&[u8], ProgramError> {
        data.get(Self::SIZE..).ok_or(ProgramError::AccountDataTooSmall)
    }

    pub fn payload_mut(data: &mut [u8]) -> Result<&mut [u8], ProgramError> {
        data.get_mut(Self::SIZE..).ok_or(ProgramError::AccountDataTooSmall)
    }

    pub fn fill_payload(data: &mut [u8], seed: u64) -> Result<(), ProgramError> {
        Self::fill(Self::payload_mut(data)?, seed, 1);
        Ok(())
    }

    pub(crate) fn fill(payload: &mut [u8], seed: u64, step: u64) {
        for (index, chunk) in payload.chunks_mut(size_of::<u64>()).enumerate() {
            let word = seed.wrapping_add((index as u64).wrapping_mul(step)).to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }
}

#[derive(SchemaRead, SchemaWrite)]
pub struct HashState {
    pub hash: [u8; 32],
}

impl HashState {
    pub const SIZE: usize = 32;

    pub fn chain(mut hash: [u8; 32], rounds: u32) -> [u8; 32] {
        for round in 0..rounds {
            let mut hasher = Sha256::new();
            hasher.update(hash);
            hasher.update(round.to_le_bytes());
            hash.copy_from_slice(&hasher.finalize());
        }
        hash
    }

    pub fn fold(salt: u64, hashes: &[[u8; 32]], rounds: u32) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(salt.to_le_bytes());
        for hash in hashes {
            hasher.update(hash);
        }
        Self::chain(hasher.finalize().into(), rounds)
    }
}
