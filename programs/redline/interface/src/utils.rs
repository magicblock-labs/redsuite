use pubkey::Pubkey;
use sha2::{Digest, Sha256};

pub fn hash_chain(mut hash: [u8; 32], iters: u32) -> [u8; 32] {
    for round in 0..iters {
        let mut hasher = Sha256::new();
        hasher.update(hash);
        hasher.update(round.to_le_bytes());
        hash.copy_from_slice(&hasher.finalize());
    }
    hash
}

pub fn fold_hash(id: u64, hashes: &[[u8; 32]], iters: u32) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(id.to_le_bytes());
    for hash in hashes {
        hasher.update(hash);
    }
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&hasher.finalize());
    hash_chain(digest, iters)
}

pub fn derive_pda(
    program_id: &Pubkey,
    base: Pubkey,
    space: u32,
    seed: u8,
    authority: Pubkey,
) -> (Pubkey, u8) {
    let seed = pda_seed(space, seed, &authority);
    Pubkey::find_program_address(&[base.as_ref(), &seed], program_id)
}

pub fn pda_seed(space: u32, seed: u8, authority: &Pubkey) -> Vec<u8> {
    let mut bytes = space.to_le_bytes().to_vec();
    bytes.push(seed);
    bytes.extend_from_slice(&authority.as_ref()[..16]);
    bytes
}
