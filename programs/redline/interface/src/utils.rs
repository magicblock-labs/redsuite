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
    let mut seeds = space.to_le_bytes().to_vec();
    seeds.push(seed);
    seeds.extend_from_slice(&authority.as_ref()[..16]);
    let seeds = &[base.as_ref(), &seeds];
    Pubkey::find_program_address(seeds, program_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_chain_is_deterministic_and_iteration_sensitive() {
        let init = [7u8; 32];
        assert_eq!(hash_chain(init, 0), init);
        assert_eq!(hash_chain(init, 32), hash_chain(init, 32));
        assert_ne!(hash_chain(init, 32), hash_chain(init, 31));
        assert_ne!(hash_chain([8u8; 32], 32), hash_chain(init, 32));
    }

    #[test]
    fn fold_hash_pins_id_input_order_and_rounds() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        assert_eq!(fold_hash(7, &[a, b], 0), fold_hash(7, &[a, b], 0));
        assert_ne!(fold_hash(7, &[a, b], 0), fold_hash(8, &[a, b], 0));
        assert_ne!(fold_hash(7, &[a, b], 0), fold_hash(7, &[b, a], 0));
        assert_ne!(fold_hash(7, &[a], 0), fold_hash(7, &[a, b], 0));
        assert_ne!(fold_hash(7, &[a, b], 0), fold_hash(7, &[a, b], 1));

        let x_then_y = fold_hash(2, &[fold_hash(1, &[a], 0), b], 0);
        let y_then_x = fold_hash(1, &[fold_hash(2, &[a, b], 0)], 0);
        assert_ne!(x_then_y, y_then_x);
    }

    #[test]
    fn derivation_is_deterministic_and_parameter_sensitive() {
        let program = crate::ID;
        let base = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (pda, bump) = derive_pda(&program, base, 128, 1, authority);
        assert_eq!(derive_pda(&program, base, 128, 1, authority), (pda, bump));
        assert!(!pda.is_on_curve());

        assert_ne!(derive_pda(&program, base, 256, 1, authority).0, pda);
        assert_ne!(derive_pda(&program, base, 128, 2, authority).0, pda);
        assert_ne!(
            derive_pda(&program, Pubkey::new_unique(), 128, 1, authority).0,
            pda
        );
        assert_ne!(
            derive_pda(&Pubkey::new_unique(), base, 128, 1, authority).0,
            pda
        );
    }
}
