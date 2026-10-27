pub use rand::Rng;
use rand::{rngs::StdRng, SeedableRng};
use sha2::{Digest, Sha256};

pub fn rng(seed: u64, keys: &[&str], index: u64) -> StdRng {
    let mut hash = Sha256::new();
    hash.update(b"redsuite.random.v1");
    hash.update(seed.to_le_bytes());
    for key in keys {
        hash.update((key.len() as u64).to_le_bytes());
        hash.update(key.as_bytes());
    }
    hash.update(index.to_le_bytes());
    StdRng::from_seed(hash.finalize().into())
}

pub fn keypair(rng: &mut impl Rng) -> keypair::Keypair {
    let mut secret = [0u8; 32];
    rng.fill(&mut secret);
    keypair::Keypair::new_from_array(secret)
}
