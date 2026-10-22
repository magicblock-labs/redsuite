use std::{fs::File, io, path::Path, sync::OnceLock};

pub use rand::Rng;
use rand::{rngs::StdRng, SeedableRng};
use sha2::{Digest, Sha256};

use crate::{catalog::Fixture, topology};

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

pub fn revisions() -> &'static str {
    static REVISIONS: OnceLock<String> = OnceLock::new();
    REVISIONS.get_or_init(|| {
        [
            ("redsuite", std::env::current_exe().ok()),
            ("validator", topology::er_bin_path().ok()),
        ]
        .into_iter()
        .chain(
            Fixture::ALL
                .map(|fixture| (fixture.so_name(), fixture.resolve().ok())),
        )
        .map(|(name, path)| {
            let revision = path
                .and_then(|path| content_hash(&path))
                .unwrap_or_else(|| "unavailable".into());
            format!("{name}={revision}")
        })
        .collect::<Vec<_>>()
        .join(", ")
    })
}

fn content_hash(path: &Path) -> Option<String> {
    let mut digest = Sha256::new();
    io::copy(&mut File::open(path).ok()?, &mut digest).ok()?;
    Some(format!("{:x}", digest.finalize()))
}
