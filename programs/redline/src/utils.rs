use pubkey::Pubkey;

pub fn derive_pda(
    base: Pubkey,
    space: u32,
    seed: u8,
    authority: Pubkey,
) -> (Pubkey, u8) {
    let mut seeds = space.to_le_bytes().to_vec();
    seeds.push(seed);
    seeds.extend_from_slice(&authority.as_ref()[..16]);
    let seeds = &[base.as_ref(), &seeds];
    Pubkey::find_program_address(seeds, &crate::ID)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derivation_is_deterministic_and_parameter_sensitive() {
        let base = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (pda, bump) = derive_pda(base, 128, 1, authority);
        assert_eq!(derive_pda(base, 128, 1, authority), (pda, bump));
        assert!(!pda.is_on_curve());

        assert_ne!(derive_pda(base, 256, 1, authority).0, pda);
        assert_ne!(derive_pda(base, 128, 2, authority).0, pda);
        assert_ne!(derive_pda(Pubkey::new_unique(), 128, 1, authority).0, pda);
    }
}
