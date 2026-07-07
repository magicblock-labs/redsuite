# base-accounts

Pinned genesis account fixtures, loaded into every base L1 via `--account`.
Copied from the magicblock-validator repo (master @ `79e7be7f`, 2026-07-06).

The shared stack boots the ER with the well-known MagicBlock test identity
`mAGicPQYBMvcYveUZA5F5UNNwyHvfYh5xkLS2Fr1mev` (keypair embedded in
`redsuite-core::topology`). The vaults below are dlp PDAs derived from that
identity. This is load-bearing: dlp's `InitMagicFeeVault` requires the
per-validator fees vault to already exist and be dlp-owned, so an ER with a
fresh random identity exits at the fee-vault startup gate — pre-seeding
these fixtures is the sanctioned path.

| file | pubkey | owner | role |
|---|---|---|---|
| validator-authority.json | `mAGicPQ…1mev` | system | ER identity, 20 SOL |
| validator-fees-vault.json | `EpJnX7…bvDX` | dlp | per-validator fees vault |
| protocol-fees-vault.json | `7Jrkjm…AgXg` | dlp | protocol fees vault |
| magic-fee-vault.json | `8wdZfg…rAFp` | dlp | magic fee vault |
| magic-fee-vault-delegation-record.json | `AKBsnB…tdkH` | dlp | its delegation record |
