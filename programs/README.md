# Scenario programs

These small Solana programs are fixtures for RedSuite scenarios, not production
contracts. They provide controlled workloads and failure modes for checking the
validator's execution, cloning, delegation and settlement behavior.

| Program | Purpose |
|---|---|
| Redline | Cell workloads: reads, writes, copying, hashing and settlement. |
| Redshift | Cell lifecycle, scheduled execution, settlement actions and callbacks. |
| Redhat | CPI composition for ownership and privilege-boundary checks. |
| Upgrade probe | A small two-version program for upgrade visibility, CPI writes and rollback checks. |

## Structure

The three programs share a cell model and a typed instruction set in `fixture`.
Cells have a stable identity, value and revision, followed by an optional workload
payload. Ordered batches compose state operations, CPI calls and settlement
without introducing an implicit CPI boundary. Tasks and settlement actions reuse
these batches instead of dedicated scenario instructions.

Scenarios discover builders through the harness's
[central program APIs](../crates/redsuite-core/src/programs.rs), which own encoding,
account order and PDA derivation. Each family interface supplies its program
identity; the same executor also works at dynamically deployed addresses.
Fixture-owned messages use Wincode. External calls and callbacks retain the
formats required by their protocols.

The upgrade probe keeps its single payload alongside its implementation. It is
built twice; the upgrade scenario deploys both versions under the same address.

## Building and running

Run `cargo xtask programs` to build and stage all five SBF artifacts. Redline,
Redshift and Redhat load when the base stack starts; upgrade probes are deployed
by their scenario.

Host interfaces and fixtures must be rebuilt together after schema changes.
Stop an existing stack with `cargo xtask stack down` before running the rebuilt
fixtures. Fixture ABIs are internal to RedSuite and are not versioned for
backward compatibility.
