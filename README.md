# RedSuite

Black-box test harness for the MagicBlock validator. Scenarios drive a live
base L1 + ephemeral rollup purely through the public surface — transactions,
JSON-RPC, WebSocket and the Prometheus `/metrics` endpoint — and emit a
performance / correctness / security report that can be diffed
release-over-release.

RedSuite is built on top of [redline](https://github.com/magicblock-labs/redline),
the MagicBlock validator load-testing tool: redline's engine (transport pools,
rate limiting, confirmation tracking, streaming stats, account prep) is being
extracted into `redsuite-core`, topped with a net-new topology harness and a
scenario/assertion layer.

## Quick start

One-time setup:

1. rustup — the toolchain is pinned by `rust-toolchain.toml`.
2. The Solana CLI on PATH — provides `solana-test-validator` (the base L1)
   and `cargo-build-sbf` (SBF builds).
3. A source build of the validator under test, pointed at via env:

       git clone https://github.com/magicblock-labs/magicblock-validator
       cargo build --release --manifest-path magicblock-validator/Cargo.toml
       export MAGICBLOCK_VALIDATOR_BIN=$PWD/magicblock-validator/target/release/magicblock-validator

   The committor program is picked up from that same build tree
   (`target/deploy/magicblock_committor_program.so`).

Then, in this repo:

    cargo xtask programs      # build the family SBF programs
    cargo test --workspace    # or: cargo nextest run

The first test boots the base + ER; every other test — and every later run —
reuses them. `cargo xtask stack down` stops the stack.

## Layout

    crates/redsuite-core/   the engine: topology, contexts, transport, prep, scenario, stats
    redline/                performance scenarios (load)
    redshift/               correctness scenarios (observed state vs expected model)
    redhat/                 security scenarios (adversarial, must-be-rejected)
    cli/                    the `redsuite` binary — run scenarios without cargo
    programs/               on-chain SBF programs, one per family
    base-programs/          pinned third-party base-L1 programs (dlp, mdp) + provenance manifest
    base-accounts/          pinned genesis account fixtures (test identity + dlp fee vaults)
    xtask/                  cargo xtask automation (SBF builds, base-program refresh, stack control)

## Writing a scenario

Scenarios live in the family libraries under `<family>/src/scenarios/
<subsystem>/<name>.rs`: one `Scenario` impl each — the harness owns process
spawning, ports, funding and teardown. See
`redshift/src/scenarios/committor/commit_roundtrip.rs`.

Each scenario is reachable two ways, and a new one needs both:

- a test shim in `<family>/tests/<subsystem>/<name>.rs` (four lines, calling
  `run_scenario`) plus its `[[test]]` entry in the family `Cargo.toml`, so it
  runs under `cargo test` / `cargo nextest`;
- an entry in the registry in `cli/src/main.rs`, so it runs under the
  `redsuite` binary.

## The redsuite binary

The whole suite also builds into one executable, which is what CI and
benchmark hosts use — no cargo, no checkout of the tests:

    cargo build --release -p redsuite

    redsuite list                             # every scenario
    redsuite run redline/high_cu              # one scenario (short names work too)
    redsuite run redline --profile full       # a whole family
    redsuite run all                          # everything, sequentially
    redsuite stack status                     # ports, pids, health
    redsuite stack down                       # stop the shared stack
    redsuite report compare                   # diff the latest two runs

It still needs `solana-test-validator` on PATH and the ER binary under test
(`MAGICBLOCK_VALIDATOR_BIN`), and it reads the fixtures and built programs from
the workspace — set `REDSUITE_ROOT` when running it outside a checkout.

## Running

    cargo nextest run -p redshift --test commit_roundtrip   # one scenario
    cargo nextest run -p redline                            # one family
    cargo nextest run                                       # everything

(plain `cargo test` works identically; cargo-nextest is not required)

All scenarios share **one boot-once stack**: the first test to need it boots
base + ER on dynamically allocated ports and leaves them running; every later
test — in the same run or the next — health-checks and reuses them. A dead or
unhealthy stack is killed and rebooted transparently. Coordination lives in
`target/redsuite-stack/` (`state.json`, a cross-process flock, logs, ledgers).

    cargo xtask stack status    # ports, pids, health, log locations
    cargo xtask stack down      # stop the stack and clear its state

Scenario isolation comes from fresh keypairs, not fresh chains — state left
behind by earlier runs is invisible to new scenarios. Scenarios that must
kill/restart validators (ledger-restore) will get private topologies and must
not run against the shared stack.

The harness needs two binaries:

- base — `solana-test-validator` on PATH;
- ER — `$MAGICBLOCK_VALIDATOR_BIN`, else `magicblock-validator` on PATH.

## redline scenarios

The performance tests, one file each under `redline/tests/<subsystem>/`.
`REDSUITE_PROFILE=lite` (default) is a quick local run, `full` produces the
real numbers.

aperture (RPC / websocket ingress):

- `simple_load` — smoke test for the whole pipeline: create and delegate a
  few accounts, send some writes to the ER, check the data actually changed
  and that account updates arrive over websocket.
- `rpc_warm_ingress` — sends transactions at a steady rate and measures how
  long acceptance, confirmation and the websocket account update take. This
  is the baseline.
- `ws_fanout_threshold` — opens an increasing number of websocket
  connections, all subscribed to the same accounts, while writes are
  running. Every connection must receive every update.
- `ws_conn_capacity` — opens thousands of websocket connections, subscribes,
  unsubscribes and closes them, watching the validator's file descriptors
  and memory the whole time. Anything left behind is a leak.
- `rpc_capacity_blast` — fires transactions as fast as the client can push
  for a few seconds and records how many per second the validator accepted.
- `high_cu` — meant to stress execution with compute-heavy transactions

scheduler:

- `hot_account_cliff` — sends a fixed transaction rate at fewer and fewer
  accounts (32 down to 4) so they all fight over the same account locks.

chainlink (account cloning / subscriptions):

- `clone_lru_churn` — makes the validator track more accounts than its
  configured cap and keeps reading random ones. Every miss forces an evict +
  refetch cycle; the test checks evictions and refetches match one-to-one
  and measures how fast that churn can go.
- `ensure_gate_stall` — same oversubscription (working set 8x
  the cap) with multi-account transactions: each transaction waits until all
  its accounts are present.
- `cold_hydration_tail` — how expensive is the first-ever read of an account
  versus reading it again, and how much slower a burst of transactions gets
  when they all wait on the same missing dependencies.

committor (ER → base commits):

- `commit_width_envelope` — commits 1, 2 and 4 accounts back to the base
  chain and records what each input costs: round-trip latency and number of
  base transactions.
- `commit_throughput_ceiling` — schedules 150 wide commits over
  never-committed accounts, faster than the committor can process them.

storage:

- `storage_prodsize_sustain` — sustained load against a validator with
  production-sized storage settings, measured as two equal back-to-back
  windows.

harness:

- `protocol_boundary_selftest` — tests the test harness itself. Runs the
  same load with one thread and with eight: if the single-threaded
  run reports huge latency while the validator-side numbers stay flat, the
  slowness was our client, not the validator — and must never be reported
  as a validator regression.

## Base-chain programs & accounts

Every stack starts the base L1 with the delegation program (dlp), the domain
registry (mdp), the committor program and all built family programs, plus the
genesis account fixtures. Sourced by ownership:

- `base-programs/` — pinned third-party artifacts (`dlp.so`, `mdp.so`);
  provenance in `base-programs/MANIFEST.toml`. Refresh with `cargo xtask
  refresh-base-programs <name>` (source build at the pinned rev) or
  `… <name> --from-chain <url>` (dump the deployed bytes); `cargo xtask
  check-base-programs` verifies integrity.
- `base-accounts/` — the well-known test identity and the dlp fee-vault PDAs
  derived from it.
- `target/deploy/` — our family programs: `cargo xtask programs` (build-sbf).
- the validator build under test — `magicblock_committor_program.so` is
  version-coupled to the ER binary and taken from the ER binary's own build
  tree, never vendored.
