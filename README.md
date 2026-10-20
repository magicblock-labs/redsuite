<h1 align="center">RedSuite</h1>

<p align="center">
  <b>Performance, correctness, and adversarial testing for MagicBlock Validator.</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Rust-test_harness-orange?logo=rust" alt="Rust test harness">
  <img src="https://img.shields.io/badge/platform-Linux_%7C_macOS-blue" alt="Linux and macOS">
  <img src="https://img.shields.io/badge/testing-black--box-6d4bd6" alt="Black-box testing">
</p>

<p align="center">
  <a href="#-what-you-can-validate">Capabilities</a> ·
  <a href="#-get-started">Get started</a> ·
  <a href="#-results-and-diagnostics">Results</a>
</p>

---

RedSuite tests [MagicBlock Validator](https://github.com/magicblock-labs/magicblock-validator)
against a live local Solana base chain and Ephemeral Rollups (ERs). It submits
transactions, queries JSON-RPC, follows WebSocket updates, and reads Prometheus
metrics to check externally observable behavior.

Use it to validate validator changes, reproduce failures across the base-chain
and rollup boundary, or measure performance under controlled workloads. One
runner manages the local processes, fixtures, funding, and cleanup for all three
test families.

## ✨ What you can validate

| Family | Purpose | Result |
| :-- | :-- | :-- |
| **Redline** | Measure latency, throughput, capacity, and resource use under load. | Benchmark reports with workload settings and measurements. |
| **Redshift** | Check observed state and behavior against expected outcomes. | Pass/fail verdicts with diagnostics. |
| **Redhat** | Exercise ownership, authorization, and fee rules with adversarial transactions. | Required rejections and state-integrity checks. |

The families cover complementary parts of the validator's behavior:

- **Execution and client APIs:** transaction admission, simulation, retries,
  conflicting workloads, RPC responses, and live notification delivery.
- **Account lifecycle and settlement:** cloning and freshness, delegation,
  commits, undelegation, program upgrades, and sponsored ephemeral accounts.
- **Recovery and replication:** restarts, interrupted settlement, checkpoint
  durability, retained history, and leader/verifier consistency.
- **Application services:** recurring tasks and base-chain actions with callbacks.
- **Performance under pressure:** compute-heavy traffic, hot accounts, cache
  churn, subscription fanout, storage growth, and restart behavior under load.

Redhat checks specific adversarial behaviors; it is not a comprehensive security
assessment of the validator.

## 🧩 How it works

**Scenario → public APIs → local base chain and rollups → observations and checks**

The harness boots the base chain on demand, loads the required programs, and
allocates ports and funded identities. Scenarios use real transactions and
client-facing interfaces rather than calling validator internals. Client-side
observations establish behavior; validator metrics provide additional context.

### Managed execution and isolation

The runner reuses compatible rollups within a run. Scenarios declare whether
that access can be shared, must be exclusive, or needs a separate topology.
Fresh keys isolate scenario-owned accounts; checks involving global balances or
process-wide changes require exclusive access. Scenarios can also manage a
leader with independently restartable verifiers.

Compatible functional checks run concurrently. Benchmarks run alone, after the
functional checks in a normal run. The runner stops the rollups it owns and
replaces failed or unhealthy group rollups before subsequent members use them.
The base chain normally remains available for later runs.

### Controlled failures, real traffic

Fault scenarios can delay, drop, reject, or disconnect the rollup's base-chain
HTTP and WebSocket traffic. The proxies forward real traffic and do not invent
account contents or slots. A separate base-chain client lets the scenario check
what actually executed, and a requested interception must fire for its check
to succeed. Proxy faults are cleared between scenarios.

## 🚀 Get started

RedSuite runs on **Linux and macOS**. Install Rust through `rustup` and
Agave 4.2.2, with `solana-test-validator` and `cargo-build-sbf` on your `PATH`.
Use this Agave version with Engine 0.14.3; Agave 4.1.1's RPC size limit rejects
larger native v1 commits. Each checkout uses its pinned Rust toolchain.

### Prepare the validator under test

In a [MagicBlock Validator checkout](https://github.com/magicblock-labs/magicblock-validator),
build the leader, verifier, and committor program:

```bash
cargo build --release --locked -p magicblock-validator -p magicblock-verifier
cargo build-sbf --manifest-path programs/committor-program/Cargo.toml \
  --sbf-out-dir target/deploy
export MAGICBLOCK_VALIDATOR_BIN="$PWD/target/release/magicblock-validator"
```

The verifier is needed for replication checks and is discovered beside the
leader binary. The harness loads `magicblock_committor_program.so` from that
build tree's `target/deploy`; a host build alone does not produce it.

On macOS, the validator's open-file limit requires these settings before a run:

```bash
sudo launchctl limit maxfiles 1000000 1000000  # once per boot
ulimit -n 1000000                             # in the shell running RedSuite
```

### Build and run RedSuite

In the RedSuite checkout, using the same shell:

```bash
cargo xtask programs
cargo build --release --locked -p redsuite
export PATH="$PWD/target/release:$PATH"

redsuite list
redsuite run redshift
```

The first base-chain boot needs access to an RPC endpoint to clone its programs;
this defaults to mainnet-beta. Later runs reuse the local base. Stop the stack
when finished with `redsuite stack down`.

## 🛠️ Choose a run

| Task | Command |
| :-- | :-- |
| Discover available scenarios | `redsuite list` |
| Preview a selection | `redsuite list redshift/rpc_` |
| Check correctness or adversarial behavior | `redsuite run redshift` or `redsuite run redhat` |
| Measure performance | `redsuite run redline` |
| Use the larger benchmark workloads | `redsuite run redline --profile full` |
| Run the complete suite | `redsuite run all` |
| Reduce concurrent storage use | `redsuite run all --serial` |
| Inspect processes, endpoints, and logs | `redsuite stack status` |

Selections accept a family, a full or short scenario name, a name prefix, or
`all`. An exact name takes precedence over prefix matches. Use `redsuite list`
as the current scenario catalog.

Redline defaults to the shorter `lite` workloads. `--profile full` changes only
Redline workloads, and the flag requires a selection that includes Redline.
Correctness and adversarial checks use fixed workloads.

### Host and storage considerations

Run **one suite invocation at a time**. Keep the benchmark host free of other
workloads; the runner isolates its own benchmarks, not unrelated host activity.
For comparisons, keep the hardware, validator build settings, and workload
profile consistent.

Validators preallocate storage in multi-gigabyte steps, so parallel runs need
space for several rollups. `--serial` runs scenarios one at a time and removes
each group's storage before moving on. Scenarios with multiple validators still
need space for their topology.

Serial runs finish with `stack down` by default. Add `--keep-storage` to skip
that final teardown; it does not preserve rollups already cleaned up during the
run. **`stack down` stops the base and leftover rollups and deletes their
storage, including separately located accounts databases; logs remain.** The
harness also reclaims private rollups left behind by failed scenarios.

## 📊 Results and diagnostics

Redshift and Redhat return verdicts in the console without persisting benchmark
reports. Failed scenarios include diagnostic details; set `RUST_LOG=debug`
to include phase and workload progress. `RUST_LOG` also applies to launched validators. A failed run
returns a nonzero exit status.

Redline writes reports under `target/redsuite-reports/`. Reports include workload
settings, measurements, and process-launch details. Runs containing Redline
also record suite wall time and execution-lane durations. Concurrent lane times
overlap, so they should not be added together as elapsed time.

Read the measurements together:

- **Latency and delivery:** transaction latency is client-observed from one
  timestamp taken immediately before the `sendTransaction` HTTP request, after
  signing, blockhash refresh and subscription setup. Delivery is that request's
  round trip, including Reqwest connection pooling, transport and response
  processing; signature confirmation and account-update arrival start from the
  same instant. Account reads and WebSocket churn start immediately before
  their respective requests. None of these is validator-only processing time,
  so read them next to the validator's own processing metrics in the same report.
- **Client scheduling:** `scheduling delay us` is the gap between the pacer
  admitting a transaction and its task starting, reported apart from request
  latency. A scheduling delay p95 above the pacing interval, or above the
  achieved inter-arrival interval for unlimited loads, marks the run
  `client saturated`: its latency is a client limit. A run that delivered
  under 90 % of its offered rate is marked `offered rate not sustained`,
  which validator backpressure can also cause, so read it next to the
  validator processing metrics in the same report. The BMF export withholds
  client latency for both. Reports using these boundaries carry a
  `latency origin` setting: `client, immediately before sendTransaction` for
  transaction loads and `client, immediately before the request` for account
  reads and WebSocket churn, listing both when one report mixes them. Older
  reports without this setting keep their legacy series and should be compared
  separately.
- **Load and capacity:** compare offered and achieved traffic before interpreting
  latency at a claimed load; use fanout and contention workloads to explore limits.
- **Resources and validator metrics:** CPU, memory, file descriptors, and
  Prometheus observations help explain changes alongside client-side results.

Export the latest report campaign's supported latency and throughput measurements
in Bencher Metric Format:

```bash
redsuite report bmf --out redsuite-bmf.json
```

The exporter excludes failed and incomplete runs. It withholds client latency
for `client saturated` or `offered rate not sustained` runs while retaining
diagnostic measurements. Client latency series have timing-origin suffixes
(`@send`, `@request`) to keep them separate from legacy series.

## ⚙️ Configure the environment

| Variable | Use |
| :-- | :-- |
| `MAGICBLOCK_VALIDATOR_BIN` | Select the validator under test; otherwise discover `magicblock-validator` on `PATH`. |
| `MAGICBLOCK_VERIFIER_BIN` | Select a verifier; otherwise discover it beside the leader binary, then on `PATH`. |
| `REDSUITE_ROOT` | Set the resource root when running outside the original checkout. |
| `REDSUITE_STACK_DIR` | Place ledgers, stack state, and logs on another disk; defaults to `target/redsuite-stack` under the resource root. |
| `REDSUITE_ACCOUNTSDB_DIR` | Place rollup and verifier accounts databases separately from their ledgers. |
| `REDSUITE_CLONE_URL` | Choose the RPC endpoint used to acquire base-chain programs on a cold boot. |
| `REDSUITE_PROFILE` | Set the default Redline workload to `lite` or `full`; `--profile` overrides it. |
| `RUST_LOG` | Log filter for RedSuite and launched validators; defaults to `info`. Use `debug` for passing-run details. |

The compiled runner works without Cargo or a source checkout at runtime, but it
still needs the validator binaries, `solana-test-validator`, and staged fixture
artifacts and their manifest under the resource root's `target/deploy`. Preserve
the committor artifact's location relative to the validator binary when moving a
build to another host.

### Optional gRPC coverage

The base chain attempts to load a compatible Yellowstone Geyser plugin. Missing
or incompatible plugins fail only checks that require the gRPC feed; other
checks can run without it.

Automatic acquisition uses upstream binaries for **x86_64 Linux**. On other
hosts, set `REDSUITE_YELLOWSTONE_PLUGIN` to a compatible locally built plugin.
`REDSUITE_YELLOWSTONE_DIR` controls the download cache. Rollups receive the feed
only when their scenario explicitly requests it.

## 🦀 Extend the suite

Add a scenario to the family that owns its outcome. The harness provides process
management, clients, funding, and fixtures; the scenario defines its workload
and checks. Correctness and adversarial scenarios return `Result<()>`;
benchmarks return a `ScenarioReport`.

Start with the [example scenario](redshift/src/scenarios/harness/example.rs),
then register the scenario in the [catalog](cli/src/catalog.rs) with its
fixtures and execution contract. Declare shared, exclusive, or fresh execution
according to the state and topology the check needs. The CLI runs scenarios;
Cargo tests do not run the suite.

For on-chain fixture instructions and compatibility contracts, see the
[fixture reference](programs/README.md). After changing fixtures, rebuild with
`cargo xtask programs` and stop the old stack so the next base boot loads them.
