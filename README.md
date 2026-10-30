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
| **Redline** | Measure latency, throughput, and resource use under sustained load. | Benchmark reports with workload settings and measurements. |
| **Redshift** | Check observed state and behavior against expected outcomes. | Pass/fail verdicts with diagnostics. |
| **Redhat** | Exercise ownership, authorization, and fee rules with adversarial transactions. | Required rejections and state-integrity checks. |

The families cover complementary parts of the validator's behavior:

- **Execution and client APIs:** transaction admission, simulation, retries,
  conflicting workloads, RPC responses, and live notification delivery.
- **Account lifecycle and settlement:** cloning and freshness, delegation,
  commits, undelegation, program upgrades, and sponsored ephemeral accounts.
- **Recovery and replication:** restarts, interrupted settlement, checkpoint
  recovery, retained history, and leader/verifier recovery.
- **Application services:** recurring tasks and base-chain actions with callbacks.
- **Performance under pressure:** compute-heavy traffic, conflicting workloads,
  subscription fanout, connection churn, and sustained execution with storage observations.

Redhat checks specific adversarial behaviors; it is not a comprehensive security
assessment of the validator.

## 🧩 How it works

**Scenario → public APIs → base chain and ER → observations and checks**

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

Compatible functional checks run concurrently. Benchmarks run sequentially.
Group rollups are reused for the run without automatic recovery after failures.
The runner stops the rollups it owns when their lane finishes.
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

redsuite redshift list
redsuite redshift run
```

The first base-chain boot needs access to an RPC endpoint to clone its programs;
this defaults to mainnet-beta. Later runs reuse the local base. Stop the stack
when finished with `redsuite stack down`.

## 🛠️ Choose a run

| Task | Command |
| :-- | :-- |
| Discover a family's scenarios | `redsuite redshift list` |
| Preview a selection | `redsuite redshift list rpc_` |
| Run correctness checks (Redshift) | `redsuite redshift run` |
| Run security checks (Redhat) | `redsuite redhat run` |
| Measure performance (Redline) | `redsuite redline run --duration 1m` |
| Use the larger benchmark workloads | `redsuite redline run --profile full --duration 5m` |
| Generate an hour of mixed load | `redsuite redline run mixed_sustained_load --profile full --duration 1h` |
| Soak mixed scenarios on one ER | `redsuite continuous --duration 1m` |
| Reduce concurrent storage use | `redsuite redshift run --serial` |
| Stop the stack after a run | `redsuite redshift run --cleanup` |
| Inspect processes, endpoints, and logs | `redsuite stack status` |

Selections accept full or short scenario names and name prefixes within the
chosen family. Multiple selections run together without repeating scenarios;
an exact name takes precedence over prefix matches. Omit selections to run the
whole family. Use the family's `list` command to preview selections.

Redline defaults to the shorter `lite` workloads. Only `redline run` accepts
`--profile` or reads `REDSUITE_PROFILE`. Correctness and
adversarial checks use fixed workloads. Redline benchmarks run sequentially;
checks can use `--fresh-concurrency` to bound concurrent fresh scenarios.

`redshift run` and `redhat run` accept `--seed <U64>` to set the run seed,
for example `redsuite redhat run --seed 12345`. When omitted, a random seed
is generated. The chosen seed is logged at `info` before stack startup.

Redline requires `--duration` for each selected workload; there is no iteration
limit. Setup and warmup are outside measurement. At expiry or Ctrl+C, admission
stops, admitted work drains, and workload-specific state checks run. Ctrl+C
returns 130 and marks reports interrupted. Performance is reported, not judged
against machine-wide thresholds or saved baselines. Redline assertions, including
state checks, only log warnings; use Bencher metrics to catch regressions.
Operational errors, such as failed RPC calls and confirmation timeouts, still
fail the workload.

`redsuite continuous` repeatedly runs the Redshift, Redhat, and Redline scenarios
that use the default ER, all on one ER, until a scenario fails or Ctrl+C. Each
round shuffles the scenarios and runs shared ones concurrently, bounded by
`--shared-concurrency`. An exclusive scenario waits for running scenarios to
finish, runs alone, and then shared scenarios resume. Scenarios that need another
ER configuration or a fresh ER are skipped. The first failure stops admission and
returns a nonzero status after running scenarios finish. Continuous runs do not
write Redline reports, because concurrent load makes their measurements
incomparable.

Each round logs its order and a seed derived from the run seed. Pass the logged
run seed to `--seed` on a fresh stack (`redsuite stack down`) to replay the same
orders; a stack rejects seeded fixtures it has already created. Scenario timing
and interleaving are not reproduced.

The active workloads are transaction load (write/copy presets), compute load,
scheduler contention, sustained mixed load, WebSocket fanout, and connection
churn. `load_generator_thread_scaling` is an explicit-only diagnostic.
Periodic logs retain interval observations without keeping every request in memory.
The [coverage limits](docs/scenario-gaps.md) distinguish existing proofs from
deferred scenarios and oracle improvements.

For a live devnet-backed ER, supply the matching base/ER HTTP and WebSocket
endpoints, the ER metrics endpoint, and a funded setup keypair:

```sh
redsuite redline run transaction_load_copy --duration 5m \
  --base-rpc https://api.devnet.solana.com --base-ws wss://api.devnet.solana.com \
  --er-rpc http://localhost:8899 --er-ws ws://localhost:8900 \
  --metrics http://localhost:9000 --payer /path/to/setup-keypair.json
```

Required programs must already be deployed. RedSuite creates fresh run accounts
before timing; it does not provision, reconfigure, restart, or stop supplied
services. Omit ER endpoints to launch a managed ER; omit all environment options
to use a managed local stack. Host FD/RSS observations are available only for
locally owned processes.

### Host and storage considerations

Run **one suite invocation at a time**. Keep the benchmark host free of other
workloads; the runner isolates its own benchmarks, not unrelated host activity.
For comparisons, keep the hardware, validator build settings, and workload
profile consistent.

Validators preallocate storage in multi-gigabyte steps, so parallel runs need
space for several rollups. `--serial` runs scenarios one at a time, releasing a
group's storage when the next scenario cannot reuse it. Scenarios with multiple
validators still need space for their topology.

Scheduling and cleanup are independent: `--serial` does not stop the shared
base. Add `--cleanup` to a managed run to finish with `stack down`;
this does not preserve rollups already cleaned up during the run.
**`stack down` stops the base and leftover rollups and deletes their
storage, including separately located accounts databases; logs remain.** The
harness also reclaims private rollups left behind by failed scenarios.

## 📊 Results and diagnostics

Redshift and Redhat return verdicts in the console without persisting benchmark
reports. Stage errors are logged with method context; set `RUST_LOG=debug`
to include workload progress. Logging defaults
to `info`; `RUST_LOG` also applies to launched validators. A failed run
returns a nonzero exit status.

Redline writes reports under `target/redsuite-reports/`. Reports include workload
settings, measurements, and process-launch details. Benchmark runs also record
suite wall time and execution-lane durations. Continuous-load reports separate
setup, generation, request drain, and validator drain time.

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
Each invocation writes one `campaign.json` under `target/redsuite-reports`,
including cells recorded before a scenario finishes. The `latest` file selects
the campaign to export; older report layouts are not read.

## ⚙️ Configure the environment

| Variable | Use |
| :-- | :-- |
| `MAGICBLOCK_VALIDATOR_BIN` | Select the validator under test; otherwise discover `magicblock-validator` on `PATH`. |
| `MAGICBLOCK_VERIFIER_BIN` | Select a verifier; otherwise discover it beside the leader binary, then on `PATH`. |
| `REDSUITE_ROOT` | Set the resource root when running outside the original checkout. |
| `REDSUITE_STACK_DIR` | Place ledgers, stack state, and logs on another disk; defaults to `target/redsuite-stack` under the resource root. |
| `REDSUITE_ACCOUNTSDB_DIR` | Place rollup and verifier accounts databases separately from their ledgers. |
| `REDSUITE_CLONE_URL` | Choose the RPC endpoint used to acquire base-chain programs on a cold boot. |
| `REDSUITE_PROFILE` | Set the Redline run/load preset to `lite` or `full`; `--profile` overrides it. Other commands do not read it. |
| `RUST_LOG` | Log filter for RedSuite and launched validators; defaults to `info`. Use `debug` for passing-run details. |

The compiled runner works without Cargo or a source checkout at runtime, but it
still needs the validator binaries, `solana-test-validator`, and staged fixture
artifacts under the resource root's `target/deploy`. Preserve the committor
artifact's location relative to the validator binary when moving a build to
another host.

### gRPC setup

The base chain requires a compatible Yellowstone Geyser plugin. Plugin setup or
startup failures stop the run; the harness does not retry without gRPC.

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
then register the scenario in its family's catalog with its
fixtures and execution contract. Declare shared, exclusive, or fresh execution
according to the state and topology the check needs. The CLI runs scenarios;
Cargo tests do not run the suite.

For on-chain fixture instructions and compatibility contracts, see the
[fixture reference](programs/README.md). After changing fixtures, rebuild with
`cargo xtask programs` and stop the old stack so the next base boot loads them.
