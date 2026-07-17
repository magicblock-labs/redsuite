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
    xtask/                  cargo xtask automation (SBF builds, stack control, reports)

## Writing a scenario

Scenarios live in the family libraries under `<family>/src/scenarios/
<subsystem>/<name>.rs`: one `Scenario` impl each — the harness owns process
spawning, ports, funding and teardown. See
`redshift/src/scenarios/harness/example.rs` — a small working scenario
meant to be copied as the starting point.

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

The performance tests, one file each under `redline/src/scenarios/<subsystem>/`.
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
- `high_cu` — stresses execution: the same rate of transactions, once with a
  trivial amount of work per transaction and once with a heavy one (a hash
  computed over and over, close to the compute limit). Reading the final hash
  back off the accounts proves the validator really did the work.

scheduler:

- `hot_account_cliff` — sends a fixed transaction rate at fewer and fewer
  accounts (32 down to 4) so they all fight over the same account locks.
- `executor_saturation` — pre-signs a large pile of transactions and fires
  them all at once, spread over many independent accounts and several copies
  of the test program.

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

### What the test client measures

| Metric                  | What it measures                                                                                                               | If it grows, it means                                                                                   |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `delivery us`           | How long you wait for the validator to confirm it received txn. No execution yet — just acceptance.                            | The validator is struggling to receive traffic. The problem is at the entrance, before any real work.   |
| `signature latency us`  | How long from sending until the validator confirms your transaction actually ran.                                              | The work itself got slower - scheduling, waiting for account locks, or execution.                       |
| `account-update lag us` | How long until someone watching an account over websocket sees your change.                                                    | Something in the pipeline slowed down                                                                   |
| `sync round-trip us`    | Cost of one isolated transaction: send it, wait for confirmation, only then send next one.                                     | The true per-transaction cost went up                                                                   |
| `fanout lag us`         | One account changes while many clients watch it. Times how long until each watcher gets the notification.                      | Broadcasting one update to many listeners doesn't scale — the more watchers, the longer they all wait.  |
| `churn op us`           | How long one subscribe, unsubscribe, or connection close takes.                                                                | Managing subscriptions itself became slow                                                               |
| `cold first-touch us`   | Reading an account the validator has never seen — it must first fetch it from the base chain.                                  | Fetching accounts from the base chain got slower.                                                       |
| `warm repeat us`        | Reading that same account again. Should be fast and stay flat — it's the control.                                              | The cache is broken. If only cold reads slow down, cloning is the problem; if warm ones do, caching is. |
| `read latency us`       | Read speed while we force the account cache to constantly throw accounts out and re-fetch them.                                | Each evict-and-refetch cycle costs more than it used to.                                                |
| `commit round-trip us`  | How long from "save this state to the base chain" until it's actually confirmed there.                                         | The commit pipeline is slowing down                                                                     |
| `er delivery us`        | Just the rollup's commit time.                                                                                                 | Tells you rollup side takes longer for commit                                                           |
| `achieved rps`          | How many transactions the client managed to send each second (`sendTransaction` calls), counted second by second over the run. | A low average means you hit a ceiling; big swings mean throughput is unstable.                          |

Everything is a distribution, not an average — problems show up in the
p95/max tail long before the average moves.

### One-number verdicts per run

| Metric                                              | What it measures                                                                                  | If it's off, it means                                                                                       |
|-----------------------------------------------------|---------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| `achieved tps` vs `offered tps`                     | We *tried* to send at some rate — did we actually manage to?                                      | A gap means something saturated — and every other result was measured under less load than the test claims. |
| `base txs per commit`                               | How many base-chain transactions one commit costs.                                                | Commits got more expensive — worse batching, or lookup tables aren't being reused.                          |
| `fresh drain intents/s` vs `reused drain intents/s` | Commit speed for brand-new accounts (setup needed) vs already-committed ones (setup exists).      | These are two different code paths — this tells you which one regressed.                                    |
| `cliff at ws conns`                                 | At how many websocket connections update delivery fell over.                                      | The capacity edge moved closer. 0 means we never hit an edge — good, or the test didn't push far enough.    |
| `thrash p50 slowdown x`                             | How many times slower a typical read gets once the account cache starts thrashing.                | Going over the cache limit hurts more than it used to.                                                      |
| `heavy/light validator avg ratio`                   | Did compute-heavy transactions take proportionally longer inside the validator than trivial ones? | A ratio near 1.0 proves the validator *didn't actually do the work*.                                        |
| `writes produced` vs `received total`               | We caused N updates — did N notifications actually arrive?                                        | Any shortfall is the validator silently dropping messages                                                   |
| `incomplete conn-account pairs`                     | How many watchers missed at least one update.                                                     | How widespread the dropping is.                                                                             |
| `missing final states`                              | How many watchers never got the *last* update — they're stuck believing stale state forever.      | Clients that are permanently wrong, not just briefly behind.                                                |
| `received per-conn min/max`                         | The best- and worst-served connection.                                                            | A big spread means drops starve specific connections instead of spreading evenly.                           |
| `baseline fds` / `fds after churn`                  | Open file handles before vs after churning thousands of connections. They must match.             | A handle leak                                                                                               |
| `baseline rss kb` / `rss after churn kb`            | Same check for memory.                                                                            | A memory leak.                                                                                              |
| `validator cores`                                   | How much CPU the validator actually used during the test.                                         | Low CPU *at* a throughput ceiling means the bottleneck is a lock or a serial stage                          |
| `top thread cores`, `busy threads`                  | How the work spread across threads.                                                               | One thread maxed out while the rest sit idle: a single-threaded bottleneck.                                 |

### What the validator reports about itself (scraped from `/metrics`, counted over the test window)

| Metric                                                            | What it measures                                                                                                                   | If it moves the wrong way, it means…                                                                                                                         |
|-------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mbv_transaction_count`, `mbv_failed_transactions_count`          | The validator's own count of transactions it ran, and how many failed.                                                             | If it didn't move at all during a load test, the test measured nothing — the run is thrown out as INVALID.                                                   |
| `mbv_transaction_processing_time`                                 | The validator timing its own execution of each transaction.                                                                        | If this stays flat while the client's numbers grow, time is being lost *waiting in queues between stages* — the most useful disagreement in the whole suite. |
| `mbv_ensure_accounts_time`                                        | How long transactions stood at the door waiting for their accounts to be fetched first.                                            | Account fetching is stalling admission — one missing account holds up whole transactions.                                                                    |
| `mbv_monitored_accounts_gauge`                                    | How many accounts the validator is currently tracking.                                                                             | Used as proof: the oversubscription test really did push past the configured limit.                                                                          |
| `mbv_evicted_accounts_count` vs `mbv_account_fetches_found_count` | Accounts thrown out of the cache vs accounts fetched back in                                                                       | If they drift apart, accounts are getting lost or fetched twice.                                                                                             |
| `mbv_max_lock_contention_queue_size`                              | The deepest pile-up of transactions waiting behind one popular account's lock.                                                     | Traffic on hot accounts is serializing harder.                                                                                                               |
| `mbv_committor_intents_count`, `_failed_intents_count`            | How many commits the validator processed and how many failed.                                                                      | Zero during a commit test invalidates the run.                                                                                                               |
| `mbv_committor_intent_backlog_count`                              | How many commits are waiting in line.                                                                                              | Commits are being created faster than the workers can process them.                                                                                          |
| `mbv_committor_executors_busy_count`                              | How many of the commit workers are busy.                                                                                           | All busy *and* the line growing: the pipeline is  saturated                                                                                                  |
| `mbv_committor_intent_alt_count`, `_alt_preparation_time`         | How many address lookup tables each commit needs, and how long building them takes (each build waits on base-chain confirmations). | The expensive setup step of committing new accounts got even more expensive.                                                                                 |


## Base-chain programs & accounts

- dlp and mdp are cloned from a live cluster
- the ER identity and the dlp admin are fresh keypairs generated
- `magicblock_committor_program.so` is taken from the ER binary's own build
