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
    cargo nextest run         # run everything

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
<subsystem>/<name>.rs`: one `Scenario` impl each, exported from the
subsystem's `mod.rs`. The harness owns process spawning, ports, funding and
teardown. See `redshift/src/scenarios/harness/example.rs`.

Registration is one declaration: an entry in the catalog
(`cli/src/catalog.rs`, one `scenario_catalog!` block per family). The entry
names the scenario's short name, its runner type, and its metadata
(topology, resources, fixtures, and optionally profiles). From that one
entry the macro generates both ways to run the scenario: a `#[tokio::test]`
function (so it runs under `cargo nextest`, named
`catalog::<family>::<short_name>`) and a catalog record the `redsuite`
binary dispatches from. Unit tests check the catalog against the `Scenario`
impls and against the nextest groups, so a wrong or missing entry fails
before anything boots.

`profiles` defaults to every profile; write it only to narrow. `redsuite
run` reads it before booting anything and skips a scenario whose profile
set excludes the requested profile.

## The redsuite binary

The whole suite also builds into one executable, which is what CI and
benchmark hosts use — no cargo, no checkout of the tests:

    cargo build --release -p redsuite

    redsuite list                             # every scenario
    redsuite run redline/high_cu              # one scenario (short names work too)
    redsuite run redline --profile full       # a whole family
    redsuite run all                          # everything (redline last, alone)
    redsuite stack status                     # ports, pids, health
    redsuite stack down                       # stop the shared stack
    redsuite report compare                   # diff the latest run against its nearest baseline

It still needs `solana-test-validator` on PATH and the ER binary under test
(`MAGICBLOCK_VALIDATOR_BIN`), and it reads the built programs from the
workspace. Set `REDSUITE_ROOT` when it runs outside a checkout.

`run all` uses three lanes: every shared-stack scenario at once, at most two
private-ER scenarios beside them, then the redline family last and alone.
Benchmarks must never share the box, so keep that last lane exclusive.

## Running

    cargo nextest run commit_roundtrip               # one scenario
    cargo nextest run -E 'test(catalog::redline::)'  # one family
    cargo nextest run                                # everything

Use cargo-nextest. The concurrency limits live in `.config/nextest.toml`:
private-ER scenarios run two at a time, and the redline family runs alone.
`cargo test` ignores those limits, so it can start a benchmark next to a
neighbour that spoils it. Run one suite invocation at a time either way.

All scenarios share **one boot-once stack**: the first test to need it boots
base + ER on dynamically allocated ports and leaves them running; every later
test — in the same run or the next — health-checks and reuses them. A dead or
unhealthy stack is killed and rebooted transparently. Coordination lives in
`target/redsuite-stack/` (`state.json`, `identity-pool.json`, a cross-process
flock, `genesis-accounts/`, logs, ledgers).

    cargo xtask stack status    # ports, pids, health, log locations
    cargo xtask stack down      # stop the stack and clear its state

Scenario isolation comes from fresh keypairs, not fresh chains.
Scenarios that kill a validator, restart one, or need their own config boot
private ERs. `task_scheduler`, `config_gates`, `aml_gate`, and
`ledger_retention` run on one instead of the shared ER; `restart_under_load`,
`ws_conn_capacity`, `clone_lru_churn`, `cold_hydration_tail`,
`ensure_gate_stall`, `storage_prodsize_sustain`, and
`superblock_boundary_latency` boot theirs beside the shared stack. Each takes
its own identity from a 32-slot pool minted at genesis, so private ERs never
collide with the shared one or each other.

The harness needs two binaries:

- base — `solana-test-validator` on PATH;
- ER — `$MAGICBLOCK_VALIDATOR_BIN`, else `magicblock-validator` on PATH.

## Environment

| variable | what |
|---|---|
| `MAGICBLOCK_VALIDATOR_BIN` | the ER binary under test; else `magicblock-validator` on PATH |
| `REDSUITE_ROOT` | workspace root, when the `redsuite` binary runs outside a checkout |
| `REDSUITE_CLONE_URL` | where a cold boot clones base programs from; defaults to mainnet-beta |
| `REDSUITE_PROFILE` | scenario profile: `lite` (default), `full`, `soak`, or `deep` |
| `REDSUITE_LOOP` | S1 loop mode: `open` (default) or `closed` |

A cold boot clones its base programs from `REDSUITE_CLONE_URL`, so the first
boot needs that endpoint. A warm stack does not, and neither does a rerun.

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
  them all at once, one payer per delegated account so no two in-flight
  transactions share a writable key; reports the engine's busy-executor
  gauge and ordering-dependency count alongside throughput.
- `mixed_sustained_load` — holds a fixed transaction rate over many payer
  lanes while cycling the workload every 20 slots: two slots of high-CU
  sha256 work, nine of simple writes, nine of read-write transactions. The
  validator must keep up without drops or failures and drain the backlog.
- `conflict_ordering` — proves conflicting transactions execute in their
  accepted order regardless of executor timing. Account pairs receive
  repeated `X(A) -> Y(A,B) -> Z(B)` chains, each step folding the accounts'
  current hashes into a new one, while high-CU work on disjoint accounts
  keeps every executor busy: first with a bounded number of unconfirmed
  transactions (a deep ready queue), then open loop (the sequencer's
  pending-work bound engaged). After each batch the pipeline must drain to
  zero blocked, busy and pending work, and every pair must hold exactly the
  fold of its accepted order — any reordering changes every later digest.
  Needs a host whose engine runs at least two executors.

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
- `superblock_boundary_latency` — boots a private ER with short superblocks,
  fills its ledger, then offers a steady rate and compares the latency of
  requests that land on a superblock seal against the ones that do not.

lifecycle (restart / shutdown):

- `restart_under_load` — runs twice on fresh private validators, once
  stopped with SIGTERM and once with SIGKILL, never resetting storage. Lanes
  write to one account each and track every transaction; at termination new
  work stops and each transaction is confirmed, failed, or unresolved. After
  the relaunch every unresolved signature is resolved, every confirmed one
  must still be in the ledger, and each account must hold the id of its last
  confirmed write. Load then resumes until another superblock seals and drains
  cleanly, and a second restart must preserve that state. Times both restarts.

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


## redshift scenarios

The correctness tests, one file each under `redshift/src/scenarios/<subsystem>/`.
Ported from the validator repo's test-integration suites.

chainlink (account cloning):

- `clone_on_access` — writes an account on base and reads it on the ER: same
  bytes, same owner, same balance, and later update on a base is cloned on ER. A missing
  account is read as missing, and an ER write to a delegated account stays off
  base until a commit.
- `account_info_semantics` — query ER for an account that has never
  existed, for ordinary funded wallets, and for an escrow payer, first
  one account at a time and then in mixed batches. Every balance has to match
  the base chain exactly, a top-up on base has to show through on the next ER
  read, and an account that does not exist has to be considered as missing.
- `parallel_cloning` — funds ten wallets on base and then reads all ten for
  the first time at once, through five overlapping requests. The validator is
  cloning ten accounts it has never seen.
- `multi_program_clone` — sends one transaction that calls two programs the ER
  has never seen. The validator has to fetch both program accounts and both of
  their program-data accounts together before it can execute anything
- `loader_matrix` — exercises all four BPF loaders in one run: memo v1 and
  memo v2 cloned from mainnet with their original loaders intact, the redshift
  fixture preloaded as an upgradeable v3 program, and a fourth copy that the
  scenario deploys from scratch, calls, and then upgrades to a second build.
- `escrow_cloning` — creates an escrowed payer, lets the ER clone the escrow
  once, then tops that escrow up on base. Delegated escrow belongs to the ER, and base-side changes to it 
  are not reflected on ER.
- `post_delegation_token_transfer` — attaches an SPL transfer to a delegation,
  so the validator runs that transfer the moment the account lands on
  the ER. It also checks the projection that makes this work.
- `aml_gate` — screens the owner of an incoming token account against a risk
  API before the tokens are merged. An owner scored above the threshold never
  gets a merge: no transaction goes near the destination, and the account is
  handed back undelegated.

committor (ER → base commits):

- `commit_roundtrip` — writes two delegated accounts on the ER, commits one
  and checks it lands on base byte-for-byte while the other doesn't move.
  Then commits and undelegates both — the owning program gets its accounts
  back with the exact final bytes.
- `commits` — commits one delegated account, then two of them together, and
  checks that the receipt names exactly the accounts that were committed and
  that a single base transaction carried them. Then it reaches for an account
  delegated to a *different* validator: this ER owns neither the state nor
  the right to release it, so committing it and undelegating it must both be
  rejected.
- `commit_and_undelegate` — walks the whole round trip and then back again:
  commit, undelegate, write to the account on base so that the program owns
  it once more, and delegate it a second time. Along the way it pushes a 10 KB
  order book through the same pipeline to check that large state is preserved
  byte-for-byte, and it files one undelegation so the owning program refuses
  it. The account then keeps its committed state but stays delegated, which is
  the failure mode worth knowing about.
- `claim_fees` — funds the validator's own fee vault on base and then claims
  it, signed by the validator itself. The lamports land on the identity and the
  vault is left sitting at its rent-exempt floor.
- `table_mania` — drives the base chain's address lookup table program, the
  one the committor uses when a commit outgrows a single transaction.
  Creates a table, extends it to the 256-key cap, decodes the account and
  requires the stored keys to match what was put in, then refuses the key that
  would overflow it and deactivates the table.

harness:

- `api_invariants` — reads each transaction's block timestamp three
  different ways and checks all three agree, every time. Also registers,
  updates and removes a validator record in the domain registry.
- `config_gates` — boots two private ERs: one whose allow list contains
  a single program, and one with no list at all. The restricted validator has
  to clone the program it was instructed. Neither of them may create a lookup table on base,
  at startup or while cloning.
- `example` — the template from *Writing a scenario*.

scheduler:

- `task_scheduler` — schedules tasks on a private ER and watches the
  crank work through them: a short repeating task that finishes and clears its
  row, one cancelled halfway through, one rescheduled by its owner, one that
  fails until its retries run out, and one that a second authority tries to
  steal. The counters on the ER and the task database on disk both have to
  tell the same story.

pubsub (websocket subscriptions):

- `pubsub_contracts` — subscribes to accounts, logs, programs, signatures
  and slots, then makes transfers and checks every promised notification
  arrives with the right content. After unsubscribing, sends more transfers
  and checks nothing arrives.

storage (ledger retention):

- `ledger_retention` — boots a private ER with 40-slot superblocks and a
  one-byte ledger size limit, so retention purges the oldest sealed
  superblock at every check. Sends counter adds across superblocks, and at
  each of at least three purges confirms only the oldest history vanished:
  pruned transactions and their blocks return null, retained ones stay
  queryable, getSignaturesForAddress lists exactly the retained set, and the
  counter holds every add. Then restarts the ER in place without reset and
  checks the same history and state survive.

## redhat scenarios

The security tests, one file each under `redhat/src/scenarios/<subsystem>/`.
Each one performs an attack and requires the platform to refuse it.

chainlink:

- `illegal_writable` — tries four ways to commit accounts the caller has no
  claim to: straight from a wallet, the same call buried between two innocent
  transfers, and two variants routed through a second program that calls the
  real owner first to look legitimate. Every one has to fail, the validator has
  to say why in its logs, and the accounts have to be exactly as they were on
  base once the dust settles.

aperture:

- `fee_payer_rules` — takes the fee rule from both sides. A delegated payer is
  allowed to commit itself, and that commit has to reach base. A payer that is
  not delegated may not pay ER fees at all, so its transaction is turned
  away before anything runs.

## Base-chain programs & accounts

- dlp, mdp, SPL Token, both ATA programs and memo v1/v2 are cloned from
  `REDSUITE_CLONE_URL` when the base chain starts. dlp keeps its real mainnet
  upgrade authority, so no local key is its admin, and dlp calls that need the
  admin cannot be tested.
- ER identities are random keypairs
- `magicblock_committor_program.so` is taken from the ER binary's own build.
