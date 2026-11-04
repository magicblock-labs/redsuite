# Scenario coverage limits

A passing run establishes only the behavior its scenarios observe. The gaps
below require stronger observations or clarified contracts; they are not
covered by performance measurements or successful process startup.

| Area | Not established by the current checks |
| --- | --- |
| Commit recovery | Some action/duplicate-rejection branches accept weaker receipt evidence than a successful truthful receipt. |
| Replay after restart | A blockhash checked before restart is not proven still valid afterward. |
| Activation contention | Joining contenders does not establish that each entered the intended race. |
| Crash recovery | A missing indexed transaction is not proof its effect was unapplied; a wholly dropped confirmed lane can evade the existing suffix oracle. |
| Replication | Final causal verification uses the leader; seal progress is not direct follower-state comparison. Ordinary replay does not cover forged streams, storage corruption, duplicate-work identity or materialization leases. |
| Resize/read overlap | Background readers and final lengths do not establish that reads overlapped relocation or snapshot boundaries. |
| Scheduler cancellation | Canceling a fast task does not establish it executed first or that cancellation stopped an executing task. |
| Signature unsubscribe | The RPC lifecycle performs the requests but does not prove notification suppression after signature unsubscribe. |
| Commit-size probes | Alternative rejection paths or global-counter fallbacks are not definitive address-lookup-table or size-limit regression proofs. |
| Account-query bound | Oversized account queries must be refused; acceptance is not evidence of supported leniency. |
| External timing | Live base-chain fetch and settlement latency are excluded from active Redline measurements; controlled replacements remain deferred. |

Performance observations alone are not a no-regression verdict, and a progress
timeout is not a diagnosis of deadlock.

## Public outcome signals

The Redhat scenarios for [#261](https://github.com/magicblock-labs/redsuite/issues/261)
use the seed, full account-image checks, live controls, and pass/fail rules from
[#235](https://github.com/magicblock-labs/redsuite/issues/235). Each scenario can
be selected by its short name with `redsuite redhat run <name> --seed 42`.

| Scenario | Trigger and required observation |
| --- | --- |
| `outcome_signals` | Witness a cold-account preparation refusal and an exact execution error. Check RPC attribution, signature/log/failure diagnostics, full account images, and ledger inclusion or absence. Missing expected diagnostics fail with their channel and signature; failed executions missing from logs remain validator#1889. A skip-preflight invalid-blockhash drop may stay silent, as in Agave; `transaction_admission` checks that it never lands. |
| `divergence_signals` | Change a caught-up verifier's persisted cell payload. Require a new named integrity diagnostic or a named fail-stop, then restore the payload and rejoin a clean follower. Quiet continuation fails. |
| `slow_subscriber_signals` | Stop reading an independently subscribed socket while fast account/log/signature readers verify committed writes. Lost updates require a named limit on the client connection; the eviction counter only confirms the overrun (validator#1874). Validate received images against committed states, then verify normal delivery and resubscription. An unreached overrun fails. |
| `pruned_read_signals` | Confirm a landmark transaction, witness physical removal of all superblocks present after publication, and read its old slot. Retention must be advertised by a public boundary or a specific retention error; generic missing-block errors and silent omissions fail. The zero boundary and listed pruned slots remain validator#1888. |
| `ledger_index_signals` | Publish instruction and loading failures, witness their signatures independently in blocks, and require transaction lookup plus exactly one address-history entry for every static key. A ledger record without an addressable lookup fails. |

Account, log, signature, and failure observers use separate WebSocket connections.
Recovery signature subscriptions are registered before submission. A duplicate
notification cannot replace a missing channel, and account notifications must
match the complete committed image. Correctness of a refusal does not excuse a
missing signal: documented absences remain failing findings against #261.

Payload faults operate only on locally owned fixture storage in accountsdb format
1. They match a complete cell image, change only a value byte, and restore active
and shadow copies on success or early return. Unsupported formats, absent or
ambiguous targets, and unexplained process exits fail. A named fail-stop may pass;
SIGABRT, SIGSEGV, and a timeout cannot establish successful detection.

Synthetic empty `AccountIndex` records and lost indexer messages cannot be created
through a valid public transaction: the pinned Engine builds the index from
static transaction keys. Those injections belong in Engine's
`ledger/src/tests/integration.rs` and `keeper/src/tests/subscriptions.rs`.
`ledger_index_signals` covers the public loading-failure boundary without claiming
to inject those internal faults. Direct byte/index races, forged replication
streams, and power-loss durability remain outside these scenarios.
