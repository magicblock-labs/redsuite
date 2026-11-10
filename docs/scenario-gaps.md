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
| Replication | Runtime and bootstrap checks inspect follower images and transaction results; causal-load recovery still checks final state on the leader. Forged streams, storage corruption, duplicate-work identity and materialization leases remain uncovered. |
| Resize/read overlap | Background readers and final lengths do not establish that reads overlapped relocation or snapshot boundaries. |
| Scheduler cancellation | Canceling a fast task does not establish it executed first or that cancellation stopped an executing task. |
| Signature unsubscribe | The RPC lifecycle performs the requests but does not prove notification suppression after signature unsubscribe. |
| Commit-size probes | Alternative rejection paths or global-counter fallbacks are not definitive address-lookup-table or size-limit regression proofs. |
| Account-query bound | Oversized account queries must be refused; acceptance is not evidence of supported leniency. |
| External timing | Live base-chain fetch and settlement latency are excluded from active Redline measurements; controlled replacements remain deferred. |

Performance observations alone are not a no-regression verdict, and a progress
timeout is not a diagnosis of deadlock.
