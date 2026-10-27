# Scenario coverage limits

The suite reset prunes and consolidates existing proofs. These limits remain
separate from the scenarios it currently establishes; resolving them requires
contract clarification or a later scenario-design pass.

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
| Commit-size probes | The removed Redline probes allowed alternative rejection paths or global-counter fallbacks; they are not definitive ALT/size-limit regression proofs. |
| Account-query bound | Main requires refusal of oversized account queries; acceptance remains validator#1845 and cannot be classified as supported leniency. |
| External timing | Live base-chain fetch and settlement latency are excluded from active Redline measurements; controlled replacements remain deferred. |

New race/edge-case scenarios and the broader Redline redesign follow assertion
pruning and consolidation. Performance observations alone are not a no-regression
verdict, and a progress timeout is not a diagnosis of deadlock.
