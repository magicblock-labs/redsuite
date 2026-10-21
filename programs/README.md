# Fixture instructions

Build host interfaces and all five fixture artifacts together with
`cargo xtask programs`. A fresh stack loads the rebuilt programs; stop an old
stack with `cargo xtask stack down` before testing fixture changes.
The upgrade probe is built twice with different versions; neither is loaded at
base startup. The upgrade scenario deploys and upgrades its own program.

The tables name a current scenario or indirect producer for every live
instruction. Scenario paths are relative to `<family>/src/scenarios/`.

| Redline instruction (bincode tag) | Consumer |
|---|---|
| `CommitAccounts` (0), `CommitAndUndelegateAccounts` (1) | Redshift `chainlink/clone_on_access`; Redline commit benchmarks use tag 0 |
| `InitAccount` (2), `Delegate` (3) | `redsuite-core::redline::Accounts`, used by Redline workloads and Redshift lifecycle scenarios |
| `SimpleByteSet` (4) | Redline `aperture/simple_load`; Redshift lifecycle writes and upgrade-probe CPIs |
| `ExpensiveHashCompute` (5) | Redline `aperture/high_cu`, scheduler workloads, `redsuite-core::redline::causal` |
| `MultiAccountRead` (6) | Redline `scheduler/mixed_sustained_load` |
| `AccountDataCopy` (7) | `redsuite-core::redline::copy_three`, used by Redline ingress and scheduler benchmarks |
| `ReadAccountsData` (8) | Redline `chainlink/ensure_gate_stall`, `chainlink/cold_hydration_tail`; Redshift retry subscription checks |
| `HashFold` (10) | `redsuite-core::redline::causal`, used by Redline ordering workloads; Redshift `chainlink/cache_lifecycle` |
| SDK undelegation callback | Delegation program after commit-and-undelegate |

Redline reserves tag 9 and rejects it: bincode encodes variant positions, so
removing the slot formerly used by `CloseAccount` would renumber `HashFold`.

| Redshift top-level tag | Consumer |
|---|---|
| `LOG_MSG_TAG` (4) | `chainlink/loader_matrix`; Redhat `magicblock/ephemeral_accounts` |
| `SCHEDULE_COMMIT_TAG` (5) | Account, commit, task and action instructions below |
| `EPHEMERAL_TAG` (8) | Redhat `magicblock/ephemeral_accounts` |
| SDK undelegation callback | Delegation program after counter or order-book undelegation; `FAIL_UNDELEGATION_COUNT` injects the failure in `committor/commit_and_undelegate` |

The dedicated upgrade probe accepts a failure flag followed by a little-endian
`u64` ID. It logs its compiled version and makes two version-dependent Redline
write CPIs, then optionally fails so `chainlink/program_upgrade` can check rollback.

| Redshift schedule-commit instruction (Borsh tag) | Consumer |
|---|---|
| `Init` (0), `DelegateCpi` (1) | `redsuite-core::schedulecommit` committee and counter preparation; correctness and security scenarios |
| `ScheduleCommitCpi` (2) | `committor/commit_and_undelegate` exercises finalize modes; `committor/delegation_session_isolation` exercises non-finalize modes; Redhat attack CPIs |
| `ScheduleCommitAndUndelegateCpiModAfter` (4), `ScheduleCommitAndUndelegateCpiTwice` (5) | `committor/commit_and_undelegate` rejection checks |
| `SetCount` (7) | `committor/commit_and_undelegate`, `committor/delegation_session_isolation` |
| `InitOrderBook` (8), `GrowOrderBook` (9) | `storage/snapshot_read_race`; order-book initialization also serves `committor/commit_and_undelegate` |
| `DelegateOrderBook` (10), `UpdateOrderBook` (11), `ScheduleCommitForOrderBook` (13) | `committor/commit_and_undelegate` |
| `RequestUndelegation` (16) | `committor/commit_and_undelegate` |
| `AdjustCount` (17) | Signed and unsigned writes in activation, storage, retry and commit scenarios; the scheduler's signed-task rejection |
| `Fail` (18) | Retry, activation and checkpoint rollback checks |
| `ScheduleCounterTask` (19), `CancelCounterTask` (20) | `scheduler/task_scheduler`, including signer rejection |
| `CreateActionIntent` (21), `AddActionHandler` (22) | `committor/commit_exactly_once` standalone base action |
| `ScheduleIntents` (23), `BaseAction` (24), `TransferIntent` (25) | `committor/scheduled_intents` commit actions, undelegation actions and callback refunds |

`ScheduleCommitType` retains tags 0–3 for `Commit`, `CommitAndUndelegate`,
`CommitFinalize`, and `CommitFinalizeAndUndelegate`. The raw magic-program
`direct_schedule_commit` builder serves Redhat's ownership and fee-payer
rejection checks, including the fee-vault account.

Redhat retains `SiblingScheduleCommitCpis` (0), `NonCpi` (1), and
`DirectScheduleCommitCpi` (2). All three serve `chainlink/illegal_writable`;
the sibling path invokes the owning Redshift program before its direct attack.

## Wire compatibility

Redshift uses explicit Borsh discriminants so retired tags are not reassigned.
Its committees and counters share the schedule-commit PDA and a 48-byte
`MainAccount` layout containing the player, count and update count. The separate
counter instruction family and variable-length layout are no longer supported.
Rebuild host interfaces and fixtures together, then stop the old stack so the
next base boot loads the matching programs.
