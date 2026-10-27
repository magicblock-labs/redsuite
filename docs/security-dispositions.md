# Security case ownership

[Coverage limits](scenario-gaps.md) · [Scenario guide](../README.md#-extend-the-suite) · [Security qualification #235](https://github.com/magicblock-labs/redsuite/issues/235)

Each row assigns one primary owner to a retained case or candidate. `implement-now`
identifies an issue extension; it does not assert that the extension has passed a
live run. A closed issue is not proof of every broader candidate described here.
Rows assigned to an owning crate or blocked prerequisite remain unproved by
RedSuite. Folded and equivalent rows add no separate case to the test count.

## Registers and fixtures

| Register | Suite and scenario | Shared fixture and stream |
| --- | --- | --- |
| SA | Redhat [`settlement_authorization`](../redhat/src/scenarios/chainlink/settlement_authorization.rs) | Owned delegated cells and full ER/base images. New streams: `redhat/commit_order`, `redhat/commit_buffers`, `redhat/settlement_authorization/roles`, `redhat/callback_provenance`. |
| EA | Redhat [`ephemeral_account_security`](../redhat/src/scenarios/magicblock/ephemeral_account_security.rs) | Independent account/deposit/sponsor ledger, funded target and victim. New streams: `redhat/ephemeral_account_security`, `redhat/confined_accounts`, `redhat/failure_placement`, `redhat/commit_fee_roles`. |
| PI | Redhat [`protocol_inputs`](../redhat/src/scenarios/aperture/protocol.rs) | Warm/cold/protected cells, separate client connections and notifications; `redhat/protocol_inputs`. |
| TS / TA / AD | Redhat [`transaction_structure`, `transaction_authority`, `transaction_admission`](../redhat/src/scenarios/aperture/mod.rs) | Signed transaction and repaired control; corresponding `redhat/transaction_*` streams. |
| RR | Redhat `replication_settlement_recovery`, reusing Redshift [`ReplicationRecovery`](../redshift/src/scenarios/replication/replication_recovery.rs) | Leader/two verifiers and the existing causal workload; added settlement fixture uses `redhat/replication_settlement`. |
| IA | Redshift [`intent_execution_and_accounting`](../redshift/src/scenarios/committor/intent_execution_and_accounting.rs) | Existing delegated actors, action targets, fee vault and callback refund fixtures; fixed baseline cases. |
| CR / UR | Redshift [`commit_recovery`](../redshift/src/scenarios/committor/commit_recovery.rs) / [`undelegation_recovery`](../redshift/src/scenarios/committor/undelegation_recovery.rs) | Accepted intents, held delivery requests, restart and independently confirmed base state; fixed baseline cases. |
| CP / TR | Redshift [`checkpoint_recovery`](../redshift/src/scenarios/storage/checkpoint_recovery.rs) / `transaction_replay_protection` in [`TransactionLifecycle`](../redshift/src/scenarios/aperture/transaction_lifecycle.rs) | Retained transaction records, restored storage and original signed bytes; fixed baseline cases. |

Seed streams name input generation, not OS scheduling. SA logs its generated
index/alias/signer plan before funding and separates case order, fixtures and
`mapped_signer` streams. S1 takes both the intended and the foreign commit
authority from the harness identity pool under fixed role labels, because DLP
settlement requires each authority's canonical fee vault provisioned at genesis;
both resolved keys and vaults are logged. S2 takes only its buffer authority
from the pool: its foreign signer exercises the committor buffer's own authority
check, which reads no fee vault, so a seeded payer is sufficient there. These
identities are fixtures of the base generation and its slot assignments; the
seed reproduces payers, account addresses, markers and case
order. EA separates its `plan`, `targets` and `fixtures` streams and logs sizes and
target addresses before funding. The inherited RR workload still uses unseeded
payers and a recovery probe. These fixtures and bounded replays do not establish
H1's complete pre-generated independent plan requirement.

[`ErCtx::local_images`](../crates/redsuite-core/src/context/mod.rs) uses the shared
owner registry in [`topology::config`](../crates/redsuite-core/src/topology/config.rs)
without triggering cloning. Known-present protected images must be present before
comparison; filtered absence cannot prove unchanged state.
`ErCtx::local_program_accounts` supplies ordered maps for the shared
`SERVICE_OWNERS` publication checks in F1 and M2.

## Settlement, authority and callbacks

| Case | Primary owner / register / stream | Fixture and action | Oracle and expected refusal | Legitimate control | Disposition |
| --- | --- | --- | --- | --- | --- |
| S1 Exact successor | #243 / SA / `commit_order` | Isolated disposable authorities with canonical vaults, including the wrong-authority fixture; direct and buffered zero, stale, consumed and gap nonces; wrong/unsigned authority. | Full account and metadata images stay unchanged; `NonceOutOfOrder`, `InvalidAuthority`, or `MissingRequiredSignature` at the targeted instruction. | Next authorized successor on each attacked address. | implement-now |
| S2 Buffer association and ranges | #244 / SA / `commit_buffers` | Canonical account/nonce buffer; foreign authority/account/commit substitutions, alignment and overrun. | Full protected images unchanged; association `Custom(1)`, alignment `Custom(0x69002)`, overrun `Custom(0x69003)`, unsigned close `MissingRequiredSignature`. | Complete and close the authorized buffer. | implement-now |
| S3 Buffer freshness and bundle rollback | #244 / SA / `commit_buffers` | Two committed members; partial/repeated/generated-order chunks; failed bundle then same nonce/PDA with different bytes. | Targets unchanged before installation; exact failing member rejects the whole base transaction; final bytes equal the new complete image. | Legal buffered successor for both members. | implement-now |
| S4 Accepted outbox authority | #244 / SA / `commit_buffers` | Hold a live large-account preparation request after confirmed buffer initialization; witness the new local outbox row; attacker/unsigned close, sent and stage requests. | Exact `IncorrectAuthority`, `InvalidArgument`, or `MissingRequiredSignature`; complete outbox/context/account images unchanged. | Release the original work, require strict successful receipt/base confirmation, then commit again. | implement-now |
| S4b Outbox creation authority | #244 / magic-program / internal `OutboxIntentBundle` fixture | The observed large record exceeds public transaction framing; its record builder/decoder belongs to magic-program. | Bundle decoding precedes authority checks, so truncation or empty bytes cannot establish the permission gate. Require a bounded valid internal record and the exact authority/signature refusal with unchanged pending work. | Existing legitimate acceptance creates the real pending outbox. | blocked-on-bounded-valid-outbox-record-fixture |
| S5 Forged outbox acceptance | #240 / TA / `transaction_authority` | Attacker-signed and unsigned validator-reference `AcceptScheduleCommits`. | Intended native authority error and no protected-state effect. | Repaired signed owner write. | equivalent-coverage-in-TA |
| S6 Missing chunks prematurely published internally | #244 / committor / no public seed fixture | A signer-authorized from-buffer instruction is not the committor's completeness gate. | Require a committor-owned accepted-stage fixture that attempts publication before its tracked chunks complete; no invented public rejection. | Complete the same tracked delivery. | owning-crate-committor |
| A1 Direct, nested and sibling authority | #249 / SA / `settlement_authorization/roles` | Fixed direct, nested and sibling cases around owned cells; the seed changes their order. | Full ER/base images unchanged and intended ownership/frame diagnostic. | Same-cell owner write and strictly confirmed settlement. | implement-now |
| A2 Private wire and privileged native authority | #240 / TA / `transaction_authority` | Private transaction version, substituted validator identity, attacker-signed privileged instructions. | Separate framing/signature rejection from executed authority refusal; retained outcomes when execution was witnessed. | Matching repaired transaction. | equivalent-coverage-in-TA |
| A3 MagicRoot Patch/Finalize/Delete/PostFinalize and mode races | #249 / Engine processor / no public seed fixture | Privileged materialization instructions and undelegating account state require authenticated internal inputs. | Each operation must reach its own runtime permission gate; an earlier private-wire rejection is insufficient. | Authorized internal materialization and subsequent write. | owning-crate-magicblock-processor |
| A4 Effective writability and permission union | #249 / SA / `settlement_authorization/roles` | Readonly owner write and an entered nested CPI requesting a writable upgrade. | Exact `ReadonlyDataModified` or `PrivilegeEscalation`, matching gate logs and complete protected local/base rollback. | A readonly inspect plus writable owner update in one transaction uses the valid transaction-wide permission union and strictly confirmed settlement. | implement-now |
| A5 Canonical MagicContext | #249 / SA / `settlement_authorization/roles` | Signed owner CPI substitutes a foreign cell for MagicContext. | `MissingAccount` with the invalid-context diagnostic; full protected local/base images unchanged. | Same-cell owner write and settlement with the canonical context. | implement-now |
| A6 Settlement fee-vault substitution | #253 / EA / `commit_fee_roles` | Reuse E9's omitted/substituted/readonly canonical fee-vault cases. | E9's exact refusal and payer/vault/context/base rollback oracle. | E9's strictly confirmed rightful commit. | equivalent-coverage-in-E9 |
| A7 Generated account-role dimensions | #249 / SA / `settlement_authorization/roles` | A plan logged before funding selects an owned cell, leading account index, two or three duplicate aliases and the alias carrying a signer bit. | Readonly owner writes and unsigned aliased PDA CPI requests reach exact readonly/privilege errors; complete protected ER/base images remain unchanged. | The same aliases use the owning program's PDA signer; independently modeled owner writes settle successfully. | implement-now |
| C1 Callback attachment provenance | #255 / SA / `callback_provenance` | Two tagged action cells; wrong index, destination, source, payer, duplicate attachment and forbidden account roles. | Exact instruction index plus `InvalidInstructionData`, `InvalidAccountData`, `Immutable`, `IncorrectAuthority`, or `MissingRequiredSignature`; gate diagnostic and full payer/vault/context/base/local rollback. | Source-authorized attachment and independently checked delivery. | implement-now |
| C2 Forged callback delivery | #255 / SA / `callback_provenance` | Callback executor with attacker authority or unsigned expected validator. | `IncorrectAuthority` or `MissingRequiredSignature` before target effects; complete protected images unchanged. | C1's real validator-delivered callback. | implement-now |
| C3 Cross-action delivery and action categories | #255 / SA / `callback_provenance` | Successful and failing actions mapped to different callback cells; delivery controls extend across after-commit, standalone and after-undelegate actions. | Full independent cell models explain exactly one response per destination; retained terminal callback records, exact payer/vault charges and confirmed base effects. | Commit callback results on the same cells with a strict receipt. | implement-now |
| C4 Callback fee rollback proposed under fee fuzzing | #255 / SA / `callback_provenance` | Reuse C1's payer/vault preimages and native callback precharge. | Failed attachment retains no 5,000-lamport charge. | C3's successful charged attachment. | fold-into-C1 |
| C5 Callback late attachment freshness | #255 / magic-program / no live target barrier | A later RPC request can see a drained context rather than the intended old slot/blockhash row. | Deterministic latest-intent fixture must reach the specific stale-slot/blockhash gate. | Same-slot attachment to that action. | owning-crate-magicblock-magic-program |
| C6 Mode change between accepted attachment and delivery | #255 / SA / `callback_provenance` | Requires an accepted callback row held across a selected mode transition. | Preserve source binding, correct response destination and fee accounting across the transition. | Release the same action and confirm its base effect/callback. | blocked-on-accepted-callback-stage-barrier |
| C7 Arbitrary callback signer bits | #255 / magic-program / no public seed fixture | Attachment `ShortAccountMeta` encodes key/writability, not arbitrary signer bits. | Internal execute-callback validation rejects any signer except the callback PDA. | Correct callback-PDA signer. | owning-crate-magicblock-magic-program |
| C8 Shared callback request-size limits | #241 / PI / `protocol_inputs` | Existing HTTP/WS and transaction-size boundaries; no distinct blanket callback-payload rejection was found. | Assert actual transport/transaction contract, rather than invent a callback-only limit. | Supported unusual request and fresh valid write. | equivalent-coverage-in-PI |
| C9 Callback compute-budget leniency | #255 / SA / `callback_provenance` | Accepted callbacks request zero and 20,000 compute units; the current delivery builder does not apply that field. | Each callback has a retained successful execution record and matching terminal status; independent models require exactly one response at its own destination. | Strict base-confirmed action and settlement of its callback result. | implement-now |
| C10 Callback compute-budget enforcement | #255 / services / no enforced callback-budget fixture | `ActionsCallbackService::build_transactions` emits no compute-budget instruction from `compute_units`. | The owning service must define and enforce the requested limit before a finite-budget exhaustion case can prove a terminal compute failure and rollback; a timeout is insufficient. | The same callback executes with a sufficient enforced budget. | owning-crate-magicblock-services |
| C11 Stale callback replay | #255 / magic-program / no client delivery path | A recorded response re-applied after its target moved on. | Delivery is gated on the expected validator authority before any payload or target state is read, so a client replay is refused for its authority rather than its staleness; the fixture now carries the destination program meta the real path supplies, and the remaining gate is internal. | A validator-delivered callback on the same destination. | owning-crate-magicblock-magic-program |

## Ephemeral state, rent, fees and rollback

| Case | Primary owner / register / stream | Fixture and action | Oracle and expected refusal | Legitimate control | Disposition |
| --- | --- | --- | --- | --- | --- |
| E1 Ephemeral lifecycle model | #252 / EA / `ephemeral_account_security` | Seeded create/resize/shrink/close/recreate, size boundaries, owners and signer roles. | Full bytes/ownership and independent per-account/per-sponsor deposits/refunds; zeroing and each transaction's own conservation. | Same-address legal continuation. | implement-now |
| E2 Insufficient rent | #252 / EA / `ephemeral_account_security` | Sponsor balance below required rent; checked model arithmetic. | Specific `InsufficientFunds`, no retained deposit or account effect. | Affordable create then close. | implement-now |
| E3 Raw SBPF resize | #252 / EA / `ephemeral_account_security` | Caller-owned raw resize probe, no Magic CPI; deltas only -1/+1. | Runtime `InvalidRealloc`; length, full state and accounting unchanged. | Equal-length receipt succeeds; distinguishable byte change only for nonempty writable data. | implement-now |
| E4 Conflicting lifecycle batch | #252 / EA / `ephemeral_account_security` | Existing six-operation bounded batch; no assumed submission serialization. | One legal ordering explains every receipt, full final image and external rent ledger. | Continued legal lifecycle on the same target. | implement-now |
| E5 Confined origin and funding provenance | #251 / EA / `confined_accounts` | Zero-authority base-derived confined cell, recorded metadata rent payer, zero initial local deposit and separate funded victim. | Base image checked on every case; retained full victim ER/base images checked throughout, including before final close accounting; no refund exceeds this cell's own deposit and the hard vault floor survives. | Funded growth/shrink/close plus unrelated delegated commit. | implement-now |
| E6 Confined raw resize and local-only settlement | #251 / EA / `confined_accounts` | Reuse E3's raw probe; confined and delegated same-program cells; mixed commit/undelegate bundle. | Confined raw delta reaches `InvalidRealloc`; local-only settlement reaches its mode gate; full delegated ER/base images and context remain unchanged after refused mixed bundles. | Equal-length control, same delta on delegated cell, strict delegated settlement. | implement-now |
| E7 Cross-origin sponsor substitution | #251 / EA / `confined_accounts` | Reuse the sponsor-aware E8 model for alternate-funded confined growth beside an unrelated victim deposit. | Refunds remain capped by the target's own deposit; full victim/base images and each sponsor tally remain consistent. | Funded resize/shrink and rightful close. | implement-now |
| E8 Rent vault and sponsor roles | #253 / EA / `ephemeral_account_security` | Payer/sponsor aliases, substitute/readonly vault and a pre-existing independently tracked victim deposit. | Specific role/funds error; full independent images and each transaction's conservation; floor equals starting vault balance minus that prior refundable deposit. | Exact victim refund may lower the starting balance while preserving the floor. | implement-now |
| E9 Commit fee vault roles | #253 / EA / `commit_fee_roles` | Omitted/substituted/readonly canonical commit vault. | `MissingAccount` or intended readonly-owner error; no payer/vault/context/base mutation. | Strictly confirmed ordinary delegated commit. | implement-now |
| E10 Existing action, paid-commit and refund accounting | #253 / IA / fixed baseline | Existing sponsored quota, paid threshold, action fees and callback refunds. | Exact independently read payer/vault/target deltas and strict successful settlement. | Existing charged commit and success/failure callbacks. | equivalent-coverage-in-IA |
| E11 Explicit confined cache eviction | #251 / Redshift cache lifecycle / fixed pressure fixture | Eligibility and eviction must be witnessed in the cache-owner topology. | Local absence alone cannot prove eviction; preserve funded provenance and authoritative state. | Rightful reload/write after actual pressure. | blocked-on-cache-eviction-witness |
| E12 Authorized rent-vault surplus | #253 / EA / `ephemeral_account_security` | Standard signed system transfer credits one known lamport separately from refundable deposits. | Successful terminal receipt, own pre/post balances and fresh full images show the exact credit; later rent refunds retain that separate surplus. | E8's authorized prior-deposit close leaves floor plus surplus. | implement-now |
| E13 Cloned/projected funding attribution | #253 / EA and P2 / no attributed materialization fixture | E5 starts its confined account with zero tracked local deposit; P2 checks projected token claims and overlays without a rent-vault funding ledger. | Distinguish cloned account lamports from refundable rent and any actual vault credit; neither account presence nor token conservation proves positive vault funding. | Materialize a known funded account with independent source/account/vault preimages and an attributable funding transition. | blocked-on-attributed-materialization-fixture |
| E14 Duplicate committee members | #253 / magic-program / deduplicated by design | A commit whose committee repeats one delegated account. | `process_schedule_commit` merges duplicates by final restored pubkey and keeps the first occurrence, so there is no refusal to assert; the case asserts the merge contract instead, one charge and one committed target. | The same account committed once. | owning-crate-magicblock-magic-program |
| E15 Commit fee charge boundary | #253 / core / unreachable charge | A payer swept to exactly affordable and one short of a commit fee. | `COMMIT_FEE_LAMPORTS` applies only once an account's commit nonce reaches `ACTUAL_COMMIT_LIMIT`, so every commit reachable here is charged zero and no boundary exists to sweep; the asserted fact is the zero charge itself. | A charged ephemeral rent boundary, swept on both sides. | owning-crate-magicblock-core |
| F1 Failure placement | #250 / EA / `failure_placement` | Commit/undelegate crossed with failure first/between/last around write, rent resize and task request. | Exact failing index `Custom(250)`, zero ER fee, full account/task/outbox/context rollback after observed slots. | Same-address resize/close and strict delegated settlement. | implement-now |
| F2 Duplicate lifecycle rollback matrix | #250 / EA / `ephemeral_account_security` | Carry rent preimages and model into E1/E4 instead of a second concurrent harness. | Each rejected transition and one legal serialization explain state/accounting. | E1's same-address continuation. | fold-into-E4 |
| F3 Conflicting and disjoint failure batches | #250 / EA / `failure_placement` | Prebuild two same-target failures and two distinct-target failures; seed their submission order. | Every retained receipt fails at index 1 with zero charge; complete local/base/service images stay unchanged for any execution order. | F1's same-address continuation and settlement. | implement-now |

## Simulation, recovery and internal boundaries

| Case | Primary owner / register / stream | Fixture and action | Oracle and expected refusal | Legitimate control | Disposition |
| --- | --- | --- | --- | --- | --- |
| M1 Signature/flag isolation and unchanged-byte submission | #254 / PI / `protocol_inputs` | Valid/forged signed write, signature verification and replacement flags, both submission preflight modes. | Prove simulated body runs; no live effects/records; real forged submission gets signature refusal. | Submit the valid simulated bytes unchanged once, then recover. | implement-now |
| M2 Simulated service publication | #254 / PI / `protocol_inputs` | Commit, undelegate, callback/action charges, ephemeral creation and task requests. | Positive body execution evidence; full local/base/vault/context images and `ErCtx::local_program_accounts` task/outbox maps unchanged, separate from permitted hydration. | Real owner execution and strict confirmed settlement. | implement-now |
| M3 Existing simple write simulation | #241 / PI / `protocol_inputs` | Keep the earlier simulation write fixtures and flag matrix in M1. | Same no-effect/statusless oracle. | M1's unchanged-byte actual execution. | fold-into-M1 |
| R1 Ordinary replicated settlement and retained outcomes | #247 / RR / `replication_settlement` | Owned commit/action/callback, executed failed write, retained receipt records and leader restart under the retained-ledger profile. | Full leader/base images, exact retained records and single ledger membership; positive authenticated seal progress; available mismatch counts must be zero. Retention must cover the observation window. | Fresh strict base-confirmed owner settlement after reconnect. | implement-now |
| R2 Existing verifier recovery | #247 / Redshift ReplicationRecovery / fixed baseline | Retained/pruned cursor, snapshots, killed peer, survivor and wiped rejoin. | Existing cursor/seal/progress assertions; no direct follower-image claim. | Surviving/rejoined verifier catches the fixed final boundary. | equivalent-coverage-in-ReplicationRecovery |
| R3 Direct follower images/history | #247 / replicator / no follower RPC fixture | Verifier has no account/history RPC. Existing `streams_and_resumes_without_duplicate_application` checks an incremented value, sponsor balance, seals and height. | Full follower images and complete membership/order remain broader than those assertions. | Existing resumed stream converges and accepts another increment. | owning-crate-magicblock-replicator |
| R4 Forged authenticated replication inputs | #247 / replicator / internal protocol fixture | Existing `authenticates_boundary_records` covers valid, tampered and wrong-key block/seal/reset/checkpoint records. Handshake/transaction/framing cases remain separate. | The existing test requires exact signature rejection and no control delivery; it does not establish every malformed protocol path. | The same existing test preserves valid signed boundary records. | owning-crate-magicblock-replicator |
| R5 Duplicate follower identity | #247 / replicator / internal lease fixture | Concurrent transfer using one allowed follower identity. | Second reservation refused while original lease remains live. | Original transfer and later authorized reconnect. | owning-crate-magicblock-replicator |
| R5b Replicated work identity and successor | #247 / replicator / internal replay fixture | Duplicate application of accepted replicated work under a repeated identity, followed by a valid successor. | At most one effect for that work identity; the next legal work executes. R5's lease and R8b's base DLP nonce do not establish this boundary. | Authenticated successor converges after rejoin. | owning-crate-magicblock-replicator |
| R6 Named storage corruption | #247 / accountsdb/ledger / internal record fixture | Existing `test_corruption_detection` changes the recorded checksum in an isolated database. Broader record-format and restored-replay cases remain separate. | The existing test requires `AccountsDBError::Corruption`; unknown crash/timeout remains invalid. | Its intact database validates before corruption; restoring and reopening the image remains unproved. | owning-crate-magicblock-accountsdb |
| R7 Divergent materialization lease | #247 / accountsdb/chainlink / internal lease fixture | Concurrent ownership of one materialization target. | One publisher owns the lease; competing image cannot become usable. | Valid complete image publishes after release. | owning-crate-magicblock-accountsdb |
| R8 Transaction retry identity | #242 / TR / fixed baseline | Original signed bytes, duplicate submission, expiry and restart. | At most one retained effect within the observed hash/retention contract; invalid hash never executes. | Fresh signed write. | equivalent-coverage-in-TransactionLifecycle |
| R8b Consumed base commit identity | #243 / SA / `commit_order` | Carry retry candidate's base nonce substitution into S1. | Exact-successor gate rejects stale consumed nonce and changed bytes. | Next authorized successor. | fold-into-S1 |
| R9 Frozen handoff | #245 / UR / fixed baseline | Existing frozen snapshot and commit-before-undelegation fixtures. | Preserve the frozen image and ownership until the observed handoff completes. | Strict receipt/base confirmation and rightful owner write. | equivalent-coverage-in-UndelegationRecovery |
| R9b Durable accepted work | #246 / CR / fixed baseline | Existing accepted intent, restart and independent outcome records. | Acceptance/state/history assertions within the retention contract; acknowledgement alone is insufficient. | Strict receipt/base confirmation and fresh writes. | equivalent-coverage-in-CommitRecovery |
| R10 Synthetic signature-prefix collisions and storage ABI races | #257 / ledger/accountsdb / internal fixture | Construct collisions/record versions and exclusive maintenance races without a cryptographic RPC campaign. | Collision/format checks and lease exclusion at the owning boundary. | Noncolliding compatible record and successful reopen. | owning-crate-magicblock-ledger |
| H1 Whole-catalog deterministic plans | #236 / shared runner / keyed child streams | Complete logged plans, fixture keys and planned releases before submission; repeat two groups separately and together from equivalent clean state. | Compare plans under equal seeds, changed completion order and an added case; closed #236 and inherited unseeded fixtures do not establish this whole-catalog property. | A different seed varies inputs while preserving required classes; invalid input fails before stack startup. | blocked-on-independent-catalog-plan-validation |

## Retained lifecycle owners

| Case | Primary owner / suite and register | Fixture and action | Oracle and expected refusal | Legitimate control | Disposition |
| --- | --- | --- | --- | --- | --- |
| P1 Mirror freshness and protected state | #237 / Redshift [`account_mirroring_and_isolation`](../redshift/src/scenarios/chainlink/account_mirroring_and_isolation.rs) | Fixed cold/absent/plain/delegated fixtures; existing cache transport/eviction owners retain their own fault cases. | Base refresh cannot replace authoritative delegated bytes/balance; no inferred eviction without a witness. | Fresh mirror update and rightful transfer. | equivalent-coverage-in-AccountMirroringAndIsolation |
| P2 Token backing and overlays | #238 / Redshift [`projected_token_backing_lifecycle`](../redshift/src/scenarios/chainlink/projected_token_lifecycle.rs) | Fixed owner/foreign ATA/eATA/mint/vault fixtures, supported backing/overlay modes. | Existing target/owner rejection, expected claims, physical supply and full projected state. | Owner transfer and ordinary settlement. | equivalent-coverage-in-ProjectedTokenLifecycle |
| P3 Single activation | #239 / Redshift [`activation_single_shot`](../redshift/src/scenarios/chainlink/activation_single_shot.rs) | Fixed counter/dependency fixtures; held reads, competing triggers and recovery. | Existing generation/counter checks; an unentered race is not proved by joining callers. | Legal activation and subsequent owner lifecycle. | equivalent-coverage-in-ActivationSingleShot |
| P4 Scheduled-task authority | #256 / Redshift [`scheduled_task_lifecycle`](../redshift/src/scenarios/scheduler/scheduled_task_lifecycle.rs) | Fixed actor/task IDs, wrong authority and direct crank calls. | Specific task authority refusal, independent actor state and existing task account checks. | Legal schedule/observed execution/cancel according to current assertions. | equivalent-coverage-in-ScheduledTaskLifecycle |
| P5 Malformed scheduler graphs | #248 / TS / `transaction_structure` | Carry duplicate-key/index/lookup rows into existing transaction mutation matrix. | Exact parse/sanitize/lookup gate, no retained graph or unauthorized effect. | Repaired signed transaction. | equivalent-coverage-in-TS |
| P6 Scheduler reclamation and starvation | #248 / processor / internal executor fixture | Ticket/executor lifetime, targeted backlog and disjoint progress need an entered contention barrier. | Complete admission/outcome accounting and reclaimed work; health alone is insufficient. | Bounded independent canary and drain. | owning-crate-magicblock-processor |

## Consolidation and remaining scope

| Local branch | Change ownership |
| --- | --- |
| `235-redhat-accounting-and-replay` | Shared snapshots, native IDs and owner registry; one EA model for #250–#253; ordinary replay extension under #247. |
| `235-redhat-settlement-and-protocol` | Depends on the accounting foundation; #243/#244 share the commit fixture, #249/#255 extend SA, #254 extends PI, and #257 owns this map. |

Review the settlement/protocol delta relative to the accounting foundation. Its
full local tree includes that foundation for validation; shared code has one
owner and adds no duplicate test count. Rows retain their issue owners across
both changes. R5b records missing coverage, not a placeholder failing test.

S1 supplies the typed commit fixture reused by S2–S4. E1–E4 supply the account and
sponsor ledger, raw probe and serialization search reused by E5–E9 and F1. M1
extends PI's existing simulation method. C1 owns callback fee rollback, including
the candidate from #253; C3 owns the distinguishing cross-action delivery model.
R1 reuses the recovery implementation and keeps ordinary Redshift behavior.
These clusters retain the listed fixtures, seed streams, refusal gates and
controls; they do not gain a second scenario for each issue number.

PI retains main's account-query bound: 128 keys must be refused. Acceptance
violates [validator#1845](https://github.com/magicblock-labs/magicblock-validator/issues/1845).
The local 100-key handler guard passes PI's strict 128-key rejection. Supported
unusual inputs do not waive a known bound.

E5 reproduced a refund of 4,096 lamports against a zero deposit on the original
runtime. The local runtime correction tracks each account's funded rent in
persisted metadata and caps shrink/close refunds by that credit. With that fix,
EA passes seed 12345, including the rightful-close continuation. Older account
images contain zero credit; their historical deposits cannot be inferred from
account size or mode and require a migration policy before rollout.

The existing ordinary Recovery control passed independently. R1 now uses a
retained-ledger profile, while Recovery keeps its deliberate pruning workload;
live qualification of R1 remains blocked by a seal-checksum mismatch. At the
same seal 3, slot 1536 and transaction count 4412, preserved snapshots differ in
MagicContext, one cell and a 10,000-lamport payer/vault transfer; their storage
capacities match. Scheduling calls `MagicSys`, initialized only by the leader;
the verifier has no adapter, and nonce reads depend on mutable external state.
Scheduling also embeds a leader-signed future transaction with a process-local
counter. Replay needs the recorded host results and generated artifact without
access to the leader's private key. Live lookups or disabled checks would not
establish convergence. R4–R7 remain unimplemented by RedSuite; a
passing Recovery control cannot establish those defenses.

The local runtime fixes are isolated on validator branch
`235-redhat-runtime-contracts` and Engine branch `235-refundable-rent-credit`.
They add the 100-key query guard and refundable-credit storage/accounting.
The validator requires the local Engine override until an upstream revision can
be pinned. Neither branch changes the replay checksum or bypasses scheduling
validation. SA keeps owner writes after every refusal and settles pairs of
controls to stay below the documented ten-sponsored-commit quota.

Local serial qualification passes EA and PI at seed 12345, SA at seeds 12345 and
240, and the existing Recovery profile with two verifiers. The combined EA/SA/PI
run also passes. The existing account, query, boundary-authentication, resumed
replication and checksum-corruption tests pass; those narrower internal assertions
are reused in R3/R4/R6 without counting them as complete RedSuite scenarios.

E12 covers an authorized surplus transfer, not cloned or projected funding.
The inspected validator's Magic ATA overlay preserves cloned lamports in the
account and drops the overlay on close; that path supplies no positive rent-vault
credit witness. E13 therefore remains separate from the existing token-claim
assertions in P2 and the zero-initial-deposit provenance assertion in E5.

Retained lifecycle rows preserve only their existing assertions. Broader
cache/race claims remain limited by [the existing coverage gaps](scenario-gaps.md).
H1 records the remaining whole-catalog plan and validation prerequisite under
#236. Bounded scenario replays can establish only their compared inputs and
revision set. H1 and this ownership map (#257) are harness/documentation work,
not extra attack cases.

Clone-preparation response truncation was deliberately removed. Restoring it and
`Decision::Rewrite` is a separate scope decision; no row here depends on that
restoration. Internal protocol/lease source claims use Engine revision
`5538432a4bde44bb66e28857badfc2af66a1a396`, selected by the validator lockfile;
its public verifier surface does not expose those internal fixtures.

A refused attack passes only with its state/service oracle and legitimate control.
Supported leniency and documented statusless admission rejection may pass, with
the observed branch recorded. Any witnessed execution requires its terminal
result. Missing triggers, unexplained timeouts and unavailable execution do not
establish a defense. Suspected violations remain unconfirmed until a live run
reproduces their prohibited effect.
