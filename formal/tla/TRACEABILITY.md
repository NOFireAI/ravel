# Traceability index

One row per area, indexing into that area's own `traceability.md`. Row
counts are exact data-row counts (header and separator excluded).
`scripts/check-tla.sh traceability` resolves every Rust path and symbol
named in every area table and fails when one no longer exists.

| Area | Specification | Rows | Rust crates covered | Table |
|---|---|---|---|---|
| common | `common/RavelObjectStore.tla` | 9 | ravel-object-store, ravel-commit, ravel-ingest | [common/traceability.md](common/traceability.md) |
| commit | `commit/CommitProtocol.tla` | 21 | ravel-ingest, ravel-commit, ravel-catalog, ravel-maintain, ravel-types | [commit/traceability.md](commit/traceability.md) |
| catalog | `catalog/CatalogMVCC.tla` | 25 | ravel-catalog, ravel-commit, ravel-maintain, ravel-query | [catalog/traceability.md](catalog/traceability.md) |
| lifecycle | `lifecycle/LifecycleGC.tla` | 29 | ravel-maintain, ravel-commit, ravel-query, ravel-catalog | [lifecycle/traceability.md](lifecycle/traceability.md) |
| resharding | `resharding/OnlineResharding.tla` | 19 | ravel-ingest, ravel-catalog, ravel-commit, ravel-types, services/ravel-cli | [resharding/traceability.md](resharding/traceability.md) |
| maintenance | `maintenance/MaintenanceOwnership.tla`, `maintenance/CompactionClaims.tla` | 45 | ravel-fleet, ravel-maintain, services/ravel-cli, services/ravel-server | [maintenance/traceability.md](maintenance/traceability.md) |
| agents | `agents/AgentProtocol.tla`, `agents/AgentEffects.tla`, `agents/AgentIntegrated.tla` | 51 | ravel-agents-core, ravel-agents-store | [agents/traceability.md](agents/traceability.md) |

Total: 199 traceability rows across the suite.

## Test debt

Every row still lacking a test, gathered from each area's `traceability.md`
and tagged by area, the Rust path the row cites, and the reason no test
covers it.

### lifecycle (1 row, test gap)

- `CompleteErasure / CompletionImpliesNoPreRewriteExposure` —
  `services/ravel-server/src/maintain.rs::write_erasure_completion` — the
  production symbol writes the completion object, but no test drives it
  through the served-set branch end-to-end; only the gate
  (`bucket_erasure_completion`) is unit-tested

### maintenance (3 rows, proposal with no shipped caller)

- `GuardedPublish` — `crates/ravel-fleet/src/claim.rs::renew` — no
  `ClaimGuard` type exists; the guard is a caller convention over `renew`'s
  outcome, and no code outside ravel-fleet calls the claim primitive
  today (ADR-1029 is a proposed design). Pinned at the primitive by
  `claim_guard_abandons_publish_when_the_claim_is_lost`; a production test
  follows the first shipped caller
- `AbandonPublish` — `crates/ravel-fleet/src/claim.rs::renew` — same
  reason as `GuardedPublish`
- `LostClaimNeverPublishesThroughGuardedPath` —
  `crates/ravel-fleet/src/claim.rs::renew` — same reason as `GuardedPublish`

### agents (14 rows, test gap)

- `Destroy` — `crates/ravel-agents-core/src/kernel.rs::Kernel::tombstone` —
  the tombstone leaves a scheduled timer in HEAD and `check_active` refuses
  its fire, but no test schedules a timer, tombstones the agent and asserts
  `FireTimer` is `AgentGone` and the recreated HEAD holds no scheduled timer;
  the property test's refused fire is skipped, not asserted
- `Takeover` — `crates/ravel-agents-core/src/kernel.rs::Kernel::takeover` —
  no test asserts `Reply::Owned` with the epoch equal to the observed epoch
  plus one; the stale-commit test refuses on the owner id as well and the
  property test asserts only that the epoch does not go back
- `BeginTurn` — `crates/ravel-agents-core/src/kernel.rs::Kernel::begin_turn` —
  no test begins a turn with two pending inputs and asserts the lower
  `accept_seq` is bound
- `CancelTimer` — `crates/ravel-agents-core/src/kernel.rs::Kernel::cancel_timer`
  — no test cancels an armed generation, asserts `TimerCancelled`, and then
  asserts a fire for that generation enqueues nothing; the only cited test
  exercises the late cancel the model does not cover
- `MonotonicIdentity` — `crates/ravel-agents-core/src/kernel.rs::Kernel::publish`
  — `kernel_sequences_keep_the_head_invariants` asserts `root_revision`,
  `incarnation` and epoch but no clause on `timer_generation`; the model
  and the kernel disagree on whether it survives a recreate (#3344)
- `TenantIncarnationIsolation` —
  `crates/ravel-agents-store/src/publish.rs::AgentStore::decode_head` — no
  test reaches the "HEAD names a different agent" refusal or
  `Kernel::create`'s tenant and agent check

- `ReplayIntentDrift` (`crates/ravel-agents-core/src/kernel.rs::Kernel::commit_intent`):
  no test recommits an intent key with a different input hash and asserts
  the turn is blocked with the intent unchanged
- `AdminAuthorize` (`crates/ravel-agents-runtime/src/effects.rs::Coordinator::reconcile_authorize`):
  no test calls `reconcile_authorize` on an effect that is `Authorized` at
  an earlier attempt and asserts it is refused with nothing dispatched
- `AdminRecordResult` (`crates/ravel-agents-runtime/src/effects.rs::Coordinator::reconcile_attach`):
  no test calls `reconcile_attach` on an effect `Authorized` without a
  result and asserts it is refused with HEAD unchanged
- `CommitCancelled` (`crates/ravel-agents-core/src/kernel.rs::Kernel::complete_turn`):
  no test requests a cancel while an effect of the turn is `InDoubt` and
  asserts the completion is refused until reconciliation
- `PutPayloadLostResponse` (`crates/ravel-agents-store/src/publish.rs::AgentStore::put_object_at`):
  no test loses the response of a payload PUT that applied and asserts the
  retried enqueue is accepted with one payload object
- `StepTranscriptCompatible` (`crates/ravel-agents-core/src/kernel.rs::Kernel::commit_intent`):
  the intent half, the same gap as `ReplayIntentDrift`
- `DispatchRequiresCommittedAuthorization` (`crates/ravel-agents-core/src/kernel.rs::Kernel::authorize_attempt`):
  no test asserts the refusal of an effect that is not journaled
- `CancellationRespected` (effects form,
  `crates/ravel-agents-runtime/src/effects.rs::Coordinator::refuse_cancelled`):
  the kernel refuses an owner authorization under a cancel request
  (`kernel.rs:1764`) and ignores a result for a cancelled input
  (`kernel.rs:1856`), but no test records a step after a cancel request and
  asserts whether `require_running` refuses it

Total test-debt rows: 18 (lifecycle 1, maintenance 3, agents 14).
