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
| lifecycle | `lifecycle/LifecycleGC.tla` | 18 | ravel-maintain, ravel-commit, ravel-query, ravel-catalog | [lifecycle/traceability.md](lifecycle/traceability.md) |
| resharding | `resharding/OnlineResharding.tla` | 19 | ravel-ingest, ravel-catalog, ravel-commit, ravel-types, services/ravel-cli | [resharding/traceability.md](resharding/traceability.md) |
| maintenance | `maintenance/MaintenanceOwnership.tla`, `maintenance/CompactionClaims.tla` | 45 | ravel-fleet, ravel-maintain, services/ravel-cli, services/ravel-server | [maintenance/traceability.md](maintenance/traceability.md) |

Total: 137 traceability rows across the suite.

## Test debt

Every "new test needed" cell that is not `none` (or a `none` with a covering
note), gathered verbatim from each area's `traceability.md` and tagged by
area and the Rust path the row cites. Rows that already restate an area's
own table are not repeated in prose elsewhere in this file.

### common (4 rows)

- `PutCasVersion / CasOutcomeMatchesEffect` —
  `crates/ravel-object-store/src/memory.rs::MemoryStore::put` — "cas_version_on_absent_key_is_precondition_failed
  in crates/ravel-object-store/tests/contract.rs (no CasVersion against an
  absent key exists in the contract suite)"
- `Delete / VersionsNeverReused` —
  `crates/ravel-object-store/src/memory.rs::next_id` — "cas_with_pre_delete_version_fails_after_recreate
  in crates/ravel-object-store/tests/contract.rs (create, delete, create,
  then CasVersion with the first version must be PreconditionFailed)"
- `PutOverwriteLostResponse / LostResponseEffectApplied` —
  `crates/ravel-commit/src/publish.rs::put_data_object` — "lost_ack_after_successful_put_leaves_object_visible:
  a conformance probe in crates/ravel-object-store/src/conformance.rs that
  issues a PUT whose response is dropped and asserts the object is readable
  afterwards (the backend half stays an assumption until it exists)"
- `TransientFailure / TransientLeavesNothing` —
  `crates/ravel-ingest/src/shard.rs::FlushCtx::put_data_object_with_retry` —
  "failed_put_leaves_no_object: a conformance probe in
  crates/ravel-object-store/src/conformance.rs that injects a failed PUT and
  asserts no object or partial object is readable afterwards (the backend
  half stays an assumption until it exists)"

### commit (7 rows)

- `BufferedAck` — `crates/ravel-ingest/src/router.rs::write_points` —
  "buffered_ack_returns_before_any_durable_object as a new test in the
  failure-tests crate: no test pins that a buffered acknowledgement precedes
  the data PUT"
- `AckTimeout` — `crates/ravel-ingest/src/error.rs::AckTimeout` —
  "ack_timeout_then_late_commit_is_durable_and_unobservable as a new test in
  the failure-tests crate: issue #1130 tracks the gap"
- `PartialReportingMatchesSignal` —
  `crates/ravel-ingest/src/log_router.rs::await_strict_acks` —
  "partial_multi_shard_commit_reports_durable_tokens for all three signals as
  a new test in the failure-tests crate: no test pins the durable-tokens
  contents across a partial commit for any signal"
- `ClientRetry` — `crates/ravel-ingest/src/log_router.rs::write` —
  "logs_and_spans_lost_ack_retry_serves_the_record_twice as a new test in the
  failure-tests crate: the existing test covers the metrics dedup, not the
  logs and spans duplicate a resend can leave"
- `TombstoneBucket` — `crates/ravel-maintain/src/retention.rs::write_tombstone`
  — "tombstoned_bucket_token_query_reports_tombstoned as a new test in
  ravel-maintain: no test pins the catalog answer through a tombstone write"
- `SupersedeRecord` — `crates/ravel-maintain/src/compact.rs::compact_bucket`
  — "superseded_record_token_query_reports_superseded as a new test in
  ravel-maintain: no test pins the catalog answer through a compaction"
- `DuplicateUnreachable, the at-least-once obligation` —
  `crates/ravel-types/src/lib.rs::CommitToken` —
  "logs_and_spans_lost_ack_retry_serves_the_record_twice as a new test in the
  failure-tests crate: the existing test covers the metrics dedup, not the
  logs and spans duplicate"

  (This is the same proposed test name as `ClientRetry` above, cited against
  a different Rust path and a different TLA+ row; the table names each row's
  cell separately, and both are reproduced here rather than merged.)

### catalog (3 rows)

- `DoFoldCas fold-lifetime bound / DoFoldRebase` —
  `crates/ravel-catalog/src/fold.rs::reconcile_one_bucket` —
  "fold_that_lists_before_a_compaction_and_cas_after_the_sweep_horizon_does_not_name_a_swept_input
  in crates/ravel-catalog/src/fold.rs (no test pins the fold-versus-sweep
  timing race the horizon bounds)"
- `DoSweepSuperseded covering-part guard / CorruptHeadFailsClosedOnDeletePaths`
  — `crates/ravel-maintain/src/reachability.rs::SnapshotReachability::object_gate::ensure_part`
  — "an_unreadable_covering_part_blocks_the_superseded_sweep_fail_closed in
  crates/ravel-maintain/tests/superseded_head_gate.rs (no test pins the
  covering-part-unreadable trigger separately from HEAD-unreadable)"
- `DoSweepSuperseded entry-identity guard / CorruptHeadFailsClosedOnDeletePaths`
  — `crates/ravel-maintain/src/reachability.rs::snapshot_object` —
  "an_undecodable_covering_entry_blocks_the_superseded_sweep_fail_closed in
  crates/ravel-maintain/tests/superseded_head_gate.rs (no test pins the
  entry-identity-undecodable trigger separately from HEAD-unreadable)"

### lifecycle (6 rows)

- `SetRefresh / RefreshFailureNeverSweeps` —
  `crates/ravel-maintain/src/legal_hold.rs::refresh` — "add a sweep-tick test
  that injects a failed refresh and asserts no delete; the tick-skip on a
  failed refresh is not yet a distinct production symbol, only the fallible
  refresh is"
- `RequestErasure` — `crates/ravel-commit/src/keys.rs::erasure_request_key::DREQ_SUFFIX`
  — "gap: no production symbol PUTs the request marker, only the key builder
  and tests exist"
- `CompleteErasure / CompletionImpliesNoPreRewriteExposure` —
  `crates/ravel-maintain/src/erasure_rewrite.rs::bucket_erasure_completion::bucket_serves_subject`
  — "gap: the gate is computed but no production symbol writes the
  completion object"
- `CompleteErasure / CompletionRespectsLegalHold` —
  `crates/ravel-maintain/src/erasure_rewrite.rs::bucket_erasure_completion::bucket_is_held`
  — "gap: no production test isolates the legal-hold branch of
  bucket_erasure_completion from the served-set branch"
- `DreqSweep / DreqSweepRespectsLegalHold` —
  `crates/ravel-maintain/src/sweep.rs::sweep_erasure_requests_inner::chain_groups_held_by_legal_hold`
  — "gap: no production test isolates the legal-hold branch of the
  request-marker sweep from the horizon/reachability branch"
- `PerformRewrite (tombstone guard)` —
  `crates/ravel-maintain/src/erasure_rewrite.rs::erasure_rewrite_bucket::ErasureRewriteOutcome::Tombstoned`
  — "gap: no production test isolates the tombstoned-bucket branch of
  erasure_rewrite_bucket from its other refusal outcomes"

### resharding (3 rows)

- `` `AdmitFailClosed` fence `` — `` `crates/ravel-ingest/src/error.rs::StaleProvisioningView` ``
  — "a fence-off route today has no unit test; the model's no-writer-fence
  control stands in for it"
- `Reshard lead enforcement (CLI)` — `` `services/ravel-cli/src/provision.rs::reshard::MIN_LEAD_HOURS` ``
  — "a direct unit test on the lead check would pin the boundary"
- `` `DoAdmit` write path `` — `` `crates/ravel-ingest/src/router.rs::write_points` ``
  — "a focused test asserting the admitted shard equals the routed count
  would pin `EveryAdmittedWriteInScanSet` at the code level"

### maintenance (6 rows)

- `CliRecord` — `` `services/ravel-cli/src/maintain.rs::compact_bucket` `` —
  "a CLI-publishes-without-ownership test"
- `HeartbeatAndMemoNeverCas` — `` `crates/ravel-fleet/src/worker_set.rs::WorkerSet::write_heartbeat` ``
  — "a test asserting both PUTs use PutMode::Overwrite, not a CAS put-mode"
- `GuardedPublish` — `` `crates/ravel-fleet/src/claim.rs::renew` `` —
  "a ClaimGuard-abandons-on-lost-claim test"
- `UngatedPublish` — `` `services/ravel-cli/src/maintain.rs::compact_bucket` ``
  — "a claim-participation CLI test"
- `AbandonPublish` — `` `crates/ravel-fleet/src/claim.rs::renew` `` —
  "a ClaimGuard-abandons-on-lost-claim test"
- `LostClaimNeverPublishesThroughGuardedPath` —
  `` `crates/ravel-fleet/src/claim.rs::renew` `` — "a ClaimGuard-abandons-on-lost-claim
  test"

Total test-debt rows gathered: 29 (common 4, commit 7, catalog 3, lifecycle
6, resharding 3, maintenance 6).
