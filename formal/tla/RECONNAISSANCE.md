# Reconnaissance for ADR-1113

Tree inspected: origin/main at commit bfae457a, 2026-09-02. Five read-only passes, one per protocol area. Paths and symbol names are given without line numbers.

Each section carries the area's findings and its reconnaissance-matrix rows. The ADR's Context summarizes them; this file is the record.

---

# Recon: object-store contract + commit publication / ack / retry / read-your-write

origin/main bfae457a.

## ADR statuses
ADR-0002 Accepted (verify step never shipped; token v2 per ADR-0010). ADR-0010 Accepted (pinned flush identity s1, token v2 s2, writer identity s3, data CreateIfAbsent + 16 hex s7, skew s8, GC interlock s11, store contract amendment s12). ADR-0050/0051/0052/0055/0058 (amended by 0077)/0059 (d5 hold gate)/0064/0067/0068/0071/0073/0076/0077/0089 Accepted. ADR-0807 Proposed (d3,d5 superseded in-file). ADR-0873 Proposed but `CommitRecord.declared_column_stats` + `ravel_commit::declared_stats::stamp_commit_record` ship. ADR-1029 Proposed yet cited normatively by docs/object-store-contract.md for last_modified time base.

## 1. Object store ops
Trait `ravel_object_store::ObjectStoreBackend` (crates/ravel-object-store/src/lib.rs); blanket for Arc<T>.
Methods: `put(key,data,opts)->PutOutcome`; `get(key,range)->GetOutcome` (S3 splits Full into ranged reads with If-Match; If-Match failure remapped PreconditionFailed->Transient); `put_multipart` (default refuses Permanent; MemoryStore+S3Store override); `head`; `list(prefix,page)->ListPage` (MemoryStore page 1000, `with_page_size`); `list_after` (default client-side filter); `list_delimited`; `delete` idempotent (NotFound=>Ok; `assert_idempotent_delete`); `capabilities()`.
Free fn `list_all` drains pages and dedups by key (HashSet) - only dedup point; direct `list` callers unprotected. MemoryStore never emits duplicates.
Types: `PutMode::{Overwrite, CreateIfAbsent, CasVersion(Version)}`, `PutOptions{mode,checksum}` default Overwrite, `create_if_absent()`, `with_checksum`; `UploadChecksum::Crc32c`; `GetRange::{Full,Range,Suffix}`; `PutOutcome{etag,version}`, `GetOutcome`, `ObjectMeta{key,size,etag,version,last_modified_unix_ms}`, `ListPage{objects,next}`.
Errors: `StoreError::{NotFound, AlreadyExists, PreconditionFailed, AccessDenied, Throttled{retry_after_ms}, Timeout, Corrupted, InvalidRange, Transient, Permanent}`; `is_retryable` = Throttled|Timeout|Transient. `s3::map_put_error` normalizes 409/412 by caller mode: CreateIfAbsent->AlreadyExists, CasVersion->PreconditionFailed. `MemoryStore::put`: AlreadyExists for present key under CreateIfAbsent; PreconditionFailed for version mismatch AND missing key under CasVersion.
Capabilities `{consistent_read, consistent_list, create_if_absent, cas_version, suffix_range, upload_checksum, prefix_list, multipart}`, `mandatory()`, `satisfies`; gate `ravel_server::store::check_capabilities` / `required_capabilities(mode)` (multipart for Maintain) in `build_store`.
Conformance: `conformance::{run_conformance_suite, Property::{ConditionalWriteCreateIfAbsent, ConditionalWriteCasVersion, ConsistentReadAfterWrite, ConsistentListAfterWrite}, probe_object_lock, ...}`; cross-page listing + multipart visibility probes NOT implemented (doc admits).
CAS used by: catalog HEAD, provisioning, fleet claims, auth token map, metrics metadata, key epoch. Never on commit path.

Doc/impl disagreements: (1) upload_checksum on S3 is config-conditional (`S3Store::capabilities` = `upload_integrity.is_enabled()`), but `Capabilities::mandatory` doc comment and docs/catalog-and-mvcc.md step 2 say always false; caller CRC32C never on wire remains true. (2) docs/ingest.md flush step 2 says data PUT (Overwrite); impl is CreateIfAbsent (`publish::put_data_object`). (3) delete idempotency: MemoryStore cannot return NotFound; treat delete as total. (4) cross-page duplicates never produced in-tree. (5) last_modified advisory on commit path, but ADR-0058 d3 derives reconstructed created_unix_ns from it (dedup tiebreak + horizon anchor). (6) contract doc cites Proposed ADR-1029. (7) InstrumentedStore passes put_multipart uncounted.

## 2. Commit protocol steps (metrics shard.rs; log_shard.rs/span_shard.rs mirror)
0 admission `IngestRouter::write_points` (`IngestByteBudget::try_charge`).
0a marker lookup (logs/spans keyed) `ravel_ingest::read_marker` from services/ravel-server/src/logs_ingest.rs, traces_ingest.rs; hit -> replay.
1 route + enqueue `ShardMsg::Write` with oneshot ack (Strict, non-empty) or None (Buffered). send fail -> `ShardUnavailable`.
1b BUFFERED ACK returns here (`Ok(WriteReceipt::default())`), not durable.
2 `ShardActor::handle_write` -> `TenantBuf::merge`; waiter pushed.
3 PIN: `ShardActor::flush_tenant` builds `PinnedFlush{seq, ingest_hour_bucket, flush_open_ns, deadline_ns, series, waiters, charges}`; seq = next_seq++; `checked_ingest_hour_bucket(flush_open_ns)`; deadline = flush_open_ns + max_flush_lifetime.
3b permit `semaphore.acquire_owned()`, `InFlightFlushGuard`, `handle_flush_join_result`.
4 serialize RSEG + blake3 once: `FlushCtx::run_flush` -> `SegmentWriter::write_histograms_with_exemplars`.
5 key `ravel_commit::keys::data_key` -> `t/<tenant>/<signal>/l0/<shard>/<writer_id>.<epoch>.<seq:020>.<hash16>.rseg`.
6 DATA PUT CreateIfAbsent: `FlushCtx::put_data_object_with_retry` -> `ravel_commit::publish::put_data_object`. DURABLE. crash after -> orphan, invisible, GC after grace+max_flush_lifetime.
7 `ravel_commit::record::build(NewCommitRecord{created_unix_ns: flush_open_ns, ingest_hour_bucket})`, `record::validate`.
8 COMMIT PUT CreateIfAbsent: `FlushCtx::publish_with_retry` -> `publish::publish` -> `publish_with_rng` -> `keys::commit_key_for_record` -> put create_if_absent + Crc32c. DURABLE = VISIBILITY POINT.
9 token `record::token_for` -> `ravel_types::CommitToken`.
10 `FlushCtx::ack_waiters` (waiters moved out at pin time: ack isolation).
11 router collects tokens: metrics `IngestRouter::write_points`; logs `LogIngestRouter::await_strict_acks`; spans inline loop.
11b MARKER PUT (logs/spans keyed, all shards durable): `ravel_ingest::write_marker` CreateIfAbsent, after router.write Ok, before return. crash between commit and marker -> duplicate on keyed retry.
12 STRICT ACK: `otlp_http::otlp_response` sets `COMMIT_TOKEN_HEADER` (x-ravel-commit-token) via `encode_commit_tokens`; grpc otlp_grpc*.rs; remote_write.rs (Strict only); otap_grpc.rs.
Mode: `otlp_http::write_mode_from_headers` (x-ravel-ingest-mode: buffered).
Retry: outer `put_data_object_with_retry`/`publish_with_retry` with `IngestConfig::put_retry_max_attempts`, `FlushCtx::backoff_sleep` on injected Clock, each attempt raced against deadline via `FlushCtx::bound_to_deadline` (select! vs Clock::sleep). Inner `RetryPolicy{max_attempts:0}` for commit (one attempt per call); `put_data_object` single attempt. `RetryPolicy::default()` (5, 20ms, 2s) for direct publish callers (maintain, reconstruct). Deadline -> `WriteError::Abandoned` (ADR-0010 s11 interlock).
SplitBrain: `publish_with_retry` panics on `PublishError::SplitBrain`; `handle_flush_join_result` resume_unwind -> shard actor dies -> `ShardUnavailable`, `IngestRouter::mark_shard_dead`. Permanent per-shard stop.
Undocumented crash points: `AckTimeout` (all three routers drop join_all on ack_deadline; flush continues and may commit; token unobservable). Multi-shard partial commit.

## 3. Pinned identity / reuse prevention
seq per (writer_id, epoch, shard) at pin time (ADR-0067 accepts out-of-seq publication, gaps). writer_id fresh Uuid per shard-actor set (`IngestRouter::with_rng` factory). Bytes+blake3 once, reused across retries. `PinnedFlush` moved by ownership; `FlushCtx` Arc immutable.
ASYMMETRY: commit record enforced: `publish_with_rng` on AlreadyExists -> `resolve_already_exists` GET+decode, compare content_hash; equal -> idempotent same token; differ -> `PublishError::SplitBrain{this, stored}`. Data object NOT enforced: `put_data_object` returns Ok on AlreadyExists with no read-back; safety by pinning + key (writer.epoch.seq.hash16); hash16 not load-bearing. Model: data-PUT idempotency = assumption; commit-PUT = checked.
Reader: `record::build` computes object_key; `keys::reconstruct_data_key`, `keys::verify_object_key` fatal on mismatch. `record::validate`: hour check skipped when either created_unix_ns or ingest_hour_bucket is 0.

## 4. Multi-shard, markers, at-least-once
Fan-out: metrics `IngestRouter::write_points` `tokens.push(inner?)` early return, siblings dropped, no PartialWrite. Logs `LogIngestRouter::await_strict_acks` -> `LogWriteError::PartialWrite{inner, durable}`, `durable_tokens()`. Spans inline early return; `SpanWriteError` no PartialWrite. AckTimeout: no durable tokens recoverable anywhere.
Markers crates/ravel-ingest/src/idempotency.rs: `keyhash32`, `marker_prefix`, `marker_key`, `encode_marker/decode_marker`, `read_marker`, `read_marker_at`, `write_marker` (CreateIfAbsent; lost race -> read winner `WriteOutcome::Existing`), `IdempotencyReceipt{written_count, commit_token}`, `LookupOutcome::{Hit,Miss,Corrupt}`, `MARKER_SUFFIX="idm"`, `IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS=1`; `DEFAULT_IDEM_DEDUP_WINDOW_HOURS=24` in crates/ravel-maintain/src/config.rs; `MAX_IDEMPOTENCY_KEY_BYTES=128` otlp_http.rs. Key `t/<th>/<signal>/idem/<keyhash32>.<ingest_hour>.idm`. Lookup: prefix list_all, window [now-window, now+1h], highest hour, exact GET. Marker written only when `encode_commit_tokens` Some (never buffered, never Err/PartialWrite). Marker hour = `otlp_http::request_ingest_hour_bucket(ingest_ts_ns)` at request receive, NOT flush-open hour (docstring on `marker_key` claims otherwise). Fail-open: Miss/Corrupt/Err all fall through (warn); write failure warn + ack; `request_ingest_hour_bucket == None` disables both.
Replay: `LogIngestOutcome{replayed_commit_token}`, `commit_token_header`.
ADR-0058 all four implemented: `orphans_present` gauge (maintain.rs), services/ravel-cli/src/reconstruct.rs (metrics+logs only, refuses spans), IAM template test, docs/guides/disaster-recovery.md. ADR-0059 d5: `FaultStore::hold`, `GateHandle`, `Occurrence`.

## 5. Commit-token resolution
`CommitToken{shard, writer_id, epoch, seq, ingest_hour_bucket}`; decode distinguishes `UnsupportedCommitTokenVersion` vs `InvalidCommitToken`. Query: handlers.rs `decode_commit_tokens(params.all("min_commit_token"))`; federation forwards. `Catalog::resolve` -> `resolve_impl` -> `resolve_min_token` -> `commit_key_for_token` exact GET.
Four outcomes: (1) present -> `SegmentOrigin::TokenResolved` (exempt from max_segments cap). (2) tombstone: `resolve_min_token_fallback` lists `commit_shard_hour_prefix`; `BucketEntry::Tombstone` -> Ok with ZERO segments + `invalidate_bucket_cache` (documented in catalog-and-mvcc step 5, NOT consistency-model). (3) compaction/rewrite: fallback loads CompactionRecord/RewriteRecord, `resolve_rewrite_supersession`, `check_rewrite_siblings`; live compaction inputs contain (writer,epoch,seq) -> `build_l1_segment_ref`; live rewrite -> `build_rewrite_l1_segment_ref`; superseded skipped. (4) missing -> `CatalogError::UnsatisfiableToken` -> HTTP 503 `MSG_UNSATISFIABLE`.
Two independent retry budgets in `resolve_min_token` (NotFound, transient), `MIN_TOKEN_RETRY_DELAY`. Vanished data object -> `QueryError::SnapshotInvalidated` after one re-resolve (engine.rs), 503; distrib status.

## 6. Tests
Contract crates/ravel-object-store/tests/contract.rs `run_contract_suite`: assert_satisfies_mandatory_capabilities, assert_create_if_absent_atomicity, assert_cas_version_semantics, assert_range_and_suffix_reads, assert_paginated_listing_completeness, assert_start_after_listing, assert_delimited_listing, assert_idempotent_delete, assert_upload_checksum_verification, assert_multipart_*; backends memory_store_contract, memory_store_paged_contract, fault_store_empty_plan_contract, instrumented_memory_store_contract, kms_routing_store_contract_with_no_configured_tenants, minio_contract, floci_contract.
S3 faults tests/s3_http_faults.rs (many).
Publish crates/ravel-commit/src/publish.rs: publish_then_get_round_trips, republishing_identical_record_is_idempotent, republishing_with_different_content_hash_is_split_brain, put_data_object_is_idempotent_on_already_exists. record.rs: build_computes_object_key_and_validates, token_for_round_trips_identity_fields, validate_allows/rejects_hour_bucket_*, validate_skips_hour_check_when_created_unix_ns_is_zero. keys: data_key_round_trips, reconstruct_data_key_matches_data_key, verify_object_key_detects_mismatch, commit_key_for_token_matches_commit_key.
Crash matrix crates/ravel-failure-tests/tests/crash_matrix.rs: crash_before_data_put_leaves_nothing_stored_or_visible, crash_after_data_put_before_commit_orphans_then_spec_model_gc_sweeps_after_grace (GC half is spec_model_sweep_orphans not real sweeper; real: crates/ravel-maintain/tests/sweep_crash_matrix.rs orphan_gc_respects_live_records_and_age_gate, row12_token_get_notfound_post_sweep_found_in_input_list). Rows 3-4 untested there.
ack_and_duplicates.rs: ack_lost_after_commit_then_client_retry_dedups_to_one_value, duplicate_otlp_delivery_normalized_twice_does_not_double_count (metrics).
retry_and_restart.rs: retry_storm_on_every_put_fault_kind_still_commits_exactly_once, restart_from_empty_local_state_sees_both_writer_generations.
completion_ordering.rs: multipart_parts_completing_out_of_submission_order_assemble_correctly, cross_shard_commit_visibility_is_per_shard_independent.
Token: catalog.rs min_token_resolves_even_when_its_hour_bucket_is_outside_the_listing_window, min_token_unsatisfiable_when_commit_record_is_missing, min_token_transient_then_notfound_still_resolves_the_real_commit, min_token_two_notfound_blips_surface_unsatisfiable_not_over_probing, token_over_tombstoned_bucket_is_satisfied_with_zero_segments, tombstoned_bucket_is_excluded_from_resolution, tombstone_observation_invalidates_cached_commit_records; tests/compaction_resolution.rs token_fallback_serves_via_l1_when_commit_record_swept, token_fallback_tombstone_satisfied_with_zero_segments, token_fallback_unsatisfiable_when_not_in_any_input_list; ordering_and_catalog.rs unsatisfiable_min_commit_token_errors_instead_of_returning_stale_data; ravel-query e2e min_commit_token_finds_segment_outside_the_listing_window; recent_hours_admission token_segments_always_admitted; error.rs unsatisfiable_token_is_a_distinct_stable_class.
MVCC: within_segment_duplicate_and_out_of_order_timestamps_resolve_correctly, cross_segment_duplicate_timestamp_later_commit_wins_regardless_of_publish_order. concurrent.rs concurrent_ingest_and_query_never_sees_a_partially_visible_flush. corruption.rs two tests.
folder_crash_matrix.rs: folder_down_for_hours_never_loses_data_only_widens_listing, corrupt_head_falls_back_to_listing_never_to_an_error, missing_snapshot_part_falls_back_to_listing_after_one_head_reread, concurrent_folders_race_head_cas_without_losing_or_duplicating_data, stale_head_cache_widens_listing_but_never_misses_new_data, commit_in_wrongly_sealed_bucket_is_invisible_until_head_rebuild_repairs_it.
ravel-sim tests: ack_implies_durable_and_token_resolves.rs (vacuous re fold), hold_release_gates_drive_cycle.rs, compaction_equivalence_under_faults.rs, compaction_phase_faults.rs, fold_makes_resolve_list_free.rs, wide_interval_spans_ingest_hours.rs.
Gaps: metrics/spans partial commit; AckTimeout late-landing flush; cross-page duplicate keys; multipart-complete visibility probe; span keyed partial.

## 7. Findings
1 docs/ingest.md says data PUT Overwrite; impl CreateIfAbsent. 2 catalog-and-mvcc.md step 2 upload_checksum absolute claim stale. 3 `Capabilities::mandatory` doc comment stale. 4 consistency-model.md read-your-write lists 2 outcomes; there are 4; tombstone returns Ok empty. 5 consistency-model.md treats PartialWrite as general; logs only; spans cannot even log. 6 marker hour derived at request receive, not flush open; `marker_key` docstring wrong. 7 crash matrix lacks AckTimeout row. 8 ADR-0058 d2 reconstruct refuses spans (narrower than ADR). 9 cross-page duplicates unexercised. 10 validate skips hour check on 0. 11 PreconditionFailed remapped to Transient on split read. 12 nested retry loops. 13 seq at pin time, out-of-order publication. 14 SplitBrain = permanent per-shard stop. 15 contract doc cites Proposed ADR-1029.
No Accepted-ADR-without-symbol in this area. Inverse: ADR-0873 Proposed but shipped.

## 8. Matrix
| Protocol | Normative source | Rust implementation | Existing tests | Status | Model priority |
|---|---|---|---|---|---|
| Conditional-write algebra | object-store-contract.md; ADR-0010 s12 | `PutMode`, `StoreError`, `s3::map_put_error`, `MemoryStore::put` | assert_create_if_absent_atomicity, assert_cas_version_semantics | Implemented | P0 |
| List pagination + duplicates | object-store-contract.md | `list`/`list_after`, `list_all` | assert_paginated_listing_completeness | Implemented; dups never produced | P1 |
| Idempotent delete; last_modified advisory | contract | `delete`, `ObjectMeta` | assert_idempotent_delete | Implemented | P2 |
| Multipart | contract | `MultipartUpload`, `PartSequence` | assert_multipart_*, completion_ordering | Implemented; no production caller | P2 |
| Pinned flush identity | ADR-0010 s1; catalog-and-mvcc | `PinnedFlush`, `ShardActor::flush_tenant`, `FlushCtx::run_flush` | retry_storm_* | Implemented | P0 |
| Two-object commit | ADR-0002; ADR-0010 s7 | `put_data_object`, `publish_with_rng`, `data_key`, `commit_key_for_record` | publish tests; crash_matrix rows 1-2 | Implemented; ingest.md wrong | P0 |
| Retry idempotency asymmetry | ADR-0002 | `put_data_object` vs `resolve_already_exists`/`SplitBrain` | put_data_object_is_idempotent_on_already_exists, republishing_with_different_content_hash_is_split_brain | Implemented | P0 |
| Strict ack point | consistency-model | `ack_waiters` -> router -> `otlp_response` | ack_and_duplicates, sim ack_implies_durable | Implemented | P0 |
| Buffered ack | consistency-model | `write_points` early return | none | Implemented | P1 |
| Flush abandonment / GC interlock | ADR-0010 s11 | `bound_to_deadline`, `Abandoned` | crash_matrix row 2, sweep_crash_matrix | Implemented | P1 |
| Multi-shard partial commit | consistency-model | logs `PartialWrite`; metrics/spans none | partial_write_carries_tokens_* | Logs only | P0 |
| AckTimeout late flush | undocumented | router timeout(join_all) | none | Implemented, undocumented | P0 |
| Idempotency markers | consistency-model; ADR-0051 s5 | idempotency.rs, logs_ingest.rs, traces_ingest.rs | unit+proptests; sweep tests | Implemented; hour derivation diverges | P1 |
| Marker fail-open | consistency-model | Miss/Corrupt/Err fall through | handler tests | Implemented | P1 |
| Token v2 | ADR-0010 s2 | `CommitToken`, `token_for`, `encode_commit_tokens` | round-trip tests | Implemented | P1 |
| Token present | consistency-model | `resolve_min_token`, `TokenResolved` | min_token_resolves_* | Implemented | P0 |
| Token retired | catalog-and-mvcc step 5 | `resolve_min_token_fallback`, `resolve_rewrite_supersession` | token_fallback_*, tombstone tests | Implemented; absent from consistency-model | P0 |
| Token missing | consistency-model | `UnsatisfiableToken` -> 503 | min_token_unsatisfiable_*, unsatisfiable_min_commit_token_errors_* | Implemented | P0 |
| Token retry budgets | catalog-and-mvcc step 4 | two counters | min_token_transient_*, min_token_two_notfound_* | Implemented | P1 |
| Snapshot invalidation | ADR-0010 s11 | `SnapshotInvalidated` | engine unit | Implemented | P1 |
| Capability gate | contract; ADR-0050 | `check_capabilities` | assert_satisfies_mandatory_capabilities | Implemented | P2 |
| Qualification | contract; ADR-0050 s6 | `run_conformance_suite` | conformance tests | 2/6 probes missing | P2 |
| Reconstruction/DR | ADR-0058 | `orphans_present`, reconstruct.rs | reconstruct tests | Implemented; spans unsupported | P2 |
| FaultStore + hold gates | contract; ADR-0059 d5 | `FaultStore`, `FaultPlan`, `Rule`, `Sequence`, `ScriptedFault`, `FaultKind`, `hold`, `GateHandle`, `Occurrence` | completion_ordering, retry_and_restart, hold_release_gates | Implemented | P1 |

---

# Recon: catalog fold, snapshots, compaction, MVCC

Worktree: `/private/tmp/claude-501/-Users-pmoust-nofire-store/77fda7c0-8229-48d6-be50-41849f388093/scratchpad/wt-tla`
(origin/main at `bfae457a`). No line numbers below by design; every claim is
anchored on a file path plus a Rust symbol.

## 0. ADR status roll-call (as read from each file)

| ADR | Status line as written |
|---|---|
| 0018 l0-l1-compaction | `Status: Accepted` (amended by 0026, 0027, and **0092** which reverses verbatim run preservation for metrics) |
| 0019 age-based-retention | `Status: Accepted` |
| 0020 metric-index | `Status: Accepted` |
| 0027 single-rseg-version-pre-release | `Status: Accepted (superseded at first public release by ADR-0066)` |
| 0032 rlog-compaction-and-generic-maintain | `Status: Accepted` + two in-file amendments each `Status: accepted` |
| 0048 maintenance-safety-and-coverage | `Status: Accepted` |
| 0050 fail-closed-isolation | `Status: Accepted` |
| 0056 catalog-resolve-prefix-list-traversal | `Status: Accepted` |
| 0058 commit-record-reconstruction-and-dr-posture | `Status: Accepted` |
| 0059 durability-hardening | `Status: Accepted` |
| 0063 multi-part-parallel-fold | `Status: Accepted` |
| 0064 selective-subject-erasure | `Status: Accepted` |
| 0065 leased-distributed-maintenance | `Status: Accepted` |
| 0066 format-migration-machinery | `Status: Accepted` |
| 0073 recent-hours-read-path | `Status: Accepted` |
| 0078 fold-retention-frontier-deployment-default | `Status: accepted` |
| 0092 run-merged-l1-and-rseg-v7 | `Status: Accepted` |
| 0815 clustered-compaction-and-object-pruning | `Status: Proposed.` |
| **0849 snapshot-bound-index-plane** | **no `Status:` line anywhere in the file** |
| 0850 logs-typed-column-statistics | `Status: Accepted.` |
| 0873 commit-record-declared-min-max | `Status: Proposed. Issue #873` |
| 0942 cstat-snapshot-part-binding | `Status: Accepted (2026-08-30)` |
| 0979 bounded-memory-rlog-compaction-merge | `Status: Proposed` |
| 1029 advisory-compaction-claims | `Status: Proposed` |

---

## 1. Logical state of the catalog

All keys are under one bucket root. `<th>` = 32 hex chars of `TenantHash`;
`<sig>` = one-letter signal prefix from `Signal::key_prefix()`
(`m` metrics, `l` logs, `s` spans, `a` alerts, `u` audit).

### 1.1 Data / commit plane (per shard, per ingest hour)

| Object | Key shape | Builder / verifier symbol | Publish mode | Mutability |
|---|---|---|---|---|
| L0 data object | `t/<th>/<sig>/l0/<shard>/<writer_id>.<epoch>.<seq>.<hash16>.rseg` | `ravel_commit::keys::data_key`, `keys::parse_data_key`, `keys::reconstruct_data_key`, `keys::verify_object_key` | `ravel_commit::publish::put_data_object`, `PutMode::CreateIfAbsent` (`AlreadyExists` = success) | immutable, content-addressed (`hash16` = first 16 hex of the object's blake3) |
| L0 commit record | `t/<th>/<sig>/c/<shard>/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt` | `keys::commit_key`, `keys::commit_key_for_record`, `keys::commit_key_for_token`, `keys::parse_commit_key`; body `ravel_commit::record::{build, validate, encode, decode}`, `record::FORMAT_VERSION = 1` | `ravel_commit::publish::publish` / `publish_with_rng`, `PutMode::CreateIfAbsent`; `AlreadyExists` → `publish::resolve_already_exists` (same `content_hash` = idempotent success, different = split-brain, `PublishError`) | immutable |
| L1 part | `t/<th>/<sig>/l1/<shard>/<ingest_hour>/<input_set_hash16>.<part:04>.<hash16>.rseg` | `keys::l1_part_key`, `keys::parse_l1_part_key`, `keys::reconstruct_l1_part_key(&CompactionRecord, &CompactionPart)`; rewrite outputs `keys::reconstruct_rewrite_part_key` | `ravel_maintain::build::put_part` / `put_part_with_ledger`, `PutMode::CreateIfAbsent` | immutable, content-addressed (`hash16` = the part object's own blake3, carried on `ravel_maintain::build::BuiltPart`) |
| Compaction record | `t/<th>/<sig>/c/<shard>/<ingest_hour>/l1.<input_set_hash16>.cmt` | `keys::compaction_record_key`, `keys::compaction_record_key_for`, `keys::verify_compaction_record_key`; tag const `keys::COMPACTION_RECORD_TAG = "l1"` | `ravel_maintain::publish::publish_record` → `publish_record_with_conservation`, `PutMode::CreateIfAbsent` | immutable |
| Rewrite record (selective erasure, ADR-0064) | `t/<th>/<sig>/c/<shard>/<ingest_hour>/rw.<input_set_hash16>.cmt` | `keys::rewrite_record_key`, `keys::rewrite_record_key_for`, `keys::verify_rewrite_record_key`, `keys::parse_rewrite_record_key`; tag const `keys::REWRITE_RECORD_TAG = "rw"`; hash `ravel_commit::erasure::compute_rewrite_input_set_hash` (distinct blake3 domain from compaction, so the two can never collide) | `ravel_maintain::rewrite` / `erasure_rewrite`, via `publish::publish_record_with_conservation` with a non-exact predicate, `PutMode::CreateIfAbsent` | immutable |
| Retention tombstone | `t/<th>/<sig>/c/<shard>/<ingest_hour>/retire.tmb` (fixed filename, `keys::TOMBSTONE_FILENAME`) | `keys::retention_tombstone_key`, `keys::retention_tombstone_key_for` | `ravel_maintain::retention::write_tombstone`, `PutMode::CreateIfAbsent`; `AlreadyExists` mapped to `Ok` | immutable; irreversible (ADR-0019 decision 2) |
| Erasure request / completion | `t/<th>/<sig>/del/<request_id>.dreq` / `.done` | `keys::erasure_request_key(_for)`, `keys::erasure_completion_key(_for)`, `keys::verify_erasure_request_key` | CreateIfAbsent | immutable |
| Idempotency marker (logs/spans) | `t/<th>/<sig>/idem/<keyhash32>.<ingest_hour>.idm` | `crates/ravel-ingest/src/idempotency.rs`; frame magic `RIDM` v1 | additive write | immutable until swept |
| Maint cursor | `t/<th>/<sig>/maint/<shard>/cursor` | `keys::maint_cursor_key` | CAS | mutable, advisory. **Dead in the running worker** , ADR-0018 §7 records that `scan_and_compact` (its only consumer) has no non-test caller |

**Bucket-key classification is one funnel:** `ravel_commit::keys::partition_bucket_entry`
returns `keys::BucketEntry::{CommitRecord, CompactionRecord, RewriteRecord, Tombstone}`
and errors with `KeyError::UnknownBucketEntryShape` otherwise. Two callers:
`Catalog::process_bucket` (resolve; propagates the error → fail-loud) and
`Catalog::classify_bucket` (fold; counts into `FoldReport::layout_drift_count`
and `warn!`-skips). This asymmetry is deliberate and documented in
`docs/catalog-and-mvcc.md`.

### 1.2 Catalog (index) plane , per `(tenant, signal)`, no shard dimension

| Object | Key shape | Symbols | Publish mode | Mutability |
|---|---|---|---|---|
| Snapshot part | `t/<th>/catalog/<sig>/snap/<watermark>.<hash16>.csnap` | key `ravel_catalog::fold::part_object_key`; body `snapshot_format::encode_part_ranged` / `decode_part`; envelope magic `snapshot_format::MAGIC = "RCS1"`, `VERSION = 1`; header field `SnapshotPartHeader.min_hour`; per-part entry ordering + duplicate check `snapshot_format::part::validate_entries` | `PutMode::CreateIfAbsent` inside `Catalog::fold`, `UploadChecksum::Crc32c` attached; `StoreError::AlreadyExists` swallowed as success | immutable, content-addressed |
| Name postings | `t/<th>/catalog/<sig>/idx/<watermark>.<hash16>.npost` | `fold::postings_object_key`; `snapshot_format::encode_postings`; magic `POSTINGS_MAGIC = "RNP1"`, `POSTINGS_VERSION = 1` | CreateIfAbsent, best-effort (`Ok`/`AlreadyExists` → attach ref; any other error → `warn!` and fold without a postings ref) | immutable |
| Column statistics | `t/<th>/catalog/<sig>/idx/<watermark>.<hash16>.cstat` | `fold::column_stats_object_key`; `snapshot_format::encode_column_stats_v2`; magic `COLUMN_STATS_MAGIC = "RCST"`, `COLUMN_STATS_WRITE_VERSION = 2`, `COLUMN_STATS_ACCEPTED_READ_VERSIONS = [1, 2]` | CreateIfAbsent, best-effort | immutable |
| **HEAD** | `t/<th>/catalog/<sig>/HEAD` | `ravel_catalog::fold::head_object_key` (`pub(crate)`); duplicated locally as `ravel_maintain::sweep::catalog_head_key` and `ravel_maintain::retention::catalog_head_key` because no public builder is exported. Body: bare protobuf `ravel.catalog.v1.SnapshotHead`, no envelope; `snapshot_format::head::{encode_head, decode_head, validate_head}`, `HEAD_FORMAT_VERSION = 1` | **`PutMode::CasVersion(version)`** when the fold read a `Valid` or `Corrupt` HEAD; **`PutMode::CreateIfAbsent`** when `Absent`; **never written at all** when `UnsupportedVersion` | **the single mutable object and the single unit of atomic visibility** |

`SnapshotHead` fields the model needs: `format_version`, `tenant_hash`,
`signal`, `shard_count` (the fan-out ceiling at fold time, from
`fold::fold_shard_ceiling`), `watermark_hour`, `parts:
repeated SnapshotPartRef {key, blake3, size, entry_count, watermark_hour,
min_hour}`, `postings: Option<SnapshotPostingsRef>`, `column_stats` (field 11,
v1 identity-keyed), `column_stats_part` (field 13, v2 part-hash-keyed),
`folder_id`, `created_unix_ns`, `shard_generation_count`.

`validate_head` enforces: `format_version == 1` (else
`SnapshotFormatError::UnsupportedHeadVersion`), 16-byte `tenant_hash`,
16-byte `folder_id`, non-empty `parts`, 32-byte per-part `blake3`, non-empty
part keys, and , only when `parts.len() > 1` , per-part `min_hour <=
watermark_hour`, ascending `min_hour`, disjoint ranges, and
`watermark_hour == max over parts`.

`SnapshotEntry` (in a part) carries `level` (0 or 1), `shard`,
`ingest_hour_bucket`, `writer_id`, `writer_epoch`, `writer_seq`,
`content_hash`, `object_size`, `min/max_event_ts_ns`, `sample_count`,
`series_count`, `segment_format_version`, `created_unix_ns`,
`declared_column_stats` (field 15). For a **level-1** entry the `writer_*`
slots are overloaded: `writer_id` holds the parent record's 32-byte
`input_set_hash`, `writer_epoch` holds `part_index`
(`fold::build_l1_snapshot_entry`, `fold::build_rewrite_l1_snapshot_entry`);
`part::validate_entries` enforces 16 bytes at level 0 and 32 at level 1.

### 1.3 Root-level (no tenant dimension)

`sys/maintain/workers/<pid>` (Overwrite, `ravel_fleet::worker_set`),
`sys/maintain/memo/<pid>` (Overwrite, `ravel_maintain::memo_snapshot`),
`sys/maintain/claims/compaction/<work_id_hex>` (CreateIfAbsent then
`CasVersion`, `ravel_fleet::claim`, `claim::COMPACTION_CLAIMS_PREFIX`) ,
see finding **F2**: implemented, documented as live in the key layout, and
**called by nothing**.

---

## 2. The fold protocol as ordered durable steps

Entry point: `Catalog::fold(tenant, signal, folder_id, now_ns,
_transactions: &[Transaction], default_retention_ns: Option<i64>)` in
`crates/ravel-catalog/src/fold.rs`. Returns `FoldReport`.
`Transaction` has no public constructor, so `_transactions` is always empty
(dead extension point).

Production driver: `services/ravel-server/src/fold.rs` (`spawn`, `run_loop`,
one loop per `FOLD_SIGNALS` entry, `DEFAULT_FOLD_INTERVAL = 5 min`),
tenants re-enumerated each tick via
`crate::tenant_discovery::discover_and_restrict_by_lifecycle`. On-demand
admin path: `services/ravel-server/src/fold_on_demand.rs`. CLI:
`services/ravel-cli/src/catalog.rs::fold` / `inspect` / `verify`.

### Pre-loop (once per `fold` call, not per CAS attempt)

| # | Step | Symbol | Durable effect |
|---|---|---|---|
| P1 | read generation history fresh | `Catalog::read_scan_generations` | none (read) |
| P2 | read `t/<th>/config` | `crate::tenant_config::read_config_values`; failure → `warn!` and `None` | none |
| P3 | resolve effective retention | `TenantConfig.retention_ns` **or** `default_retention_ns` (ADR-0078 overlay) | none |
| P4 | construct `column_stats_build::SegmentColumnStatsCache` | reused across CAS attempts | none |

### CAS attempt loop (`loop { ... }`, bounded by `MAX_HEAD_CAS_ATTEMPTS = 8`)

| # | Step | Symbol | Crash here leaves |
|---|---|---|---|
| 1 | GET HEAD, classify | `Catalog::get_head` → `HeadState::{Valid{head,version}, Absent, Corrupt{version}, UnsupportedVersion{format_version}}` | nothing |
| 2 | compute watermark; bail if not advanced | `fold::sealed_watermark_hour`, `fold::no_op_report` | nothing |
| 3a | `Valid` → load + blake3-verify every part | `Catalog::load_previous_entries` | nothing |
| 3b | `Valid` but a part unreadable, or `Absent`/`Corrupt` → **rebuild** | `Catalog::discover_buckets` (delimited LIST per shard, then per-bucket LIST) | nothing |
| 3c | `UnsupportedVersion` → **hard error, no write** | `CatalogError::UnsupportedHeadVersion` | nothing |
| 4 | incremental hour range `(wm_old, wm_new]` | `fold::incremental_buckets`, `fold::fold_shard_ceiling` | nothing |
| 5 | concurrent per-bucket LIST | `Catalog::discover_bucket_listings` (`buffered(fold_bucket_concurrency)`, input order preserved so the fold stays byte-reproducible) | nothing |
| 6 | classify each bucket, merge entries | `Catalog::classify_bucket` → `fold::fold_in_entry` (dedup by `fold::entry_identity`) | nothing |
| 7 | fixed-window reconcile | `fold::hour_range_buckets`, `fold::bucket_needs_reconcile`, `Catalog::reconcile_one_bucket`, `fold::dedup_contribution`, `fold::same_entry_set` → `dirty_hours` | nothing |
| 8 | retention-frontier reconcile | `fold::retirement_frontier_hour`, `fold::frontier_hour_set_buckets`, same `reconcile_one_bucket` | nothing |
| 9 | global sort by `(ingest_hour_bucket, shard, writer_id, writer_epoch, writer_seq)` | inline in `fold` | nothing |
| 10 | partition into hour-range parts | `fold::partition_parts` → `fold::PartSpan` | nothing |
| 11 | build carry-forward reuse map | `existing_by_blake3`, gated on `!rebuilt` **and** `!fold::part_covers_dirty_hour` | nothing |
| **12** | **encode + PUT each changed part, CreateIfAbsent** | `snapshot_format::encode_part_ranged`, `blake3::hash`, `fold::part_object_key`, `store.put(..., PutOptions::create_if_absent().with_checksum(Crc32c))` | **orphan parts.** HEAD unchanged; the old snapshot is fully intact; the k new parts are invisible. Retry recomputes byte-identical parts (deterministic input + bucket order), lands on the same content-addressed keys, and `AlreadyExists` makes the retry free. Never-referenced parts age out under `sweep_unreferenced_catalog_objects` |
| 13 | postings build/PUT (best effort) | `Catalog::build_postings`, `Catalog::load_previous_postings`, `fold::fetch_segment_names`, `snapshot_format::encode_postings` | orphan `.npost` |
| 14 | column-stats v1 + v2 build/PUT (best effort) | `column_stats_build::*`, `snapshot_format::encode_column_stats_v2`, `fold::sort_and_dedup_part_segments` | orphan `.cstat` |
| **15** | **encode HEAD and publish** | `snapshot_format::encode_head`; `put_mode` = `CasVersion(version)` for `Valid`/`Corrupt`, `CreateIfAbsent` for `Absent`, hard error for `UnsupportedVersion` | **this is the only visibility event.** Crash before it = nothing published. Crash after it = fold complete |
| 16 | CAS lost (`PreconditionFailed` or `AlreadyExists`) | `attempt += 1`; loop back to step 1 | nothing; a full re-read + rebase |
| 17 | budget exhausted | `CatalogError::FoldCasRetriesExhausted { attempts, watermark_hour }` | nothing |

### Racing folders

Any number may run. The parts they write are content-addressed, so two folders
folding the same input write the same key and `AlreadyExists` is idempotent
success. Serialization happens exactly once, at the HEAD `CasVersion` PUT. The
loser re-GETs HEAD next iteration; if the winner's `watermark_hour >=` its own,
the top-of-loop check returns `no_op_report` cleanly; otherwise it rebases onto
the winner's part set (only the tail plus any dirty parts differ) and retries.
There are **no per-part CAS operations** , ADR-0063 §5 explicitly refuses a
multi-object atomic commit.

### What a stale HEAD reader sees

A reader either sees the old HEAD (old complete part set , every part still
present, because a superseded part is only deleted after HEAD stops naming it
**plus** `protection_horizon_ns`) or the new HEAD (new complete set, all parts
PUT before the CAS was attempted). Nothing in between is nameable. Staleness
sources: `HeadCache` TTL (`head_cache_ttl_ns`, default 30 s, capacity
`head_cache_capacity` default 10 000 (tenant, signal) entries, FIFO) and a
folder that is simply behind. Both only widen the listing suffix
(`listing_start_hour = window.watermark_hour + 1`), never change which commits
a query sees.

---

## 3. Sealing and the reconciliation windows

### 3.1 There are TWO seal predicates in this codebase

**Fold's** (`ravel_catalog::fold::sealed_watermark_hour`):

```
margin      = max_flush_lifetime_ns + clock_skew_allowance_ns + fold_safety_margin_ns
threshold   = now_ns - margin                (saturating; < 0 → None)
floor_hours = threshold.div_euclid(3_600e9)  (< 1 → None)
watermark   = floor_hours - 1                (as u32)
```

i.e. the greatest `H` with `now >= end(H) + margin`, `end(H) = (H+1)*1h`.

`CatalogConfig` fields and defaults (`crates/ravel-catalog/src/config.rs`):

| Field | Default const | Value |
|---|---|---|
| `max_flush_lifetime_ns` | `DEFAULT_MAX_FLUSH_LIFETIME_NS` | 1 h |
| `clock_skew_allowance_ns` | `DEFAULT_CLOCK_SKEW_ALLOWANCE_NS` | 5 min |
| `fold_safety_margin_ns` | `DEFAULT_FOLD_SAFETY_MARGIN_NS` | 15 min |
| `fold_reconcile_window_hours` | `DEFAULT_FOLD_RECONCILE_WINDOW_HOURS` | 26 |
| `frontier_reconcile_max_hours` | `DEFAULT_FRONTIER_RECONCILE_MAX_HOURS` | 168 |
| `protection_horizon_ns` | `DEFAULT_PROTECTION_HORIZON_NS` | 25 h 5 min (mirrored from ravel-maintain) |
| `snapshot_part_max_entries` | `DEFAULT_SNAPSHOT_PART_MAX_ENTRIES` | 250 000 |
| `fold_bucket_concurrency` | `DEFAULT_FOLD_BUCKET_CONCURRENCY` | 8 |
| `max_snapshot_part_bytes` | `DEFAULT_MAX_SNAPSHOT_PART_BYTES` | 256 MiB |
| `max_ingest_lag_ns` | `DEFAULT_MAX_INGEST_LAG_NS` | 2 h |
| `head_cache_ttl_ns` | `DEFAULT_HEAD_CACHE_TTL_NS` | 30 s |

**Compaction/retention/erasure's** (`ravel_maintain::bucket::Bucket::is_sealed`
over `CompactorConfig::seal_margin_ns`):

```
seal_margin_ns() = max_flush_lifetime_ns + clock_skew_allowance_ns   // NO fold_safety_margin
is_sealed(now)   = now >= end_ns() + seal_margin_ns()
```

Callers: `compact::compact_bucket`, `retention::retention_sweep_bucket_with_reach`,
`erasure_rewrite`, `migrate`.

**Consequence for the model: compaction/retention consider an hour sealed 15
minutes before fold does.** That ordering is what makes a compaction record
normally land *before* the fold that first folds its hour , i.e. usually inside
the incremental range, not "late". This is not stated anywhere in
`docs/catalog-and-mvcc.md`, whose "Sealed hours" section defines only the fold
rule; ADR-0018 decision 1 states the compaction rule (without
`fold_safety_margin`) and both are simultaneously true. See **F1**.

### 3.2 Fixed reconcile window

**Both reconcile passes run only on a watermark-advancing fold, not on every
tick.** The no-op guard

```
let Some(watermark_hour) = sealed_watermark_hour(now_ns, config) else { return no_op_report(..) };
if let Some(watermark_hour_old) = head_state.watermark_hour()
    && watermark_hour_old >= watermark_hour { return no_op_report(..) }
```

sits **above** `load_previous_entries`, the incremental bucket loop, and both
reconcile blocks in `Catalog::fold`. `sealed_watermark_hour` advances once per
wall-clock hour; `services/ravel-server/src/fold.rs::DEFAULT_FOLD_INTERVAL` is
5 minutes. So roughly 11 of every 12 fold ticks return `no_op_report` before any
reconcile work is evaluated. Model the reconcile transition as guarded on
*watermark advance*, never on *tick*. See **F16** and **F17** for the two doc
claims this invalidates.


`[watermark_hour_old.saturating_sub(fold_reconcile_window_hours),
watermark_hour_old]`, inclusive at both ends, adjacent to (never overlapping)
the incremental range `(watermark_hour_old, watermark_hour_new]`.
`watermark_hour_old` is `reconcile_watermark`, which is `Some` **only** when
`HeadState::Valid` and `!rebuilt` , so the pass is skipped on the first fold
for a tenant and on any rebuild.

Cheap common case: `fold::bucket_needs_reconcile(listing)` returns `false` for a
bucket holding only immutable L0 records (seal lemma), so the pass costs only
the window LISTs. Only a bucket whose listing contains an `l1.*.cmt`, `rw.*.cmt`
or `retire.tmb` is classified and diffed.

Diff/apply: `Catalog::reconcile_one_bucket` builds `desired` from
`classify_bucket` + `dedup_contribution`, compares order-insensitively against
the entries currently carrying that `(shard, ingest_hour_bucket)` via
`same_entry_set`, and on difference does `entries.retain(...)`, **rebuilds the
whole `seen` map** (the retain invalidates cached indices), re-folds the desired
entries through `fold_in_entry`, and inserts the hour into `dirty_hours`.

Sizing: 26 h vs `protection_horizon` 25 h 5 min = 55 min of slack.

### 3.3 Retention-frontier reconcile (the fold half of the ADR-0020 delete blocker)

Runs **inside** the `if let Some(watermark_hour_old) = reconcile_watermark`
block, so it inherits the same skip-on-first-fold and skip-on-rebuild rules.

```
retirement_frontier_hour(now_ns, retention_window_ns, protection_horizon_ns)
    → frontier_hi_raw = (now - R + protection_horizon) / hour
frontier_hi     = frontier_hi_raw.min(lo.saturating_sub(1))   // strictly below the fixed window
candidate_hours = { e.ingest_hour_bucket : e in entries, e.ingest_hour_bucket <= frontier_hi }
                  sorted ascending (oldest first)
take            = min(len, frontier_reconcile_max_hours)
FoldReport::frontier_hours_reconciled = take
FoldReport::frontier_hours_deferred   = len - take
```

Buckets from `fold::frontier_hour_set_buckets(generations, hours)`, listed
through the same `discover_bucket_listings`, applied through the same
`reconcile_one_bucket`. Everything folds into the same single HEAD CAS.

Effective `R`: `TenantConfig.retention_ns` (durable, `t/<th>/config`) when
present, else `default_retention_ns` supplied by the caller. **ADR-0078 is
implemented**: `services/ravel-server/src/lib.rs` builds
`fold_retention = Arc::new(config.maintain.retention.clone())` and passes it to
`fold::spawn`; `services/ravel-server/src/fold.rs::run_loop` calls
`retention.window_for(&tenant)` per tenant per tick and threads the result into
`Catalog::fold`. A tenant with neither an override nor a deployment default gets
no frontier reconcile at all.

### 3.4 Postings interaction

`reconciled = !dirty_hours.is_empty()`. When true (or `rebuilt`), the postings
forward-merge baseline is discarded (`decode_start = 0`) because a supersession
or removal shifts every later ordinal. Column statistics are exempt: their
baseline is joined by `entry_identity`, not by ordinal.

---

## 4. Compaction publication

### 4.1 Naming inputs and outputs

`ravel_maintain::publish::publish_record_with_conservation` assembles
`ravel.commit.v1.CompactionRecord`:

- `inputs: Vec<CompactionInputIdentity { writer_id, writer_epoch, writer_seq }>`
  taken in the sorted order the caller supplies (`InputRecord` list).
- `input_set_hash: [u8; 32]` , blake3 over the canonical sorted input identity
  encoding, supplied by the caller and echoed into the record.
- `level = 1`, `format_version = 1`, `created_unix_ns = clock.now_ns()` at
  publish time (this is the supersession-horizon anchor for `sweep_superseded`).
- `parts: Vec<CompactionPart>` (part_index, series-id range, `content_hash`,
  size, counts, event bounds, `segment_format_version`,
  `declared_column_stats` field 12).

Record key: `keys::compaction_record_key_for(&record)` = first 8 bytes of
`input_set_hash`, hex-encoded to 16 chars. Verified on read with
`keys::verify_compaction_record_key`.

Part key: **never trusted from a stored string.** Readers call
`keys::reconstruct_l1_part_key(&record, &part)`, which composes
`<input_set_hash16>.<part:04>.<hash16>.rseg` where `hash16` is the first 16 hex
chars of the part's own `content_hash`. That is the content addressing:
two compactors that produce byte-identical parts write the same key, and
`CreateIfAbsent` `AlreadyExists` is idempotent success
(`build::put_part`, `BuiltPart::put_already_existed`).

### 4.2 Publication order and the racing-compactor resolution

1. `build::build_parts` → `Vec<BuiltPart>`, each PUT CreateIfAbsent.
2. **Abandonment gate**: `now - start_ns > config.max_compaction_lifetime_ns`
   → `PublishOutcome::Abandoned`, no record published.
3. **Conservation gate**: `checked_sample_sum(inputs)` vs
   `checked_sample_sum(parts)` under `ConservationPredicate`; compaction and the
   format-migration rewrite pass `conserve_exact()` (`input == output`); the
   erasure rewrite passes "inputs minus erased". Failure →
   `MaintainError::ConservationViolation`, nothing published, L0 inputs stay
   live. Runs under `dry_run` too.
4. PUT the record, `PutMode::CreateIfAbsent` + crc32c.
   - `Ok` → `publish::verify_already_existed_parts` HEADs every part whose PUT
     answered `AlreadyExists` (those carry an abandoned run's `last_modified`
     and could have been swept mid-run). A missing one is
     `MaintainError::AlreadyExistsPartVanished` , loud, re-runnable, never a
     silent success. → `PublishOutcome::Published`.
   - `AlreadyExists` → `publish::resolve_already_exists`: GET the winner,
     `verify_compaction_record_key`, compare `input_set_hash`.
     - equal → HEAD each winner part; re-PUT any we still hold bytes for
       (`put_part_with_ledger`), else `MaintainError::ConvergedWinnerPartMissing`.
       → `PublishOutcome::Converged { parts_repaired }`.
     - different → `MaintainError::InputSetHashDivergence`, alarm, delete nothing.

### 4.3 Concurrency with snapshot resolution

There is no lock and no coordination. A resolve or a fold may observe:

- inputs only (pre-record): normal Phase-1 include.
- record + all/some inputs: `process_bucket` / `classify_bucket` include the
  record's parts and exclude exactly the L0 identities in `inputs`; an L0 in the
  bucket not named by any input list stays included and, if its
  `created_unix_ns` postdates the newest compaction/rewrite record, bumps
  `Catalog::interlock_violations` (`ravel_catalog_interlock_violations_total`).
- two records with different `input_set_hash` in one bucket: both parts sets are
  included plus every L0 covered by neither, and
  `Catalog::compaction_input_set_conflicts`
  (`ravel_catalog_compaction_input_set_conflicts_total`) is raised. Harmless for
  metrics under overlap harmlessness; for logs/spans this **double-serves** the
  overlap until a human reconciles (there is no query-time dedup on those
  signals). The counter is the only signal.
- a rewrite record superseding a compaction record: overlap harmlessness does
  **not** hold, so the superseded record's parts are excluded outright
  (`superseded_records`, `catalog::resolve_rewrite_supersession`, bounded and
  cycle-checked at `MAX_REWRITE_SUPERSESSION_DEPTH = 64`).

A running query is unaffected: its `Snapshot` was pinned before the record
landed. The only visible effect of a concurrent compaction plus sweep is a
`NotFound` on a pinned segment → one re-resolve (see §5).

### 4.4 Abandoned / unreferenced part sweeping

`ravel_maintain::sweep::sweep_unreferenced_parts` →
`sweep_unreferenced_parts_impl`:

- Reference map: `sweep::bucket_reference_map_scoped` LISTs the shard's commit
  prefix once and, for each `CompactionRecord`/`RewriteRecord`, reconstructs
  every part key it names; also collects tombstoned hours.
- `sweep::classify_part` → `Option<sweep::PartBranch>`:
  - bucket has a record and it names this key → `None` (never sweep)
  - bucket has a record and it does not name this key →
    `PartBranch::UnreferencedWithRecord`
  - bucket has no record but has a tombstone →
    `PartBranch::TombstonedRecordless`
  - bucket has neither → `None` (a future compaction may still name it)
- Age gate: `CompactorConfig::unreferenced_part_age_gate_ns()` =
  `grace_ns + max_compaction_lifetime_ns`.
- `LeaseCheck::is_protected` consulted.
- **Pre-delete re-verify**: a fresh strongly-consistent reference map is built
  and `classify_part` must return the *same branch*, not merely "still
  collectable".
- `dry_run` counts without deleting.

Superseded L0 inputs: `sweep::sweep_superseded` / `sweep_superseded_impl`,
anchored on the record's own `created_unix_ns + protection_horizon`, deleting
input commit records then input data objects. Orphan data objects:
`sweep::sweep_orphans` (`orphan_age_gate_ns()` = `grace + max_flush_lifetime`,
plus a mass-orphan breaker).

Catalog objects: `sweep::sweep_unreferenced_catalog_objects` , LIST `snap/` and
`idx/` **first**, then `sweep::read_head_reference`:
`HeadReference::Absent` → sweep nothing (the no-anchor rule: a recovery fold
rebuilding from no HEAD adopts surviving old keys via `AlreadyExists`);
undecodable HEAD → `MaintainError::Invariant`, the whole pass fails without
deleting. Reference set = every `parts[].key`, `postings.key`,
`column_stats.key` (field 11) and `column_stats_part.key` (field 13). Age gate
`protection_horizon_ns` (a reader-pinning buffer here, not a writer interlock,
because adoption-via-`AlreadyExists` never refreshes `last_modified`). A fresh
re-verify GET of HEAD runs immediately before the delete loop; an object a
fold's CAS named between the two reads is spared, and a HEAD that vanished
between them spares everything.

### 4.5 What guarantees record **multiset** preservation, not just counts

**Nothing at runtime.** Be explicit about this in the model:

- The only publish-time gate is `publish::conserve_exact()`, which compares the
  **sum of `sample_count`** over inputs against the sum over built parts. It
  cannot distinguish "dropped sample X, invented sample Y".
- Multiset-with-priorities preservation rests on two things, neither of which is
  a runtime invariant:
  1. **Per-sample provenance carriage.** Since ADR-0092 the metrics L1 merge
     decodes every contributing run and re-encodes one merged run per series,
     carrying each sample's original `(created_unix_ns, writer_epoch,
     writer_seq, in_page_index)` in RSEG v7 provenance columns:
     `ravel_maintain::build::{sample_provenance, provenance_key,
     sort_merged_scalar, merge_scalar_runs, merge_histogram_runs,
     merged_run_prefix}`. This is what preserves *priorities*, so the merged run
     reproduces the same candidate multiset the pre-compaction snapshot would.
  2. **An offline differential proof.**
     `crates/ravel-query/tests/differential_compaction.rs` drives the real
     `ravel_maintain::compact_bucket` over adversarial and proptest-generated
     L0 populations and compares the deduped sample streams. Its oracle is a
     pure function of the per-(series, timestamp) candidate multiset under
     `engine::is_greater`'s order, so equal outputs imply equal candidate
     multisets. Tests: `differential_adversarial_fixed`,
     `differential_distinct_provenance`, plus the proptest strategies.
- Logs and spans are a **separate** merge implementation
  (`ravel_maintain::rlog`, `ravel_maintain::rspan_codec`) with their own
  determinism suites (`crates/ravel-maintain/tests/rlog_determinism.rs`,
  `rspan_determinism.rs`, `determinism.rs::same_inputs_same_bytes_and_keys`).
  The same "counts only" limitation applies to their conservation gate.

For TLA+: model this as an **assumption discharged by an offline differential
test**, not as an invariant the protocol enforces.

---

## 5. Query snapshot pinning

### 5.1 Where a snapshot is resolved

`Catalog::resolve` / `resolve_with_accounting` / `resolve_pruned` /
`resolve_pruned_with_accounting` / `resolve_pruned_with_admission` /
`resolve_pruned_with_generations` → `Catalog::resolve_impl` →
`Catalog::resolve_fanout` → `ravel_catalog::snapshot::Snapshot { segments,
segments_pruned, pending_erasure }`.

`resolve_fanout` order:

1. `enforce_provisioning_once` (opt-in), `read_scan_generations` (fresh, every
   resolve).
2. `Catalog::window_hour_bounds(range, now_ns)` , window is
   `[range.start_ns - max_ingest_lag, now_ns + clock_skew_allowance]`, upper
   bound anchored on **`now_ns`**, not `range.end_ns`.
3. `Catalog::resolve_snapshot_window(..., want_postings = name_filter.is_some(),
   window_start_hour, window_end_hour, generations, accounting)`.
4. If a window came back and `window.watermark_hour >= window_start_hour`:
   `SnapshotWindow::extract_into` fills `segments` from the parts (tagged
   `SegmentOrigin::SealedBelowWatermark`), and
   `listing_start_hour = watermark_hour + 1`. Otherwise
   `listing_start_hour = window_start_hour` , i.e. **the whole window is
   listed**.
5. Suffix traversal: `Catalog::list_window_bounded` (per-shard bounded LIST via
   `list_shard_hours`) or `Catalog::list_window_by_prefix` (chosen at
   `prefix_list_crossover_requests` = 720 buckets, or when the per-bucket
   estimate would exceed `max_catalog_list_requests` = 100 000). The prefix scan
   carries a runtime cap and aborts with `WindowTooWide` (HTTP 422).
   Both join with `Catalog::list_pending_erasure` via `tokio::join!`.
6. `Catalog::resolve_min_token` per token , **always an exact commit-key GET,
   never through the snapshot**; on `NotFound` after one propagation retry,
   `Catalog::resolve_min_token_fallback` LISTs the bucket for compaction records
   and a tombstone.
7. `segments.sort_by_key(catalog::segment_sort_key)` , the deterministic total
   order `(created_unix_ns, writer_epoch, writer_seq, shard, writer_id, level,
   input_set_hash, part_index)`.

### 5.2 Can a query's snapshot change mid-execution?

**Within one attempt: no.** `Snapshot` is a value handed to the executor;
nothing re-resolves during scan/fetch.

**Across the whole query: exactly once.**
`ravel_query::engine::QueryEngine::resolve_snapshot_with_retry` resolves
(`resolve_bounded`), runs `attempt`, and on
`QueryError::Fetch(FetchError::Store { source: StoreError::NotFound })`
re-resolves against a *fresh* snapshot and re-runs the whole attempt once. A
second `NotFound` yields `QueryError::SnapshotInvalidated`, mapped to HTTP 503
(`ravel_query::http::error`). Distributed fan-out mirrors this:
`pb::status::Code::SnapshotInvalidated` from a remote sets `invalidated` in
`ravel_query::distrib` and surfaces the same way.

So the model needs: *snapshot is immutable per attempt; at most two attempts;
the second attempt's snapshot may differ arbitrarily from the first's.*

### 5.3 Index-missing degrades to listing

`Catalog::resolve_snapshot_window` returns `Ok((None, _))` , meaning "fall back
to full listing" , for every one of:

- HEAD `StoreError::NotFound` (`read_head` → `Ok(None)`)
- HEAD GET error of any other kind (`warn!` + `Ok(None)`)
- `snapshot_format::decode_head` failure, **including
  `UnsupportedHeadVersion`** (`warn!` + `Ok(None)`)
- HEAD `signal` mismatch
- `PartLoadOutcome::Unusable`: part GET error, blake3 mismatch against the
  HEAD ref, or `decode_part` failure (`load_one_part` → `OnePartOutcome`)
- postings failures of every kind (`load_snapshot_postings` → `Ok(None)`;
  pruning simply does not apply)

`PartLoadOutcome::NotFoundRace` (a part GET returned `NotFound`, racing GC of a
just-superseded part) triggers **exactly one** HEAD re-read with
`bypass_cache = true`, then one more part-load attempt; failure after that falls
back to listing.

### 5.4 Corrupt / unreadable HEAD: four different behaviours by design

| Path | Symbol | Corrupt (decode fails) | Unsupported `format_version` | Absent |
|---|---|---|---|---|
| **Fold** | `Catalog::get_head`, `Catalog::fold` | `HeadState::Corrupt { version }` → treated as absent, full rebuild from the commit layout, then **CAS-clobbers the corrupt object** with `PutMode::CasVersion(version)` | `HeadState::UnsupportedVersion` → `CatalogError::UnsupportedHeadVersion`, **never written** (ADR-0066 decision 2, fail-closed-on-newer) | `HeadState::Absent` → rebuild, `PutMode::CreateIfAbsent` |
| **Query resolve** | `Catalog::read_head` | **fail open**: `warn!`, `Ok(None)`, full listing | same , decode error, falls back to listing | `Ok(None)`, full listing |
| **Retention sweep** | `retention::SnapshotReachability::ensure_head` / `bucket_gate` | **fail closed**: `HeadLoad::Unreadable` → `SnapshotGate::Blocked(SnapshotBlock::Unreadable)` → `RetentionOutcome::BlockedBySnapshot`, nothing deleted, tombstone kept | same (a newer format is undecodable here) → blocked | `HeadLoad::Absent` → `SnapshotGate::Clear`, sweep proceeds |
| **Catalog-object sweep** | `sweep::read_head_reference` | **fail closed harder**: `MaintainError::Invariant`, the whole pass errors, nothing deleted | same | `HeadReference::Absent` → sweep **nothing** (the no-anchor rule) |

Two additional query-side conditions are **hard failures with no fallback**:

- `head.tenant_hash != tenant` → `Catalog::record_isolation_breach()` +
  `CatalogError::FieldMismatch` (ADR-0050 §2). Same for a snapshot part header's
  `tenant_hash` (`OnePartOutcome::IsolationBreach`) and for a postings object's.
- shard-generation disagreement →
  `snapshot_resolve::head_generations_acceptable` /
  `Catalog::validate_head_against_generations`: one fresh uncached
  `read_scan_generations` re-read, then `head_shard_count_mismatch`
  (`CatalogError::FieldMismatch`). A re-read that *succeeds* returns
  `Some(fresh)` and the caller **must** rebuild its Phase 1 scan set from it.

Also note: a part GET whose object was truncated/corrupted is caught by
`fetch_content_addressed` + explicit `blake3::hash(&data)` comparison before
`decode_part`.

---

## 6. Signal-specific duplicate semantics

### Metrics , query-time dedup by `(series_id, timestamp)`

`crates/ravel-query/src/engine.rs`:

- `engine::is_greater(a: &Candidate, b: &Candidate)` , greatest wins under
  `(created_unix_ns, writer_epoch, writer_seq, in_page_index)`, then the raw
  f64 **bit pattern** as final tiebreak (so NaN payloads and `-0.0` are
  deterministic).
- `engine::merge_series_runs` , k-way min-heap merge by timestamp, drains all
  same-ts heads, keeps the `is_greater` winner. Never arrival order.
- Native histograms: `engine::histogram_is_greater`,
  `engine::merge_histogram_runs` , same shape, structural tiebreak.

Provenance for an L0 segment comes from its commit record (segment-level); for
an L1 part the fetcher emits one `FetchedSeriesSoa` per **(series, run)** with
per-run provenance, which is exactly what makes query-over-L1 reproduce the
pre-compaction candidate set.

Catalog-side ordering that feeds this: `catalog::segment_sort_key` and the
identical rule in `fold`'s entry sort; `crates/ravel-catalog/tests/snapshot_sort_order.rs`.

### Logs and spans , no query-time dedup at all

`crates/ravel-query/src/distrib/mod.rs`:

- `distrib::merge_log_records(per_slice: Vec<Vec<LogRecord>>) -> Vec<LogRecord>`
- `distrib::merge_spans(per_slice: Vec<Vec<SpanRow>>) -> Vec<SpanRow>`

Both impose the stated cross-segment total order and **explicitly never dedup**;
their doc comments cite `docs/consistency-model.md` "logs and spans" and
ADR-0051 §5. A retry after a lost ack produces byte-identical duplicate records
that a query returns twice, unless the request carried an idempotency key and
a valid, in-window marker exists, in which case `read_marker` replays the stored
commit tokens instead of writing again.

The duplicate control for logs/spans is therefore **at ingest, not at query**:

- `crates/ravel-ingest/src/idempotency.rs` writes
  `t/<th>/<sig>/idem/<keyhash32>.<ingest_hour>.idm`, a `RIDM` v1 crc32c-covered
  frame holding `written_count` and the comma-separated `x-ravel-commit-token`
  set. `keyhash32` = first 16 bytes of
  `blake3("ravel-idem-v1" || tenant_id || client_key)`.
- Every decode failure (bad magic, bad version, crc mismatch, truncation) is a
  **marker miss** , fail-open to at-least-once, never a panic.
- Markers are aged out by `sweep::sweep_idempotency_markers` past
  `CompactorConfig::idem_dedup_window_hours` (default 24 h), skipping (not
  erroring on) unparseable keys, and honouring `dry_run`.

Practical consequence for §4.3: the two-input-set compaction conflict is a
correctness non-event for metrics and a **duplicate-serving event for logs and
spans**.

---

## 7. Existing tests, by protocol step

### Sealing / watermark
- `fold.rs` unit tests: `seal_boundary_is_inclusive_and_hour_by_hour`,
  `no_hour_sealed_yet_is_a_no_op`, `empty_tenant_still_produces_a_valid_empty_fold`

### Fold: incremental, rebuild, idempotence
- `fold.rs`: `first_fold_rebuilds_from_commit_layout_and_second_call_is_idempotent`,
  `incremental_fold_preserves_previous_entries_and_folds_only_new_hours`,
  `unreadable_previous_part_falls_back_to_rebuild`,
  `rebuild_restores_a_deleted_sealed_part_instead_of_reusing_its_ref`,
  `duplicate_commit_identity_across_buckets_skips_and_advances`,
  `unrecognized_bucket_key_shape_skips_and_advances`,
  `fold_report_get_requests_equals_observed_store_gets`

### HEAD corruption / version
- `fold.rs`: `corrupt_head_falls_back_to_rebuild_and_recovers_all_entries`,
  `newer_format_head_fails_loudly_and_never_clobbers`,
  `head_failing_late_validation_still_rebuilds_via_corrupt_path`
- `crates/ravel-failure-tests/tests/folder_crash_matrix.rs`:
  `corrupt_head_falls_back_to_listing_never_to_an_error`
- `crates/ravel-maintain/src/sweep.rs`: `catalog_sweep_corrupt_head_deletes_nothing`

### HEAD CAS race / crash before HEAD
- `fold.rs`: `two_concurrent_first_folds_race_head_cas_and_only_one_advances`,
  `crash_after_partial_part_puts_leaves_old_head_intact`,
  `reconcile_preserves_single_head_cas`
- `crates/ravel-failure-tests/tests/folder_crash_matrix.rs`:
  `concurrent_folders_race_head_cas_without_losing_or_duplicating_data`,
  `folder_down_for_hours_never_loses_data_only_widens_listing`,
  `stale_head_cache_widens_listing_but_never_misses_new_data`,
  `missing_snapshot_part_falls_back_to_listing_after_one_head_reread`,
  `commit_in_wrongly_sealed_bucket_is_invisible_until_head_rebuild_repairs_it`
  (the one documented clock-skew visibility gap)

### Multi-part fold (ADR-0063)
- `fold.rs`: `partition_parts_seals_at_hour_boundaries`,
  `ceiling_crossing_produces_multiple_parts`,
  `range_scoped_resolve_fetches_only_intersecting_parts`,
  `duplicate_part_content_hashes_are_collapsed_before_encoding`
- `snapshot_resolve.rs`: `multi_part_postings_binding_applies_and_falls_back`,
  `extract_into_prunes_across_multiple_parts`
- `services/ravel-server/tests/fold_e2e.rs`

### Reconcile (fixed window)
- `fold.rs`: `reconcile_applies_late_compaction_before_horizon`,
  `reconcile_applies_late_tombstone`,
  `reconcile_ignores_late_record_outside_window`,
  `reconcile_never_reintroduces_a_drifted_duplicate`,
  `reconcile_no_change_carries_sealed_parts_forward`,
  `first_and_rebuilt_folds_skip_reconcile`,
  `mixed_l0_l1_entries_still_build_postings`
- `crates/ravel-catalog/tests/erasure_resolution.rs`:
  `reconcile_picks_up_a_rewrite_published_into_an_already_folded_bucket`,
  `fold_recognizes_rewrite_records_and_matches_resolve`

### Reconcile (retention frontier)
- `fold.rs`: `frontier_reconcile_applies_out_of_window_tombstone`,
  `frontier_reconcile_is_bounded_and_carries_remainder`,
  `retention_overlay_precedence`, helper `frontier_reconciled_with`

### Fold vs. listing equivalence
- `crates/ravel-catalog/tests/fold_compaction_differential.rs`:
  `fold_then_resolve_matches_direct_listing_with_compaction_and_tombstone`
- `crates/ravel-catalog/src/seal_divergence.rs` +
  `services/ravel-cli/src/catalog.rs::verify` (`ravel-cli catalog verify`),
  scheduled via `services/ravel-server/src/scrub.rs`

### Snapshot resolution / isolation
- `snapshot_resolve.rs`: `head_tenant_hash_mismatch_is_hard_error`,
  `part_tenant_hash_mismatch_is_hard_error`,
  `postings_tenant_hash_mismatch_is_hard_error`,
  `postings_foreign_tenant_unbound_still_hard_fails`,
  `head_generations_predicate_covers_all_arms`,
  `older_head_reaching_unknown_generation_active_hours_is_rejected`,
  `decrease_past_slack_head_validates_against_reader_ceiling`
- `crates/ravel-catalog/tests/snapshot_resolve.rs`,
  `resolve_bounded_listing.rs` (`bounded_listing_matches_reference_in_nine_lists`,
  `erasure_list_overlaps_shard_fanout`),
  `resolve_prefix_traversal.rs` (`differential_paths_return_identical_snapshots`),
  `window_ceiling.rs`, `hot_record_cache.rs`, `snapshot_format_corrupt.rs`,
  `snapshot_format_roundtrip.rs`, `postings_exactness.rs`,
  `postings_get_gating.rs`, `resharding.rs`, `resharding_prefix_traversal.rs`

### Compaction resolution rules
- `crates/ravel-catalog/tests/compaction_resolution.rs`:
  `compaction_record_includes_parts_and_excludes_input_l0s`,
  `part_event_bounds_filter_against_query_range`,
  `unlisted_l0_included_and_interlock_metric_only_when_postdating`,
  `tombstoned_bucket_contributes_nothing`,
  `two_records_with_different_input_set_hash_include_both_and_alarm`,
  `mixed_level_snapshot_sort_is_deterministic`,
  `unknown_bucket_entry_shape_is_a_fail_loud_error`,
  `row5_l1_and_inputs_both_visible_do_not_double_count`,
  `row6_parts_without_record_are_ignored_across_list_pages`,
  `token_fallback_serves_via_l1_when_commit_record_swept`,
  `token_fallback_tombstone_satisfied_with_zero_segments`,
  `token_fallback_unsatisfiable_when_not_in_any_input_list`

### Compaction publication crash matrix
- `crates/ravel-maintain/tests/crash_matrix.rs`: `row1_crash_during_list`,
  `row2_3_crash_after_parts_before_record`, `row4_partial_part_upload`,
  `row10_racing_compactors_loser_converges_and_repairs`,
  `row11_already_exists_different_hash_alarms`,
  `row13_past_deadline_abandons_without_publishing`
- `crates/ravel-maintain/tests/tombstone_race.rs`:
  `tombstone_deleting_an_already_exists_part_fails_loud`,
  `all_parts_already_exists_retry_retains_nothing_and_publishes`,
  `rerun_after_vanished_part_converges_by_presence`,
  `rerun_with_revanished_part_fails_typed_not_converged`
- `crates/ravel-maintain/src/publish.rs` unit tests:
  `conservation_mismatch_aborts_publish`, `conservation_surplus_also_aborts`,
  `conserving_publish_writes_record`,
  `conservation_gate_still_fires_with_stamped_parts`
- `crates/ravel-maintain/tests/determinism.rs`:
  `same_inputs_same_bytes_and_keys`

### Sweeps
- `crates/ravel-maintain/tests/sweep_crash_matrix.rs`:
  `row7_partial_input_records_deleted_reswept_converges`,
  `row8_records_deleted_data_not_orphan_gc_converges`,
  `row9_pinned_query_races_sweep_then_reresolves_against_l1`,
  `row12_token_get_notfound_post_sweep_found_in_input_list`,
  `unreferenced_part_swept_only_after_age_gate`,
  `recovery_over_abandoned_parts_never_loses_a_named_part`,
  `tombstoned_abandoned_parts_collected_reverify_proven`,
  `young_tombstoned_recordless_part_survives_age_gate`,
  `recordless_untombstoned_part_is_never_swept`,
  `orphan_gc_respects_live_records_and_age_gate`,
  `no_delete_before_horizon_boundary_stepped`,
  `dry_run_sweep_reports_eligible_set_but_deletes_nothing`,
  `sweep_shard_zoned_defers_out_of_scope_hour_to_full_sweep`,
  `tombstoned_interior_bucket_swept_no_later_than_full_pass`
- `crates/ravel-maintain/src/sweep.rs` unit tests:
  `catalog_sweep_spares_referenced_and_young`,
  `catalog_sweep_deletes_old_unreferenced`,
  `catalog_sweep_spares_referenced_postings`,
  `catalog_sweep_spares_referenced_column_stats(_part)`,
  `catalog_sweep_absent_head_sweeps_nothing`,
  `catalog_sweep_reverify_spares_object_a_racing_fold_adopts`,
  `catalog_sweep_reverify_head_get_faults_before_delete`,
  `catalog_sweep_corrupt_head_deletes_nothing`,
  `catalog_sweep_spares_legal_hold`,
  `mass_orphan_trips_breaker_and_deletes_nothing`,
  `batched_reverify_lists_commit_prefix_once_per_pass`
- `crates/ravel-maintain/tests/retention.rs`, `legal_hold.rs`,
  `erasure_sweep.rs`

### MVCC / duplicate semantics
- `crates/ravel-failure-tests/tests/ordering_and_catalog.rs`:
  `within_segment_duplicate_and_out_of_order_timestamps_resolve_correctly`,
  `cross_segment_duplicate_timestamp_later_commit_wins_regardless_of_publish_order`,
  `unsatisfiable_min_commit_token_errors_instead_of_returning_stale_data`
- `crates/ravel-failure-tests/tests/concurrent.rs`:
  `concurrent_ingest_and_query_never_sees_a_partially_visible_flush`
- `crates/ravel-failure-tests/tests/ack_and_duplicates.rs`:
  `ack_lost_after_commit_then_client_retry_dedups_to_one_value`,
  `duplicate_otlp_delivery_normalized_twice_does_not_double_count`
- `crates/ravel-failure-tests/tests/crash_matrix.rs`:
  `crash_before_data_put_leaves_nothing_stored_or_visible`,
  `crash_after_data_put_before_commit_orphans_then_spec_model_gc_sweeps_after_grace`
- `crates/ravel-query/tests/differential_compaction.rs` (see §4.5)
- `crates/ravel-catalog/src/catalog.rs` unit tests:
  `data_object_without_commit_record_is_never_visible`,
  `duplicate_publish_same_content_hash_is_idempotent_single_segment`,
  `different_content_hash_same_identity_is_split_brain`,
  `snapshot_is_stable_after_further_publishes`,
  `min_token_resolves_even_when_its_hour_bucket_is_outside_the_listing_window`,
  `min_token_unsatisfiable_when_commit_record_is_missing`,
  `object_key_mismatch_is_fatal_during_resolve`,
  `rewrite_supersession_cycle_is_a_typed_error`,
  `rewrite_supersession_over_deep_chain_is_a_typed_error`,
  `rewrite_supersession_depth_bound_is_exact_at_the_maximum`
- `crates/ravel-query/src/engine.rs` retry test around
  `resolve_snapshot_with_retry` (a `SliceFetcher` double that reports
  `SnapshotInvalidated` and proves exactly one retry)

---

## 8. Findings

### F1 , Two seal predicates; only one is in `catalog-and-mvcc.md`
`fold::sealed_watermark_hour` uses `max_flush_lifetime + clock_skew_allowance +
fold_safety_margin`. `Bucket::is_sealed` / `CompactorConfig::seal_margin_ns`
uses `max_flush_lifetime + clock_skew_allowance` only , no
`fold_safety_margin`. ADR-0018 decision 1 states the latter; ADR-0020 and
`docs/catalog-and-mvcc.md` "Sealed hours" state the former as if it were the
only rule. Both are correct as implemented; the docs never put them side by
side. **The 15-minute gap is load-bearing for the model**: compaction, retention,
and erasure act on an hour before the fold's watermark reaches it, which is
precisely why a compaction record normally arrives *before* the fold that first
folds its hour, and why "late record" is the exception rather than the rule.
Model both predicates.

### F2 , ADR-1029 is `Proposed`, the code exists, and nothing calls it
`crates/ravel-fleet/src/claim.rs` fully implements the advisory compaction
claim (`claim::COMPACTION_CLAIMS_PREFIX = "sys/maintain/claims/compaction/"`,
`CompactionClaim` payload, CreateIfAbsent-then-`CasVersion`,
`MAX_OBSERVED_LEASE_MS`), and `docs/catalog-and-mvcc.md` documents the prefix
in the **live key layout**. But `crates/ravel-fleet/src/lib.rs` exposes
`pub mod claim` and `ravel_maintain::lib` re-exports only `worker_set`; a
workspace-wide grep for `acquire_claim` / `ClaimOutcome` / `claim::` outside the
module itself returns nothing. **Accepted-but-unimplemented in the wiring
sense.** Racing compactors are still serialized only by
`CreateIfAbsent` on the record plus content-addressed part keys , which is what
the model should assume.

### F3 , ADR-0979 is `Proposed` but decision 3 is already shipped
`ravel_maintain::publish` cites "ADR-0979 decision 3" for
`BuiltPart::put_already_existed`, `publish::verify_already_existed_parts`,
`MaintainError::AlreadyExistsPartVanished`, and the "cannot repair from RAM"
arm producing `MaintainError::ConvergedWinnerPartMissing`. Corresponding tests
exist in `crates/ravel-maintain/tests/tombstone_race.rs`. The bounded-memory
merge (`rlog.rs` cursor fan-out, `PartSink` retention) is the part still
outstanding.

### F4 , Other status/implementation mismatches
- **ADR-0849 has no `Status:` line at all.** Its content (a snapshot-bound index
  plane) overlaps the postings/cstat work that *is* shipped.
- **ADR-0873 is `Proposed`** yet `declared_column_stats` is fully threaded:
  `CommitRecord` field 20, `CompactionPart` field 12, `SnapshotEntry` field 15,
  `ravel_catalog::declared_stats::carry_commit_record`,
  `crates/ravel-catalog/tests/declared_stats_carriage.rs`.
- **ADR-0815 is `Proposed`** (clustered compaction / object pruning) , no
  corresponding implementation observed on the compaction path.

### F5 , The ADR-0020 delete blocker is enforced on **one** delete path only, and the other path's soundness argument is conditional
ADR-0020: "GC must treat reachability from HEAD-referenced snapshots (within the
protection horizon) as a delete blocker."

Enforced in `retention::SnapshotReachability::bucket_gate`, threaded from
`scan::scan_and_maintain_with_memo` through `retention::maintain_bucket_with_reach`
into `retention::physical_sweep`, surfacing as
`RetentionOutcome::BlockedBySnapshot` / `SnapshotBlock::{Named, Unreadable}` and
counted on `scan::ScanReport::blocked_by_snapshot`.

**Not enforced** in:
- `sweep::sweep_superseded` / `sweep_superseded_impl` (deletes the L0 commit
  records and data objects a compaction superseded). Its signature takes no
  `SnapshotReachability`; its only gates are
  `record.created_unix_ns + protection_horizon`, `LeaseCheck`, and the record's
  own input list.
- `sweep::sweep_orphans` (age gate + commit-record-absence re-verify only).
- `sweep::sweep_unreferenced_parts` (branch classification + age gate only).

What stands in for the gate on the compaction-supersession path is ADR-0063 §4's
soundness argument: "the sweeper only deletes a compaction input after
`created_unix_ns + protection_horizon`, so any record whose supersession could
invalidate snapshot entries is guaranteed to be observed by a reconcile pass
before its inputs can be physically deleted."

**That argument is conditional, and the condition is unstated.** The two
quantities are measured on different clocks:

- `fold_reconcile_window_hours` (26) is a count of **ingest-hour buckets**
  measured backwards from `watermark_hour_old`.
- `protection_horizon_ns` (25 h 5 min) is **nanoseconds** measured forward from
  `record.created_unix_ns`, the compactor's publish-time clock reading.

Subtracting one from the other is not meaningful. The argument only closes when
`record.created_unix_ns` is close to `end(H) + seal_margin` , i.e. when the
compactor publishes promptly after hour `H` seals. `docs/catalog-and-mvcc.md`
states that assumption openly for compaction ("Compaction does: it targets hours
near the watermark") and states the failure explicitly for selective-erasure
rewrites. **Neither doc states the lagging-compactor case.**

The lagging case: a compaction that publishes long after its hour sealed pushes
`created_unix_ns + protection_horizon` past the point at which hour `H` has
already fallen out of `[watermark_hour_old - 26, watermark_hour_old]`. The
frontier band does not reach `H` until `H` approaches retirement (days). In that
gap `sweep_superseded` deletes L0 inputs that a HEAD-referenced snapshot part
still names.

The failure is **not self-healing**: `QueryEngine::resolve_snapshot_with_retry`'s
second attempt re-resolves against the *same unchanged HEAD*, so the same
`NotFound` recurs and the query terminates in a persistent
`QueryError::SnapshotInvalidated` (503) for that window until an operator forces
a HEAD rebuild. ADR-0063's Consequences describes exactly this failure and claims
the reconcile pass "fixes this structurally"; it bounds it, conditionally.

For TLA+: model `compaction_publish_lag` as a free variable, not as zero. The
invariant to check is *no object named by the current HEAD is deleted*, and the
counterexample is a compaction whose publish lags its hour's seal by more than
the window.

### F6 , Fold CAS-clobbers a corrupt HEAD, deliberately
`HeadState::Corrupt { version }` keeps the store `Version` specifically so the
rebuild's HEAD PUT can use `PutMode::CasVersion(version)`. `HeadState::UnsupportedVersion`
carries no `Version` at all, so a newer-format HEAD cannot be clobbered even by
a refactor. This closes the hazard ADR-0063's Consequences flagged and ADR-0066
decision 2 fixed. For the model: a corrupt HEAD is a *recoverable* state with a
well-defined single-writer transition; an unsupported HEAD is a terminal error
for that folder.

### F7 , Frontier `frontier_hi` clamp degenerates at hour 0
`frontier_hi = frontier_hi_raw.min(lo.saturating_sub(1))`. When
`watermark_hour_old <= fold_reconcile_window_hours`, `lo == 0` and
`lo.saturating_sub(1) == 0`, so `frontier_hi == 0` and hour 0 remains a
candidate for both the fixed window and the frontier band. Harmless in practice
(hour 0 is 1970) and the reconcile is idempotent, but the "no bucket is listed
by both passes" comment is not literally true at that boundary.

### F8 , "Commit records are never deleted by this design" is scoped, not global
ADR-0020's decision text says the snapshot design never deletes commit records.
`sweep::sweep_superseded` (authorized by ADR-0018 decision 6) and the retention
`physical_sweep` (ADR-0019) both do. The reconciliation is that a snapshot entry
can legitimately outlive its commit record, which is why
`seal_divergence::verify_seal_divergence` mirrors the fold's supersession
exclusion on the ground-truth side and excludes a superseded L0 rather than
reporting it missing. Worth stating as an explicit model invariant:
*snapshot entries are not a subset of live commit records.*

### F9 , `Transaction` is a permanently dead extension point
`fold::Transaction` has a `_private: ()` field and no public constructor.
`Catalog::fold`'s `_transactions: &[Transaction]` parameter is always empty.
Compaction and retention are discovered from the same per-bucket listing, so
the parameter has no use. It is pure API surface; the model should ignore it.

### F10 , The advisory maint cursor is dead
ADR-0018 decision 7 already records this in the ADR text: `scan_and_compact`
(the cursor's only consumer) has no non-test caller, and `--mode maintain` runs
`scan_and_maintain`, which never touches it. `keys::maint_cursor_key` and the
CAS plumbing remain. Do not model the cursor.

### F11 , Column statistics are dual-published every fold
Each fold writes both the v1 identity-keyed object (`SnapshotHead.column_stats`,
field 11, level-0 entries only) and the v2 part-hash-keyed object
(`column_stats_part`, field 13, L0 and L1 uniformly), per ADR-0942. Both are
CreateIfAbsent `.cstat` objects under the same `idx/` prefix, and
`sweep::read_head_reference` spares both. `FoldReport` reports them
independently. This is a documented transition window, not a bug, but it doubles
the `idx/` object churn.

### F12 , `estimated_catalog_requests` upper-envelope claim carries an
acknowledged soft spot
`SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND = 3` absorbs the snapshot-window path plus,
on the non-empty-window branch, the unconditional `del/` LIST , the code's own
comment says the envelope "still exceeds real usage with it included, so the
envelope holds without a dedicated term (though with less margin than before; a
future addition to that path should re-check this)". The empty-window branch has
`PENDING_ERASURE_LIST_UPPER_BOUND = 1` counted explicitly. If the model cares
about request-count bounds, this term is a documented approximation, not a proof.

### F13 , Fold's postings are suppressed by supersession, not by level
The `reconciled` flag (derived from `dirty_hours`) is what forces
`decode_start = 0`. A bucket that arrives already compacted on its *first* fold
is a plain hour-major append and keeps the forward merge. ADR-0063 §7 phrased
the gate as "any L1 entry present => no postings ref"; the shipped code is
narrower and better. `fold.rs::mixed_l0_l1_entries_still_build_postings` pins the
newer behaviour, so the ADR text is stale relative to the code.

### F14 , Doc/ADR window-anchor wording
ADR-0063 §4 writes the reconcile window as `[watermark -
fold_reconcile_window_hours, W_old]`, which reads as anchoring the low end on the
*new* watermark. `docs/catalog-and-mvcc.md` and the code both anchor both ends on
`watermark_hour_old`. The code is authoritative; the ADR wording is loose.

### F15 , `SnapshotReachability` is a per-pass cache with an argued-safe staleness
`retention::SnapshotReachability` reads HEAD once per `scan_and_maintain` pass
and each covering part once. Its safety argument is that a retention tombstone is
irreversible, so a bucket the fold has dropped from HEAD is never re-added, and
a HEAD read that does not name a bucket cannot later start naming it. That holds
because `classify_bucket` returns an empty contribution for any tombstoned
bucket , including on the rebuild path. Worth asserting as a model invariant:
*once a bucket holds `retire.tmb`, every subsequent fold contributes zero entries
for it.*

---

### F16 , "Caught on the very next fold" means the next watermark-advancing fold
`docs/catalog-and-mvcc.md` (fold reconcile pass): "a compaction landing hours
after its bucket sealed is caught on the very next fold (this is the common case
the window is sized for)." Because the no-op guard precedes the reconcile blocks
(see §3.2), "the very next fold" is the next fold that *advances the watermark* ,
up to roughly one hour later at the 5-minute default tick, not the next tick.
The bound still holds; the latency the sentence implies does not.

### F17 , The frontier band's stated oversampling margin does not exist
`docs/catalog-and-mvcc.md` (retention-frontier reconcile): "In steady state the
frontier advances one hour per hour and folds run far more often than hourly, so
this is a handful of buckets." The folds that actually *do* frontier work are
exactly the watermark-advancing ones, which occur about hourly. The ratio is
therefore ~1:1, not comfortably oversampled. The per-fold candidate set is still
bounded (`frontier_reconcile_max_hours`, oldest-first, with
`FoldReport::frontier_hours_deferred` carrying the remainder), so the safety
property survives , but the doc's justification for why the backlog stays small
rests on a false premise about fold cadence.

### F18 , Fold and resolve disagree on exactly one input: an unrecognized key shape
`Catalog::classify_bucket` (fold) counts an unclassifiable key into
`FoldReport::layout_drift_count`, `warn!`s, and **advances the watermark**.
`Catalog::process_bucket` (resolve) propagates the `KeyError` and **fails the
query**. Both behaviours are deliberately pinned:
`fold.rs::unrecognized_bucket_key_shape_skips_and_advances` versus
`compaction_resolution.rs::unknown_bucket_entry_shape_is_a_fail_loud_error`.

Consequence: a single malformed key in a sealed bucket lets the fold keep
advancing and folding, while every query whose listing suffix still covers that
bucket errors out. Once the fold's watermark passes the bucket, queries stop
listing it and start serving it from the snapshot, so the query recovers , but
only for windows entirely below the watermark, and a `min_token` fallback or a
retention/sweep path that re-lists the bucket hits the hard error again. The
"fold and a live resolve derive identical bucket state" claim in
`classify_bucket`'s own doc comment, and the fold-vs-listing equivalence row in
the matrix below, both assume agreement that is tested to be absent on this one
input.

---

## 9. Reconnaissance matrix

| Protocol | Normative source | Rust implementation | Existing tests | Status | Model priority |
|---|---|---|---|---|---|
| L0 commit publish (data then record, CreateIfAbsent, split-brain check) | ADR-0002/0010; `docs/catalog-and-mvcc.md` "Commit sequence" | `ravel_commit::publish::{publish, put_data_object, resolve_already_exists}`, `record::{build, validate}` | `crash_matrix.rs` rows 1–2; `catalog.rs::{duplicate_publish_same_content_hash_is_idempotent_single_segment, different_content_hash_same_identity_is_split_brain, data_object_without_commit_record_is_never_visible}` | Implemented, matches doc | **High** , base transition of the whole model |
| Sealed-hour watermark (fold) | ADR-0020; `catalog-and-mvcc.md` "Sealed hours" | `fold::sealed_watermark_hour`, `CatalogConfig::{max_flush_lifetime_ns, clock_skew_allowance_ns, fold_safety_margin_ns}` | `seal_boundary_is_inclusive_and_hour_by_hour`, `no_hour_sealed_yet_is_a_no_op` | Implemented | **High** |
| Sealed-hour predicate (compaction/retention) | ADR-0018 decision 1 | `Bucket::is_sealed`, `CompactorConfig::seal_margin_ns` | `end_to_end.rs::not_sealed_is_skipped` | Implemented; **differs from fold by `fold_safety_margin`** (F1) | **High** , the 15-min gap decides "late record" frequency |
| Snapshot part build + content addressing | ADR-0020, ADR-0063 §1–2 | `fold::{partition_parts, PartSpan, part_object_key}`, `snapshot_format::{encode_part_ranged, decode_part}`, `part::validate_entries` | `partition_parts_seals_at_hour_boundaries`, `ceiling_crossing_produces_multiple_parts`, `snapshot_format_roundtrip.rs`, `snapshot_format_corrupt.rs` | Implemented | **High** |
| HEAD publish with version CAS | ADR-0020, ADR-0063 §5 | `Catalog::fold` final `store.put(head_key, ..., PutMode::CasVersion \| CreateIfAbsent)`, `MAX_HEAD_CAS_ATTEMPTS = 8`, `CatalogError::FoldCasRetriesExhausted` | `two_concurrent_first_folds_race_head_cas_and_only_one_advances`, `reconcile_preserves_single_head_cas`, `folder_crash_matrix.rs::concurrent_folders_race_head_cas_without_losing_or_duplicating_data` | Implemented | **Highest** , the single serialization point |
| Crash before HEAD (orphan parts, retry converges) | ADR-0063 §5 crash matrix | content-addressed part PUT + `AlreadyExists` swallow; `existing_by_blake3` reuse gated on `!rebuilt` | `crash_after_partial_part_puts_leaves_old_head_intact`, `rebuild_restores_a_deleted_sealed_part_instead_of_reusing_its_ref` | Implemented | **Highest** |
| Racing folder rebase / stand-down | ADR-0020, ADR-0063 §5 | CAS-loss arm re-enters the loop; top-of-loop `watermark_hour_old >= watermark_hour` → `no_op_report` | as above | Implemented | **High** |
| Fixed-window reconcile (late compaction / rewrite / tombstone) | ADR-0063 §4; `catalog-and-mvcc.md` "Fold reconcile pass" | `fold::{hour_range_buckets, bucket_needs_reconcile, dedup_contribution, same_entry_set, part_covers_dirty_hour}`, `Catalog::reconcile_one_bucket` | `reconcile_applies_late_compaction_before_horizon`, `reconcile_applies_late_tombstone`, `reconcile_ignores_late_record_outside_window`, `reconcile_never_reintroduces_a_drifted_duplicate`, `reconcile_no_change_carries_sealed_parts_forward` | Implemented; runs only on a watermark-advancing fold (§3.2, F16) | **High** |
| Retention-frontier reconcile | ADR-0020 delete blocker; ADR-0078; `catalog-and-mvcc.md` | `fold::{retirement_frontier_hour, frontier_hour_set_buckets}`, `FoldReport::{frontier_hours_reconciled, frontier_hours_deferred}` | `frontier_reconcile_applies_out_of_window_tombstone`, `frontier_reconcile_is_bounded_and_carries_remainder`, `retention_overlay_precedence` | Implemented **and wired** (`services/ravel-server/src/fold.rs::run_loop` → `RetentionConfig::window_for`); runs only on a watermark-advancing fold (F17) | **High** |
| ADR-0020 delete blocker, sweep half | ADR-0020; `catalog-and-mvcc.md` "MVCC rules" | `retention::{SnapshotReachability, SnapshotGate, SnapshotBlock, RetentionOutcome::BlockedBySnapshot}`, `physical_sweep` | `crates/ravel-maintain/tests/retention.rs`, `services/ravel-server/tests/bucket_protection_e2e.rs` | Implemented for retention; **absent on `sweep_superseded` / `sweep_orphans` / `sweep_unreferenced_parts`**, where the substitute is a conditional timing argument (F5) | **Highest** , the gap is the interesting invariant |
| Compaction publish (parts then record, CreateIfAbsent) | ADR-0018 decision 4 | `ravel_maintain::publish::{publish_record, publish_record_with_conservation, resolve_already_exists, verify_already_existed_parts}`, `build::put_part` | `crash_matrix.rs` rows 2/3/4/10/11/13; `tombstone_race.rs` (4 tests) | Implemented | **High** |
| Conservation gate (counts) | ADR-0048 decision 6 | `publish::{ConservationPredicate, conserve_exact, checked_sample_sum}`, `MaintainError::ConservationViolation` | `conservation_mismatch_aborts_publish`, `conservation_surplus_also_aborts`, `conservation_gate_still_fires_with_stamped_parts` | Implemented , **counts only** | Medium |
| Record **multiset** preservation | ADR-0018 "overlap harmlessness"; ADR-0092 | `build::{sample_provenance, provenance_key, sort_merged_scalar, merge_scalar_runs, merge_histogram_runs}` (RSEG v7 per-sample provenance) | `crates/ravel-query/tests/differential_compaction.rs` (+ proptest regressions) | **Not a runtime invariant.** An offline differential test discharges it (F/§4.5) | **Highest** , must be an explicit TLA+ *assumption*, not a checked property |
| Overlap harmlessness at resolve | ADR-0018 decision 5 | `Catalog::process_bucket` (include parts, exclude named inputs, alarm on divergent input sets), `Catalog::classify_bucket` (fold twin; diverges on an unknown key shape, F18) | `compaction_resolution.rs` (11 tests), `fold_compaction_differential.rs` | Implemented; **false for logs/spans** on a two-input-set conflict (no query dedup) | **High** |
| Rewrite supersession (erasure) breaks overlap harmlessness | ADR-0064 decision 3 | `catalog::resolve_rewrite_supersession` (`MAX_REWRITE_SUPERSESSION_DEPTH = 64`, cycle-checked), `build_rewrite_l1_segment_ref`, `fold::build_rewrite_l1_snapshot_entry`, `Catalog::check_rewrite_siblings` | `erasure_resolution.rs` (13 tests), `rewrite_supersession_{cycle,over_deep_chain,depth_bound}` | Implemented | Medium-High |
| Abandoned / unreferenced L1 part sweep | ADR-0018 decision 6; ADR-0048 | `sweep::{sweep_unreferenced_parts, classify_part, PartBranch, bucket_reference_map_scoped}`, `CompactorConfig::unreferenced_part_age_gate_ns` | `sweep_crash_matrix.rs::{unreferenced_part_swept_only_after_age_gate, recovery_over_abandoned_parts_never_loses_a_named_part, tombstoned_abandoned_parts_collected_reverify_proven, recordless_untombstoned_part_is_never_swept}` | Implemented | Medium-High |
| Catalog object (csnap/npost/cstat) sweep | ADR-0063 §6; `catalog-and-mvcc.md` | `sweep::{sweep_unreferenced_catalog_objects, read_head_reference, HeadReference}` | `catalog_sweep_*` (9 unit tests in `sweep.rs`) | Implemented; absent-HEAD = no-anchor, corrupt-HEAD = whole pass fails | Medium |
| Query snapshot pinning + one retry | `catalog-and-mvcc.md` step 7; ADR-0010 §11 | `Catalog::{resolve, resolve_impl, resolve_fanout}` → `Snapshot`; `ravel_query::engine::QueryEngine::resolve_snapshot_with_retry`; `QueryError::SnapshotInvalidated` → 503 | `sweep_crash_matrix.rs::row9_pinned_query_races_sweep_then_reresolves_against_l1`; `engine.rs` retry test; `concurrent.rs::concurrent_ingest_and_query_never_sees_a_partially_visible_flush` | Implemented | **Highest** |
| Snapshot-backed resolve + fallback to listing | ADR-0020 "Resolution"; `catalog-and-mvcc.md` "Snapshot resolution from a folded snapshot" | `Catalog::{resolve_snapshot_window, read_head, load_snapshot_parts, load_one_part, load_snapshot_postings}`, `SnapshotWindow::extract_into`, `parts_intersecting`, `PartLoadOutcome`, `OnePartOutcome` | `folder_crash_matrix.rs` (6 tests), `snapshot_resolve.rs` (7 unit tests), `fold_compaction_differential.rs` | Implemented | **Highest** |
| Corrupt HEAD handling (four divergent policies) | ADR-0020; ADR-0066 decision 2; `catalog-and-mvcc.md` | `fold::get_head` / `HeadState`; `snapshot_resolve::read_head`; `retention::SnapshotReachability::ensure_head`; `sweep::read_head_reference` | `corrupt_head_falls_back_to_rebuild_and_recovers_all_entries`, `newer_format_head_fails_loudly_and_never_clobbers`, `corrupt_head_falls_back_to_listing_never_to_an_error`, `catalog_sweep_corrupt_head_deletes_nothing` | Implemented; fail-open on read, fail-closed on both delete paths, clobber-and-rebuild on fold | **Highest** |
| min-token read-your-write (never via snapshot) | `catalog-and-mvcc.md` steps 5 / 4; ADR-0018 decision 5 | `Catalog::{resolve_min_token, resolve_min_token_fallback}`, `catalog::unsatisfiable_token`, `MIN_TOKEN_RETRY_DELAY`, independent NotFound/transient retry budgets | `min_token_resolves_even_when_its_hour_bucket_is_outside_the_listing_window`, `min_token_unsatisfiable_when_commit_record_is_missing`, `token_fallback_*` (3), `ordering_and_catalog.rs::unsatisfiable_min_commit_token_errors_instead_of_returning_stale_data` | Implemented | **High** |
| Metrics duplicate resolution | ADR-0010 §5; `catalog-and-mvcc.md` "Cross-segment duplicate samples" | `engine::{is_greater, merge_series_runs, histogram_is_greater, merge_histogram_runs}`; `catalog::segment_sort_key`; fold entry sort | `ordering_and_catalog.rs` (2), `snapshot_sort_order.rs`, `mixed_level_snapshot_sort_is_deterministic`, `differential_compaction.rs` | Implemented | **High** |
| Logs/spans: no query dedup, ingest idempotency instead | `docs/consistency-model.md` "logs and spans"; ADR-0051 §5 | `distrib::{merge_log_records, merge_spans}` (never dedup); `ravel-ingest/src/idempotency.rs` (`RIDM` v1); `sweep::sweep_idempotency_markers` | `distrib/tests.rs` duplicate-preservation tests; `ack_and_duplicates.rs`; `idem_*` tests in `sweep.rs`; `services/ravel-server/tests/idempotency_e2e.rs` | Implemented | **High** |
| Seal-divergence audit | ADR-0020 consequences; ADR-0059 decision 2 | `seal_divergence::verify_seal_divergence` (level-0 only, mirrors the fold's supersession exclusion); `ravel-cli catalog verify`; `ravel_server::scrub` | `services/ravel-server/tests/scrub_e2e.rs`; `folder_crash_matrix.rs::commit_in_wrongly_sealed_bucket_is_invisible_until_head_rebuild_repairs_it` | Implemented; detects, never repairs | Medium |
| Advisory compaction claims | ADR-1029 (`Proposed`) | `ravel_fleet::claim` (`COMPACTION_CLAIMS_PREFIX`, `CompactionClaim`, CreateIfAbsent + `CasVersion`) | in-module only | **Implemented, documented as live, called by nothing** (F2) | Low for correctness; **do not model as a lock** |
| RSEG single version | ADR-0027 (`Accepted`, superseded at first release by ADR-0066) | `ravel_segment::SUPPORTED_VERSIONS`, `build::OUTPUT_FORMAT_VERSION = SUPPORTED_VERSIONS.newest()` | `crates/ravel-catalog/tests/postings_exactness.rs` (cross-version), segment-format suites | Implemented | Low |
| Bounded-memory RLOG compaction merge | ADR-0979 (`Proposed`) | decision 3 shipped in `publish.rs`; the cursor/`PartSink` bounding in `rlog.rs` is the outstanding part | `tombstone_race.rs` (4), `crates/ravel-maintain/tests/memory.rs` | Partially implemented under a `Proposed` status (F3) | Low for the catalog model |


---

## Appendix A , symbols verified after the matrix was written

- `ravel_maintain::scan::ScanReport::blocked_by_snapshot` (and its
  `Unreadable`-reason sibling field) is the counter the ADR-0020 delete-blocker
  raises; incremented in `scan_and_maintain_with_memo`'s
  `RetentionOutcome::BlockedBySnapshot(SnapshotBlock::Named)` arm. This is the
  gauge ADR-0078's Context says "climbs every cycle with no operator remedy"
  when the fold half is missing.
- `crates/ravel-catalog/tests/snapshot_sort_order.rs`:
  `snapshot_order_is_a_deterministic_total_order` (single test; pins
  `catalog::segment_sort_key`).
- `crates/ravel-catalog/tests/window_ceiling.rs`:
  `estimate_formula_is_unchanged`, `wide_sparse_window_is_served_by_the_prefix_scan`,
  `oversized_corpus_trips_the_runtime_cap`, `ordinary_window_still_returns_records`
  (these pin `estimated_catalog_requests`, the ADR-0056 crossover, and the
  `WindowTooWide` runtime cap referenced in F12).

## Appendix B , the shortest useful TLA+ shape

If the model needs one sentence per plane:

1. **Commit plane** is append-only and content-addressed. Its only writer
   transition is `CreateIfAbsent`. Deletion is a separate, horizon-gated,
   re-verified action.
2. **Catalog plane** is one CAS register (`HEAD`) pointing at an immutable,
   content-addressed part set. Every non-HEAD write is `CreateIfAbsent`, so
   part writes commute and only the HEAD CAS serializes.
3. **Readers** never write. A reader either uses HEAD (any of its failure modes
   degrade it to listing the commit plane) or lists directly; both derivations
   are specified to produce the same segment set for sealed hours whose buckets
   hold only recognized key shapes, and `fold_compaction_differential.rs` plus
   `seal_divergence` are the two places that claim is checked. The one input on
   which they diverge is an unrecognized key shape (F18): the fold skips it and
   advances, the live resolve fails the query. A model of the equivalence must
   exclude that input or carry the divergence explicitly.
4. **The one cross-plane interlock that exists** is the retention sweep's
   HEAD-reachability gate (`retention::SnapshotReachability::bucket_gate`). The
   compaction-input sweep (`sweep::sweep_superseded`) has no such gate; it relies
   on the reconcile pass observing a supersession before
   `record.created_unix_ns + protection_horizon` elapses. That holds only while
   compaction publishes promptly after its hour seals (F5). Model
   compaction-publish lag as free, and check *no object named by the current HEAD
   is deleted*.
5. **Reconcile is guarded on watermark advance, not on fold tick** (§3.2, F16,
   F17). At the shipped 5-minute interval and 1-hour watermark granularity, about
   one fold in twelve does reconcile work.

---

# Recon: retention, selective erasure, legal holds, physical GC

origin/main bfae457a. Normative: docs/consistency-model.md "Deletion and GC" + "Selective subject erasure" (guarantees); docs/deletion-and-gc.md (mechanism); docs/catalog-and-mvcc.md "Fold reconcile pass" + "Retention-frontier reconcile" (clearing).

## 0. ADR statuses
0018 Accepted; 0019 Accepted; 0020 Accepted (HEAD delete blocker origin); 0042 Accepted (legal hold); 0048 Accepted; 0050 Accepted (s4 sys/gc); 0055 Accepted (amended by 0064 s6); 0058; 0062 (audit keyspace excluded); 0063 Accepted (s4 fold reconcile window); 0064 Accepted BUT two in-place Amendments + a Correction whose "out-of-window case remains open"; 0065; 0077; 0078 accepted (fold retention frontier deployment default); 0082; 1029 Proposed.

## 1. Lifecycle phases
Retention (ADR-0019):
R1 expiry: `retention::max_event_ts`, `retention::is_expired`; driver `retention_sweep_bucket_with_reach`.
R2 tombstone: `retention::write_tombstone` -> `RetentionTombstone` at `t/<th>/<sig>/c/<shard>/<hour>/retire.tmb` (`keys::retention_tombstone_key_for`), CreateIfAbsent + Crc32c, AlreadyExists = success.
R3 exclusion: `Catalog::process_bucket` (has_tombstone -> empty + `invalidate_bucket_cache`); fold `Catalog::classify_bucket`.
R4 HEAD gate: `retention::SnapshotReachability::bucket_gate` (+`ensure_head`, `ensure_part`, `HeadLoad`, `HeadStatus`, `SnapshotGate`, `SnapshotBlock`).
R5 physical delete: `retention::physical_sweep` -> `delete_all` -> `bucket_is_empty_but_tombstone`; order: commit_keys, compaction_record_keys, reconstructed L0 data keys (`keys::reconstruct_data_key`), fresh LIST of `l1/<shard>/<hour>/`, tombstone last. Idempotent DELETE.
`RetentionOutcome::{NoPolicy, NotSealed, NotExpired, Tombstoned, SweptPartial, Swept, BlockedBySnapshot(SnapshotBlock)}`.
Erasure (ADR-0064):
E1 request: `ravel_cli::erase::submit` (`ravel_commit::erasure::validate_request`) -> `ErasureRequest` at `t/<th>/<sig>/del/<request_id>.dreq` (`keys::erasure_request_key`), CreateIfAbsent; AlreadyExists -> read-back `same_erasure` -> `SubmitOutcome::AlreadyPresent` or `SubmitError::ConflictingRequest`.
E2 exclusion: `Catalog::list_pending_erasure` (one LIST of `keys::del_prefix`) -> `Snapshot::pending_erasure`; `ravel_query::erasure::{retain_series_soa, retain_series_aos, retain_histogram_series, retain_series_entries, retain_log_records, is_erased_span}`; `LogQuery::with_erasure`; `engine::snapshot_pending_erasure_predicates`.
E3 rewrite: `erasure_rewrite::erasure_rewrite_bucket` -> `resolve_live_inputs`/`load_live_catalogs_and_target` -> `build_rewrite`|`build_rewrite_logs`|`build_rewrite_spans` -> `publish_rewrite_record`; parts via `build::put_part` BEFORE record; `RewriteRecord` at `c/<shard>/<hour>/rw.<input_set_hash16>.cmt` (`keys::rewrite_record_key_for`), CreateIfAbsent+Crc32c; AlreadyExists -> `resolve_already_exists_rewrite` (same hash -> `Converged{parts_repaired}`; differ -> `InputSetHashDivergence`).
E4 completion: `erasure_rewrite::bucket_erasure_completion` (+`bucket_serves_subject`), driven by `ravel_server::maintain::erasure_rewrite_pass` (`ErasureRewritePass{deferred, catalog_blocked}`) gated in `run_erasure_pass`; `ErasureCompletion` at `del/<request_id>.done` via `write_erasure_completion`, CreateIfAbsent, AlreadyExists -> Ok(false).
E5 superseded delete: `sweep::sweep_superseded` -> `sweep_superseded_impl` arm `BucketEntry::RewriteRecord`, `gather_l0_inputs`, `gather_superseded_predecessor`; records first then data; predecessor record + its L1 parts.
E6 `.dreq` cleanup: `sweep::sweep_erasure_requests` (`ErasureRequestSweepOutcome{deleted, kept}`); deletes .dreq only; unknown del/ entry -> `UnknownBucketEntry`.
Other sweeps: `sweep_orphans`, `sweep_unreferenced_parts`/`_impl` (`classify_part`, `bucket_reference_map_scoped`), `sweep_idempotency_markers` (`parse_marker_hour`, `is_keyhash32`), `sweep_unreferenced_catalog_objects` (`read_head_reference`); entry `sweep_shard` (full), `sweep_shard_zoned`.

## 2. Horizon
`gc_config::GcConfigValues::satisfies_constraint(skew)`: protection_horizon >= max_query_duration +sat grace +sat clock_skew_allowance.
Defaults (`ravel_maintain::config`): `DEFAULT_MAX_QUERY_DURATION_NS`=1h, `DEFAULT_GRACE_NS`=24h, `DEFAULT_CLOCK_SKEW_ALLOWANCE_NS`=5min, `DEFAULT_PROTECTION_HORIZON_NS` = sum = 25h05m, `DEFAULT_MAX_FLUSH_LIFETIME_NS`=1h, `DEFAULT_MAX_COMPACTION_LIFETIME_NS`=1h. max_flush_lifetime NOT a horizon term (feeds `CompactorConfig::orphan_age_gate_ns()` = grace + max_flush_lifetime; unreferenced-part uses grace + max_compaction_lifetime). Fold reconcile window (26h) sized longer than PH by slack; `CatalogConfig::protection_horizon_ns` mirror. clock_skew_allowance not persisted in sys/gc.
sys/gc: `GC_CONFIG_KEY="sys/gc"`, `GC_FORMAT_VERSION=1`, proto `GcConfig`. Validation: main.rs every mode `ravel_server::gc_config::bootstrap` -> `bootstrap_gc_config(store, maintain_defaults(), now)`; Maintain mode `validate_maintain(stored, PH, grace)` exact equality; All|Query `validate_query_deadline(stored, deadline)`; `maintain::spawn` `validate_maintain_skew` -> `GcConfigError::MaintainSkewUncovered`; Flight `validate_flight_ceiling` (PH - grace). Mutation `set_gc_config`: `validate()` all four > 0, `satisfies_constraint`, CasVersion swap (`CasConflict`) or CreateIfAbsent.
Missing/invalid: absent -> bootstrap defaults (starts, not fail-closed); decode fail / UnsupportedVersion / Store -> fail closed; MaintainMismatch / MaintainSkewUncovered / QueryDeadlineExceedsHorizon fail closed. GAP: `Mode::All` skips validate_maintain (sound only because all-mode runs no maintenance; assert it).

## 3. Reachability (highest value)
Doc rule (deletion-and-gc.md retention row): now >= tombstone.retired_at_ns + PH; live HEAD names no object in bucket; bucket LIST-verified empty before tombstone delete. Erasure sweep explicitly has NO HEAD blocker ("deletes an input on schedule even when a snapshot part the fold has not reconciled still names it").
Code: `SnapshotReachability::bucket_gate` once at top of `physical_sweep`. `ensure_head` GETs `t/<tenant_hex>/catalog/<signal>/HEAD` (`retention::catalog_head_key`); covering parts = head.parts with min_hour <= bucket hour <= watermark_hour; block if any entry shard==bucket.shard && ingest_hour_bucket==bucket hour -> `SnapshotBlock::Named`. HEAD NotFound -> `HeadLoad::Absent` -> Clear (NOT a block). HEAD undecodable -> `Unreadable` block. Part blake3 mismatch / decode fail / NotFound -> `Unreadable`. Transient store error -> Err, nothing deleted. Cached once per `scan_and_maintain_with_memo` pass. CURRENT HEAD ONLY; no older-HEAD window; justification: tombstone irreversible.
Negative: `sweep_superseded_impl`, `sweep_orphans`, `sweep_unreferenced_parts`, `sweep_idempotency_markers`, `sweep_erasure_requests` read no HEAD.
Gap A (documented, ADR-0064 OPEN): rewrite in dead band (older than 26h window, newer than frontier) re-listed by neither pass; `sweep_superseded_impl` deletes inputs at rewrite.created + PH with no HEAD check -> stale HEAD names deleted X -> permanent `SnapshotInvalidated` until reconcile/rebuild. Safety argument premises: A1 .dreq deleted at done.completed + PH; A2 inputs deleted at rewrite.created + PH; A3 done.completed >= rewrite.created; A4 hence filter outlives inputs. Eligibility, not execution.
Gap B (candidate counterexample, retention): C1 now >= retired_at + PH (in `retention_sweep_bucket_with_reach`); C2 current HEAD names nothing (in `bucket_gate`). No horizon between C2 flipping and delete. Interleaving: t=0 tombstone; folder stalled / frontier backlog (`frontier_reconcile_max_hours` 168) / fold R != sweep R; HEAD keeps naming H; t=PH+delta C1 true, every sweep BlockedBySnapshot; query Q resolves HEAD_k naming X, holds up to max_query_duration; fold CASes HEAD_{k+1} dropping bucket; next tick Clear -> delete X; Q GETs X -> NotFound -> SnapshotInvalidated. Premises: P1 reader holds <= max_query_duration (`ravel_query::config::DEFAULT_DEADLINE` 30s, `validate_query_deadline`); P2 delete >= retired_at + PH; P3 PH >= mqd + grace + skew; P4 (REQUIRED, NOT ENFORCED) HEAD stopped naming bucket >= mqd + skew before delete; P5 (implicit substitute) frontier look-ahead `frontier_hi = (now - R + PH)/hour` drops bucket a full horizon before P2, only when folder current and fold R == sweep R. Test `sweep_respects_pre_fold_head_then_deletes_after_fold_drops_bucket` = steps 4-5 with no reader. Availability failure (503, one re-resolve) but contradicts literal normative wording.

## 4. Legal holds
Records: ADR-0040 audit records on `Signal::Audit`, shard `legal_hold::AUDIT_HOLD_SHARD=0`, stream `ravel.legal_hold` v1, type `legal_hold`; `write_hold_set`/`write_hold_clear`; `ravel_cli::hold::{set, clear, list}`. `LegalHoldCheck::refresh(store, tenant)` -> `load_hold_records` -> `fold_active_holds` (per scope, greatest (ts_ns, HoldOp::rank()); Set rank 1 > Clear 0, Set wins tie) -> `held_prefixes`. `impl LeaseCheck for LegalHoldCheck`: prefix match. `shard_hold_scopes(tenant, signal, shard)` = `l0/<shard>/`, `commit_shard_prefix`, `l1/<shard>/`; does NOT include `del/`. Refresh failure: server `run_tick` -> error!, `record_legal_hold_refresh_failure`, `return MaintainReport::default()` (whole tenant tick skipped, fail closed, no NoLeases fallback; test `hold_refresh_failure_skips_tenant_tick_and_deletes_nothing`); CLI aborts. LeaseCheck consulted per key in `retention::delete_all`, tombstone delete, `sweep_superseded_impl`, `sweep_orphans` phase a, `sweep_unreferenced_parts_impl`, `sweep_idempotency_markers`, `sweep_unreferenced_catalog_objects`, `sweep_erasure_requests`; per bucket `erasure_rewrite::bucket_is_held` (-> `ErasureRewriteOutcome::Held`; completion `unresolved=true`). Doc conflict F1: deletion-and-gc.md and sweep.rs trait doc say LeaseCheck always unprotected / only NoLeases.

## 5. Mass-orphan breaker
Recomputed predicate, no latch. `sweep_orphans` phase c: `would_trip = count >= orphan_breaker_min_count && count > max_ratio * l0_objects_listed`; `!force_orphan_gc` -> `OrphanBreakerTripped` before any delete. Defaults `DEFAULT_ORPHAN_BREAKER_MIN_COUNT=50`, `DEFAULT_ORPHAN_BREAKER_MAX_RATIO=0.10`. Phases: a LIST + `referenced_l0_identities` + age gate `orphan_age_gate_ns()` + lease; b fresh re-verify; c breaker; d delete. Override `force_orphan_gc` via `ravel-cli maintain sweep --override-orphan-breaker`. Documented limits: dilution (55/500 trips; 55/700 not); partial restoration (49 < 50); per-shard scope; never trips below min_count; up to 10% deletable. Undocumented F5: lease filtering lowers candidate_count not denominator -> hold can untrip.

## 6. Erasure specifics
Identity: `ravel_commit::erasure::compute_rewrite_input_set_hash(inputs, superseded_record_key, applied_request_ids)`: domain `REWRITE_INPUT_SET_HASH_DOMAIN`, discriminant byte, inputs or predecessor key, THEN count + each request id (callers pre-sort; `publish_rewrite_record` sorts). Recomputed in `validate_rewrite`. Amendment collision bug closed.
Predecessor: `RewriteRecord.superseded_record_key` (field 11) = exact key of live CompactionRecord (`l1.<hash16>.cmt`) or prior RewriteRecord (`rw.<hash16>.cmt`); exactly one of inputs/superseded; `RewriteSupersession::{RawL0, Existing}`.
Chase: `ravel_catalog::resolve_rewrite_supersession` (pub) used by `process_bucket`, `classify_bucket`, `resolve_min_token_fallback`, `bucket_erasure_completion`; cycle -> `RewriteSupersessionCycle`; depth `MAX_REWRITE_SUPERSESSION_DEPTH` -> `RewriteSupersessionChainTooDeep`; ABSENT predecessor ends chase cleanly, excludes only what found (F11).
Repeated erasure: `erasure_rewrite_bucket` `AlreadyApplied` guard via `live_rewrite_applied_request_ids`; new request falls through; drops name every overlapping request incl. applied at dropped_count 0.
Conservation: `publish_rewrite_record` output + sum(drops) == input (`checked_sample_sum`) else `ErasureConservationViolation`; `input_footer_cross_check` (logs/spans) -> `ErasureInputConservationViolation`; abandonment `max_compaction_lifetime_ns` -> `Abandoned` before gate.
Completion (three layers): per-bucket `bucket_erasure_completion` front gates (!sealed -> out of scope blocks nothing; tombstone -> nothing; held -> unresolved); reconstruct served set -> `bucket_serves_subject(request, live_l0, live_compactions, live_rewrites)` true on live L0 overlap / live compaction part / live rewrite whose drops do NOT name request (sibling case) -> `blocked`; `erasure_rewrite_pass` accumulates deferred/catalog_blocked; `run_erasure_pass` writes .done only when !deferred && not blocked.
Cache exclusion: filter after fetch and cache in every path; logs columnar fast path refuses when erasure non-empty; disk residue bounded by `ravel_cache` `max_entry_age_ns`.
.dreq cleanup `sweep_erasure_requests`: needs matching .done; `completed_unix_ns != 0`; now >= completed + PH; `!lease.is_protected(dreq_key)`.

## 7. Tests
retention.rs: retention_config_floor_boundary, no_sample_younger_than_r_is_ever_excluded (proptest), tombstone_irreversible_when_r_is_raised, compactor_racing_tombstone_retention_wins, partial_sweep_crash_then_converges, l1_delete_crash_then_converges, retention_lifecycle_uncompacted_bucket, no_policy_is_a_noop, sweep_blocked_when_head_names_bucket, sweep_proceeds_when_head_absent, sweep_blocked_fail_closed_when_head_undecodable, sweep_respects_pre_fold_head_then_deletes_after_fold_drops_bucket, retention_of_out_of_window_hour_never_leaves_snapshot_naming_deleted_objects, deployment_default_retention_drives_frontier_reconcile_and_sweep, scan_reports_blocked_by_snapshot_counter, scan_reports_blocked_by_unreadable_head_counter.
catalog.rs: tombstoned_bucket_is_excluded_from_resolution, token_over_tombstoned_bucket_is_satisfied_with_zero_segments, tombstone_observation_invalidates_cached_commit_records. fold.rs: reconcile_applies_late_compaction_before_horizon, reconcile_applies_late_tombstone, reconcile_ignores_late_record_outside_window, frontier_reconcile_applies_out_of_window_tombstone, frontier_reconcile_is_bounded_and_carries_remainder, reconcile_never_reintroduces_a_drifted_duplicate, reconcile_no_change_carries_sealed_parts_forward, first_and_rebuilt_folds_skip_reconcile, reconcile_preserves_single_head_cas.
sweep_crash_matrix.rs: row7_partial_input_records_deleted_reswept_converges, row8_records_deleted_data_not_orphan_gc_converges, row9_pinned_query_races_sweep_then_reresolves_against_l1, row12_token_get_notfound_post_sweep_found_in_input_list, convergence_crash_mid_sweep_then_reruns_clean, no_delete_before_horizon_boundary_stepped, unreferenced_part_swept_only_after_age_gate, recovery_over_abandoned_parts_never_loses_a_named_part, tombstoned_abandoned_parts_collected_reverify_proven, young_tombstoned_recordless_part_survives_age_gate, recordless_untombstoned_part_is_never_swept, orphan_gc_respects_live_records_and_age_gate, dry_run_sweep_reports_eligible_set_but_deletes_nothing, sweep_shard_zoned_defers_out_of_scope_hour_to_full_sweep, tombstoned_interior_bucket_swept_no_later_than_full_pass.
sweep.rs: mass_orphan_trips_breaker_and_deletes_nothing, below_threshold_pass_still_deletes_normally, forced_pass_deletes_and_reports_override, batched_reverify_lists_commit_prefix_once_per_pass; maintain.rs: orphan_breaker_withheld_gauge_drops_but_trip_counter_does_not, orphans_present_gauge_tracks_latest_pass_and_is_not_sticky.
legal_hold.rs: active_hold_blocks_delete_that_would_otherwise_happen, cleared_hold_lets_the_next_pass_delete, conflicting_holds_resolve_by_latest_record, empty_holds_equals_no_leases, empty_scope_is_rejected_before_any_write; maintain.rs: held_bucket_survives_retention_tick, hold_refresh_failure_skips_tenant_tick_and_deletes_nothing, a_held_bucket_defers_erasure_completion_and_keeps_the_dreq.
erasure_rewrite.rs: rewrite_drops_matching_series_preserves_others_bit_identically, conservation_mismatch_aborts_rewrite_publish, legal_hold_skips_bucket_leaves_dreq_pending, republishing_the_same_rewrite_converges_without_double_counting, completed_request_does_not_republish_on_second_pass, truncated_input_page_yields_typed_error_not_panic, rewrite_merges_same_series_runs_across_multiple_l0_inputs, windowed_request_matches_backfilled_samples_outside_ingest_hour, rewrite_reencodes_partially_surviving_series_bit_identically; logs_* and spans_* variants; maintain_reconstruction_agrees_with_the_query_served_set.
catalog.rs: rewrite_supersession_cycle_is_a_typed_error, rewrite_supersession_over_deep_chain_is_a_typed_error, rewrite_supersession_depth_bound_is_exact_at_the_maximum.
erasure_sweep.rs: rewrite_raw_l0_inputs_swept_only_after_horizon, rewrite_superseded_inputs_survive_under_legal_hold, rewrite_predecessor_record_and_parts_swept_after_horizon, dreq_without_done_is_never_removed, dreq_removed_at_horizon_boundary_not_a_nanosecond_early, held_dreq_survives_past_horizon, zero_completion_timestamp_keeps_dreq; maintain.rs: done_follows_the_catalog_resolver_not_the_one_hop_live_record, run_tick_rewrites_for_an_erasure_request_then_sweeps_its_dreq, second_tick_over_an_already_erased_bucket_publishes_no_new_rewrite, a_failed_done_write_keeps_the_dreq_and_completes_on_a_later_tick, erasure_predicate_hash_is_order_stable_and_carries_no_plaintext.
gc_config_e2e.rs: startup_rejects_deadline_exceeding_horizon, fresh_bucket_bootstraps_gc_config_and_starts_cleanly, fresh_bucket_no_process_refuses_startup, maintain_starts_after_gc_config_set_and_matching_flags, maintain_still_refuses_on_genuine_mismatch, query_starts_after_gc_config_set_and_matching_deadline_flag; spawn_fails_closed_when_running_sweeper_skew_exceeds_stored_horizon.
Holes: no pinned pre-fold reader vs post-fold delete (Gap B); no hold arriving after .done (F2); no fold-R vs sweep-R divergence (F4); no sweep_superseded vs stale folded snapshot (Gap A).

## 8. Findings
F1 LeaseCheck docs stale (deletion-and-gc.md "Reader leases are not implemented... nothing depends on it"; sweep.rs trait doc) - LegalHoldCheck is production, correctness depends on it.
F2 CANDIDATE COUNTEREXAMPLE (safety): hold-scope asymmetry. `shard_hold_scopes` covers l0/c/l1 not del/. Hold set AFTER .done (completion blocks while held, so hold must arrive after). At done + PH: `sweep_superseded_impl` skips held inputs (live, GET-able; test `rewrite_superseded_inputs_survive_under_legal_hold`); same tick `sweep_erasure_requests` deletes .dreq (`is_protected(dreq_key)` false). `list_pending_erasure` empty; filter gone. Bucket hour in dead band -> folded snapshot names pre-rewrite input which still exists -> query serves erased subject permanently, no alarm. Same shape from delete fault, maintain stopped mid-cycle, Object Lock. Invariant: dreq_deleted(r) => forall superseded inputs i: !exists(i) \/ !named_by_any_resolvable_snapshot(i).
F3 ADR-0048 "sticky halt" wording wrong; deletion-and-gc.md correct (stateless predicate).
F4 Two sources of R: sweep `RetentionConfig::window_for` (CLI flags only; no `retention_ns`/`TenantConfig` in ravel-maintain); fold overlays durable `TenantConfig.retention_ns` (ADR-0078). Durable R > CLI R -> tombstoned hour never a frontier candidate, HEAD names it, BlockedBySnapshot forever until hour ages past durable R (liveness stall; precondition for Gap B).
F5 Undocumented breaker dilution via hold.
F6 Accepted but unimplemented: `erasure_rewrite_deadline` (no knob, no metric); blocked request silent. Existing metric families: tenants_discovered, tenants_maintained, tenant_discovery_failures_total, legal_hold_refresh_failures_total, conservation_aborts_total, orphan_breaker_tripped_total, orphans_withheld, orphans_present, workers_live, units_owned, units_stalled, memo_warm_start_units_total, full_sweep_passes_total, rlog_merge_peak_bytes.
F7 Accepted but unimplemented: `deferral_cause` (write_erasure_completion hardcodes Unspecified; bucket_drops empty; only runs when !deferred).
F8 Field drift: docs say done.created_unix_ns; proto has requested_unix_ns=7, completed_unix_ns=8; code anchors completed. `completed_unix_ns: now.max(request.created_unix_ns)` - backwards clock shortens .dreq retention.
F9 Mode::All skips validate_maintain.
F10 .done sufficiency rests on unenforced premise (subject ids never in metric names); also audit keyspace, DeleteMarkerReplication, lifecycle on t/** sys/** unenforced.
F11 Absent-predecessor under-exclusion; completion check uses same function, same hole; reachability question for model.
F12 SnapshotReachability cache freshness leans on tombstone irreversibility; invariant once_dropped_from_HEAD(b) => [] !named_by_HEAD(b).
Minor: `sweep_erasure_requests` runs every tick per (tenant, signal) while interior superseded delete only on full sweep (6h cadence) -> hold-free route to F2 inversion in window between done+PH and next full sweep. `physical_sweep` LISTs l1 fresh. `check_rewrite_siblings` detect-only.

## 9. Matrix
| Protocol | Normative source | Rust implementation | Existing tests | Status | Model priority |
|---|---|---|---|---|---|
| Tombstone write + irreversibility | ADR-0019 d1-2 | `retention::{is_expired, max_event_ts, write_tombstone}`, `RetentionConfig` | retention_config_floor_boundary, tombstone_irreversible_when_r_is_raised, compactor_racing_tombstone_retention_wins | Implemented | Medium |
| Tombstone exclusion + cache invalidation | ADR-0019 d3; ADR-0010 s10 | `Catalog::process_bucket` | tombstoned_bucket_is_excluded_from_resolution, tombstone_observation_invalidates_cached_commit_records | Implemented | Low |
| Horizon formula + sys/gc fences | ADR-0050 s4; deletion-and-gc.md | `GcConfigValues::{satisfies_constraint, validate, flight_ceiling_ns}`, `bootstrap_gc_config`, `set_gc_config`, `validate_maintain`, `validate_maintain_skew`, `validate_query_deadline`, `validate_flight_ceiling` | gc_config_e2e.rs (6), spawn_fails_closed_* | Implemented | High |
| HEAD delete blocker (retention) | ADR-0020; deletion-and-gc.md | `SnapshotReachability::{bucket_gate, ensure_head, ensure_part}`, `SnapshotBlock`, `physical_sweep` | sweep_blocked_when_head_names_bucket, sweep_proceeds_when_head_absent, sweep_blocked_fail_closed_when_head_undecodable, sweep_respects_pre_fold_head_* | Implemented; current HEAD only, no post-drop horizon (Gap B) | Highest |
| Fold reconcile + frontier band | ADR-0063 s4, 0020, 0078 | `Catalog::fold` (`retirement_frontier_hour`, `frontier_hour_set_buckets`, `classify_bucket`), `FoldReport` | reconcile_*, frontier_reconcile_*, retention_of_out_of_window_hour_*, deployment_default_retention_* | Implemented; fold R != sweep R (F4) | Highest |
| Superseded-input sweep | ADR-0018; ADR-0064 d3.6 | `sweep_superseded_impl`, `gather_l0_inputs`, `gather_superseded_predecessor` | row7, row9, no_delete_before_horizon_boundary_stepped, rewrite_raw_l0_inputs_swept_only_after_horizon, rewrite_predecessor_record_and_parts_swept_after_horizon | Implemented; no HEAD gate by design; out-of-window OPEN (Gap A) | Highest |
| Orphan GC | ADR-0010 s11; 0048 d5 | `sweep_orphans` a,b; `referenced_l0_identities`; `orphan_age_gate_ns` | orphan_gc_respects_live_records_and_age_gate, batched_reverify_* | Implemented | Medium |
| Mass-orphan breaker | ADR-0048 d4 | `sweep_orphans` c `would_trip`; `OrphanBreakerTripped`; config | mass_orphan_trips_breaker_and_deletes_nothing, below_threshold_*, forced_pass_* | Recomputed predicate; ADR wording wrong (F3); hold dilution (F5) | Medium-High |
| Unreferenced-part sweep | ADR-0018 | `sweep_unreferenced_parts_impl`, `classify_part` | unreferenced_part_swept_only_after_age_gate, ... | Implemented | Low |
| Marker sweep | ADR-0051 s5 | `sweep_idempotency_markers` | idem_markers_* | Implemented | Low |
| Catalog-object sweep | sweep.rs doc | `sweep_unreferenced_catalog_objects`, `read_head_reference` | catalog_sweep_* (11) | Implemented, fail closed on corrupt HEAD | Low |
| Legal hold observation + fold | ADR-0042 d2; 0040 d3; 0048 d1-2 | `LegalHoldCheck::{refresh, is_protected}`, `fold_active_holds`, `HoldOp::rank`, `shard_hold_scopes`, `write_hold_set/clear` | active_hold_blocks_*, cleared_hold_*, conflicting_holds_*, held_bucket_survives_retention_tick | Implemented; docs stale (F1) | High |
| Hold refresh failure | ADR-0048 d1 | `run_tick` early return | hold_refresh_failure_skips_tenant_tick_and_deletes_nothing | Implemented fail closed | Medium |
| LeaseCheck hook | deletion-and-gc.md (stale) | `LeaseCheck`, `NoLeases`, `LegalHoldCheck` | empty_holds_equals_no_leases | Implemented, doc wrong | Medium |
| Erasure request (.dreq) | ADR-0064 d1 | `erase::{submit, same_erasure}`, `validate_request`, `erasure_request_key` | erase.rs tests; run_tick_rewrites_* | Implemented | High |
| Immediate query exclusion | ADR-0064 d2 | `list_pending_erasure`, `Snapshot::pending_erasure`, `retain_*`, `is_erased_span`, `with_erasure`, columnar refusal, cache `max_entry_age_ns` | engine tests; rewritten_log_segment_index_no_longer_resolves_the_erased_subject | Implemented | High |
| Rewrite identity | ADR-0064 Am1 | `compute_rewrite_input_set_hash`, `validate_rewrite`, `superseded_record_key` | republishing_the_same_rewrite_converges_* | Implemented | Medium |
| Supersession chase | ADR-0064 Am1 + s4 | `resolve_rewrite_supersession`, `MAX_REWRITE_SUPERSESSION_DEPTH` | rewrite_supersession_* | Implemented; absent-predecessor open (F11) | High |
| Rewrite conservation | ADR-0064 d3.4; 0048 d6 | `publish_rewrite_record`, `checked_sample_sum`, `input_footer_cross_check` | conservation_mismatch_* | Implemented | Medium |
| Completion verification | ADR-0064 s4 + Am2 | `bucket_erasure_completion`, `bucket_serves_subject`, `erasure_rewrite_pass`, `run_erasure_pass` | done_follows_the_catalog_resolver_*, a_held_bucket_defers_*, a_failed_done_write_* | Implemented; no deadline alarm (F6); deferral_cause dead (F7) | Highest |
| Sibling detection | ADR-0064 s4 | `check_rewrite_siblings` (detect), `bucket_serves_subject` sibling arm (block) | none for check_rewrite_siblings | Implemented | High |
| .dreq cleanup | ADR-0064 d5 | `sweep_erasure_requests` | dreq_without_done_is_never_removed, dreq_removed_at_horizon_boundary_*, held_dreq_survives_past_horizon, zero_completion_timestamp_keeps_dreq | Implemented; hold-scope counterexample (F2); anchor drift + clamp (F8) | Highest |
| .done permanence | ADR-0064 d1 s6; 0055 am | `write_erasure_completion` | zero_completion_timestamp_keeps_dreq | Implemented; IAM out of band | Medium |
| Retention window resolution | ADR-0078 | fold overlay vs `RetentionConfig::window_for` | deployment_default_retention_* | Divergent (F4) | High |
| Zoned vs full sweep | ADR-0065 d3 | `sweep_shard_zoned`, `full_sweep_due` | sweep_shard_zoned_defers_*, tombstoned_interior_bucket_* | Implemented | Medium |
| Erasure deadline alarm | ADR-0064 s4 | none | none | Accepted, unimplemented (F6) | Medium |
| Subject-in-name prohibition | ADR-0064 s7 | none | none | Environment assumption (F10) | Medium |

Suggested modelling order: Gap B; F2; Gap A + F4; F3/F5; F11.

---

# Recon: generation-versioned online resharding (ADR-0052)

Worktree: origin/main @ bfae457a

## 0. Two premises corrected

(a) Write-side routing uses wall clock at record arrival, not the pinned flush hour. `Router::write_points` (crates/ravel-ingest/src/router.rs) calls `self.active_set(tenant.hash(), self.clock.now_ns())`, then `shard_for(&point.series_id, set.len())`. Set size from `active_shard_count(&view.generations, hour_of(now_ns))`. Ingest-hour bucket pinned separately, later, at flush open by `checked_ingest_hour_bucket(flush_open_ns)` (crates/ravel-ingest/src/config.rs). Gap between divisor hour and key hour is what slack S covers. log_router.rs / span_router.rs identical shape.

(b) ADR inequality is about L and C only: `L >= ceil(C) + 1`. max_flush_lifetime and skew are terms of S: `S = FLUSH_BOUND_SLACK_HOURS + TOLERATED_CLOCK_SKEW_HOURS = 2 + 1 = 3` (in-file Amendment, normative). Coupling clause: reverting read slack while keeping grace window reopens split-brain; reverting grace while keeping slack only over-scans.

## 1. Status: IMPLEMENTED (Accepted, shipped)

| Piece | Symbol | Where |
|---|---|---|
| Generation record | `ShardGeneration`; `ProvisioningRecord.generations` field 6 | proto/ravel/sys.proto |
| Key | `provisioning_key` -> `t/<tenant_hash>/<sig>/prov` | crates/ravel-catalog/src/provisioning.rs |
| History + validation | `ShardGeneration`, `read_generations`, `read_generations_checked`, `read_generations_from_store`, `read_generations_accounted`, `GenerationDefect` | provisioning.rs |
| CAS append | `append_generation`, `ReshardOutcome`, `ProvisioningError::ReshardCasConflict` (`PutMode::CasVersion`) | provisioning.rs |
| Write routing | `active_shard_count` | provisioning.rs |
| Router cache | `GenerationSwitch`, `TenantView`, `Routed::{Fresh,Stale}`, `route_cached`, `refresh`, `ensure_set`, `all_sets`, `evict_idle` | crates/ravel-ingest/src/generation.rs |
| Writer fence | `Router::active_set`/`LogRouter::active_set`/`SpanRouter::active_set`; `WriteError::StaleProvisioningView` (+Log/Span variants); `IngestMetrics::record_stale_provisioning_flush` | ravel-ingest routers |
| Degraded grace | `GenerationSwitch::try_grace_extend`, `min_lead_hours`, `record_grace_extended_stale_flush` | generation.rs |
| Scan slack | `scan_count`, `max_scan_count_over_range`, `DEFAULT_SCAN_SLACK_HOURS` | provisioning.rs |
| Reader fan-out | `Catalog::read_scan_generations`, `Catalog::resolve_pruned_with_generations`, `Catalog::list_window_bounded`, `Catalog::list_window_by_prefix` | crates/ravel-catalog/src/catalog.rs |
| Fold scan set | `hour_range_buckets`, `frontier_hour_set_buckets`, `fold_shard_ceiling` | crates/ravel-catalog/src/fold.rs |
| Fold ceiling/HEAD stamp | `shard_ceiling`; `SnapshotHead.shard_generation_count` (field 10) = `generations.len()` | provisioning.rs, fold.rs, proto/ravel/catalog.proto |
| Reader fence / safely-old HEAD | `head_generations_acceptable`, `SnapshotResolve::validate_head_against_generations`, `head_shard_count_mismatch` | crates/ravel-catalog/src/snapshot_resolve.rs |
| Commit token | `CommitToken` (v2), `commit_key_for_token` | crates/ravel-types/src/lib.rs, crates/ravel-commit/src/keys.rs |
| CLI | `ravel_cli::provision::reshard`, `MIN_LEAD_HOURS`, `write_reshard_audit` | services/ravel-cli/src/provision.rs, crates/ravel-maintain/src/provision_audit.rs |
| Operator | `reshard_tenant_signal`, `MIN_RESHARD_LEAD_HOURS`, `RESHARD_CAS_ATTEMPTS`, `ShardOverridesSpec` | services/ravel-operator/src/{controller,crd}.rs |
| Pushdown gate | `stable_generation_for_hour`, `all_hours_in_one_stable_generation`, `is_pushdown_eligible` | provisioning.rs, crates/ravel-query/src/distrib/pushdown.rs |
| Background refresher on C | NO SYMBOL | refresh is lazy, write-triggered |
| ADR-0082 drift tolerance | NOT APPLIED; `ShardCountMismatch` branch still in `validate_record` | provisioning.rs |

Amendments: in-file amendment (grace + skew slack) shipped. ADR-0082 (Accepted) not implemented.

## 2. Constants

| Symbol | Value | Config | Location |
|---|---|---|---|
| C = `DEFAULT_REFRESH_INTERVAL_NS` | 60 s | not configurable | generation.rs |
| `min_lead_hours(C)` | ceil_hours(C)+1 = 2 | derived | generation.rs |
| L = `MIN_LEAD_HOURS` | 2 | CLI `--lead-hours` | provision.rs |
| L = `MIN_RESHARD_LEAD_HOURS` | 2 | CRD `spec.shardOverrides.leadHours` | crd.rs |
| S = `DEFAULT_SCAN_SLACK_HOURS` | 3 | compile-time | provisioning.rs |
| `FLUSH_BOUND_SLACK_HOURS` | 2 | ceil(max_flush_delay_idle + max_flush_lifetime) | provisioning.rs |
| `TOLERATED_CLOCK_SKEW_HOURS` | 1 | mirrors `IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS` | provisioning.rs |
| `max_flush_lifetime` | 3600 s | `IngestConfig`, not a flag | config.rs |
| `max_flush_delay` / `_idle` | 2 s / 40 s | flags | config.rs |
| `FLUSH_BOUND_SLACK_HOURS_NS` | startup bound on --max-flush-delay-idle | services/ravel-server/src/config.rs |
| `MAX_SHARD_COUNT` | 10000 | at append | provisioning.rs |
| `RESHARD_CAS_ATTEMPTS` | 3 | operator | controller.rs |

Enforcement: L>=ceil(C)+1 at callers only (CLI bails, operator `ShardOverrideError::LeadHoursTooShort`), never in `append_generation` (only checks activation > last and > now_hour: `ActivationInPast`). S>=flush bound: `Cli::validate` rejects `--max-flush-delay-idle` past `FLUSH_BOUND_SLACK_HOURS_NS` (tests `flush_delay_exceeding_flush_bound_slack_hours_is_rejected_at_startup`, `flush_delay_idle_..._rejected_at_startup`).

## 3. Write path

Generation selection: `write_points` -> `active_set(tenant_hash, clock.now_ns())` -> `route_cached` -> `active_shard_count(gens, hour_of(now))` = count of latest gen with activation_hour <= hour(now). `ensure_set` keeps one actor set per distinct count; sets never removed; retiring sets drain. Fence: `route_cached` Fresh only if `now - refreshed_at <= C`; Stale on first touch (no gen-0 assumption). On Stale: `load_generations` (NotFound -> implicit gen 0 at default count); on success refresh+route; on failure `try_grace_extend`: Some(set) if `hour_of(now) < hour_of(refreshed_at) + min_lead_hours(C)` (route on last-known-good, metric); None -> `StaleProvisioningView` fail closed.

## 4. Read/fold path

scan_count(h) = max{count(g) : activation(g) <= h and h < activation(g+1) + S}, successor activation = infinity if none. `max_scan_count_over_range(gens, start, end, S)`: g contributes iff activation(g) <= end and start < succ_activation(g) + S. Decrease: retiring wider gen stays in scan set for h < activation(g+1)+S. `list_window_bounded` uses union bound then per-bucket re-filter dropping shard >= hour_scan. All call sites pass `DEFAULT_SCAN_SLACK_HOURS`.

Generations source: `Catalog::read_scan_generations` fresh uncached on every resolve/fold via `guarded_get`. `enforce_provisioning == false` -> implicit gen 0, no store read (most in-crate, ravel-query, ravel-sql callers). `true` (server `build_catalog`) -> real read; absent -> gen 0; corrupt -> hard `CatalogError`.

Reader fence `validate_head_against_generations`: (1) `head_generations_acceptable` -> accept; (2) head.shard_generation_count == reader count but ceiling disagrees -> fail closed, no re-read (`head_shard_count_mismatch`, FieldMismatch, no listing fallback); (3) else exactly one uncached re-read, re-check, propagate fresher history, else fail closed.

Safely-old HEAD `head_generations_acceptable`: accept if head.shard_count == shard_ceiling(gens, head.watermark_hour); reject if head.shard_generation_count >= reader count; else first_unknown = max(head.shard_generation_count,1); accept iff gens[first_unknown] exists and head.watermark_hour < its activation_hour.

Asymmetry: `shard_ceiling(gens, watermark)` = max{count(g): activation(g) <= watermark}, monotone, no slack bound. `scan_count` non-monotone. Both fold writer (`fold_shard_ceiling`) and reader use `shard_ceiling` to avoid the past-slack divergence that once hard-failed every query.

Maintain: server maintain.rs computes scan_shards = max over all gens (incl. not-yet-active) inline; CLI maintain.rs uses `shard_ceiling(&gens, now_hour)`, `ShardCountDisagreement`, `NoProvisioningRecord`.

## 5. Commit token

`CommitToken { shard, writer_id, epoch, seq, ingest_hour_bucket }`, wire `v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`. No generation, no count. `commit_key_for_token` -> `commit_key(...)` exact key GET, no count param, no list. Negative MUST (no validation of token.shard against count) holds by absence; grep found zero comparisons; no test asserts it.

## 6. Tests

provisioning.rs: active_shard_count_*, scan_count_* (single, increase_boundary, decrease_slack_window, zero_slack_has_no_straggler_window, three_generations, no_overflow), max_scan_range_*, stable_generation_for_hour_*, nf_stable_generation_for_hour_slack_margin_is_load_bearing, nf3_default_scan_slack_hours_includes_clock_skew_term, nf3_clock_skewed_straggler_visible_only_after_slack_widening, empty_generations_*, read_generations_checked_rejects_*, corrupt_generation_histories_are_typed_errors, append_generation_* (succeeds, rejects_past_activation, rejects_same_count, rejects_missing_record, cas_conflict_on_concurrent_append); proptests stable_generation_for_hour_matches_scan_set, stable_hour_scan_count_is_that_generations_count, stable_generation_for_hour_slack_window_edges.
generation.rs: first_touch_tenant_is_stale_and_requires_a_real_read, refresh_switches_routing_and_old_set_survives, cached_view_goes_stale_past_c, min_lead_hours_matches_ceil_c_plus_one, try_grace_extend_* (4), all_sets_includes_every_generation.
routers: activation_routes_to_new_generation_set, stale_view_routes_via_grace_window_then_fails_closed_past_horizon (span_router), nf2_grace_window_survives_sustained_store_latency_then_fails_closed, nf2_healthy_store_never_takes_the_grace_path (log_router).
snapshot_resolve.rs: head_generations_predicate_covers_all_arms, decrease_past_slack_head_validates_against_reader_ceiling, older_head_reaching_unknown_generation_active_hours_is_rejected.
catalog.rs: provisioning_generation_read_is_accounted_get, provisioning_read_holds_a_resolve_semaphore_permit, resolve_rejects_older_head_reaching_unknown_generation_hours, resolve_reread_widens_listing_scan_set, resolve_enforces_provisioning_record_mismatch, resolve_without_enforcement_ignores_provisioning_record.
crates/ravel-catalog/tests/resharding.rs: resolve_across_increase_returns_both_shard_ranges, resolve_across_decrease_finds_straggler_in_slack_window, fold_records_ceiling_and_generation_count, pre_reshard_head_is_accepted_after_a_later_reshard, head_ahead_of_reader_fails_closed.
tests/resharding_prefix_traversal.rs: prefix_decrease_finds_straggler_in_retiring_shard_and_matches_per_bucket, prefix_increase_returns_both_shard_ranges.
services/ravel-server/tests/reshard_e2e.rs: increase_with_concurrent_ingest_returns_complete_results, decrease_with_concurrent_ingest_returns_complete_results, decrease_straggler_write_inside_slack_window_is_found, pre_reshard_snapshot_head_still_validates_after_reshard, read_your_write_token_resolves_after_decrease, stale_view_routes_via_grace_window_then_fails_closed_past_horizon, grace_routed_decrease_straggler_is_covered_by_widened_slack.
CLI/operator/audit: reshard_appends_generation_and_writes_audit, reshard_rejects_lead_below_minimum, reshard_rejects_same_count, reshard_tenant_signal_rejects_lead_hours_below_the_minimum_before_any_store_access, shard_overrides_is_optional_and_lead_hours_defaults_to_the_minimum, write_reshard_audit_lands_one_object_and_commit.
Gap: no test drives the CAS loser's re-read-and-retry to a successful second append (operator has RESHARD_CAS_ATTEMPTS loop; CLI one-shot).

## 7. Findings

1. ADR-0082 Accepted but NOT implemented: `validate_record` still scalar-equality `ShardCountMismatch`; `ProvisioningCheck::Matched` unrenamed; CLI adopt message unchanged. Global `--shards` default change breaks ingest/startup/query/maintain for existing tenants. Availability hazard, not per-tenant reshard hazard.
2. Background refresher on C does not exist. `refresh_generations` has no production caller. router.rs doc comment and docs/catalog-and-mvcc.md ("Every live writer re-reads the record at least once per C") are false. Refresh is lazy at write. Fence still sound (evaluated at write). Model: TTL checked at write, no poll action.
3. Two divergent maintain scan rules: server maintain.rs inline max over all gens (incl. inactive; over-scan, safe); CLI uses `shard_ceiling`. Drift risk.
4. `Catalog::estimated_catalog_requests` uses synthesized gen 0, not real history (advisory only).
5. L >= ceil(C)+1 enforced at callers only; `append_generation` never checks. Safe today because C is a const and both `2` literals agree. Model L as caller-supplied; check L=1.
6. `FLUSH_BOUND_SLACK_HOURS` manual const; startup guard covers `--max-flush-delay-idle` drift only, not `max_flush_lifetime` drift.
7. Clock skew bound (appender within TOLERATED_CLOCK_SKEW_HOURS of every router) has no runtime check. Prime TLA+ target: unconstrained skew, find first break of "no admitted write routed to a shard index the scan set for its hour does not cover".
8. Open questions answered in code not ADR; `TOLERATED_CLOCK_SKEW_HOURS` doc comment stale (asks for a future amendment that already exists in-file).
9. Two hour clocks in write path (divisor hour at admission; key hour at flush open); divergence bounded only by max_flush_delay_idle + max_flush_lifetime; nowhere a checkable invariant.
10. `enforce_provisioning == false` is a distinct mode: generations degenerate to one. Model as precondition.

## 8. Matrix

| Protocol | Normative source | Rust implementation | Existing tests | Status | Model priority |
|---|---|---|---|---|---|
| Generation history object | ADR-0052 s1; sys.proto; docs/catalog-and-mvcc.md | `provisioning_key`, `ShardGeneration`, field 6 | decode/corrupt tests | Implemented | Medium |
| CAS append | ADR-0052 s1 | `append_generation`, `PutMode::CasVersion`, `ReshardCasConflict` | append_generation_* | Implemented | High |
| History validation | ADR-0052 s1,s7 | `read_generations_checked`, `GenerationDefect` | corrupt_generation_histories_are_typed_errors | Implemented | Medium |
| Write routing (wall-clock hour) | ADR-0052 s2; docs/ingest.md | `active_shard_count`; `Router::write_points`/`active_set` | active_shard_count_*, activation_routes_to_new_generation_set | Implemented | High |
| Router cache + C | ADR-0052 s2-3 | `GenerationSwitch`, `route_cached`, `DEFAULT_REFRESH_INTERVAL_NS` | cached_view_goes_stale_past_c | Implemented, lazy not periodic | High |
| Writer fence | ADR-0052 s2 | `active_set` x3, `Routed::Stale`, `StaleProvisioningView` | nf2_*, stale_view_routes_via_grace_window_then_fails_closed_past_horizon | Implemented | High |
| Degraded grace | Amendment | `try_grace_extend`, `min_lead_hours` | try_grace_extend_* | Implemented | High |
| Lead L | ADR-0052 s3 | `MIN_LEAD_HOURS`, `MIN_RESHARD_LEAD_HOURS`, `min_lead_hours`; not in append | min_lead_hours_matches_ceil_c_plus_one, reshard_rejects_lead_below_minimum | Callers only | High |
| Scan slack S | s4 + Amendment | `scan_count`, `max_scan_count_over_range`, `DEFAULT_SCAN_SLACK_HOURS` | scan_count_*, nf3_*, decrease_straggler_* | Implemented | Highest |
| Reader fan-out | s4 | `read_scan_generations`, `resolve_pruned_with_generations`, `list_window_bounded`, `list_window_by_prefix` | resolve_across_*, prefix_* | Implemented | High |
| Fold buckets | s4 | `hour_range_buckets`, `frontier_hour_set_buckets`, `fold_shard_ceiling` | fold_records_ceiling_and_generation_count | Implemented | Medium |
| Safely-old HEAD + reader fence | s5 | `head_generations_acceptable`, `validate_head_against_generations` | head_generations_predicate_covers_all_arms, head_ahead_of_reader_fails_closed, pre_reshard_* | Implemented | High |
| Commit token | s6 | `CommitToken` v2, `commit_key_for_token` | read_your_write_token_resolves_after_decrease | Implemented; MUST holds by absence | Medium |
| Maintain scan range | s4 | server inline max; CLI `shard_ceiling` | indirect | Two divergent copies | Medium |
| validate_record semantics | ADR-0082 | `validate_record` scalar equality | resolve_enforces_provisioning_record_mismatch | ADR-0082 NOT implemented | Medium |
| Clock-skew bound | Amendment | none | none | Assumption only | Highest |
| Pushdown eligibility | ADR-0103 | `stable_generation_for_hour`, `is_pushdown_eligible` | stable_generation_for_hour_* | Implemented | Medium |
| Reshard CLI/operator/CAS loser | s1 | `reshard` one-shot; `reshard_tenant_signal` retry x3; `write_reshard_audit` | reshard_*; no loser-retry-success test | Implemented | High |

---

# Recon: maintenance ownership (ADR-0065), advisory claims (ADR-1029), sim harness (ADR-0068)

origin/main bfae457a. Status lines are NOT implementation evidence in this repo (ADR-0979 Proposed yet `MaintainError::ConvergedWinnerPartMissing` live; ADR-1029 Proposed yet primitive landed).

## 1. Verdicts
- ADR-0065 Accepted: IMPLEMENTED (d1,d2,d3; d4 extended by 0979). `ravel_fleet::worker_set::{WorkerSet, owner, owns, unit_key, run_bounded, heartbeat_key}`; `ravel_maintain::memo_snapshot::{MEMO_PREFIX, memo_key, write_memo_snapshot, read_all_memo_snapshots}`; `ravel_maintain::scan::{MaintainMemo, Zone, classify_zone, TerminalState}`; wiring services/ravel-server/src/maintain.rs `{run_loop, run_discovery_cycle, run_tick_with_clock, note_owned_units, seed_memo_from_snapshots, persist_memo_snapshot, membership_changed}`.
- ADR-1029 Proposed: PARTIAL, wave 1 only. crates/ravel-fleet/src/claim.rs (`acquire`, `renew`, `steal`, `mark_completed`, `observe`, `jitter_ms`, `WorkIdentity`, `WorkId`, `ClaimConfig`, `ClaimOwner`, `ClaimHolder`, `ClaimObservation`, `Acquisition`, `Renewal`, `Steal`, `StealRefused`, `Completion`, `COMPACTION_CLAIMS_PREFIX`); proto `ravel_proto::sys::v1::{CompactionClaim, ClaimState}`; docs/catalog-and-mvcc.md key entry; docs/object-store-contract.md last_modified widening. ZERO callers outside claim.rs tests. No `ClaimGuard`, no cancellation checkpoints, no config, no `--no-claim`, no metrics, no `coordinate` ledger phase. Model = proposed design + landed primitive.
- ADR-0068 Accepted: IMPLEMENTED (crates/ravel-sim: `seed::MasterSeed`, `driver::run_cycle`, `fault_plan::generate`, `workload::generate`, `digest::Digest`; `ravel_commit::rng::{RngSource, SeededRng, SystemRng}`), but NO ownership/claim actor in sim.
- ADR-0048 Accepted: IMPLEMENTED. `sweep::LeaseCheck` in all sweep rules; `impl LeaseCheck for LegalHoldCheck` (crates/ravel-maintain/src/legal_hold.rs); `ConservationPredicate`/`conserve_exact` (publish.rs); orphan breaker in `sweep::sweep_orphans`.

## 2. Ownership as implemented
Heartbeat: `WorkerSet::write_heartbeat(store, now_ns)`, key `sys/maintain/workers/<uuid>` (`WORKERS_PREFIX`), `PutMode::Overwrite`, payload `WorkerHeartbeat{format_version:1, process_id, started_unix_ns, heartbeat_unix_ns}` (writer's injected clock). Failure logged+dropped. `DEFAULT_HEARTBEAT_INTERVAL=60s`, dedicated spawned task in `run_loop` (not a select! arm).
Live set: `WorkerSet::live_set(store, now_ns)`: `list_all(WORKERS_PREFIX)` + one GET per sibling. Self unconditionally included. Skip unparsable/undecodable/future format_version/stale. `worker_set::is_stale(now, hb, window)`: stale = (now-hb > window) || (hb-now > window), BIDIRECTIONAL; window = `DEFAULT_LIVENESS_FACTOR(3) * heartbeat_interval`. Reader-local clock vs writer stamp; last_modified never consulted. On Err: watch channel not updated, previous live set persists; initial = `solo_live_set()` = {self} (fail-open own everything).
Rendezvous: `unit_key(tenant_hash, signal, shard)` = 16B ‖ 1B ‖ 4B BE; `owner(unit_key, live_set)` = argmax blake3(unit_key ‖ pid), tie -> larger pid; `owns`, `WorkerSet::owns_unit`. Gates in `run_tick_with_clock`: `owned_shards` filter (skip at discovery, never mid-work abort); marker sweep, unreferenced-catalog sweep, erasure pass gated on shard 0 owner; query-audit compaction on `QUERY_AUDIT_SHARD`. Not gated: tenant discovery, legal-hold refresh. `run_bounded(unit_concurrency)` `DEFAULT_UNIT_CONCURRENCY=4`; `MaintainMemo::split_unit/merge_unit`.
Restart: `WorkerSet::new` -> `Uuid::new_v4()`; `with_process_id` test-only. Old worker/memo keys never deleted (no sweeper; docs say "Nothing deletes these snapshots"). Successor warm-starts from ALL snapshots.
Asymmetric views: `owner` pure in (unit_key, live_set). Double ownership accepted. ZERO ownership possible (A sees {A,B}->B; B sees {B,C}->C; C sees {C,A}->A); undetected; `units_stalled` counts only owned units. Double-ownership window wider than ADR's "3H + one heartbeat": live set is per-cycle frozen `watch` snapshot (`live_rx.borrow().clone()` once per discovery cycle) -> bound 3H + H + cycle_duration (unbounded). `owned_shards` computed once before fan-out.
Duplicate-work safety stack (none reads ownership/claims): (1) content-addressed L1 keys, byte-deterministic (`build.rs` `BuiltPart.key`, `put_part`, `put_part_with_ledger`; tests determinism.rs `same_inputs_same_bytes_and_keys`, rlog_determinism.rs, rspan_determinism.rs). (2) CreateIfAbsent on compaction record: `publish::publish_record_with_conservation` on `keys::compaction_record_key_for`. (3) terminal record is the decision: `compact_bucket_scoped` short-circuits NotSealed / Tombstoned / AlreadyCompacted / BelowMinInputs. (4) loser convergence NOT unconditional: `resolve_already_exists`: input_set_hash differs -> `InputSetHashDivergence` fatal; same -> head() each winner part (`keys::reconstruct_l1_part_key`); missing+re-PUTtable -> repair `Converged{parts_repaired}`; missing+not re-PUTtable (ADR-0979 released bytes) -> `ConvergedWinnerPartMissing` fail closed; winner-side `verify_already_existed_parts` -> `AlreadyExistsPartVanished`. Outcome alphabet {Published, Converged, Abandoned, InputSetHashDivergence, ConvergedWinnerPartMissing, AlreadyExistsPartVanished}. (5) abandonment: `now - start_ns > max_compaction_lifetime_ns` (default 1h `DEFAULT_MAX_COMPACTION_LIFETIME_NS`) -> `PublishOutcome::Abandoned` without publish; sweep orphan gate = grace + max_compaction_lifetime. (6) conservation gate before PUT. (7) sweeps idempotent, horizon-gated; tombstones CreateIfAbsent.

## 3. Memos
`MaintainMemo` entry (tenant, signal, shard, hour) -> {TerminalState::{Compacted, BelowThreshold, SweptEmpty}, verified_at_ns}; one per process. Advisory only (suppresses LIST/GET). Durable: `memo_snapshot.rs` `MEMO_PREFIX="sys/maintain/memo/"`, `write_memo_snapshot` Overwrite self-owned; `read_all_memo_snapshots` raw bytes. Encoding hand-rolled binary (NOT protobuf as ADR says): `[MEMO_SNAPSHOT_TAG][snapshot_unix_ns i64 LE][body]`, frontier run + exceptions, verified_ns = min per run. Debounce `persist_memo_snapshot` vs `last_memo_body`. Zones `Zone::{Head, Tail, Interior}`, `classify_zone`; Head/Tail bypass memo; Interior suppressed until `interior_reverify_ns` (`maintain_interior_reverify`, 6h). `sweep_shard_zoned`; `full_sweep_due`. Seed: `seed_from_snapshot(bytes, now, staleness_ns, owns)`: bidirectional whole-snapshot staleness gate (`DEFAULT_MEMO_SNAPSHOT_STALENESS_NS`); ownership filter; per-entry clamp `verified_ns.min(snapshot_unix_ns)` (anti self-propagation of future-dated entries). Corruption: `MemoSnapshotError` -> treat absent, cold start. `read_all_memo_snapshots` error -> `reseed` stays true (single-replica `membership_changed` never true). Triggers: first cycle; `membership_changed`. `invalidate` called in production by `erasure_rewrite::invalidate_after_publish` (ADR text stale).

## 4. Claims (landed primitive, unwired)
Constants `WORK_ID_DOMAIN_TAG="ravel-compaction-claim-v1"`, `CLAIM_FORMAT_VERSION=1`, `MAX_OBSERVED_LEASE_MS=24h`, `COMPACTION_CLAIMS_PREFIX="sys/maintain/claims/compaction/"`, `DEFAULT_LEASE_DURATION=300s`, `DEFAULT_JITTER_SPAN_FRACTION=0.10`. `WorkIdentity{tenant_hash, signal, shard, ingest_hour_bucket}`, `work_id()` blake3 derive_key over 25 bytes (LE shard; vs BE in unit_key); key = prefix + hex; excludes input_set_hash. Payload `CompactionClaim{format_version, owner_process_id, attempt_id, input_set_hash, state, renewed_count, lease_duration_ns, owner_clock_ns}`, `ClaimState::{Unspecified, Running, Completed}`.
acquire: CreateIfAbsent; `Acquisition::Acquired{key, work_id, version, payload}` (one PUT, zero GETs); AlreadyExists -> `observe()` one GET + one head -> `Held{observed}`; 404 -> `Vanished`. renew: `CasVersion(current_version)`, renewed_count+1; PreconditionFailed -> `Renewal::ClaimLost`. Expiry: `last_modified_unix_ms` (head) + lease_ms (holder's declared if >0 else observer's `ClaimConfig::lease_ms()`, min MAX_OBSERVED_LEASE_MS); `is_expired(now)` = now >= expiry; observer local clock vs store time (skew not eliminated). Reschedule = expiry + `jitter_ms(work_id, pid, lease, cfg)` + 1, pure hash. steal: `CasVersion(observed.version)`; local refusals `StealRefused::UnreadableClaim` (bad decode / future version / Unspecified / bad uuid) and `NotExpired`; PreconditionFailed -> `Steal::Lost`; success writes fresh payload renewed_count=0. mark_completed: CasVersion -> Completed; PreconditionFailed -> `Completion::NotOwner`. NO DELETE anywhere. Unreadable claims never stolen/overwritten (fail closed). No SystemTime, no RNG.
Cancellation checkpoints: NOT implemented (only `max_compaction_lifetime_ns` abandonment at top of `publish_record_with_conservation`). Proposed only: d2 (vacuous), d3 ClaimGuard + 5 checkpoints + renew at 1/3 lease, d4 `claim_min_input_bytes`, d5 participation/`--no-claim`/`coordination=off`, metrics.

## 5. Publication path
`publish::publish_record` -> `publish_record_with_conservation`; chain `compact_bucket` -> `compact_bucket_scoped` -> `run_pipeline` -> `rewrite::rewrite_and_publish` -> `rewrite_and_publish_scoped` -> publish. `erasure_rewrite.rs` parallel path. NO guard reads claim/heartbeat/live set in publish.rs; ravel-maintain re-exports only `worker_set`. Only gate: `owns_unit` in `run_tick_with_clock` at discovery time. Reachable ungated: (1) CLI `services/ravel-cli/src/maintain.rs` compact-bucket + compact-tenant call `compact_bucket` (pub) directly, no heartbeat; (2) paused/stale supervisor past gate; (3) benches/sim. `compact_bucket` doc: "Safe to call concurrently... CreateIfAbsent picks a single winner and losers converge." Model: ownership does not imply exclusive publication.

## 6. LeaseCheck vs ownership
`sweep.rs`: `trait LeaseCheck { fn is_protected(&self, key:&str)->bool }`, `NoLeases`. Consulted before every delete in `sweep_orphans`, `sweep_superseded`, `sweep_unreferenced_parts`, `sweep_idempotency_markers`, `sweep_unreferenced_catalog_objects`, `sweep_erasure_requests`, `sweep_shard`, `sweep_shard_zoned`. Object-key granularity, delete-time, hold snapshot, fail-closed (skip tenant tick on refresh failure; never falls back to NoLeases). sweep.rs module doc STALE ("only implementation is NoLeases"); `LegalHoldCheck` is production (ADR-0048 d1).

## 7. Sim harness
`ravel-sim`: `MasterSeed::sub_seed`, `run_cycle(MasterSeed, &CycleConfig)`, `CycleConfig{workload, shard_count, ack_deadline, query_deadline, max_jitter, inject_faults, fault_schedule, enable_gates, gate_release, fault_schedule_override, paused_clock}`, `GateRelease::{Immediate, Manual(hook)}`, `CountingGateHandle{wait_until_held, held, held_count, release}`, `SimClock`, `SharedStore`, `compact_bucket_recover`, `sweep_shard_recover`, `CycleOutcome`, `CycleError` (incl. `GateNeverHit` per gate), `Digest`, `fault_plan::{FaultSchedule, FaultScheduleConfig, GateScript{op,key_contains,occurrence}, generate, transient_then_pass_sequence}`, `workload::{WorkloadConfig, CardinalityShape, generate}`. FaultStore: `FaultStore{new, hold(op,key_contains,occurrence)->GateHandle, fault_count, counters_snapshot, sequence_progress}`, `FaultPlan{empty, with_rule, with_sequence, with_random}`, `Rule{new, with_key_contains, with_occurrence}`, `Sequence`/`SequenceStep`, `ScriptedFault`, `FaultKind`, `Occurrence::Nth`, `Op`, `GateRegistry`, `GateHandle`, `GatedMultipartUpload`. Clocks: `ravel_maintain::clock::{Clock, FixedClock}`, `ravel_ingest::clock::{Clock, SystemClock}`, `MemoryStore::set_clock_ms`; server `WallClock`, `run_tick_with_clock(&dyn Clock)`.
Replay assessment: deterministic yes (current_thread, paused time, seeded). Explicit schedule: `fault_schedule_override` + `GateScript Nth(k)` + `GateRelease::Manual` can pin an interleaving programmatically. Limits: no schedule file format/serde; no ownership/claim actor in sim; phase isolation by construction (cross-phase interleavings not expressible). Nearest multi-worker harnesses: `maintain.rs::two_replicas_partition_units_without_double_pay`, `tests/maintain_ownership_metrics_e2e.rs::two_maintain_workers_reach_and_report_full_ownership_on_real_metrics`. Recommendation: hand-written Rust tests over MemoryStore + FaultStore gates; extend ravel-sim later.

## 8. Tests
worker_set.rs: owner_is_deterministic, default_process_ids_are_distinct, with_process_id_pins_the_identity, owner_is_order_independent, solo_live_set_owns_every_unit, two_workers_partition_units_disjointly_and_completely, unit_key_is_collision_free, live_sets_converge_to_include_both_workers, stale_sibling_is_excluded_at_three_h, future_dated_sibling_is_excluded_at_three_h, future_dated_sibling_never_wins_ownership, dropped_worker_units_move_to_survivor, run_bounded_respects_the_concurrency_cap.
claim.rs: uncontended_acquire_costs_one_put_and_no_reads, an_absurd_declared_lease_is_clamped, acquire_conflict_observes_holder_and_reschedules, steal_requires_matching_version, renew_after_steal_fails_precondition, completed_mark_is_cas_guarded, steal_before_expiry_is_refused, scripted_precondition_failure_on_renew_is_claim_lost, swept_claim_on_head_surfaces_as_vanished, jitter_is_deterministic_and_distinct, a_claim_with_malformed_uuid_identity_is_never_stolen, a_corrupt_claim_payload_is_observed_and_never_stolen, a_future_version_claim_is_never_stolen, divergent_input_set_hashes_collide_on_one_claim, work_id_matches_the_frozen_golden_vector, work_id_is_stable_and_collision_free.
maintain.rs: two_replicas_partition_units_without_double_pay, second_tick_with_shared_memo_skips_terminal_buckets, discovery_arm_fires_repeatedly_despite_a_shorter_heartbeat, memo_snapshot_write_is_debounced_on_unchanged_tick, warm_start_seeds_successor_from_predecessor_snapshot_through_store, reseed_and_seeding_track_genuine_ownership_handoff, run_tick_never_exceeds_configured_unit_concurrency, units_stalled_* (3), heartbeat_continues_during_a_discovery_cycle_longer_than_the_liveness_window, shutdown_joins_the_heartbeat_task_without_leaking_it. e2e: two_maintain_workers_reach_and_report_full_ownership_on_real_metrics.
scan tests: scan_compacts_all_sealed_hours_and_advances_cursor, scan_stops_at_first_unsealed_hour, warm_memo_skips_terminal_buckets_with_fewer_list_and_get_calls, fresh_memo_after_restart_re_evaluates_and_skips_nothing, periodic_reverify_relists_terminal_bucket_and_catches_expiry, warm_start_skips_terminal_buckets_without_reads, stale_memo_snapshot_is_ignored_and_forces_cold_start, ownership_handoff_seeds_successor_from_predecessor_snapshot, interior_zone_scheduled_not_rescanned, interior_zone_expiry_transition_forces_reevaluation, invalidate_forces_reevaluation_and_survives_snapshot_round_trip; {metrics,logs,spans}_rewrite_invalidates_memoized_terminal_state.
tombstone_race.rs: tombstone_deleting_an_already_exists_part_fails_loud, all_parts_already_exists_retry_retains_nothing_and_publishes, rerun_after_vanished_part_converges_by_presence, rerun_with_revanished_part_fails_typed_not_converged. determinism.rs, rlog_determinism.rs, rspan_determinism.rs, crash_matrix.rs, sweep_crash_matrix.rs, rspan_compaction_crash.rs. ravel-sim 16 tests.

## 9. Findings
D1 sweep.rs doc stale on LeaseCheck (LegalHoldCheck is production). D2 double-ownership bound understated (per-cycle frozen live set). D3 is_stale bidirectional, ADR describes old side only. D4 ADR says invalidate test-only; erasure_rewrite calls it. D5 memo snapshot not protobuf. D6 claim docs shipped ahead of code (catalog-and-mvcc.md, object-store-contract.md). D7 ADR-0979 Proposed but implemented; loser can fail closed. D8 zero-ownership undetected. D9 wedged-worker = gauge only, no takeover. D10 workers/memo keys never cleaned. D11 claim keys no reclamation (24h bound only). D12 claim expiry crosses clock domains (observer local vs store time). D13 ravel-sim no coverage of 0065/1029. D14 architecture.md incomplete (CLI ungated path).
Ambiguities: heartbeat clock skew cross-process unconstrained; list-after-overwrite visibility; `interior_reverify_ns` governs three schedules.

## 10. Matrix
| Protocol | Normative source | Rust implementation | Existing tests | Status | Model priority |
|---|---|---|---|---|---|
| Heartbeat write | ADR-0065 s1; catalog-and-mvcc.md | `WorkerSet::write_heartbeat`, `heartbeat_key`, Overwrite, `WorkerHeartbeat`; task in `run_loop` | live_sets_converge_*, heartbeat_continues_* | Implemented | High |
| Live set / staleness | ADR-0065 s1 | `WorkerSet::live_set`, `is_stale` bidirectional, `DEFAULT_LIVENESS_FACTOR=3`, `solo_live_set` | stale_sibling_*, future_dated_* | Implemented; wider than ADR (D2), bidirectional (D3) | High |
| Rendezvous ownership | ADR-0065 s2 | `unit_key`, `owner`, `owns`, `owns_unit`; gate in `run_tick_with_clock` | owner_*, two_workers_partition_*, two_replicas_partition_units_without_double_pay | Implemented; asymmetric views untested (D8) | High |
| Restart identity | ADR-0065 s1 | `WorkerSet::new` Uuid v4 | default_process_ids_are_distinct | Implemented; key leak (D10) | High |
| Bounded unit concurrency | ADR-0065 s2 | `run_bounded`, `split_unit`/`merge_unit` | run_bounded_respects_the_concurrency_cap | Implemented | Medium |
| Wedged worker | ADR-0065 s2 | `MaintenanceOwnershipMetrics`, `--maintain-stalled-after-intervals` | units_stalled_* | Detection only (D9) | Medium |
| Handoff / warm start | ADR-0065 s3 | `membership_changed`, `seed_memo_from_snapshots`, `seed_from_snapshot` | warm_start_*, reseed_and_seeding_* | Implemented | Medium |
| Durable memo | ADR-0065 s3 | `memo_snapshot::*`, `snapshot_body`, clamp | memo_snapshot_write_is_debounced_* | Implemented; not protobuf (D5) | Medium |
| Zones / zoned sweep | ADR-0065 s3 | `Zone`, `classify_zone`, `sweep_shard_zoned`, `full_sweep_due` | interior_zone_* | Implemented | Low-Medium |
| Memo invalidation | ADR-0065 s3 | `invalidate`; `invalidate_after_publish` | invalidate_forces_* | Implemented; ADR stale (D4) | Low |
| Compaction publication | ADR-0018, 0048 s6, 0979 | `publish_record_with_conservation`, CreateIfAbsent, `PublishOutcome`, `ConservationPredicate`, `max_compaction_lifetime_ns` | all_parts_already_exists_retry_*, same_inputs_same_bytes_and_keys | Implemented | Highest |
| Loser convergence / fail closed | ADR-0979 s3 | `resolve_already_exists`, `verify_already_existed_parts`, `ConvergedWinnerPartMissing`, `AlreadyExistsPartVanished`, `InputSetHashDivergence` | rerun_after_vanished_*, rerun_with_revanished_*, tombstone_deleting_* | Implemented (status Proposed, D7) | Highest |
| Ungated publication | ADR-1029 windows 1-3 | `compact_bucket` pub; CLI 2 sites | none | Real by design | High |
| Advisory claim primitive | ADR-1029 s1 Proposed | `claim::*`; zero callers | 16 claim.rs tests | Partial, unwired (D6,D11,D12) | High (label proposed) |
| Cancellation checkpoints | ADR-1029 s3 | none; nearest `PublishOutcome::Abandoned` | none | Proposed only | Medium |
| Cost gate / participation / metrics | ADR-1029 s4-5 | none | none | Proposed only | Low |
| GC LeaseCheck | ADR-0048 s1 | `LeaseCheck`, `NoLeases`, `LegalHoldCheck`, fail closed in `run_tick_with_clock` | tests/legal_hold.rs, erasure_sweep.rs, sweep_crash_matrix.rs | Implemented; doc stale (D1) | Medium |
| Sim harness | ADR-0068 | ravel-sim symbols above | 16 tests | Implemented; no ownership actor (D13) | High as replay substrate |
