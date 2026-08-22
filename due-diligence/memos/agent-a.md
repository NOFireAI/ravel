# Agent A: Distributed datastore architecture

## Verdict

The object-storage-as-only-durable-backend model is real, not aspirational: the strict-mode ack fires only after a content-addressed data PUT and a create-if-absent commit-record PUT both return, there is no WAL and no local-disk durability dependence anywhere in the write, commit, or catalog crates, and every coordination primitive (commit visibility, catalog HEAD, provisioning, GC config, leases, membership) is built on conditional PUTs and strongly consistent LIST against a capability-gated store contract. The consistency model as implemented matches the documented one: per-shard commit tokens give read-your-write via direct key GETs, non-token reads get bounded-staleness listing-based snapshots, and duplicate resolution is a deterministic total order shared by PromQL and SQL. I found no P0 and no credible acknowledged-data-loss path; the material findings are cost/lifecycle leaks (control-plane heartbeat keys never reaped, superseded catalog index objects have no GC driver) and several bounded, mostly documented staleness and duplication windows. Architectural complexity is high (leases, horizons, generations, zones, folds), but the failure reasoning is written down and largely enforced in code rather than by convention.

## Evidence

State machine of a strict write (VERIFIED):
- Pinned flush identity (seq, hour bucket, clock reads fixed before the flush task starts): crates/ravel-ingest/src/shard.rs:425-442, 1159-1233.
- Data PUT (CreateIfAbsent, key embeds blake3 content hash) then commit PUT then ack, ack only on token: crates/ravel-ingest/src/shard.rs:453-589 (data PUT at 529-541, publish/ack at 570-588).
- Commit publish: CreateIfAbsent; AlreadyExists resolves by content-hash compare (idempotent retry) or fatal SplitBrain: crates/ravel-commit/src/publish.rs:71-148; data-object AlreadyExists-is-success: publish.rs:150-187. SplitBrain panics the shard actor: shard.rs:759-767.
- Strict ack awaits every involved shard's token; buffered acks at enqueue and returns no token: crates/ravel-ingest/src/router.rs:375-423, 401-403. Buffered is per-request opt-in (`x-ravel-ingest-mode: buffered` shape): services/ravel-server/src/otlp_http.rs:445-455.
- Writer identity: fresh UUID per shard-actor generation set per process, epoch from clock, seq in-memory from 0; key uniqueness needs only UUID uniqueness, no cross-process coordination: crates/ravel-ingest/src/router.rs:96-131.
- Key layout, token fully determines commit key: crates/ravel-commit/src/keys.rs:205-297 (data_key 208-230, commit_key 235-248, commit_key_for_token 253-267). Records verified against reconstructed keys, mismatch fatal: keys.rs:77-86, 500-512.

Store contract (VERIFIED):
- PutMode {Overwrite, CreateIfAbsent, CasVersion}; error mapping AlreadyExists/PreconditionFailed mandatory: crates/ravel-object-store/src/lib.rs:54-62, 377-404.
- Mandatory capabilities: consistent_read, consistent_list, create_if_absent, cas_version, suffix_range, prefix_list: lib.rs:346-357; S3 adapter maps CreateIfAbsent/CasVersion onto object_store's conditional modes and reports the mandatory flags true: crates/ravel-object-store/src/s3.rs:712-720, 865-871. Startup additionally gates on a durable `sys/qualification` record (services/ravel-server/src/qualification.rs:71; crates/ravel-object-store/src/conformance.rs:30-44), so conditional-write and LIST behavior is probed against the real endpoint once per bucket, not assumed.

Read path (VERIFIED):
- Listing window: `[start - max_ingest_lag (2h), now + clock_skew_allowance (5m)]`, per (shard, hour) LIST or prefix scan: crates/ravel-catalog/src/catalog.rs:1262-1276, 943-1101; defaults crates/ravel-catalog/src/config.rs:6-20.
- min_commit_token: direct GET of the token's key, one NotFound propagation retry, then compaction/tombstone fallback; tombstoned bucket satisfies with zero segments; otherwise unsatisfiable-token error, never silent stale serve: catalog.rs:1900-2033.
- Deterministic snapshot order and mixed L0/L1 tiebreaks: catalog.rs:1125-1154.
- Duplicate resolution: `(created_unix_ns, writer_epoch, writer_seq, in-page index)` then value bit pattern, greatest wins: crates/ravel-query/src/engine.rs:2101-2132.
- Snapshot HEAD published under CasVersion/CreateIfAbsent with rebase-and-retry on a racing folder: crates/ravel-catalog/src/fold.rs:1337-1420. HEAD cache staleness only widens the listed suffix (config.rs:173-177).
- Erasure predicates attached per resolve via one unconditional `del/` LIST: catalog.rs:1115-1123.

Deletion interlocks (VERIFIED for orphan GC and retention):
- Orphan sweep three-phase (candidate list, one fresh shared re-verify LIST, breaker gate) with age gate `grace + max_flush_lifetime`: crates/ravel-maintain/src/sweep.rs:313-390; gate arithmetic crates/ravel-maintain/src/config.rs:437-439. Writer-side abandonment that makes the interlock sound: every PUT attempt raced against `deadline = flush_open + max_flush_lifetime` on the injected clock, shard.rs:672-728, 1202-1203.
- Retention sweep blocked fail-closed by a present-but-unreadable HEAD or covering part; absent HEAD proceeds: crates/ravel-maintain/src/retention.rs:86-214.
- GC horizon fence enforced twice: write-time (`set_gc_config`) and maintain startup re-assert with the running sweeper's own skew (services/ravel-server/src/maintain.rs:529; crates/ravel-maintain/src/gc_config.rs exports validate_maintain_skew).
- Compaction publish-then-supersede with pre-publish sample-count conservation (crates/ravel-maintain/src/publish.rs, `conserve_exact`), racing compactors converge on CreateIfAbsent.

Membership and coordination (IMPLEMENTED):
- Maintain workers: self-owned heartbeat keys under `sys/maintain/workers/`, Overwrite, live set = self + siblings within 3*H, symmetric staleness (far-future excluded), rendezvous hash per (tenant, signal, shard): crates/ravel-fleet/src/worker_set.rs:56-158, 303-342. Double ownership during transitions is idempotent by construction (CreateIfAbsent records, idempotent deletes), so membership is a work partitioner, not a correctness dependency.
- Query workers: same pattern under `sys/query/workers/`: crates/ravel-fleet/src/query_workers.rs:228-273. Distributed slices carry the coordinator's already-resolved segment list (Scope::Pinned), so workers never re-resolve a different snapshot: crates/ravel-query/src/distrib/service.rs:314-322.
- Alert evaluation: durable per-tenant lease object, CreateIfAbsent then CAS takeover of expired leases, never displaces a live peer: services/ravel-server/src/alerting.rs:577-627. Alert state itself is folded from durable records, none held in memory across restarts.

Resharding (IMPLEMENTED): generation history CAS-appended to the provisioning record (crates/ravel-catalog/src/provisioning.rs:1018-1046, 1140-1253); writers fail closed on a stale view with a bounded grace window proven by the activation lead-time floor: crates/ravel-ingest/src/generation.rs:81-87, 263-277; readers derive a per-hour scan count with decrease-straggler slack: catalog.rs:1056-1099.

Tenant identity: process-global scheme installed once from the durable `sys/tenancy` marker, fail-closed on mismatch: crates/ravel-types/src/lib.rs:68-70, 166-185, 248-259. Bearer-token map is a durable CAS object at `sys/auth` holding keyed hashes only: crates/ravel-catalog/src/auth_token_map.rs:1-44.

Failure tests exist and target the crash matrix: crates/ravel-failure-tests/tests/crash_matrix.rs (`crash_before_data_put_leaves_nothing_stored_or_visible`, `crash_after_data_put_before_commit_orphans_then_spec_model_gc_sweeps_after_grace`), ack_and_duplicates.rs (`ack_lost_after_commit_then_client_retry_dedups_to_one_value`), concurrent.rs (`concurrent_ingest_and_query_never_sees_a_partially_visible_flush`), retry_and_restart.rs (`restart_from_empty_local_state_sees_both_writer_generations`). I did not run them (see below).

## Failure scenarios

1. Crash between data PUT and commit PUT. Data object is an orphan, invisible (visibility comes only from commit records). Orphan GC deletes it only after `grace + max_flush_lifetime` (25h default), re-verified against a fresh commit LIST, mass-loss breaker in front. Defended: sweep.rs:313-390. The breaker's dilution/partial-restoration non-latching limits are real but documented (docs/consistency-model.md); the code matches the doc (predicate recomputed per pass, no memory of prior trips, sweep.rs:358-366).

2. Crash after commit PUT, before ack. Data durable and visible; unkeyed client retry stores a duplicate. Metrics collapse at query time (engine.rs:2130-2132); logs/spans are user-visible duplicates unless the client sent an idempotency key, and the marker is written only after all shards committed and before the ack (services/ravel-server/src/logs_ingest.rs:294-310), with a multi-shard partial commit deliberately writing no marker so a retry cannot lose the uncommitted shard (crates/ravel-ingest/src/log_error.rs:78-112). Defended, with an honest at-least-once residual.

3. Two processes flushing the same shard concurrently (split brain, deploy overlap, network partition). No coordination exists or is needed: each process's shard actors hold a fresh writer UUID, keys never collide, and both writers' commits are unioned at resolve. A commit-key collision with different bytes (pinning bug, not partition) is a detected fatal SplitBrain. Defended: router.rs:96-131, publish.rs:124-148.

4. Compactor races and late commits into a sealed bucket. CreateIfAbsent picks one compaction record; a loser's parts age out as unreferenced. An L0 commit landing after the record publishes is not in the input set, so it is never superseded-swept and the resolver serves it alongside the parts (overlap-harmless union). A fast folder clock can seal an hour early and hide such a commit from non-token queries until an operator HEAD rebuild; token reads are unaffected. Partially defended, residual documented (docs/consistency-model.md "Catalog snapshot staleness"); seal margins: bucket.rs:50-52, config.rs:426-429, fold_safety_margin config.rs:17-20, plus a detect-only seal-divergence scrub (crates/ravel-catalog/src/seal_divergence.rs:147-161).

5. Sweeper clock ahead of readers (GC deletes a pinned segment). The horizon bound `protection_horizon >= max_query_duration + grace + clock_skew_allowance` is enforced at the only mutation path and re-asserted at maintain startup against the running sweeper's own skew config; a reader hitting a deleted segment gets SnapshotInvalidated and one re-resolve. Defended for declared parameters; a sweeper whose real skew exceeds its declared allowance remains a mis-declaration residual (documented).

6. All maintain processes die. Nothing is lost; compaction, retention, GC, folds, and erasure rewrites stall. Reads degrade toward wider live listing (bounded by the 25k per-query S3 request budget and 422s), storage cost grows. Recovery is restart-from-zero: all maintenance is stateless per pass. Defended by design; availability of erasure deadlines (72h alarm) depends on someone running maintain.

7. Membership overlap or stale heartbeats. Two owners of one unit both run maintenance: idempotent (CreateIfAbsent, idempotent deletes, fresh re-verifies). A phantom far-future heartbeat cannot permanently own units (symmetric staleness, worker_set.rs:154-158). Defended. What is not defended is unbounded growth of the never-deleted heartbeat/memo keys themselves (finding 2).

8. Writer wall clock more than 5 minutes ahead of a reader, near an hour boundary. The commit lands in hour H+1 while the reader's listing window (`now + clock_skew_allowance`) still ends at H: fresh data is invisible to non-token queries until the reader's clock reaches the bucket. Bounded, self-healing staleness; token reads unaffected. Not a loss. catalog.rs:1262-1276.

9. Buffered-mode crash. The acked-but-unflushed window is lost. This is the documented contract (no token returned, opt-in per request), and graceful-shutdown paths flush before exit (shard.rs:947-971, 1140-1144). Defended in the sense of being an explicit, visible trade.

## Tests or commands run

Ran no builds, no cargo, no tests (per panel rules; a central build holds the cargo lock). Method: Read/Grep/ls over crates/ravel-object-store, ravel-commit, ravel-ingest, ravel-catalog, ravel-query, ravel-maintain, ravel-fleet, ravel-types, ravel-cache, services/ravel-server (alerting, maintain, qualification, otlp_http, logs_ingest, admission_reconcile), crates/ravel-failure-tests test files; docs/architecture.md, docs/consistency-model.md, and catalog/maintain configs used only as a checklist. Greps included: PutMode/CasVersion/CreateIfAbsent usage sites, `sys/` key prefixes, heartbeat/lease/delete call sites, `std::fs`/WAL absence in write-path crates, shard_for, is_greater, is_sealed, orphan/retention sweep rules.

## Unknowns

- Whether the qualification conformance suite actually exercises LIST-after-PUT and conditional-PUT contention hard enough to catch a non-S3 "S3-compatible" endpoint that lies about consistency; I read the suite's existence and key (conformance.rs), not its probe-by-probe strength. NOT ASSESSED in depth.
- Real-world behavior of the S3 adapter's `If-None-Match`/`If-Match` mapping against MinIO versions predating conditional-write support; the capability struct asserts true unconditionally (s3.rs:865-871) and the qualification gate is the compensating control.
- Whether any test proves the recent-hours S3-request budget (25k) is reachable before gateway timeouts under a genuinely hot open hour with many writer processes; docs cite `services/ravel-server/tests/recent_hours_reachability_e2e.rs`, which exists, but I did not execute it.
- ravel-logseg / ravel-rspan flush paths were sampled only via their routers' shared shapes (log_error.rs, log_router.rs), not line-by-line like metrics; I treat their crash-matrix equivalence as STRONGLY SUPPORTED rather than VERIFIED.
- Practical fold throughput ceiling for a single (tenant, signal) with very high commit-record cardinality (fold is one CAS chain per (tenant, signal)); no benchmark evidence reviewed.

## Severity-ranked findings

- P2. Superseded catalog snapshot parts and postings objects are never garbage-collected in production. NOT IMPLEMENTED (driver). `sweep_unreferenced_catalog_objects` exists and is safe, but its own doc states "No production driver calls it yet... unreferenced catalog objects are currently collected by nothing outside tests" (crates/ravel-maintain/src/sweep.rs:1115-1121). Every content-changing fold leaks its superseded parts forever: a monotone storage-cost leak on the hottest metadata path, not a correctness bug. Also note the doc-acknowledged re-verify-to-delete race with a folding HEAD CAS that must be closed before a driver is added (sweep.rs:1099-1105).

- P2. Worker heartbeat and memo keys are never reaped, and membership refresh cost grows with cumulative process incarnations. VERIFIED. `sys/maintain/workers/<uuid>`, `sys/query/workers/<uuid>`, and `sys/maintain/memo/<uuid>` are written per process lifetime (fresh UUID each start) and no code path deletes them (crates/ravel-fleet/src/worker_set.rs:303-342, query_workers.rs:228-273, memo_snapshot.rs:39-43; no `.delete` in crate). `live_set` LISTs the prefix and GETs every listed key, stale or not, every heartbeat interval (60s) in every participating process. Under the system's own disposable-compute assumption (frequent restarts, autoscaling), control-plane LIST/GET cost grows linearly and without bound. Correctness is unaffected (stale records are skipped).

- P2. Practical read ceiling on a hot tenant's open hour is the per-query S3 request budget, and it binds before compaction catches up. IMPLEMENTED (as designed), flagged as a scalability limit rather than a bug: recent (above-watermark) segments are exempt from `max_segments` and instead capped at 25,000 S3 requests per query (docs/consistency-model.md ADR-0073 section; seam in crates/ravel-query/src/segment_admission.rs). With N gateway processes flushing every ~2s, one shard accrues ~1800*N commit records per hour; a multi-hour non-token query during compaction lag can 422 with RequestBudgetExceeded. Fail-closed and typed, but operators must treat compaction lag as a query-availability dependency.

- P3. Non-token read freshness depends on reader-vs-writer wall-clock skew staying under 5 minutes at hour boundaries. VERIFIED (catalog.rs:1262-1276, config.rs:7-8). A writer >5m ahead publishes into an hour bucket the reader's listing window does not yet cover; data is temporarily invisible to non-token queries. Bounded, self-healing, and much smaller than the documented fast-folder seal exception, which requires an operator HEAD rebuild (DOCUMENTED CLAIM, matching code margins in bucket.rs:50-52 and fold config).

- P3. Alert evaluation can double-fire across replicas within one tick. VERIFIED (alerting.rs:577-627). The lease is wall-clock-expiry based; a takeover mid-tick, or expiry skew, yields two evaluators for one tick. Consequences are duplicate notifications and redundant (but convergent, CreateIfAbsent) records; delivery is documented at-least-once. No durable-state risk.

- P3. A panicked shard actor is never restarted in-process; its shard returns ShardUnavailable until the process restarts. VERIFIED (router.rs:40-47, 391-398, 413-420). Fail-loud and counted, and multi-replica deployments route around it, but a single-process deployment loses 1/shard_count of its write keyspace until restarted.

- P3. Logs/spans multi-shard partial commit duplicates the durable shards on retry, and the recovered sibling tokens are not returned to OTLP clients. VERIFIED (log_error.rs:78-112; consistency-model.md issues #296/#460). The no-marker-on-partial decision is correct (the alternative loses data); the residual is honest duplication plus operator-log-only token recovery.

No P0 or P1 findings. The invariants that matter (ack-after-double-PUT, visibility-from-commit-records-only, immutability of data/commit/manifest objects, delete-transaction-before-physical-removal, no local durable state) held everywhere I probed, including the failure-injection test suite's shape.

## Confidence

Medium-high: high on the write/commit/read/GC core, where I read the implementing code and its tests line-by-line and every documented claim I checked matched the code; medium overall because I could not compile or execute anything, sampled logs/spans and distributed-query paths less deeply than metrics, and did not assess the store qualification suite's adversarial strength.
