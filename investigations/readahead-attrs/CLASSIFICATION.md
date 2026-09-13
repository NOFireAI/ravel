# Classification

One row per mechanism or claim. Each row is exactly one of: already
implemented, configuration-only, verified gap, measured benefit, negative
result, unresolved. Evidence type in the last column.

| Mechanism or claim | Class | Evidence |
|---|---|---|
| Asynchronous concurrent range GETs per segment open, bounded by a shared semaphore | already implemented | code-observed: `log_fetcher.rs:5194`, `:5550`, `limiter.rs:29-32` |
| Cache single-flight (leader/follower coalescing) in both cache tiers | already implemented | code-observed: `single_flight.rs:74-123`, `cache.rs:221`, `tiered.rs:205` |
| A background request class with a floor on the store (RequestScheduler / ClassedStore) | already implemented, configuration-only to enable | code-observed: `scheduling.rs:110-129`, `store.rs:457-464`; off by default, not on the query path at default flags |
| Per-partition, per-segment scan timing (open stall, decode/build, emit, plan barrier, first batch, timeline) | measured benefit (this task's S2; overhead within noise) | LEDGER run 4: 6 and 1 percent on the two judged statements |
| Next-segment read-ahead inside a partition | negative result (for the tested workload and configuration) | LEDGER runs 5 and 6: decode is 2 to 5 percent of the partition critical path; the stall is removed by partition count with identical I/O |
| Read-ahead as a distinct mechanism from raising the partition count at a fixed in-flight object budget | unresolved (needs an S3 partition-count sweep) | S5-READAHEAD-CHECKLIST.md, BLOCKERS.md |
| Fetch-byte reservation before allocation (ADR-1170 decision 2) | verified gap | code-observed: zero `FetchMemoryExhausted` in tree; `engine.rs:446-452`; PR #1284 in flight, not merged |
| Bounding live (not idle) assembly buffers | verified gap | code-observed: `log_fetcher.rs:3144-3186`; under the stock policy the live object is the whole-object `Bytes` instead and is likewise unreserved |
| Latency-first fetch objective (ADR-1196) | configuration-only, and only partly: the named policy resolves like byte-minimal and sets no concurrency | code-observed: `config.rs:307-309`, `:202-204` |
| Exposed open stall on the fast path scales as ceil(segments / partitions) times per-GET latency | measured (injected-stall backend) | LEDGER runs 5, 6: 107 / 65 / 43 ms at 8 / 16 / 32 |
| Planning-path whole-object carry is bounded by the partition count; the remaining objects are re-fetched by the scan | verified gap (measured 1.8x wire bytes at 8 partitions, no cache) | code-observed `logs_scan.rs:2630-2700`; LEDGER B3.2, B5.4 misses; run 6 shows 24 and 8 re-fetches at 16 and 32 |
| attrs['k'] forcing the merged map and the row path over every attribute column | verified gap, measured cost | code-observed `map_field_planner.rs:64-80`, `logs_scan.rs:512-565`, `:623-625`; LEDGER B3.4/B3.5: 24x pages decoded, 53x to 107x decode time, 220x cold latency, 180x to 360x process CPU on the 33-attribute fixture |
| Byte and request savings from attribute pushdown under the stock policy | negative result (there are none: whole-object reads make I/O projection-independent) | LEDGER B3.3: identical bytes on all three comparable statements |
| Declared-column range predicate taking the planning path and spending more bytes than the map form on an unprunable fixture | measured (1.8x bytes, 1/150 CPU) | LEDGER `dur_sum_threshold` |
| Semantic agreement of attrs['k'] and a declared "k" | measured (test) with one defined divergence | `crates/ravel-sql/tests/attrs_map_vs_declared.rs`: agree on Str, resource fallback, record-over-resource, absent, dotted literal, I64 via TRY_CAST, attrs_raw overflow; diverge on a wrong-variant value under a str declaration (map renders text, declared reads NULL) |
| Unrelated attrs_raw overflow forcing the rest of the (partition, segment) onto the row path | verified gap | code-observed `logs_scan.rs:3578`, `:3631-3694`; #474's fix made the double decode visible in `pages_decoded`, it did not narrow the scope |
| LIMIT as a fetch-stop hint on the local scan (#362) | verified gap | code-observed `logs_provider.rs:347`, `:390` |
| Progressive decode within one open (decode block i while block i+1 is in flight) | verified gap (larger separate experiment, not attempted) | code-observed `log_fetcher.rs:1232`, `:3304-3306`; measured first-batch = one open + one block |
| Additional decode thread pool | negative result (no evidence of scheduler starvation; the pressure is the row path's map build) | LEDGER run 3a `cpu_ms` and `emit_elapsed` |
| Instrumentation reuse: no parallel copy of StoreMetrics, QueryAccounting, BlockRangeStats, ScanStats | already implemented (reused) | S2 adds only `Time` metrics beside the existing `Count`s in `BlockMetrics`, folded through the same `metrics()` walk the block counters use |
