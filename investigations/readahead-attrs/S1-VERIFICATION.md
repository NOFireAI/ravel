# S1: code-observed verification of the carried findings

Revision `ee2070a00f41b1add0d3225d420fbc7313112e06`. Every statement here is
code-observed unless marked otherwise; nothing in this file is measured.
Line numbers are at that revision, before the S2 instrumentation diff.

## Carried findings

| # | Finding | Verdict | Evidence |
|---|---|---|---|
| 1 | Ravel already performs asynchronous concurrent reads and cache single-flight. More async APIs are not the objective. | VERIFIED | Concurrent range GETs per segment open: `crates/ravel-query/src/log_fetcher.rs:5194` (`join_all` over coalesced BLOCKS runs in `fetch_chunk_ranges`) and `:5550` (`fetch_block_ranges`). Segment fan-out is `buffer_unordered` at `crates/ravel-query/src/engine.rs:1847,2373,2681`. Limiter: `GetLimiter`, an `Arc<tokio::sync::Semaphore>` at `crates/ravel-query/src/limiter.rs:29-32`, permit per leaf GET at `log_fetcher.rs:3796-3805`, `:2177`, `:2213`; default 16 permits `log_fetcher.rs:2711`. Single-flight: `crates/ravel-cache/src/single_flight.rs:74-77,101,119-123`, wired at `crates/ravel-cache/src/cache.rs:82,221` and `tiered.rs:103,205`; consumed by the log fetcher via `ReadCache::get_or_fetch` at `log_fetcher.rs:3889-3898`, `:2208-2221`. |
| 2 | LogScanStream drains a segment before advancing within a partition. Other partitions can overlap work, so the global effect is unmeasured. | VERIFIED, REFINED | One slot per partition: `struct LogScanStream` holds one `state` and one `current_seg` (`crates/ravel-sql/src/logs_scan.rs:3192,3237`). `Opening` is constructed only in the `NextSegment` arm (`:3478-3507`), re-entered only at segment exhaustion (`:3629`, `:3725`, `:3752`, `:3526`). Work is a `VecDeque<OwnedSeg>` popped front to back (`:3180`, `:3451`). Refinement: assignment has three modes. Fast path assigns whole segment `j` to partition `j % n` (`:2752-2777`, `n` at `:2394`); planned path with a read cache stripes blocks `(global+local) % n` (`:2698-2718`), so several partitions can own one segment; planned path without a cache is segment-granular (`:2719-2737`). The module doc names the unbuilt alternative at `:174-176`. No timing existed anywhere in the file (every `MetricBuilder` call is `.counter`, `:1209-1221`), so the global effect was indeed unmeasured before S2. |
| 3 | Eligible `whole_segment_fast_path` queries bypass the shared planning barrier. Narrow projection alone does not imply that planning runs. | VERIFIED | Eligibility has exactly four rejections in order: `PendingErasure` (`:1952`), `BlockPredicate` (`:1955`), `SegmentNotContained` (`:1964-1969`), `FewerSegmentsThanPartitions` (`:1971`). The `Ok` arm goes straight to `LogScanState::NextSegment` (`:2392-2398`) and no `plan_counts_future` is built. The barrier exists only on the `Err` arm (`:2408-2414`), awaited through `OnceCell::get_or_try_init` (`:2499-2511`) in the `Planning` poll arm (`:3420-3449`). Projection width is not in the predicate; a narrow projection stays inside the fast path and picks the ranged read at open time (`:1942-1945`, `:3479`, `:2816-2819`). |
| 4 | The internal `Pending` enum holds buffered output. It is unrelated to `Poll::Pending`. | VERIFIED | `enum Pending { None, Rows { records, pos }, Batches(VecDeque<RecordBatch>) }` at `:3061-3074`, field `pending` at `:3187`, drained via `has_pending` (`:3358-3364`). `Poll::Pending` returns appear in the same `poll_next` at `:3449`, `:3528`. |
| 5 | Decode, Arrow construction and row-batch building execute synchronously inside `poll_next`. Timing `Poll::Pending` does not measure that work. | VERIFIED | Inside `poll_next` (`:3398-3760`): `scan.next_block_columnar()` (`:3571`), `build_columnar_batches` (`:3595`), `scan.next_block()` (`:3717`, `:3731`; `LogSegmentScan::next_block` is a plain `pub fn`, `crates/ravel-query/src/log_fetcher.rs:353`), `build_batch` inside `emit_next_row_batch` (`:3276`). No `.await`, `spawn`, or `spawn_blocking`; the only polls are the plan/open futures (`:3420`, `:3513`, `:3530`). |
| 6 | `NextSegment` performs synchronous selection and transition; `Opening` drives an asynchronous open. | VERIFIED | `NextSegment` is `pop_front`, field assignment, route choice (`open_by_column_chunk`, I/O-free, `:3479` -> `:2816-2819`), metric recording and future construction (`:3451-3507`). `Opening(OpenFuture)` (`:2918`) is polled at `:3513-3529`. The future's target by route: whole fast path `open_segment_whole` -> `scan_whole_accounted_with_tenant` (`:2959-2975`, `log_fetcher.rs:1270`, one `GetRange::Full` through `whole_object_bytes`, `log_fetcher.rs:2109`, limiter permit at `:2177-2188`/`:2213`); narrow fast path `open_segment_ranged` -> `scan_accounted_with_tenant` (`:2989-3005`, `log_fetcher.rs:1208`); planned path `open_segment_subset` -> `scan_accounted_with_tenant_subset` (`:2925-2950`, `log_fetcher.rs:1858`). The whole-object versus ranged decision for the latter two is made in the fetcher at `log_fetcher.rs:2019-2085`. |
| 7 | `AssemblyBufferPool` bounds idle retained buffers. Its idle limits do not bound all active buffers. | VERIFIED | Fields `max_idle_bufs`, `max_idle_bytes` (`log_fetcher.rs:3107-3118`), defaults 16 buffers (`:3063`) and 128 MiB (`:3073`). `acquire` is infallible and never consults a live budget (`:3144-3172`); the bounds are applied only in `release` (`:3175-3186`). Nothing caps checked-out buffers: the `GetLimiter` permit is per leaf GET and released before the assembler is done (`:3796-3805`), `segment_admission.rs:18-21` counts segments, `request_budgets.rs:36-46` counts bytes scanned and requests. `stats()` exposes `allocated`/`reused`/`zeroed_bytes` only, no live gauge (`:3188`, `:3469`). |
| 8 | Active assembly buffers can be object-sized even for narrow range reads and stay alive with their scans. | VERIFIED | `ObjectAssembler::new(&self.assembly_pool, total)` with `total = seg_ref.object_size` at `log_fetcher.rs:4527-4529` and `:3676-3678`; rationale at `:3190-3197` (absolute block offsets). `into_bytes` aliases the buffer (`Bytes::from_owner(self.buf)`, `:3304-3306`); `LogSegmentScan { bytes, .. }` holds it for the scan's life (`:314-316`, `:1232`, `:1287`, `:1888`). Return to the pool happens in `AssemblyBuffer::drop` (`:3228-3232`), documented at `:3197-3201`. Under the stock `u64::MAX` policy this path is not taken at all: every object is read whole (see policy below), so the object-sized buffer is the whole-object `Bytes` instead. |
| 9 | Existing decoded-memory reservations and bounded planning carry do not automatically govern future read-ahead buffers. | VERIFIED (stronger than stated) | `ravel-memory` has one dependent crate, `ravel-sql` (`crates/ravel-sql/Cargo.toml:53`); `ravel-query` does not depend on it. Reservations on the logs path cover decoded blocks and Arrow batches only: `logs_scan.rs:3288`, `:3326`, `:3351`, contract at `:193-196`. No reservation covers the compressed object bytes held by `LogSegmentScan.bytes`. `FetchMemoryExhausted` from ADR-1170 decision 2 has zero occurrences in the workspace. `SqlExecutor::with_process_memory_budget` is never called by `ravel-server` (definition `executor.rs:735`, one test use), so the process budget is `MemoryBudget::unlimited()` (`executor.rs:721`). `crates/ravel-query/src/engine.rs:446-452` states that peak assembly memory scales with fan-out, not with the GET permit count. PR #1284 is not in this tree. |
| 10 | RLOG already splits attributes into physical columns, yet `attrs['key']` access can still request the merged map and force the row path. | VERIFIED | Layout: one dynamic column per distinct `(name, type)` (`crates/ravel-logseg/src/writer.rs:373-441`, default 1000 columns `:71`), `attrs_raw` overflow column id 9 (`record.rs:27`), resource/scope attrs in `STREAM_DIR` (`columns.rs:16-18`). `attrs['k']` lowers to `get_field(attrs, 'k')` over the whole `Map(Utf8,Utf8)` column at index 8 (`crates/ravel-sql/src/map_field_planner.rs:64-80`, `logs_schema.rs:73-77`). `resolve_columns` maps index 8 to `all_attrs = true` (`logs_scan.rs:512`, `:558`) which selects `attrs_raw` plus every FIELD_DIR column (`columns.rs:168-171`) and sets the projected width to the whole object (`logs_scan.rs:565`). Columnar eligibility excludes index 8: `columnar_static_eligible` at `:623-625`. A predicate-only use also forces it: the pushdown is `Inexact` (`logs_provider.rs:318-341`) so the residual `FilterExec` keeps column 8 in the scan projection (`logs_provider.rs:391-399`). ADR-0087 records per-key projection through `attrs['k']` as out of scope (`docs/adrs/0087-streaming-projected-logs-scan.md:76-81`). |
| 11 | Cache, spill and process-memory policy changed over time; establish the rules at this revision. | REFINED (rules below) | See "Policy at this revision". |

## Policy at this revision

- Spill: ADR-0954 (Accepted 2026-08-30) allows exact spill for eligible
  aggregate-only plans, per query, bounded by `RAVEL_SQL_SPILL_MAX_BYTES`, into
  a per-query directory under `RAVEL_SQL_SPILL_DIR`. Off by default:
  `crates/ravel-sql/src/config.rs:426`, `services/ravel-server/src/query.rs:325`.
  No CLI flag; env only (`config.rs:125,129`). Per-tenant and node-wide quotas
  required by ADR-0954 requirement 2 are not implemented
  (`crates/ravel-sql/src/spill.rs:26` says so).
- Process memory budget: ADR-1170 is Proposed. `MemoryBudget` counts SQL-side
  reservations (`crates/ravel-sql/src/memory.rs:117`) and is unlimited in the
  server. No carve from cgroup memory, no fetch-byte reservation. The enforced
  ceilings are independent: `--sql-max-query-bytes`, `--sql-tenant-max-bytes`,
  `--cache-max-bytes` (25 percent of MemTotal), catalog cache 5 percent.
  `docs/guides/caching.md:110-118` states that their sum can exceed RAM.
- Cache tiers: RAM S3-FIFO tier on by default (`services/ravel-server/src/store.rs:61-63`),
  disk tier opt-in via `--cache-dir` (`:71-73`) sized to the RAM limit. Max
  entry age 23 h with a 1 h sweep (`crates/ravel-cache/src/limits.rs:14,27`),
  not configurable. The span fetcher has no cache seam (`query.rs:361-364`).
- Fetch objective: `LogsFetchPolicy::{RequestMinimal, ByteMinimal, CostBased, LatencyFirst}`
  at `crates/ravel-query/src/config.rs:155-193`. ADR-1196 is Proposed;
  `LatencyFirst` resolves identically to `ByteMinimal` (`config.rs:307-309`,
  pinned by `latency_first_resolves_like_byte_minimal`) and sets no concurrency.
- Intra-segment partitioning: ADR-0102 block striping applies only on the
  planned path with a read cache wired (`logs_scan.rs:2698-2718`).

## Effective fetch policy default (declared variable for every run)

- Server: `--logs-fetch-policy` defaults to `cost-based`
  (`services/ravel-server/src/config.rs:929`); with the reference profile
  `s3-intra-region-2026` (`crates/ravel-types/src/cost_profile.rs:149-156`,
  zero transfer and retrieval price) `resolve_cost_based_rate` returns
  `u64::MAX` (`crates/ravel-query/src/config.rs:337-344`) and
  `block_range_threshold` saturates to `u64::MAX` (`:316-322`). The
  comparison that makes whole-object always win is
  `seg_ref.object_size <= self.effective_whole_object_threshold()` at
  `crates/ravel-query/src/log_fetcher.rs:4492` with
  `saturating_mul(5)` at `:3729-3735`, and `ranged_projection_pays`
  returning false (`:881-888`).
- `DEFAULT_LOG_REQUEST_COST_BYTES = 1_887_437` (`log_fetcher.rs:2667`) is only
  the `byte-minimal`/`latency-first` input and the library `EngineConfig`
  default. It is not what a stock server runs.
- `sql_latency_bench --generate` resolves through the same `resolve_logs_fetch`
  and its report header shows `requested=1887437 effective=18446744073709551615`
  under the default `cost-based` policy (measured in the smoke run, see LEDGER).

## Server wiring (who constructs what)

- Fetch policy is resolved once at startup:
  `QueryBudgets::logs_fetch_resolution` (`services/ravel-server/src/config.rs:1787-1798`)
  and folded into `EngineConfig` by `apply_to_engine` (`:1852-1866`), called from
  `services/ravel-server/src/lib.rs:2045-2057`.
- Fetchers: `LogSegmentFetcher::new(store).with_block_range_threshold(..).with_get_limiter(..).with_request_cost_bytes(config.engine.logs_request_cost_bytes).with_max_fetch_run_bytes(..)`
  at `services/ravel-server/src/query.rs:354-359`; RSEG at `:330`; spans at `:367`.
- The one process-wide `GetLimiter`: `services/ravel-server/src/lib.rs:1833-1836`,
  handed to `QueryEngine::with_get_limiter` (`query.rs:180`), which replaces the
  engine's own (`crates/ravel-query/src/engine.rs:533-538`).
- Concurrency defaults (ADR-1195, Proposed): four knobs, each falling back to
  `--fetch-concurrency`, which derives to `max(8, 2 * cores)`
  (`services/ravel-server/src/config.rs:2147,2152,2416-2430`). On this 16-core
  box a stock server resolves 32; `sql_latency_bench` defaults to the library
  value 8 (`crates/ravel-query/src/config.rs:17`).
- `RequestScheduler`/`ClassedStore` is constructed only in
  `services/ravel-server/src/store.rs:457-464`, behind `--store-scheduling`
  (default off, `config.rs:1598`); in passthrough both handles are the same
  `Arc` (`store.rs:206-215`). No file in `ravel-query` or `ravel-sql` names it.
  The query read path traverses no scheduler at default settings.

## Feature gates

Neither `services/ravel-server/Cargo.toml` nor `crates/ravel-sql/Cargo.toml`
declares a `default` feature. `sql` (`ravel-server/Cargo.toml:216`),
`flight-sql` (`:223`) and `mcp` (`:229-235`) are off; `ravel-sql`'s
`flight-sql` (`crates/ravel-sql/Cargo.toml:169-177`) is off. `ravel-bench`'s
`sql-latency`, `flight-lane`, `parquet-baseline`, `profiling`, `stage-timing`
are all off by default.

## Which bench binaries exercise which store

| Binary | Feature | Store | Counting | Dataset |
|---|---|---|---|---|
| `sql_latency_bench --generate` | `sql-latency` | `MemoryStore` unless `--store s3` with `RAVEL_S3_*` (`crates/ravel-bench/src/harness.rs:80-89`) | `QueryAccounting` calls, not billed attempts (`sql_latency.rs:983-987`); no `InstrumentedStore` | one stream, `duration_ms:I64` plus `attr_k:Str` filler, no overflow (`sql_latency.rs:2565-2606`) |
| `logs_scan_scaling_bench` | `sql-latency` | same selection | `QueryAccounting` | `attrs: Vec::new()`, query `SELECT ts, body FROM logs` (`logs_scan_scaling.rs:1031`, `:183`) |
| `read_path_accounting` | `parquet-baseline` | MinIO if reachable else in-process | `CountingBackend` | RSEG metrics, not logs |
| `selective_read_accounting` | default | in-memory object | own meter | RSEG metrics |

A documentation defect found on the way: `crates/ravel-bench/src/bin/sql_latency_bench.rs:782`
refers to `backend_bills_requests` "in the report's request accounting"; no
such field exists on `SqlLatencyReport` (it lives on `report.rs:102`). Reported,
not fixed.

## Existing per-query counters (reused by S2, nothing duplicated)

- DataFusion counters in `BlockMetrics` (`logs_scan.rs:1165-1223`): `blocks_total`,
  `blocks_scanned`, `blocks_pruned_by_postings`, `pages_decoded`, `pages_skipped`,
  `columnar_batches`, `rowpath_batches`, `plan_full_reads`,
  `fast_path_whole_object_segments`, `fast_path_ranged_segments`, and the
  lazily created `fast_path_rejected_*` reasons (`:1145-1154`).
- `QueryAccounting` / `PhaseAccounting` (`crates/ravel-types/src/accounting.rs:162`,
  `crates/ravel-query/src/phase_accounting.rs:241-248`): requests and bytes per
  op and per phase (resolve, plan, probe, scan), cache hits/misses/bytes,
  `page_bytes_fetched`, `page_bytes_decoded`, `logs_whole_object_opens`,
  `logs_ranged_opens`.
- `BlockRangeStats` per open (`log_fetcher.rs:2730-2763`), `ScanStats` per
  decode (`crates/ravel-logseg/src/reader.rs:42-75`).
- No timing metric of any kind existed on the scan before S2.
- LIMIT is not propagated to the local scan (`logs_provider.rs:347`, `:390`;
  `build_scan` has no limit parameter, `:231-255`), confirming #362 stands.
