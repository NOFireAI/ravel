# Query Engine Design (Phase 1)

Implementer contract for `ravel-query` and the query side of `ravel-server`.
ADR-0006/0007 give the reasoning; docs/consistency-model.md the semantics.

## Flow

```
HTTP /api/v1/query, /query_range, /labels, /label/{name}/values, /series
  -> auth + tenant resolve
  -> ravel_promql::plan_selectors(query, eval_start, eval_end): a static AST
     walk, independent of evaluation, reporting every selector's matchers,
     range, offset, and @-anchor (ADR-0021 SS1)
  -> Catalog::resolve(tenant, Metrics, padded_range, min_commit_token), one
     shared snapshot resolved against the union of every selector's own
     fetch window
  -> per selector (concurrency-bounded, default 8), against that one
     snapshot:
       for each snapshot segment: suffix GET (64 KiB) -> Reader::parse
         -> prune series by that selector's own matchers (SERIES_META +
            LABEL_DICT)
         -> plan page ranges, coalesce adjacent (gap <= 64 KiB)
         -> ranged GETs -> decode pages -> per-series samples
       -> selective-erasure exclusion: every series or sample matching a
          predicate the snapshot carries in `pending_erasure` is dropped
          from the decoded per-segment results (ADR-0064 decision 2)
       -> the selector's per-segment scalar SoA runs are kept UNMERGED
          (native-histogram runs are merged per selector here); the raw
          scalar runs join a single query-wide pool so the one k-way merge
          below unions across selectors AND cross-cluster federation runs
          (ADR-0071) under one total order
  -> all selectors' raw scalar runs (plus every remote cluster's federated
     runs) pooled and merged once by series id: one lazy k-way merge (each
     run already ascending by ts; emits ascending, one sample per ts, no
     final sort). Duplicate timestamps (across segments, within one run,
     and across clusters) resolve under the total order in
     docs/catalog-and-mvcc.md, not by arrival order. max_series/max_samples
     are enforced ONCE over this pooled union -- a query-wide bound, not a
     per-selector budget: the caps guard the coordinator's peak memory for
     the whole query, so a multi-selector (or federated) query cannot hold
     N times the cap. Pooling is loss-free: two selectors sharing a series
     id decode byte-identical raw runs (the decode is a pure function of
     segment bytes + series id, with no per-selector window trim), so
     merging them once yields exactly what merging each alone would, and
     per-selector window clipping still happens downstream in
     `SeriesSource::query`. max_segments stays a per-query cap on the shared
     snapshot.
  -> every merged series combined into one flat SeriesSource (later
     selectors sharing a series id keep the first merge seen; a later
     per-selector SeriesSource::query call still clips
     to that selector's own window, so which selector "wins" a shared
     series id in the combine step does not affect any single selector's
     result -- and since every selector in one query fetches against the
     same shared snapshot with no per-selector sample-level window trim,
     two selectors sharing a series id always resolve identical merged
     data, so "wins" only ever breaks a tie between equal values, never
     drops one. The combined series/histogram-series lists are then sorted
     by label set, so their order is deterministic across runs regardless
     of `HashMap` iteration order or fetch completion order, issue #801)
  -> ravel-promql Evaluator -> Value (scalar / string / instant vector /
     range matrix)
  -> Prometheus JSON envelope {status, data:{resultType, result},
     warnings, infos}
     (warnings/infos are the evaluator's Annotations: two distinct
     Prometheus fields, both omitted when empty. A quantile argument outside
     [0,1] and a malformed classic histogram are warnings; a forced
     histogram monotonicity fixup is an info. Issue #178.)
```

`padded_range`: the union, over every selector `plan_selectors` reports, of
that selector's own fetch window (its lookback or matrix range, plus its
own offset, anchored per `PlanAnchor::Window`/`Pinned`), so lookback never
misses samples stored in an earlier-only segment for any selector, however
widely the selectors' own windows differ.

Selective erasure (ADR-0064 decision 2): `Catalog::resolve` lists the
tenant's `del/` prefix once per resolve and attaches every pending erasure
request to the snapshot. The engine maps each request into one scan-time
predicate (a conjunction of exact `key = value` matchers plus an optional
half-open `[window_start_ns, window_end_ns)` event-time window, zero on a
bound meaning unset) and applies them in `fetch_all_samples_and_histograms`
and `fetch_all_series`, on decoded results, after the fetch and after any
cache tier. Filtering there rather than at fetch time is what makes stale
cached bytes of a since-erased subject unreachable: a cache hit is filtered
exactly like a fresh GET. A series matching a windowless predicate is
dropped whole, including from `/labels`, `/label/{name}/values`, and
`/series`; a windowed predicate drops only that series' in-window samples
and leaves it enumerable. The equivalent hop for the SQL logs, spans, and
metrics surfaces in `ravel-sql` is not wired yet: those scans build their
own queries and do not read `Snapshot::pending_erasure`.

Staleness: the evaluator recognizes the Prometheus staleness marker (the
exact NaN bit pattern `0x7ff0_0000_0000_0002`, compared via
`f64::to_bits()`, never `is_nan()`). A selector whose newest in-window
sample is the marker treats the series as absent at that instant; range
windows exclude marker samples. Every other NaN payload is a live value
and passes through bit-exactly (issue #75).

## Time-range and matcher pruning order

1. Segment level: commit-record event-time bounds vs padded range (already
   done by Catalog::resolve).
2. Series level: SERIES_META entry ts bounds vs padded range, then matcher
   evaluation against the decoded LabelSet. Equality matchers use dictionary
   ordinal lookups (resolve value -> ordinal once, compare ordinals);
   regex/negative matchers evaluate on materialized label sets.
3. Page level: v1 has one page pair per series; nothing further to prune.

## Segment catalog fetch: whole-object vs sparse catalog-probe (#276)

`SegmentFetcher::decode_selected` decodes one segment's catalog before it
plans page ranges. How it fetches the catalog depends on the object's v5
shape (docs/segment-format.md, ADR-0026):

- **Below the 4096-series sparse threshold** the object carries the
  whole-section SERIES_META (kind 6). The fetcher GETs LABEL_DICT +
  SERIES_IDS + SERIES_META and decodes the run-major catalog. No page bytes
  are pulled.
- **At or above the threshold** SERIES_META is replaced by the SERIES_IDX
  directory (kind 8) and the chunked SERIES_META_CHUNKS (kind 9). These four
  catalog sections (LABEL_DICT, SERIES_IDS, SERIES_META_CHUNKS, SERIES_IDX)
  sit contiguously at the object front, ahead of the TS/VAL/HIST page
  sections. When the object qualifies for the **catalog-probe path**, the
  fetcher range-GETs only that catalog prefix (coalesced into as few GETs as
  the layout allows) and decodes via `decode_catalog_v5_chunked`, skipping
  the page bytes a whole-object GET would move. The matched series' pages are
  fetched selectively afterward, exactly as the below-threshold path already
  does. Each catalog section's crc32c is re-verified on the fetched bytes, so
  a mis-ranged or corrupt fetch is a typed error, never wrong data.
- **Otherwise** (a sparse object that does not qualify) the fetcher keeps the
  unchanged whole-object GET and `decode_catalog_v5`.

Qualification (`SegmentFetcher::sparse_probe_qualifies`) requires all of: the
object carries both sparse sections; the query's matcher set is non-empty (an
empty matcher wants every series, so one whole-object GET beats a catalog GET
plus a page GET spanning the object); and the object is at least
`SPARSE_PROBE_MIN_OBJECT_SIZE`. The size floor is the crossover between one
whole-object GET and one catalog GET plus the extra selective page-range round
trips. Epic #264 Wave 1 (BENCHMARKS.md, 2026-07-31 MinIO panel) measured
per-request latency (~1-5 ms loopback, ~15-80 ms projected real S3) but did
not meter this specific within-segment crossover, so the floor is set
conservatively (256 KiB: above the four fixed 64 KiB suffix/gap probes, far
below any real compacted sparse L1 part) rather than fit to a measured point.
The within-segment GET/byte model is the `selective_read_accounting` bench.

## Endpoints (Prometheus compatibility subset)

- `POST/GET /api/v1/query` (params: query, time, timeout) instant.
- `POST/GET /api/v1/query_range` (query, start, end, step, timeout).
- `GET /api/v1/labels`, `/api/v1/label/{name}/values` (match[] optional,
  start/end optional): from snapshot SERIES_META label dictionaries, unioned
  across configured remote clusters when federation is on (see Cross-cluster
  federation).
- `GET/POST /api/v1/series` (match[] required, start/end): likewise unioned
  across remote clusters under federation.
- `GET /api/v1/status/buildinfo`: Ravel's own crate version under `version`,
  never a Prometheus version string. `revision` carries the build's git SHA
  when the build environment exported `RAVEL_GIT_SHA`, otherwise it is empty,
  as are `branch`, `buildUser`, `buildDate`, and `goVersion`.
- `GET /api/v1/metadata`: always `{"status":"success","data":{}}`. Ravel
  captures no OTLP metric type/help/unit metadata, and an empty object is a
  valid Prometheus response; inventing entries would not be.
- All accept `min_commit_token`. Errors use the Prometheus JSON error
  envelope (`status:"error"`, `errorType`, `error`) with correct HTTP codes
  (400 bad_data, 422 unprocessable for unsupported constructs, 503
  unavailable for store failures).
- Timestamps: Prometheus float seconds in, RFC3339 or float accepted like
  Prometheus; responses use float seconds with ms precision.
- `resultType`: `vector`, `matrix`, `scalar`, `string`. A top-level scalar
  or string result from `/api/v1/query_range` is rendered Prometheus-style
  as a `matrix` with one synthetic empty-labeled series repeating that
  value at every evaluated grid step (`ravel-promql`'s `eval_range`
  resolves it once; materializing the repetition is this HTTP layer's
  job, not the evaluator's). `/api/v1/query` (instant) can also return
  `matrix`, not just `vector`/`scalar`/`string`: when the top-level
  expression is itself a range vector (for example a bare subquery, or a
  range function nested inside an outer subquery), Prometheus renders
  `resultType: matrix`, and Ravel matches that. Such a matrix already
  carries its own per-series timestamps from the evaluator, so no grid
  repetition is synthesized.

## Budgets (Phase 1: static config)

Per query: max segments touched (1024, a cap on the one shared snapshot,
not per selector), max concurrent GETs (8, applied both across selectors
and across one selector's own segments), max matched series (10k) and max
samples (10M) enforced independently per selector (a query with several
selectors, e.g. a future binary/aggregate expression, grants each its own
full budget rather than splitting one budget across them), max
range-evaluation points per query (11,000, matching the Prometheus
resolution limit; issue #77), wall deadline (server maximum, default
30 s). The `timeout` param can only lower the deadline: values above the
server maximum are clamped to it (issue #58). Exceeding a budget returns a
Prometheus-style error, never a partial silent result.

### Segment admission (ADR-0073)

`max_segments` no longer counts every segment in the resolved snapshot. A
resolve tags each segment with an origin (sealed-below-watermark, recent, or
token-resolved via an explicit `min_commit_token`) and `max_segments` applies
to the sealed count only; recent and token-resolved segments are exempt, so a
hot tenant's open hour and a read-your-write query no longer 422 on count.
Their cost is bounded instead by a per-query S3 request budget
(`EngineConfig::max_s3_requests`, default 25,000), checked incrementally at
the same points `max_bytes_scanned` already is, and reported as
`RequestBudgetExceeded` (HTTP 422) when tripped.

`crates/ravel-query/src/segment_admission.rs` is the one seam both checks go
through: `admit(&snapshot, &origins, &config)` for the sealed-count check,
`request_budget_exceeded(requests, max_s3_requests)` for the incremental
budget check. `QueryEngine::resolve_bounded` (`engine.rs`) is the only call
site wired up as of RH-T1 (#901). The SQL executor, the five SQL table
providers, and the exemplars state still run their own pre-ADR-0073 checks;
moving those eight sites onto this seam is RH-T2 (#902).

Per-tenant max bytes scanned (`ByteLimit`, default `Unlimited`; ADR-0061
decision 1, issue #721) bounds the total S3 bytes one query may fetch
across every segment its shared snapshot resolves to, protecting against a
selector that passes the count caps but whose covering segments are large.
It is checked once per completed segment fetch, inside the two fetch
fan-outs (`fetch_all_series` for the labels/series endpoints,
`fetch_all_samples_and_histograms` for instant/range queries) rather than
after the merge, so a tripped budget cancels the query mid-scan: returning
early drops the fetch stream and stops polling the remaining segments'
in-flight GETs. Exceeding it returns `TooManyBytesScanned` (HTTP 422),
distinct from the count-cap errors. The default is `Unlimited` so an
existing deployment is never silently rejected on upgrade; opting in to a
bound is explicit. Like every count cap, enforcement is per completed
segment, so a single very large segment can overshoot the bound by up to
its own size before the next check fires.

An operator configures the budget in `ravel-server`'s `--limits-file`, the
same TOML file that already carries the ingest admission caps (ADR-0051
section 3): a `max_bytes_scanned` key in the `[defaults]` table, set to a
positive byte count or the string `"unlimited"`, sits beside
`max_active_series` and the other per-tenant knobs. It is read once at
startup and threaded into the one process-wide `EngineConfig` that both the
PromQL/HTTP and SQL/HTTP query surfaces share, so the configured budget is
the enforced budget on both. Absent, it stays `Unlimited`. A
`[tenants.<id>]` table accepts the same key in the operator-familiar
per-tenant shape, but the process-wide engine holds a single `EngineConfig`
and is not tenant-parameterized: it enforces the `[defaults]` budget for
every tenant, so a per-tenant override is parsed and validated but not yet
enforced differently from the default (the server logs a startup warning
naming any tenant that set one). Enforcing a distinct budget per tenant
needs a tenant-aware `EngineConfig` lookup inside the engine, not yet built;
until then, set the budget in `[defaults]`.

The point cap is
enforced independently at every subquery evaluation node (`expr[5m:1m]`),
checked against that node's own grid before it is built, so a nested
subquery whose own grid alone exceeds the cap is rejected before any
allocation or recursive evaluation, at whatever nesting depth it occurs.

A per-node point cap alone does not bound the total cost of nested or
repeatedly re-evaluated subqueries: an outer range query re-evaluates a
matrix-function argument's subquery from scratch at every one of its own
grid steps, and a subquery may itself nest another subquery, so a query
whose every individual node sits safely under 11,000 points can still
multiply that cost across nesting levels and re-evaluations (issue #193).
`ravel_promql::Evaluator` therefore also charges a shared, cross-level
evaluation budget (`Evaluator::with_max_total_eval_points`, default
`DEFAULT_MAX_TOTAL_EVAL_POINTS` = 1,000,000 grid points): every subquery
grid evaluation, at any nesting depth and however many times an enclosing
grid step re-triggers it, debits this one query-wide counter, and
exhausting it returns `Error::EvalBudgetExhausted` before the excess work
runs.

The evaluator is otherwise fully synchronous, so the wall deadline above
used to only take effect at the `tokio::time::timeout` wrapping an
instant/range query: a runaway evaluation ran to completion regardless,
and the deadline only changed how long the caller waited before seeing
the error. The evaluator now also carries its own copy of that deadline
(`Evaluator::with_deadline`) and checks it between subquery grid steps
(inside a subquery's own grid loop and inside the per-outer-step
re-evaluation loop that drives it), returning `Error::DeadlineExceeded` as
soon as it is past due. `QueryEngine::{instant,range}_with_stats` derive
this deadline from their own `Duration` parameter before evaluation starts
and fold whichever error surfaces first, the evaluator's own or the outer
timeout's, into the same `QueryError::DeadlineExceeded`.

A subquery that omits its own step (`expr[5m:]`) defaults to
`EngineConfig::default_evaluation_interval` (60 s, matching Prometheus'
global `evaluation_interval` default). Subquery grids are epoch-aligned:
the grid start is the smallest multiple of the step (measured from Unix
time zero) that is strictly greater than `end - range`, left-open like
this crate's own matrix-selector window, matching Prometheus' own
subquery alignment, not the query's own step or window start.

A subquery whose inner expression matches native-histogram data is not yet
supported: the subquery grid reducer keeps only the float value of each
step, so a histogram element would be silently dropped. When histogram data
is actually present in the fetched window, the evaluator returns
`Error::Unsupported` (`subquery over native histograms`, HTTP 422) instead
of a wrong empty answer. The trigger is the presence of matched histogram
data, not the syntactic shape: a float-only subquery, including
`rate(x[5m:1m])` over float series, is unaffected. Real histogram subquery
support is tracked by issue #220.

The max-samples budget is **count-yielded**: samples are counted as the
lazy k-way merge emits them (post-dedup), and the budget trips at exactly
`max + 1`. It does not count a fully materialized per-timestamp window
before checking, so it does not silently pass an over-budget query that
happens to dedup down to size. The count is independent of segment or
series iteration order, so the error is deterministic. It bounds only the
output size: every matched series in every matched segment is still fully
fetched and SoA-decoded before the merge runs, so peak fetch/decode memory
scales with the query's matched input, not with `max_samples`.

## Intra-cluster read fan-out (ADR-0071)

Within one cluster, a read can be spread across peer query nodes so more than
one process's NIC, CPU, and memory serve it. This is off by default: a
`QueryEngine` built without a distributed context runs the byte-identical
local path, and `--distributed-query` (plus a fragment-credential file) is
what attaches one. The fan-out changes only *where* bytes are fetched and
decoded; aggregation and evaluation do not move, so the coordinator-merged
result is bit-for-bit identical to local execution, enforced by the
`distributed_merge_equals_local_bitwise` differential test over arbitrary
corpora and slice partitions.

### Coordinator, cost gate, and slices

The query node that receives a request is that query's coordinator (a
per-query role, not a process type). It resolves ONE pinned snapshot exactly
as the local path does, then consults the same pre-execution `CostEstimate`
the accounting layer already computes: distribution trips only when the
estimate reaches 256 MiB of estimated store bytes **or** 64 segments (either
axis, `DistribThresholds`). A query below both stays fully local, so a cheap
query pays nothing for the machinery. When the gate trips, the coordinator
partitions the snapshot **shard-major** (`partition_snapshot`): a segment's
ingest shard is the primary grouping key, a shard's segments are never split
across slices, and whole shard groups are packed so the slice count never
exceeds `max_parallel_slices`. Partitioning is total and disjoint — every
segment lands in exactly one non-empty slice — and because the k-way merge is
order-insensitive over the flat pool of decoded runs, the specific
shard-to-slice assignment never changes the result.

Each slice is dispatched to a worker over the internal `SeriesFetch` gRPC
service. The worker executes the existing fetch path over only its listed
segments (reconstructing each `SegmentRef` from the shipped durable identity,
never trusting a wire key), applies the request's erasure predicates exactly
as the local path would, pre-merges its slice, and streams series runs back
(f64 values as raw bit patterns, so NaN payloads, -0.0, and the staleness
marker survive) ending with one terminal summary frame. A slice contributes
to the merge only after its summary arrives; partial frames from a failed
attempt are discarded whole, which is what makes re-dispatch safe with no
dedup bookkeeping. Worker membership is a heartbeat key per process with
rendezvous hashing of `(tenant_hash, signal, shard)` over the live set — no
leader, no assignment object, no new durable state — and the per-process
content-addressed read caches behave as one aggregate cache because segments
are immutable.

### Budgets and the fault matrix

The coordinator re-enforces the query's budgets over the folded per-slice
accounting, so a worker that under-reports or lies about its own spend cannot
overrun the caps: the distinct-series cap is checked as each slice's series
arrive, and the bytes-scanned cap is checked against the saturating fold of
every slice's reported cost (saturating, never wrapping, so a counter near
`u64::MAX` clamps rather than slipping under the cap). Every slice's real
spend is folded into the query's live accounting handle before any failure or
fallback, so the reported cost reflects work already paid for — a
histogram-bearing query that fetches remotely and then re-runs locally
reports both, never one.

Failures map to the same typed outcomes the local path produces:

- **Worker unreachable or mid-stream loss:** re-dispatch the slice to the
  next rendezvous worker once, then run it on the coordinator, then fail
  typed. Never a partial merge.
- **`SnapshotInvalidated`** (a pinned segment vanished under concurrent
  GC/compaction): all invalidated slices collapse to the single retryable
  error the local path raises for a vanished segment, so the engine
  re-resolves the snapshot and re-dispatches the whole query once, not once
  per slice; a second occurrence fails.
- **`Corrupt`:** terminal immediately, no retry, matching local semantics
  (retrying could mask corruption behind a clean local read).
- **Budget trip** on a slice or on the folded total: the same typed
  `TooManySeries` / `TooManyBytesScanned` errors the local path produces.
- **Deadline:** the coordinator deadline (already bounded by the `sys/gc`
  `max_query_duration` that keeps workers inside the GC protection horizon)
  cancels the fan-out; stream teardown reaches workers and drop-based
  cancellation frees their GETs and permits.
- **Protocol version mismatch** (rolling deploy) or a **non-metrics signal**
  or a **histogram-bearing slice:** the worker answers `Unsupported`, and the
  coordinator silently falls back to fully local execution for the whole
  query — never an error, never a partial result. Intra-cluster execution is
  all-or-nothing; only cross-cluster federation (below) ever returns partial.

Fragment admission runs under a separate internal workload class with its own
cap, so a coordinator holding a client-query permit can never deadlock
waiting on fragments that need the same pool.

## Cross-cluster federation (ADR-0071)

A coordinator can fan a read out to remote clusters and merge their series
into the same k-way merge that unions its own selectors (the pool described
under Flow). Federation is opt-in per deployment: each remote is one
repeatable `--remote-cluster` flag on `ravel-server`, whose value is a
comma-separated spec (`name`, `endpoint`, and `credential-file` required;
`tls`, `tls-ca-file`, `skip-unavailable`, and `soft-timeout` optional).
`--remote-cluster-soft-timeout <duration>` sets the default bound on how
long the coordinator waits on a remote before treating it as partially
unavailable, and a remote's own `soft-timeout` key overrides it. A
deployment with no `--remote-cluster` flag runs exactly the single-cluster
path. The operator guide is docs/guides/distributed-query.md.

Both the value-bearing endpoints (`/api/v1/query`, `/api/v1/query_range`)
and the discovery endpoints (`/api/v1/series`, `/api/v1/labels`,
`/api/v1/label/<name>/values`) federate, through the SAME `Federation`
coordinator and with identical semantics (issue #891). A discovery request
sends its matchers and window to each remote; the remote resolves under its
own tenant auth, enforces its own admission/limits/erasure, and returns
series whose identities are unioned into the local discovery pool. The only
difference is what each keeps from the remote's response: the query path
keeps sample runs and merges them per timestamp, while the discovery path
keeps only `(series_id, labels)` identities (it enumerates series, not
samples). `skip_unavailable`, the skipped-cluster warnings, and the partial
marker behave identically on both paths: `skip_unavailable=false` fails the
request typed on a remote failure or timeout, and `=true` continues, names
the skipped cluster in the response envelope's `warnings`, and sets the
partial-coverage marker on the query stats. A single-cluster deployment runs
the byte-identical local discovery path (`fetch_all_series`); no federation
context means an empty, cheap fan-out.

### Two credential models, one wire type

ADR-0071 fetches carry a `Scope` on the `FetchRequest`. The two values are
not two transports; they are two trust boundaries over the same gRPC
service, and the distinction is a security invariant, not an optimization:

- **`Pinned` (intra-cluster slice).** A worker inside the coordinator's own
  cluster is handed a short-lived fragment token and the already-resolved
  `tenant_hash` on the wire. The worker trusts the coordinator: it uses the
  wire `tenant_hash` directly. This token is a slice credential, never a
  cross-cluster credential — a remote cluster must reject it (see the
  `federation_rejects_the_fragment_token` test in `distrib.rs`).
- **`Resolve` (cross-cluster federation).** A remote cluster is a separate
  trust domain. The coordinator authenticates to it with an ordinary
  per-remote tenant credential (the `credential` on `RemoteClusterConfig`),
  and the remote resolves the tenant from *its own* `TenantResolver` applied
  to that credential — never from the wire `tenant_hash`, which it
  overwrites with the locally resolved value. A federated request therefore
  cannot name a tenant the presented credential does not authorize on the
  remote, exactly as a direct client request to that remote could not.

`RemoteClusterConfig`'s `Debug` is hand-written to print `credential:
<redacted>` so a config dump never leaks the operator secret.

### Partial coverage is always surfaced, never silent

A federated query can return a correct-but-incomplete result when a remote
is slow (soft-timeout) or unreachable (`skip_unavailable`). This is never
silent: the query stats carry `partial: true` and one operator-facing
warning per degraded remote, merged into the Prometheus JSON envelope's
`warnings`. The warnings name only the operator-facing cluster name; remote
IP:port and errno are redacted out, because a client that reads the envelope
is not entitled to the coordinator's internal topology.

A remote that streams native-histogram frames this build cannot decode
across the slice boundary is treated as a coverage gap, not a hard fault:
under `skip_unavailable` it degrades to partial coverage with a truthful
warning (the remote returned a data kind this build cannot federate yet),
and without `skip_unavailable` it fails with that same typed reason rather
than a generic transport error.

### Cross-cluster duplicate tie-break limitation

The merge resolves a duplicate `(series_id, ts)` under the total order in
docs/catalog-and-mvcc.md (created_unix_ns, writer_epoch, writer_seq,
in-page index). Those provenance fields are meaningful only *within* one
cluster: two clusters can mint the same `(writer_epoch, writer_seq)` for
unrelated writes. Federation therefore assumes disjoint series identity
across clusters — the intended deployment is region- or tenant-sharded, so
one series lives in exactly one cluster. When the same `(series_id, ts)`
does arrive from two clusters with different values, the winner is
unspecified (whichever the total order happens to order first); the merge
still emits exactly one sample per timestamp and never duplicates or
crashes. Making a cross-cluster tie-break deterministic on value would need
a cluster-identity component in the order and is out of scope for this wave.
Inventing a cross-cluster provenance scheme is a separate ADR, deliberately
not attempted here (issue #891): the behavior above is *defined* only for
disjoint cross-cluster series identity, and this limitation is recorded both
here and as a code comment at the merge site (`is_greater` in
crates/ravel-query/src/engine.rs).

The discovery endpoints (`/api/v1/series`, `/api/v1/labels`,
`/api/v1/label/<name>/values`) federate too (issue #891), but their
cross-cluster union carries no such ambiguity. Discovery enumerates series
*identity*, and a `SeriesId` is a canonical function of a series' labels, so
the same id from two clusters always carries the same `LabelSet`: the union
is a plain set union of identities with no per-sample provenance to tie-break
on, and duplicate cross-cluster identities collapse cleanly regardless of
which cluster's writers minted the samples. The provenance-tuple limitation
above is therefore specific to the sample-merge path; the discovery union is
well-defined even for mirrored-ingest deployments.

## Query cost accounting and estimate (ADR-0044)

Every query carries a `ravel_types::accounting::QueryAccounting` handle,
created once per query attempt and passed explicitly (never thread-local
or task-local, since the fetch path fans out across `buffer_unordered`).
It is recorded at existing funnels only, never at scattered call sites:

- `SegmentFetcher::guarded_get` (crates/ravel-query/src/fetcher.rs), for
  every metric-path store call: `s3_requests`/`s3_bytes` split by op
  (get/list/head), `bytes_reused` (bytes served from an already-fetched
  region without a second GET), `decompressed_bytes`, `segments_opened`,
  `series_matched`.
- `LogSegmentFetcher::fetch_accounted` (crates/ravel-query/src/
  log_fetcher.rs), so the log path can be accounted like the metric path
  instead of being silently free. `engine.rs` has no references to
  `LogSegmentFetcher` at all: the real production callers (ravel-sql's
  `logs_provider`, `alerts_scan`, `audit_scan`, `audit_provider`) still
  call the unaccounted `fetch`. Wiring them onto this funnel is issue
  #424.
- `Catalog::guarded_get`/`guarded_list_all` (crates/ravel-catalog), for
  every catalog-side store call `Catalog::resolve` makes on the query's
  behalf: `s3_requests`/`s3_bytes` for the resolve LISTs and GETs, plus
  `cache_hits`/`cache_misses`/`cache_bytes` for the record cache.
  `QueryEngine::resolve_bounded` calls `resolve_pruned_with_accounting`
  with the same handle the query attempt reports, so these counters are
  live in the snapshot today, not zero.

A `QueryAccountingSnapshot` (point-in-time copy via `.snapshot()`) is
attached to `QueryStats` alongside the existing `segments_fetched`/
`segments_pruned`/per-run `FetchStats` (`raw_f64_pages`/`raw_f64_bytes`,
narrower than `decompressed_bytes`: only `ValPageKind::RawF64` pages).

### Pre-execution cost estimate

The estimate has two parts, computed at two different moments, because
one moment cannot cover both (ADR-0044 decision 3, amended).

**The catalog term**, computed before `Catalog::resolve` runs:
`Catalog::estimated_catalog_requests` bounds the store requests resolve
will issue before it has listed anything, from inputs the planner already
has going in. Two pieces: one LIST per `(shard, hour)` pair — `shard_count`
times the number of hour buckets the padded window spans, with no snapshot
HEAD to shorten the listed suffix — plus
`SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`, a fixed constant covering the
snapshot-window path (`Catalog::resolve_snapshot_window`) that
`resolve_impl` always tries first whenever the window is non-empty, before
any LIST runs. This term is folded into `estimated_requests`
unconditionally, including when the window resolves to zero segments, so a
fully-pruned window still carries a non-zero request estimate against its
non-zero actual request count.

`SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND` is not a structural bound and is the
one open gap in this estimate (see its doc comment in catalog.rs): it
covers one HEAD GET (always attempted), one part GET (capped at 1 because
every writer today emits exactly one part — metric-index-plan.md 3.1's
"v1 writes exactly one part" — even though the wire format's `repeated
SnapshotPartRef parts` allows more), and one postings GET (worst case, for
an equality `__name__` filter). Part count is only knowable after the HEAD
GET this constant exists to avoid, so a future multi-part writer (the
sharding escape hatch the format already reserves) would silently make
this an under-estimate again. ADR-0044 decision 3 does not account for the
snapshot-acceleration path at all; this is flagged there as an open
question, not resolved by a data-dependent bound here.

**The segment term**, computed after `Catalog::resolve` returns a pinned
snapshot and before any page fetch: `engine::estimate_cost` computes the
rest of the `CostEstimate` from fields already on that snapshot's
`SegmentRef`s (`object_size`, `sample_count`, `series_count`); nothing
about the fetch plan needs to run first because it is deterministic
given the matchers.

Both terms are an upper envelope, never a prediction: every quantity the
planner cannot bound exactly takes the worst case, because
under-estimating is the failure that matters once a later ADR enforces a
budget on this number.

Constants (crates/ravel-query/src/engine.rs):

- `OPEN_REQUESTS_PER_SEGMENT = 2` — `open_segment`'s footer GET plus the
  worst-case one extra `NeedRange` chase.
- `CATALOG_REQUESTS_PER_SEGMENT = 4` — the sparse catalog-probe path's
  worst-case coalesced GET count over LABEL_DICT/SERIES_IDS/
  SERIES_META_CHUNKS/SERIES_IDX; the whole-object and below-threshold
  paths cost strictly less.
- `PAGE_REQUESTS_PER_RUN = 2` per matched series — a TS run and a VAL (or
  HIST) run, before `ensure_ranges`' `covers()` dedup can ever reduce it.
- `STORE_BYTES_SAFETY_FACTOR = 2` applied to `object_size` — covers
  re-reading a segment across a retry (snapshot invalidation) without
  claiming a tighter per-page bound than the catalog can prove pre-fetch.
- `Catalog::estimated_catalog_requests(window, now_ns)` — the catalog term:
  `shard_count * hour_buckets_spanned` LISTs plus
  `SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND` (3) for the snapshot-window path,
  computed pre-resolve.
- `segment_decompressed_bytes_upper_bound` applied per segment for
  `estimated_decompressed_bytes` — derived from the segment's own value
  kinds rather than one constant, per ADR-0044's amended requirement.
  `SegmentRef::sample_count` aggregates scalar and native-histogram
  samples with no split by kind, so this bounds every sample at
  `ReaderLimits::max_page_uncompressed_bytes` (the page-size ceiling no
  decoded sample can structurally exceed), capped again by twice
  `ReaderLimits::max_section_uncompressed_bytes` (a segment carries at
  most one VAL_PAGES and one HIST_PAGES section). This never
  under-shoots, but is deliberately loose for scalar-only segments —
  closing that gap needs a persisted per-kind sample count, which is a
  frozen-format change, not something this estimate can derive today.

`prefetch` fans out one independent fetch per selector against the same
shared snapshot, so `estimate_cost` takes a `fetch_multiplier` (the
selector/plan count; 1 for the single-fetch `resolve_series_inner` path)
and scales every estimated quantity by it — an N-selector query can cost
up to N times a single per-segment pass.

Each query records the estimate and the actual accounting snapshot side
by side, so the estimate's accuracy is itself measurable before anything
enforces it. `CostEstimate::divergence` computes the actual/estimated
ratio per quantity for that purpose. The estimate is recorded but never
enforced: nothing in this change rejects a query that runs today.

### Tracing spans

`tracing::instrument`/`debug_span!` spans on the query path, named for the
phase they cover: `catalog_resolve` (`Catalog::resolve_impl` in
`ravel-catalog`), `segment_open` (`open_segment`), `catalog_decode`
(`decode_selected`), `page_fetch`
(`fetch_scalar_pages`/`fetch_histogram_pages`), `decode`
(`build_scalar_decodes`/`build_histogram_decodes`), `evaluate` (wrapping
the evaluator call in `instant_inner`/`range_inner`). `catalog_resolve`
lives on the catalog's own resolve body rather than ravel-query's
`resolve_bounded` wrapper, so every caller of `Catalog::resolve*` gets it
— including ravel-sql's executor, which calls
`resolve_pruned_with_accounting` directly and never reaches the ravel-query
wrapper.

Each phase span also records the per-span byte/request counts ADR-0044
decision 5 requires, scoped to that call's own work rather than the whole
query. Except on `catalog_resolve`, these come from values local to the
phase — the bytes and request count of the GET/decode calls that
invocation makes itself — not from a before/after `QueryAccounting`
snapshot delta. A delta would be wrong for every fetch/decode phase: the
segment futures run concurrently over one shared `QueryAccounting` handle
(`buffer_unordered` in `engine.rs`), so a sibling segment's GETs land
between one phase's two snapshots and the delta would fold their cost into
this span. These GET counts are store-sourced only: `guarded_get` routes
cache-eligible ranges through the ADR-0046 read cache, and a cache hit
serves bytes with no store round trip at all (`record_cache_hit`, never an
`AccountedOp::Get`), so it contributes zero requests and zero bytes to the
span. Each `guarded_get` call returns its own `{requests, bytes}` cost —
`{1, len}` for a store GET (the uncached path, a cache miss's leader, or a
single-flight follower riding another caller's in-flight GET, matching the
log path's rule below), `{0, 0}` for a cache hit — and the caller folds
those in, so the store-vs-cache decision lives once, at the seam that
already knows it. `segment_open` sums the store-sourced cost of its own one
or two `guarded_get` calls; which of those can reach zero on a warm cache
depends on the first GET's shape. A segment at or below
`DEFAULT_WHOLE_OBJECT_THRESHOLD` reads its whole object with a
`GetRange::Full` first GET, which is cache-eligible, so both its GETs (and
thus the whole `segment_open` cost) are zero on a fully warm cache. A
segment above the threshold reads its footer with a `GetRange::Suffix`
first GET, which `guarded_get`'s `cacheable_range` never routes through the
cache (a suffix has no total size to fabricate a hit from), so that GET
crosses the network on every run and `segment_open`'s cost is never zero
for such a segment regardless of cache warmth. `page_fetch`
records the store-sourced count and bytes `ensure_ranges` reports for the
coalesced GETs it issued on that call; those are `GetRange::Range` GETs,
which are cache-eligible, so this phase's cost is the one that reaches zero
on a warm cache even for an above-threshold segment (a warm range adds
nothing); `decode`
sums each `decode_run`/`decode_histogram_run`'s own decompressed output. Only `catalog_resolve` still uses a snapshot delta, and correctly:
it runs once per query handle, sequentially, before any concurrent fetch
work starts, so nothing else writes the handle across its window. The
fields: `s3_requests`/`s3_bytes` on `catalog_resolve`, `segment_open`, and
`page_fetch`; `segments_pruned` on `catalog_resolve`; `series_matched` on
`catalog_decode`; `decompressed_bytes` on `decode`. The log-signal path
(`LogSegmentFetcher::fetch_accounted` and the production
`fetch_accounted_with_tenant`) carries the same `page_fetch` (recording
its one whole-object GET, or zero on a cache hit) and `decode` span names.

Span fields otherwise carry only bounded values — `tenant_hash` as a hex
string, `object_size`, matcher/series counts, and fixed-set kind strings
(`page_kind`, `eval_kind`) — never query text, label values, object keys,
or `shard` (ADR-0044 decision 5's rejected alternative 6: shard is not a
label), the same allowlist ADR-0044 sets for `/metrics` labels.

### JSON response shape

`GET /api/v1/query` and `/query_range`'s `stats` object gains two new
fields alongside the existing `segmentsFetched`/`segmentsPruned`:

- `stats.accounting` — the `QueryAccountingSnapshot`, split by op for the
  S3 counters (`s3GetRequests`/`s3GetBytes`, `s3ListRequests`/
  `s3ListBytes`, `s3HeadRequests`/`s3HeadBytes`), plus `cacheHits`/
  `cacheMisses`/`cacheBytes`, `decompressedBytes`, `segmentsOpened`,
  `seriesMatched`, `bytesReused`, `peakIntermediateBytes`, and the
  pre-existing per-run `rawF64Pages`/`rawF64Bytes`. No `segmentsPruned`
  here: `stats.segmentsPruned` (below) is the sole source, sourced from
  `Catalog::resolve`'s own count; `QueryAccounting`'s own
  `segments_pruned` counter has no caller in `ravel-query` or
  `ravel-catalog` and would only ever render 0.
- `stats.estimate` — the `CostEstimate`: `estimatedRequests`,
  `estimatedStoreBytes`, `estimatedDecompressedBytes`, `segments`,
  `series`.

## PromQL conformance (ADR-0035, issue #133)

What Ravel supports, what it deliberately refuses, and what is simply
untested, one row per construct. The classification is ADR-0035's:

1. **supported** -- implemented, with a passing test proving it. The Evidence
   column names that test: a corpus file in
   `crates/ravel-promql-difftest/corpus/` when the differential harness
   exercises the construct, or a `ravel-promql` unit test when it does not
   (which the row also reports as a corpus gap).
2. **intentionally rejected** -- refused with a typed error, never a panic and
   never silently wrong data. Every row here is proved by a rejection case in
   `crates/ravel-promql-difftest/tests/conformance_table.rs`, which asserts
   the response is a Prometheus error envelope with a stable `errorType`, a
   non-empty message, and no data payload.
3. **accepted divergence** -- a difference from Prometheus an ADR already
   investigated and accepted. These are not misses and are not re-triaged;
   the row cites the ADR.
4. **unclassified** -- implemented but untested, or claimed-supported but
   actually failing. The actionable bucket: every row here is a ticket.

The score is states 1, 2, and 3 over the full enumerated surface, so it can
neither be inflated by defining misses out of the surface (unclassified rows
stay in the table) nor deflated by constructs Ravel never claimed (a verified
typed rejection is conformant).

Surface: the 72 functions and 12 aggregation operators promql-parser 0.10
marks non-experimental, all 16 binary operators, and the AST node and modifier
categories below. Prometheus' experimental functions and aggregators
(`limitk`, `limit_ratio`, `info`, `double_exponential_smoothing`,
`sort_by_label`, `mad_over_time`, `first_over_time`, `histogram_quantiles`,
the `ts_of_*` family) are out of the scored surface: they are not part of the
stable language, so including them would move the denominator every time
Prometheus promotes one. `Expr::Extension` is out of scope for the same kind
of reason -- it is promql-parser's hook for downstream dialects and has no
Prometheus counterpart -- and Ravel rejects it with a typed
`Error::Unsupported` regardless.

One note on those out-of-surface constructs: promql-parser 0.10 parses the
experimental aggregation operators `limitk` and `limit_ratio`, so a tenant
can reach them on the query path even though they are outside the scored
surface. `ravel-promql`'s aggregation dispatch rejects both with a typed
`Error::Unsupported` naming the operator, never a panic, honoring the state-2
"rejected, not panicking" guarantee (#260). They stay out of the scored
surface (they are not part of the stable language), and are not implemented;
the clean rejection is what the guarantee requires.

The table below is generated from a run, not hand-maintained: the state
column is recomputed from which corpus entries actually exercise each
construct and whether they actually passed, so a regression appears as a diff
in the same change that caused it. Everything between the markers is
overwritten on regeneration; the prose above and below survives.

<!-- BEGIN GENERATED PROMQL CONFORMANCE TABLE -->

Generated by `cargo test -p ravel-promql-difftest --test
conformance_table`; regenerate that same command with
`RAVEL_UPDATE_CONFORMANCE_TABLE=1`. Do not edit the block between
the markers by hand.

Surface: 133 constructs over 216 corpus entries in 10 corpus files.

| State | Constructs |
| --- | --- |
| supported | 124 |
| intentionally rejected | 7 |
| accepted divergence | 2 |
| unclassified | 0 |
| **score** (supported + intentionally rejected + accepted divergence / total) | **133/133 = 100%** |

| Construct | Category | State | Evidence |
| --- | --- | --- | --- |
| aggregate expression | ast node | supported | `corpus/aggregate.txt`, 20 entries |
| binary expression | ast node | supported | `corpus/binop.txt`, 28 entries |
| function call | ast node | supported | `corpus/transform.txt`, 59 entries |
| matrix selector | ast node | supported | `corpus/selectors.txt`, 4 entries |
| number literal | ast node | supported | `corpus/binop.txt`, 14 entries |
| paren expression | ast node | supported | `corpus/selectors.txt`, 2 entries |
| string literal | ast node | supported | `corpus/transform.txt`, 6 entries |
| `subquery` | ast node | supported | `corpus/subquery.txt`, 11 entries |
| unary expression | ast node | supported | no difftest corpus entry; proven by `ravel-promql`'s `unary_minus_negates_value_and_drops_metric_name` |
| vector selector | ast node | supported | `corpus/selectors.txt`, 31 entries |
| @ <timestamp> | modifier | supported | `corpus/selectors.txt`, 2 entries |
| @ end() | modifier | supported | `corpus/selectors.txt`, 1 entry |
| @ start() | modifier | supported | `corpus/selectors.txt`, 1 entry |
| `bool` | modifier | supported | `corpus/binop.txt`, 7 entries |
| `by` | modifier | supported | `corpus/aggregate.txt`, 11 entries |
| `group_left` | modifier | supported | `corpus/binop.txt`, 2 entries |
| `group_right` | modifier | supported | `corpus/binop.txt`, 1 entry |
| histogram_fraction at range-query top level | modifier | intentionally rejected | `Unsupported: histogram_fraction in a range query (422 execution)`; rejection verified; same range-path boundary as `histogram_quantile` above |
| histogram_quantile at range-query top level | modifier | intentionally rejected | `Unsupported: histogram_quantile in a range query (422 execution)`; rejection verified; `eval_range`'s per-step matrix reduction has no shape for a function that groups a whole instant vector; still fully usable in an instant query and nested inside one |
| `ignoring` | modifier | supported | `corpus/binop.txt`, 1 entry |
| label matcher != | modifier | supported | `corpus/selectors.txt`, 1 entry |
| label matcher !~ | modifier | supported | `corpus/selectors.txt`, 1 entry |
| label matcher = | modifier | supported | `corpus/selectors.txt`, 8 entries |
| label matcher =~ | modifier | supported | `corpus/selectors.txt`, 1 entry |
| label matcher or-group | modifier | intentionally rejected | `Unsupported: label matcher or-group (422 execution)`; rejection verified; `{a="1" or b="2"}` is a Prometheus experimental matcher form; ravel-promql's `matchers::has_or_group` refuses it before any matching runs rather than silently dropping a branch |
| negative offset | modifier | supported | `corpus/selectors.txt`, 1 entry |
| `offset` | modifier | supported | `corpus/selectors.txt`, 4 entries |
| `on` | modifier | supported | `corpus/binop.txt`, 7 entries |
| subquery over native histograms | modifier | intentionally rejected | `Unsupported: subquery over native histograms (422 execution)`; rejection verified; the subquery grid reducer keeps only each step's float value, so a histogram element would be silently dropped; the trigger is matched histogram data, not the syntactic shape (issue #220) |
| vector matching fill values | modifier | intentionally rejected | `Unsupported: vector matching fill-in values (422 execution)`; rejection verified; `fill`/`fill_left`/`fill_right` are a promql-parser dialect extension with no Prometheus counterpart; ravel-promql's `binop` refuses the modifier rather than evaluating it as plain matching |
| `without` | modifier | supported | `corpus/aggregate.txt`, 1 entry |
| `!=` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `%` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `*` | binary operator | supported | `corpus/binop.txt`, 5 entries |
| `+` | binary operator | supported | `corpus/binop.txt`, 4 entries |
| `-` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `/` | binary operator | supported | no difftest corpus entry; proven by `ravel-promql`'s `scalar_scalar_arithmetic_and_bool_comparison` |
| `<` | binary operator | supported | no difftest corpus entry; proven by `ravel-promql`'s `scalar_vector_filter_and_bool_both_directions` |
| `<=` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `==` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `>` | binary operator | supported | `corpus/binop.txt`, 3 entries |
| `>=` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `^` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `and` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `atan2` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `or` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `unless` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `avg` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `bottomk` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `count` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `count_values` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `group` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `max` | aggregation operator | supported | `corpus/aggregate.txt`, 2 entries |
| `min` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `quantile` | aggregation operator | supported | `corpus/aggregate.txt`, 2 entries |
| `stddev` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `stdvar` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `sum` | aggregation operator | supported | `corpus/aggregate.txt`, 6 entries |
| `topk` | aggregation operator | supported | `corpus/aggregate.txt`, 2 entries |
| `abs` | function | supported | `corpus/transform.txt`, 1 entry |
| `absent` | function | supported | `corpus/transform.txt`, 3 entries |
| `absent_over_time` | function | supported | `corpus/over_time.txt`, 3 entries |
| `acos` | function | supported | `corpus/transform.txt`, 1 entry |
| `acosh` | function | supported | `corpus/transform.txt`, 1 entry |
| `asin` | function | supported | `corpus/transform.txt`, 1 entry |
| `asinh` | function | supported | `corpus/transform.txt`, 1 entry |
| `atan` | function | supported | `corpus/transform.txt`, 1 entry |
| `atanh` | function | supported | `corpus/transform.txt`, 1 entry |
| `avg_over_time` | function | supported | `corpus/over_time.txt`, 2 entries |
| `ceil` | function | supported | `corpus/transform.txt`, 1 entry |
| `changes` | function | supported | `corpus/rate.txt`, 2 entries |
| `clamp` | function | supported | `corpus/transform.txt`, 6 entries |
| `clamp_max` | function | supported | `corpus/transform.txt`, 2 entries |
| `clamp_min` | function | supported | `corpus/transform.txt`, 2 entries |
| `cos` | function | supported | `corpus/transform.txt`, 1 entry |
| `cosh` | function | supported | `corpus/transform.txt`, 1 entry |
| `count_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `day_of_month` | function | supported | `corpus/transform.txt`, 1 entry |
| `day_of_week` | function | supported | `corpus/transform.txt`, 1 entry |
| `day_of_year` | function | supported | `corpus/transform.txt`, 1 entry |
| `days_in_month` | function | supported | `corpus/transform.txt`, 2 entries |
| `deg` | function | supported | `corpus/transform.txt`, 1 entry |
| `delta` | function | supported | `corpus/rate.txt`, 2 entries |
| `deriv` | function | supported | `corpus/rate.txt`, 1 entry |
| `exp` | function | supported | `corpus/transform.txt`, 1 entry |
| `floor` | function | supported | `corpus/transform.txt`, 1 entry |
| `histogram_avg` | function | supported | `corpus/histogram_native.txt`, 1 entry |
| `histogram_count` | function | supported | `corpus/histogram_native.txt`, 3 entries |
| `histogram_fraction` | function | supported | `corpus/histogram_classic.txt`, 3 entries |
| `histogram_quantile` | function | supported | `corpus/histogram_classic.txt`, 8 entries |
| `histogram_stddev` | function | intentionally rejected | `Unsupported: function call (422 execution)`; rejection verified; not in ravel-promql's function registry; native-histogram dispersion is not implemented |
| `histogram_stdvar` | function | intentionally rejected | `Unsupported: function call (422 execution)`; rejection verified; not in ravel-promql's function registry; native-histogram dispersion is not implemented |
| `histogram_sum` | function | supported | `corpus/histogram_native.txt`, 3 entries |
| `hour` | function | supported | `corpus/transform.txt`, 2 entries |
| `idelta` | function | supported | `corpus/rate.txt`, 1 entry |
| `increase` | function | supported | `corpus/rate.txt`, 2 entries |
| `irate` | function | supported | `corpus/rate.txt`, 1 entry |
| `label_join` | function | supported | `corpus/transform.txt`, 2 entries |
| `label_replace` | function | supported | `corpus/transform.txt`, 4 entries |
| `last_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `ln` | function | supported | `corpus/transform.txt`, 1 entry |
| `log10` | function | supported | `corpus/transform.txt`, 1 entry |
| `log2` | function | supported | `corpus/transform.txt`, 1 entry |
| `max_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `min_over_time` | function | supported | `corpus/over_time.txt`, 2 entries |
| `minute` | function | supported | `corpus/transform.txt`, 1 entry |
| `month` | function | supported | `corpus/transform.txt`, 1 entry |
| `pi` | function | supported | `corpus/transform.txt`, 1 entry |
| `predict_linear` | function | supported | `corpus/rate.txt`, 2 entries |
| `present_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `quantile_over_time` | function | supported | `corpus/over_time.txt`, 3 entries |
| `rad` | function | supported | `corpus/transform.txt`, 1 entry |
| `rate` | function | supported | `corpus/rate.txt`, 5 entries |
| `resets` | function | supported | `corpus/rate.txt`, 1 entry |
| `round` | function | supported | `corpus/transform.txt`, 2 entries |
| `scalar` | function | supported | `corpus/transform.txt`, 2 entries |
| `sgn` | function | supported | `corpus/transform.txt`, 1 entry |
| `sin` | function | supported | `corpus/transform.txt`, 1 entry |
| `sinh` | function | supported | `corpus/transform.txt`, 1 entry |
| `sort` | function | supported | `corpus/transform.txt`, 1 entry |
| `sort_desc` | function | supported | `corpus/transform.txt`, 1 entry |
| `sqrt` | function | supported | `corpus/transform.txt`, 1 entry |
| `stddev_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `stdvar_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `sum_over_time` | function | supported | `corpus/over_time.txt`, 2 entries |
| `tan` | function | supported | `corpus/transform.txt`, 1 entry |
| `tanh` | function | supported | `corpus/transform.txt`, 1 entry |
| `time` | function | supported | `corpus/transform.txt`, 1 entry |
| `timestamp` | function | supported | `corpus/transform.txt`, 2 entries |
| `vector` | function | supported | `corpus/transform.txt`, 6 entries |
| `year` | function | supported | `corpus/transform.txt`, 1 entry |
| float-precision residue (ULP tolerance) | accepted divergence | accepted divergence | ADR-0025, 10 corpus entries; arithmetic and transcendental residue between two independent implementations of the same formula; each entry carries a written justification for its tolerance in the corpus file |
| subquery per-node point cap | accepted divergence | accepted divergence | ADR-0030, 2 corpus entries; Ravel's per-subquery-node 11,000-point budget has no Prometheus counterpart, so Ravel rejects by design where Prometheus accepts; the comparator asserts exactly that shape |
<!-- END GENERATED PROMQL CONFORMANCE TABLE -->

## SQL over logs (the `logs` table, ADR-0033)

`POST /api/v1/sql` serves two tables from one endpoint: `samples`
(`Signal::Metrics`) and `logs` (`Signal::Logs`). There is no separate logs
endpoint and no protocol change. DataFusion does not choose between two
registered tables: `SqlExecutor` decides which single table a query targets by
parsing its `FROM` clause *before* planning (through the same parser the
read-only gate uses, never a raw-text scan), resolves a snapshot for that one
signal, and registers exactly that one table in the per-query `SessionContext`
-- `Signal::Logs` when the query references `logs`, `Signal::Metrics`
otherwise. A `WITH <name> AS (...)` CTE that happens to be named `logs` or
`samples` is a query-local name, not a base-table reference, and does not
change the target (`WITH logs AS (SELECT value FROM samples) SELECT count(*)
FROM logs` is a metrics-only query). A query naming both real tables is
rejected before any catalog listing (HTTP 400): v1 admits one signal per
query, and no query needs to scan or join metrics and logs together.

Schema (fixed columns plus one map):

- `ts`, `observed_ts` — `Timestamp(ns)`.
- `severity_num` — `UInt8`; `severity_text`, `body` — `Utf8`.
- `trace_id` — `FixedSizeBinary(16)`, `span_id` — `FixedSizeBinary(8)`, both
  nullable; `flags` — `UInt32`.
- `attrs` — `Map(Utf8, Utf8)` carrying each record's resource, scope, and
  per-record dynamic attributes merged into one map. On a key collision the
  per-record value wins over the resource/scope value. Positional scope name
  and version are excluded (no synthetic `scope.name`/`scope.version` keys).

Supported predicates (all pushdown is widen-only; DataFusion always re-applies
the original predicate above the scan, so pruning can only ever widen the fetch,
never drop a true result):

- `ts` range comparisons — exact segment-level pruning from the catalog
  summary, the same shape as the metrics table's `ts` bounds.
- `has_word(body, 'literal')` — a word/phrase content search whose SQL
  semantics equal the RLOG reader's exact token filter, so it both pushes down
  (bloom-accelerated pruning inside the scan) and needs no residual correction.
  A plain `LIKE '%word%'` pattern's literal is recognized but is **not** pushed
  as a prune, because token matching is not a superset of SQL substring `LIKE`.
- `attrs['k'] = 'v'` — an attribute equality. `extract_logs` sends it to a
  prune-only channel that drives POSTINGS block pruning (ADR-0049). The channel
  never becomes the reader's per-row filter, so it cannot drop a resource-only
  match. The merged `attrs` residual evaluates the equality exactly.
  `attrs['k'] IN (...)` stays unextracted, because an `IN` list is a
  disjunction the intersecting prune channel cannot represent soundly
  (issue #519).

Both gaps ADR-0033 recorded are now closed. Both were deliberate, not
oversights.

1. ~~**`attrs['k']` subscript planning is not wired.**~~ **Closed.** This
   crate's DataFusion is built with `features = ["sql"]` only, so no
   nested-expression `ExprPlanner` (`GetFieldAccess`/`NamedStructField`) came
   registered, and the `attrs['k']` subscript failed query planning with a
   loud `GetFieldAccess not supported` error. `map_field_planner.rs` now
   registers a hand-written `ExprPlanner` for the map-field case, so
   `attrs['k'] = 'v'` plans and answers. Enabling DataFusion's `nested_expressions`
   feature would have done the same thing at the cost of a much larger
   dependency surface for one expression form.

   The planner is registered in `session.rs`'s logs session and covered end to
   end at the HTTP boundary
   (`services/ravel-server/tests/sql_endpoint.rs::a_logs_attrs_subscript_query_succeeds_over_http`),
   not only against a session the crate's own tests build. Gap 2 below is
   what remains, and it is about pruning, not planning.
2. **A stream-attribute equality prunes through a prune-only channel.**
   `attrs` merges resource, scope, and per-record attributes. On a key
   collision the record wins. A stream-level (STREAM_DIR) match therefore
   cannot prune the equality: it would drop a record whose match lives only in
   its per-record attributes, and no residual restores a dropped record. The
   reader's per-record `Equals` reads a record's own column and `attrs_raw`
   overflow only. It never reads the resource or scope blob. So it matches a
   strict subset of the merged equality. To evaluate it as the reader's exact
   per-row filter drops every resource-only match.

   ADR-0049 closes the gap in two layers. Layer 1 is the index: a version 2
   POSTINGS section prunes blocks exactly on an indexed attribute value in the
   merged view (resource, scope, and per-record attributes, the record winning
   on a key collision) -- the same view `attrs` exposes. Layer 2 is a
   prune-only channel: `extract_logs` sends `attrs['k'] = 'v'` to
   `LogsPushdown::prune`, and `RlogReader::scan_pruned` applies that channel to
   POSTINGS block pruning alone. The channel never feeds the per-row filter. If
   the POSTINGS index does not cover a field, the channel prunes nothing, so it
   stays widen-only (ADR-0013). DataFusion's `Inexact` residual still evaluates
   the equality exactly over the merged `attrs` column. The prune changes which
   blocks the fetch reads. It does not change which rows the query returns.

   Version 1 (the description below applies to version 1 objects only) indexed
   the per-record layer only. For those objects the channel declines one arm: a
   key that also appears at resource or scope level anywhere in the object. One
   record carrying a key per-record makes it an indexed column for the whole
   object, including for records whose value for that key lives in their
   resource blob. Those records are in no version-1 posting list, so probing
   the term would prune their block away. An exact index over one layer cannot
   prune a query over the union of two, so the reader declines to prune that
   key on a version 1 object and the residual does the work alone. Declining is
   widen-only; pruning wrongly is not.

   That exclusion cost the common case on version 1. `service.name` is a
   resource attribute in ordinary OTLP, so a version 1 object holding it that
   way prunes nothing for it. Version 2 removes the cost by indexing the merged
   view, so a merged-view prune on such a key is sound and applied directly; the
   exclusion is kept for version 1 objects, which are not rewritten. The move to
   the merged view is the ADR-0049 amendment (2026-08-03, issue #547), a change
   to the POSTINGS grammar version only, not the trailer version.

   The reader channel and the SQL extractor landed in issue #538, the fetch
   plumbing in issue #544: `ravel_query::LogQuery` carries a `prune` field (with
   a `with_prune` builder) that the fetch hands to `RlogReader::scan_pruned`,
   and `LogsScanExec` fills it from `LogsPushdown::prune`. A live
   `SELECT ... FROM logs WHERE attrs['k'] = 'v'` now prunes blocks through
   POSTINGS. A `LogQuery` with an empty `prune` reads exactly what it read
   before the channel existed.

   What an operator sees change is cost, not answers. `LogsScanExec` publishes
   `blocks_total`, `blocks_scanned`, and `blocks_pruned_by_postings` per
   partition, so `EXPLAIN ANALYZE` shows whether a query pruned; a selective
   equality on an indexed key reads a fraction of the blocks it read before, and
   returns the same rows. A key the object does not index reports
   `blocks_pruned_by_postings=0` and reads everything, which is the widen-only
   fallback, not a failure.

## Caching note

ADR-0046 added a content-addressed RAM read-cache tier (`ravel-cache`,
S3-FIFO eviction, single-flighted) consulted at three funnels:
`SegmentFetcher::guarded_get`, `Catalog::guarded_get`, and
`LogSegmentFetcher::fetch`. Cache keys are `(tenant_hash, content_hash, offset,
len)`, so entries survive object-key churn and two writers producing
identical bytes share one entry. Each funnel credits its own hit/miss and
byte counters to `QueryAccounting` (ADR-0044), so a query's `EXPLAIN
ANALYZE` output distinguishes cache hits from store round trips.

Alongside the byte cache, `ravel-catalog` keeps its five decoded-structure
caches (commit records, compaction records, HEAD with a TTL, snapshot
parts, postings), now with hit/miss/byte counters and a capacity bound on
`HeadCache`.

Deliberately not cached: a suffix GET (the footer-first read on segment
open) always bypasses the byte cache, because a suffix has no total object
size to key a `(offset, len)` entry on. A disk tier exists in
`ravel-cache` but is not wired to any read funnel; passing `--cache-dir`
fails startup rather than silently caching nothing.

See docs/guides/caching.md for CLI flags, metrics, and known gaps, and
docs/adrs/0046-read-cache-tier.md for the funnel/keying/eviction design
and its rejected alternatives.
